"""
Investigation Service

Manages fraud investigations using the LangGraph workflow.
Provides SSE streaming for real-time investigation progress.
"""

import asyncio
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, AsyncGenerator, Optional

import httpx

from workflow.graph import create_investigation_workflow, run_investigation, get_workflow_steps
from workflow.state import InvestigationState

logger = logging.getLogger('investigation.service')


class InvestigationService:
    """
    Service for managing fraud investigations.
    """
    
    def __init__(
        self,
        aerospike_service: Any,
        graph_service: Any,
        ollama_base_url: str = "http://localhost:11434",
        ollama_model: str = "mistral"
    ):
        self.aerospike_service = aerospike_service
        self.graph_service = graph_service
        self.ollama_base_url = ollama_base_url
        self.ollama_model = ollama_model
        
        # HTTP client for Ollama - 5 minute timeout for model loading
        self.ollama_client = httpx.AsyncClient(timeout=300.0)
        
        # Workflow instance
        self.workflow = None
        
        # Active investigations cache
        self._active_investigations: Dict[str, Dict[str, Any]] = {}
        self._investigation_results: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"Investigation service initialized with Ollama at {ollama_base_url}")
    
    async def initialize(self):
        """Initialize the investigation workflow."""
        try:
            self.workflow = create_investigation_workflow(
                self.aerospike_service,
                self.graph_service,
                self.ollama_client
            )
            logger.info("Investigation workflow initialized")
            
            # Check Ollama connectivity
            await self._check_ollama()
            
        except Exception as e:
            logger.error(f"Failed to initialize investigation service: {e}")
            raise
    
    async def _check_ollama(self):
        """Check if Ollama is available and pull model if needed."""
        try:
            response = await self.ollama_client.get(f"{self.ollama_base_url}/api/tags")
            if response.status_code == 200:
                models = response.json().get("models", [])
                model_names = [m.get("name", "").split(":")[0] for m in models]
                
                if self.ollama_model not in model_names:
                    logger.info(f"Model {self.ollama_model} not found, pulling...")
                    # Note: Model pull happens automatically on first use
                else:
                    logger.info(f"Ollama connected, model {self.ollama_model} available")
            else:
                logger.warning(f"Ollama health check returned {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Could not connect to Ollama: {e}")
            logger.warning("Investigations will use fallback logic without LLM")
    
    async def close(self):
        """Clean up resources."""
        await self.ollama_client.aclose()
        logger.info("Investigation service closed")
    
    def get_workflow_steps(self) -> list[Dict[str, str]]:
        """Get list of workflow steps for UI."""
        return get_workflow_steps()
    
    async def start_investigation(
        self,
        user_id: str,
        triggered_by: str = "manual"
    ) -> str:
        """
        Start a new investigation for a user.
        
        Args:
            user_id: User ID to investigate
            triggered_by: What triggered the investigation
            
        Returns:
            Investigation ID
        """
        investigation_id = f"inv_{uuid.uuid4().hex[:12]}"
        
        # Track active investigation
        self._active_investigations[investigation_id] = {
            "user_id": user_id,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "triggered_by": triggered_by,
            "current_step": "alert_validation"
        }
        
        logger.info(f"Started investigation {investigation_id} for user {user_id}")
        
        return investigation_id
    
    async def stream_investigation(
        self,
        user_id: str,
        investigation_id: Optional[str] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream investigation progress as SSE events.
        
        Args:
            user_id: User ID to investigate
            investigation_id: Optional existing investigation ID
            
        Yields:
            SSE event data
        """
        # Create investigation ID if not provided
        if not investigation_id:
            investigation_id = await self.start_investigation(user_id)
        
        if not self.workflow:
            await self.initialize()
        
        # Yield initial event
        yield {
            "event": "start",
            "data": {
                "investigation_id": investigation_id,
                "user_id": user_id,
                "steps": self.get_workflow_steps()
            }
        }
        
        try:
            # Run the workflow and stream events
            final_state = None
            
            async for event in run_investigation(self.workflow, user_id, investigation_id):
                event_type = event.get("type", "unknown")
                
                if event_type == "trace":
                    trace = event.get("event", {})
                    yield {
                        "event": "trace",
                        "data": trace
                    }
                    
                    # Update active investigation tracking
                    if investigation_id in self._active_investigations:
                        self._active_investigations[investigation_id]["current_step"] = trace.get("node", "")
                
                elif event_type == "state_update":
                    node = event.get("node", "")
                    data = event.get("data", {})
                    
                    yield {
                        "event": "progress",
                        "data": {
                            "node": node,
                            "phase": data.get("current_phase", ""),
                            **{k: v for k, v in data.items() if k not in ["trace_events"]}
                        }
                    }
                    
                    # Capture final state components
                    if not final_state:
                        final_state = {}
                    final_state.update(data)
                
                elif event_type == "complete":
                    yield {
                        "event": "complete",
                        "data": {
                            "investigation_id": investigation_id,
                            "user_id": user_id
                        }
                    }
                    
                elif event_type == "error":
                    yield {
                        "event": "error",
                        "data": {
                            "error": event.get("error", "Unknown error"),
                            "investigation_id": investigation_id
                        }
                    }
            
            # Store final result
            if final_state:
                self._investigation_results[investigation_id] = {
                    "user_id": user_id,
                    "completed_at": datetime.now().isoformat(),
                    "state": final_state
                }
            
            # Update status
            if investigation_id in self._active_investigations:
                self._active_investigations[investigation_id]["status"] = "completed"
                
        except Exception as e:
            logger.error(f"Investigation error: {e}")
            
            yield {
                "event": "error",
                "data": {
                    "error": str(e),
                    "investigation_id": investigation_id
                }
            }
            
            if investigation_id in self._active_investigations:
                self._active_investigations[investigation_id]["status"] = "error"
                self._active_investigations[investigation_id]["error"] = str(e)
    
    def get_investigation_status(self, investigation_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an investigation."""
        if investigation_id in self._active_investigations:
            return self._active_investigations[investigation_id]
        return None
    
    def get_investigation_result(self, investigation_id: str) -> Optional[Dict[str, Any]]:
        """Get result of a completed investigation."""
        return self._investigation_results.get(investigation_id)
    
    def get_user_investigation_history(self, user_id: str) -> list[Dict[str, Any]]:
        """Get investigation history for a user."""
        history = []
        
        for inv_id, data in self._investigation_results.items():
            if data.get("user_id") == user_id:
                history.append({
                    "investigation_id": inv_id,
                    "completed_at": data.get("completed_at"),
                    "risk_level": data.get("state", {}).get("risk_assessment", {}).get("risk_level"),
                    "recommendation": data.get("state", {}).get("decision", {}).get("recommended_action")
                })
        
        return sorted(history, key=lambda x: x.get("completed_at", ""), reverse=True)
    
    async def get_investigation_report(self, investigation_id: str) -> Optional[str]:
        """Get the markdown report for an investigation."""
        result = self._investigation_results.get(investigation_id)
        if result:
            return result.get("state", {}).get("report_markdown")
        return None


# Singleton instance (to be initialized in main.py)
investigation_service: Optional[InvestigationService] = None
