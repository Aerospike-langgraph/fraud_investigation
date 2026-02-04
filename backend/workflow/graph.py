"""
LangGraph Workflow Definition

Agentic 4-node workflow:
1. Alert Validation - Get flag context
2. Data Collection - Gather baseline evidence
3. LLM Agent - Tool-calling agent that gathers more data as needed
4. Report Generation - Generate final report

The LLM Agent handles all the reasoning and tool calling internally.
"""

from typing import Dict, Any, AsyncGenerator
import logging

from langgraph.graph import StateGraph, END

from workflow.state import InvestigationState, create_initial_state
from workflow.nodes.alert_validation import alert_validation_node
from workflow.nodes.data_collection import data_collection_node
from workflow.nodes.llm_agent import llm_agent_node
from workflow.nodes.report_generation import report_generation_node

logger = logging.getLogger('investigation.workflow')


def create_investigation_workflow(
    aerospike_service: Any,
    graph_service: Any,
    ollama_client: Any = None  # Kept for backwards compatibility but not used
) -> StateGraph:
    """
    Create the LangGraph investigation workflow.
    
    New 4-node agentic workflow:
    - alert_validation: Get flag context from KV
    - data_collection: Gather baseline evidence from KV + Graph
    - llm_agent: ReAct agent that uses tools to gather more data
    - report_generation: Generate final markdown report
    
    Args:
        aerospike_service: Aerospike KV service instance
        graph_service: Aerospike Graph service instance
        ollama_client: (Deprecated) HTTP client for Ollama - agent handles this internally
        
    Returns:
        Compiled StateGraph workflow
    """
    
    # Create the workflow graph
    workflow = StateGraph(InvestigationState)
    
    # ------------------------------------------
    # Node 1: Alert Validation
    # ------------------------------------------
    def _alert_validation(state: InvestigationState) -> Dict[str, Any]:
        return alert_validation_node(state, aerospike_service)
    
    workflow.add_node("alert_validation", _alert_validation)
    
    # ------------------------------------------
    # Node 2: Data Collection (Combined from account_deep_dive + timeline)
    # ------------------------------------------
    def _data_collection(state: InvestigationState) -> Dict[str, Any]:
        return data_collection_node(state, aerospike_service, graph_service)
    
    workflow.add_node("data_collection", _data_collection)
    
    # ------------------------------------------
    # Node 3: LLM Reasoning Agent (with tools)
    # ------------------------------------------
    def _llm_agent(state: InvestigationState) -> Dict[str, Any]:
        return llm_agent_node(state, aerospike_service, graph_service)
    
    workflow.add_node("llm_agent", _llm_agent)
    
    # ------------------------------------------
    # Node 4: Report Generation
    # ------------------------------------------
    async def _report_generation(state: InvestigationState) -> Dict[str, Any]:
        return await report_generation_node(state, ollama_client)
    
    workflow.add_node("report_generation", _report_generation)
    
    # ------------------------------------------
    # Define Edges (Linear Flow)
    # ------------------------------------------
    
    # Set entry point
    workflow.set_entry_point("alert_validation")
    
    # Linear flow through all nodes
    workflow.add_edge("alert_validation", "data_collection")
    workflow.add_edge("data_collection", "llm_agent")
    workflow.add_edge("llm_agent", "report_generation")
    workflow.add_edge("report_generation", END)
    
    # Compile the workflow
    compiled = workflow.compile()
    
    logger.info("Investigation workflow compiled (4-node agentic architecture)")
    
    return compiled


async def run_investigation(
    workflow: StateGraph,
    user_id: str,
    investigation_id: str
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Run the investigation workflow and yield trace events.
    
    Args:
        workflow: Compiled LangGraph workflow
        user_id: User ID to investigate
        investigation_id: Unique investigation ID
        
    Yields:
        Trace events for SSE streaming
    """
    # Create initial state
    initial_state = create_initial_state(investigation_id, user_id)
    
    logger.info(f"Starting investigation {investigation_id} for user {user_id}")
    
    # Run the workflow with streaming
    try:
        async for event in workflow.astream(initial_state, {"recursion_limit": 50}):
            # Extract node name and state updates
            for node_name, state_update in event.items():
                # Yield trace events from the state update
                if "trace_events" in state_update:
                    for trace_event in state_update["trace_events"]:
                        yield {
                            "type": "trace",
                            "event": trace_event
                        }
                
                # Yield tool calls if present (for real-time UI updates)
                if "tool_calls" in state_update and state_update["tool_calls"]:
                    for tool_call in state_update["tool_calls"]:
                        yield {
                            "type": "tool_call",
                            "node": node_name,
                            "data": {
                                "tool": tool_call.get("tool"),
                                "params": tool_call.get("params"),
                                "timestamp": tool_call.get("timestamp")
                            }
                        }
                
                # Yield state update (without trace events to avoid duplication)
                state_copy = {k: v for k, v in state_update.items() if k != "trace_events"}
                if state_copy:
                    yield {
                        "type": "state_update",
                        "node": node_name,
                        "data": state_copy
                    }
        
        # Yield completion event
        yield {
            "type": "complete",
            "investigation_id": investigation_id,
            "user_id": user_id
        }
        
    except Exception as e:
        logger.error(f"Investigation workflow error: {e}")
        yield {
            "type": "error",
            "investigation_id": investigation_id,
            "user_id": user_id,
            "error": str(e)
        }


def get_workflow_steps() -> list[Dict[str, str]]:
    """Get list of workflow steps for UI display."""
    return [
        {
            "id": "alert_validation",
            "name": "Alert Validation",
            "description": "Extract alert trigger context from flagged account",
            "phase": "context"
        },
        {
            "id": "data_collection",
            "name": "Data Collection",
            "description": "Gather baseline profile, accounts, devices, transactions",
            "phase": "evidence"
        },
        {
            "id": "llm_agent",
            "name": "AI Investigation Agent",
            "description": "LLM agent uses tools to gather additional evidence and make assessment",
            "phase": "reasoning"
        },
        {
            "id": "report_generation",
            "name": "Report Generation",
            "description": "Generate detailed investigation report",
            "phase": "report"
        }
    ]
