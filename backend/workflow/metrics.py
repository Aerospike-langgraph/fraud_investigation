"""
Performance Metrics Collector for AI Investigation Workflow

Tracks:
- Total duration and per-node timing
- Database calls (KV and Graph) with timing
- LLM calls with timing and token counts
- Checkpoint operations
"""

import time
import threading
from typing import Any, Dict, List, Optional
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger('investigation.metrics')


@dataclass
class DBCallRecord:
    """Record of a single database call."""
    operation: str
    target: str  # "KV" or "Graph"
    duration_ms: float
    timestamp: str
    success: bool = True
    details: Optional[str] = None


@dataclass
class LLMCallRecord:
    """Record of a single LLM call."""
    duration_ms: float
    tokens_in: int = 0
    tokens_out: int = 0
    model: str = "mistral"


@dataclass
class CheckpointRecord:
    """Record of a checkpoint operation."""
    operation: str  # "put", "get", "put_writes"
    duration_ms: float


class MetricsCollector:
    """
    Thread-safe metrics collector for investigation workflow.
    
    Usage:
        collector = MetricsCollector()
        
        with collector.track_node("data_collection"):
            # ... node execution ...
            collector.track_db_call("get_user", "KV", 15.2)
        
        metrics = collector.get_metrics()
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        self.reset()
    
    def reset(self):
        """Reset all metrics for a new investigation."""
        with self._lock:
            self.start_time = time.time()
            self.node_timings: Dict[str, float] = {}
            self.node_start_times: Dict[str, float] = {}
            self.db_calls: List[DBCallRecord] = []
            self.llm_calls: List[LLMCallRecord] = []
            self.checkpoint_calls: List[CheckpointRecord] = []
            self.tool_calls: Dict[str, int] = {}  # tool_name -> count
    
    @contextmanager
    def track_node(self, node_name: str):
        """Context manager to track node execution time."""
        start = time.time()
        with self._lock:
            self.node_start_times[node_name] = start
        
        try:
            yield
        finally:
            duration_ms = (time.time() - start) * 1000
            with self._lock:
                self.node_timings[node_name] = duration_ms
                if node_name in self.node_start_times:
                    del self.node_start_times[node_name]
            logger.debug(f"Node '{node_name}' completed in {duration_ms:.2f}ms")
    
    def start_node(self, node_name: str):
        """Start tracking a node (alternative to context manager)."""
        with self._lock:
            self.node_start_times[node_name] = time.time()
    
    def end_node(self, node_name: str):
        """End tracking a node."""
        end_time = time.time()
        with self._lock:
            if node_name in self.node_start_times:
                duration_ms = (end_time - self.node_start_times[node_name]) * 1000
                self.node_timings[node_name] = duration_ms
                del self.node_start_times[node_name]
                logger.debug(f"Node '{node_name}' completed in {duration_ms:.2f}ms")
    
    def track_db_call(
        self, 
        operation: str, 
        target: str, 
        duration_ms: float, 
        success: bool = True,
        details: Optional[str] = None
    ):
        """
        Record a database call.
        
        Args:
            operation: Name of the operation (e.g., "get_user", "gremlin_query")
            target: "KV" or "Graph"
            duration_ms: Duration in milliseconds
            success: Whether the call succeeded
            details: Optional additional details
        """
        record = DBCallRecord(
            operation=operation,
            target=target,
            duration_ms=duration_ms,
            timestamp=datetime.now().isoformat(),
            success=success,
            details=details
        )
        with self._lock:
            self.db_calls.append(record)
    
    def track_llm_call(
        self, 
        duration_ms: float, 
        tokens_in: int = 0, 
        tokens_out: int = 0,
        model: str = "mistral"
    ):
        """Record an LLM call."""
        record = LLMCallRecord(
            duration_ms=duration_ms,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            model=model
        )
        with self._lock:
            self.llm_calls.append(record)
    
    def track_checkpoint(self, operation: str, duration_ms: float):
        """Record a checkpoint operation."""
        record = CheckpointRecord(operation=operation, duration_ms=duration_ms)
        with self._lock:
            self.checkpoint_calls.append(record)
    
    def track_tool_call(self, tool_name: str):
        """Record a tool invocation."""
        with self._lock:
            self.tool_calls[tool_name] = self.tool_calls.get(tool_name, 0) + 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get aggregated metrics summary.
        
        Returns:
            Dict containing all metrics
        """
        with self._lock:
            total_duration_ms = (time.time() - self.start_time) * 1000
            
            # Separate KV and Graph calls
            kv_calls = [c for c in self.db_calls if c.target == "KV"]
            graph_calls = [c for c in self.db_calls if c.target == "Graph"]
            
            # Calculate aggregates
            kv_time_ms = sum(c.duration_ms for c in kv_calls)
            graph_time_ms = sum(c.duration_ms for c in graph_calls)
            llm_time_ms = sum(c.duration_ms for c in self.llm_calls)
            checkpoint_time_ms = sum(c.duration_ms for c in self.checkpoint_calls)
            
            # Token counts
            tokens_in = sum(c.tokens_in for c in self.llm_calls)
            tokens_out = sum(c.tokens_out for c in self.llm_calls)
            
            # Build DB call log (last 100 for debugging)
            db_call_log = [
                {
                    "operation": c.operation,
                    "target": c.target,
                    "duration_ms": round(c.duration_ms, 2),
                    "success": c.success,
                    "timestamp": c.timestamp
                }
                for c in self.db_calls[-100:]
            ]
            
            return {
                # Timing
                "total_duration_ms": round(total_duration_ms, 2),
                "node_durations": {k: round(v, 2) for k, v in self.node_timings.items()},
                
                # DB Calls
                "total_db_calls": len(self.db_calls),
                "kv_calls": len(kv_calls),
                "graph_calls": len(graph_calls),
                "kv_time_ms": round(kv_time_ms, 2),
                "graph_time_ms": round(graph_time_ms, 2),
                
                # Checkpoints
                "checkpoint_calls": len(self.checkpoint_calls),
                "checkpoint_time_ms": round(checkpoint_time_ms, 2),
                
                # LLM
                "llm_calls": len(self.llm_calls),
                "llm_time_ms": round(llm_time_ms, 2),
                "llm_tokens_in": tokens_in,
                "llm_tokens_out": tokens_out,
                
                # Tool usage
                "tool_calls_count": sum(self.tool_calls.values()),
                "tool_breakdown": dict(self.tool_calls),
                
                # Detailed log
                "db_call_log": db_call_log
            }
    
    def get_current_duration_ms(self) -> float:
        """Get current elapsed time in milliseconds."""
        with self._lock:
            return (time.time() - self.start_time) * 1000


# Global registry of collectors per investigation
_collectors: Dict[str, MetricsCollector] = {}
_registry_lock = threading.Lock()


def get_collector(investigation_id: str) -> MetricsCollector:
    """
    Get or create a metrics collector for an investigation.
    
    Args:
        investigation_id: Unique investigation identifier
        
    Returns:
        MetricsCollector instance
    """
    with _registry_lock:
        if investigation_id not in _collectors:
            _collectors[investigation_id] = MetricsCollector()
        return _collectors[investigation_id]


def remove_collector(investigation_id: str):
    """Remove a collector when investigation completes."""
    with _registry_lock:
        if investigation_id in _collectors:
            del _collectors[investigation_id]


def timed_db_call(collector: MetricsCollector, operation: str, target: str):
    """
    Decorator/context manager for timing database calls.
    
    Usage:
        with timed_db_call(collector, "get_user", "KV"):
            result = aerospike_service.get_user(user_id)
    """
    @contextmanager
    def timer():
        start = time.time()
        success = True
        try:
            yield
        except Exception:
            success = False
            raise
        finally:
            duration_ms = (time.time() - start) * 1000
            collector.track_db_call(operation, target, duration_ms, success)
    
    return timer()
