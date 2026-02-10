"""
Progress tracking service for long-running operations.

Provides in-memory storage of operation progress that can be polled by the frontend.
"""

import time
import threading
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class OperationProgress:
    """Progress state for a single operation."""
    operation_id: str
    current: int = 0
    total: int = 0
    message: str = ""
    status: str = "running"  # running, completed, failed
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def percentage(self) -> int:
        """Calculate percentage complete."""
        if self.total <= 0:
            return 0
        return min(100, int((self.current / self.total) * 100))
    
    @property
    def elapsed_seconds(self) -> float:
        """Time elapsed since operation started."""
        return time.time() - self.started_at
    
    @property
    def estimated_remaining_seconds(self) -> Optional[float]:
        """Estimate remaining time based on current rate."""
        if self.current <= 0 or self.total <= 0:
            return None
        
        elapsed = self.elapsed_seconds
        rate = self.current / elapsed if elapsed > 0 else 0
        
        if rate <= 0:
            return None
        
        remaining = self.total - self.current
        return remaining / rate
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        return {
            "operation_id": self.operation_id,
            "current": self.current,
            "total": self.total,
            "percentage": self.percentage,
            "message": self.message,
            "status": self.status,
            "elapsed_seconds": round(self.elapsed_seconds, 1),
            "estimated_remaining_seconds": round(self.estimated_remaining_seconds, 1) if self.estimated_remaining_seconds else None,
            "error": self.error,
            "extra": self.extra
        }


class ProgressService:
    """Service for tracking progress of long-running operations."""
    
    def __init__(self):
        self._progress: Dict[str, OperationProgress] = {}
        self._lock = threading.Lock()
    
    def start_operation(self, operation_id: str, total: int, message: str = "Starting...") -> OperationProgress:
        """
        Start tracking a new operation.
        
        Args:
            operation_id: Unique identifier for the operation
            total: Total number of items to process
            message: Initial status message
            
        Returns:
            The created OperationProgress object
        """
        with self._lock:
            progress = OperationProgress(
                operation_id=operation_id,
                total=total,
                message=message,
                status="running"
            )
            self._progress[operation_id] = progress
            logger.info(f"[Progress] Started operation {operation_id} with {total} items")
            return progress
    
    def update_progress(
        self, 
        operation_id: str, 
        current: int, 
        message: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> Optional[OperationProgress]:
        """
        Update progress for an operation.
        
        Args:
            operation_id: The operation to update
            current: Current number of items processed
            message: Optional status message update
            extra: Optional extra data to include
            
        Returns:
            The updated OperationProgress or None if not found
        """
        with self._lock:
            progress = self._progress.get(operation_id)
            if not progress:
                return None
            
            progress.current = current
            progress.updated_at = time.time()
            
            if message:
                progress.message = message
            
            if extra:
                progress.extra.update(extra)
            
            return progress
    
    def complete_operation(
        self, 
        operation_id: str, 
        message: str = "Completed",
        extra: Optional[Dict[str, Any]] = None
    ) -> Optional[OperationProgress]:
        """
        Mark an operation as completed.
        
        Args:
            operation_id: The operation to complete
            message: Completion message
            extra: Optional extra data to include
            
        Returns:
            The updated OperationProgress or None if not found
        """
        with self._lock:
            progress = self._progress.get(operation_id)
            if not progress:
                return None
            
            progress.current = progress.total
            progress.status = "completed"
            progress.message = message
            progress.updated_at = time.time()
            
            if extra:
                progress.extra.update(extra)
            
            logger.info(f"[Progress] Completed operation {operation_id} in {progress.elapsed_seconds:.1f}s")
            return progress
    
    def fail_operation(
        self, 
        operation_id: str, 
        error: str,
        message: str = "Failed"
    ) -> Optional[OperationProgress]:
        """
        Mark an operation as failed.
        
        Args:
            operation_id: The operation that failed
            error: Error message
            message: Status message
            
        Returns:
            The updated OperationProgress or None if not found
        """
        with self._lock:
            progress = self._progress.get(operation_id)
            if not progress:
                return None
            
            progress.status = "failed"
            progress.error = error
            progress.message = message
            progress.updated_at = time.time()
            
            logger.error(f"[Progress] Operation {operation_id} failed: {error}")
            return progress
    
    def get_progress(self, operation_id: str) -> Optional[OperationProgress]:
        """
        Get progress for an operation.
        
        Args:
            operation_id: The operation to get progress for
            
        Returns:
            The OperationProgress or None if not found
        """
        with self._lock:
            return self._progress.get(operation_id)
    
    def clear_progress(self, operation_id: str) -> bool:
        """
        Clear progress for an operation (typically after completion).
        
        Args:
            operation_id: The operation to clear
            
        Returns:
            True if the operation was found and cleared
        """
        with self._lock:
            if operation_id in self._progress:
                del self._progress[operation_id]
                return True
            return False
    
    def clear_completed(self, max_age_seconds: float = 300) -> int:
        """
        Clear all completed/failed operations older than max_age_seconds.
        
        Args:
            max_age_seconds: Maximum age in seconds for completed operations
            
        Returns:
            Number of operations cleared
        """
        with self._lock:
            now = time.time()
            to_remove = []
            
            for op_id, progress in self._progress.items():
                if progress.status in ("completed", "failed"):
                    if now - progress.updated_at > max_age_seconds:
                        to_remove.append(op_id)
            
            for op_id in to_remove:
                del self._progress[op_id]
            
            return len(to_remove)


# Singleton instance
progress_service = ProgressService()
