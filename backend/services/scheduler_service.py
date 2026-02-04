"""
Scheduler Service

This service manages scheduled jobs for the fraud detection system,
including the daily flagged account detection job.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, JobExecutionEvent

logger = logging.getLogger('fraud_detection.scheduler')


class SchedulerService:
    """
    Service for managing scheduled jobs with APScheduler.
    """
    
    JOB_ID_DETECTION = "flagged_account_detection"
    
    def __init__(self):
        self._scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,  # Combine missed runs
                'max_instances': 1  # Only one instance at a time
            }
        )
        self._detection_callback: Optional[Callable] = None
        self._last_run_result: Optional[Dict[str, Any]] = None
        self._is_running = False
        
        # Add event listeners
        self._scheduler.add_listener(self._on_job_executed, EVENT_JOB_EXECUTED)
        self._scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)
    
    def start(self):
        """Start the scheduler."""
        if not self._scheduler.running:
            self._scheduler.start()
            logger.info("Scheduler started")
    
    def shutdown(self):
        """Shutdown the scheduler."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("Scheduler shutdown")
    
    def set_detection_callback(self, callback: Callable):
        """Set the callback function for detection job."""
        self._detection_callback = callback
    
    def _on_job_executed(self, event: JobExecutionEvent):
        """Handle successful job execution."""
        if event.job_id == self.JOB_ID_DETECTION:
            self._last_run_result = {
                "status": "success",
                "scheduled_time": event.scheduled_run_time.isoformat() if event.scheduled_run_time else None,
                "execution_time": datetime.now().isoformat(),
                "result": event.retval if hasattr(event, 'retval') else None
            }
            self._is_running = False
            logger.info(f"Detection job completed successfully")
    
    def _on_job_error(self, event: JobExecutionEvent):
        """Handle job execution error."""
        if event.job_id == self.JOB_ID_DETECTION:
            self._last_run_result = {
                "status": "error",
                "scheduled_time": event.scheduled_run_time.isoformat() if event.scheduled_run_time else None,
                "execution_time": datetime.now().isoformat(),
                "error": str(event.exception) if event.exception else "Unknown error"
            }
            self._is_running = False
            logger.error(f"Detection job failed: {event.exception}")
    
    def _run_detection_job(self):
        """Internal method to run the detection job."""
        self._is_running = True
        logger.info("Starting scheduled detection job")
        
        if self._detection_callback:
            try:
                result = self._detection_callback()
                return result
            except Exception as e:
                logger.error(f"Detection callback failed: {e}")
                raise
        else:
            logger.warning("No detection callback configured")
            return {"status": "skipped", "reason": "No callback configured"}
    
    # ----------------------------------------------------------------------------------------------------------
    # Schedule Management
    # ----------------------------------------------------------------------------------------------------------
    
    def schedule_detection_job(self, hour: int, minute: int) -> Dict[str, Any]:
        """
        Schedule the detection job to run daily at the specified time.
        
        Args:
            hour: Hour (0-23)
            minute: Minute (0-59)
            
        Returns:
            Job info dictionary
        """
        try:
            # Remove existing job if any
            self.remove_detection_job()
            
            # Create cron trigger for daily execution
            trigger = CronTrigger(hour=hour, minute=minute)
            
            # Add job
            job = self._scheduler.add_job(
                self._run_detection_job,
                trigger=trigger,
                id=self.JOB_ID_DETECTION,
                name="Flagged Account Detection",
                replace_existing=True
            )
            
            logger.info(f"Detection job scheduled for {hour:02d}:{minute:02d} daily")
            
            return {
                "job_id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "schedule_time": f"{hour:02d}:{minute:02d}"
            }
            
        except Exception as e:
            logger.error(f"Failed to schedule detection job: {e}")
            raise
    
    def remove_detection_job(self) -> bool:
        """Remove the detection job from the scheduler."""
        try:
            job = self._scheduler.get_job(self.JOB_ID_DETECTION)
            if job:
                self._scheduler.remove_job(self.JOB_ID_DETECTION)
                logger.info("Detection job removed from scheduler")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to remove detection job: {e}")
            return False
    
    def pause_detection_job(self) -> bool:
        """Pause the detection job."""
        try:
            job = self._scheduler.get_job(self.JOB_ID_DETECTION)
            if job:
                self._scheduler.pause_job(self.JOB_ID_DETECTION)
                logger.info("Detection job paused")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to pause detection job: {e}")
            return False
    
    def resume_detection_job(self) -> bool:
        """Resume the detection job."""
        try:
            job = self._scheduler.get_job(self.JOB_ID_DETECTION)
            if job:
                self._scheduler.resume_job(self.JOB_ID_DETECTION)
                logger.info("Detection job resumed")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to resume detection job: {e}")
            return False
    
    def run_detection_now(self, skip_cooldown: bool = False) -> Dict[str, Any]:
        """
        Trigger the detection job to run immediately.
        
        Args:
            skip_cooldown: If True, evaluate all users regardless of cooldown period
        
        Returns:
            Job result dictionary
        """
        if self._is_running:
            return {
                "status": "skipped",
                "reason": "Job is already running"
            }
        
        cooldown_msg = " (skip cooldown)" if skip_cooldown else ""
        logger.info(f"Manual detection job trigger{cooldown_msg}")
        
        try:
            self._is_running = True
            if self._detection_callback:
                result = self._detection_callback(skip_cooldown=skip_cooldown)
                self._last_run_result = {
                    "status": "success",
                    "execution_time": datetime.now().isoformat(),
                    "trigger": "manual",
                    "skip_cooldown": skip_cooldown,
                    "result": result
                }
                return result
            else:
                return {"status": "error", "reason": "No detection callback configured"}
        except Exception as e:
            self._last_run_result = {
                "status": "error",
                "execution_time": datetime.now().isoformat(),
                "trigger": "manual",
                "error": str(e)
            }
            raise
        finally:
            self._is_running = False
    
    # ----------------------------------------------------------------------------------------------------------
    # Status & Info
    # ----------------------------------------------------------------------------------------------------------
    
    def get_job_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the detection job."""
        job = self._scheduler.get_job(self.JOB_ID_DETECTION)
        if not job:
            return None
        
        return {
            "job_id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "pending": job.pending,
            "is_running": self._is_running
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        job = self._scheduler.get_job(self.JOB_ID_DETECTION)
        
        return {
            "scheduler_running": self._scheduler.running,
            "detection_job_scheduled": job is not None,
            "detection_job_running": self._is_running,
            "next_run": job.next_run_time.isoformat() if job and job.next_run_time else None,
            "last_run_result": self._last_run_result
        }
    
    def get_all_jobs(self) -> list:
        """Get all scheduled jobs."""
        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                "job_id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "pending": job.pending
            })
        return jobs


# Singleton instance
scheduler_service = SchedulerService()
