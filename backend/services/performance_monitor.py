"""
Performance Monitoring Service
Tracks Gremlin query latencies and performance metrics for fraud detection methods

This service monitors the performance of RT1, RT2, and RT3 fraud detection
methods and provides real-time metrics for the frontend dashboard.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from collections import deque
import statistics

# Setup logging
logger = logging.getLogger('performance_monitor')

class PerformanceMonitor:
    """Performance monitoring for fraud detection methods"""
    
    def __init__(self, max_history: int = 1000000):
        self.max_history = max_history
        
        # Performance data storage
        self.rt1_metrics = deque(maxlen=max_history)
        self.rt2_metrics = deque(maxlen=max_history)
        self.rt3_metrics = deque(maxlen=max_history)
        
        # Real-time counters
        self.rt1_counter = 0
        self.rt2_counter = 0
        self.rt3_counter = 0
        
        # Success/failure counters
        self.rt1_success = 0
        self.rt1_failure = 0
        self.rt2_success = 0
        self.rt2_failure = 0
        self.rt3_success = 0
        self.rt3_failure = 0
        
        logger.info("🚀 Performance monitor initialized")
    
    def record_rt1_performance(self, execution_time: float, success: bool = True, 
                              query_complexity: str = "1-hop lookup", cache_hit: bool = False):
        """Record RT1 performance metrics"""
        metric = {
            'timestamp': datetime.now(),
            'execution_time': execution_time,
            'success': success,
            'query_complexity': query_complexity,
            'cache_hit': cache_hit,
            'method': 'RT1'
        }
        
        self.rt1_metrics.append(metric)
        self.rt1_counter += 1
        
        if success:
            self.rt1_success += 1
        else:
            self.rt1_failure += 1
    
    def record_rt2_performance(self, execution_time: float, success: bool = True,
                              query_complexity: str = "multi-hop network", cache_hit: bool = False):
        """Record RT2 performance metrics"""
        metric = {
            'timestamp': datetime.now(),
            'execution_time': execution_time,
            'success': success,
            'query_complexity': query_complexity,
            'cache_hit': cache_hit,
            'method': 'RT2'
        }
        
        self.rt2_metrics.append(metric)
        self.rt2_counter += 1
        
        if success:
            self.rt2_success += 1
        else:
            self.rt2_failure += 1
    
    def record_rt3_performance(self, execution_time: float, success: bool = True,
                              query_complexity: str = "multi-hop network", cache_hit: bool = False):
        """Record RT3 performance metrics"""
        metric = {
            'timestamp': datetime.now(),
            'execution_time': execution_time,
            'success': success,
            'query_complexity': query_complexity,
            'cache_hit': cache_hit,
            'method': 'RT3'
        }
        
        self.rt3_metrics.append(metric)
        self.rt3_counter += 1
        
        if success:
            self.rt3_success += 1
        else:
            self.rt3_failure += 1
    
    def get_rt1_stats(self, time_window_minutes: int = 5) -> Dict[str, Any]:
        """Get RT1 performance statistics"""
        return self._get_method_stats(self.rt1_metrics, time_window_minutes, 'RT1')
    
    def get_rt2_stats(self, time_window_minutes: int = 5) -> Dict[str, Any]:
        """Get RT2 performance statistics"""
        return self._get_method_stats(self.rt2_metrics, time_window_minutes, 'RT2')
    
    def get_rt3_stats(self, time_window_minutes: int = 5) -> Dict[str, Any]:
        """Get RT3 performance statistics"""
        return self._get_method_stats(self.rt3_metrics, time_window_minutes, 'RT3')
    
    def _get_method_stats(self, metrics: deque, time_window_minutes: int, method: str) -> Dict[str, Any]:
        """Calculate performance statistics for a method"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        # Filter metrics within time window
        recent_metrics = [m for m in metrics if m['timestamp'] >= cutoff_time]
        
        if not recent_metrics:
            return {
                'method': method,
                'avg_execution_time': 0,
                'max_execution_time': 0,
                'min_execution_time': 0,
                'total_queries': 0,
                'success_rate': 0,
                'queries_per_second': 0,
                'cache_enabled': self._get_cache_status(method)
            }
        
        execution_times = [m['execution_time'] for m in recent_metrics]
        success_count = sum(1 for m in recent_metrics if m['success'])
        
        # Calculate queries per second
        time_span = (datetime.now() - cutoff_time).total_seconds()
        queries_per_second = len(recent_metrics) / time_span if time_span > 0 else 0
        
        return {
            'method': method,
            'avg_execution_time': round(statistics.mean(execution_times), 2),
            'max_execution_time': round(max(execution_times), 2),
            'min_execution_time': round(min(execution_times), 2),
            'total_queries': len(recent_metrics),
            'success_rate': round((success_count / len(recent_metrics)) * 100, 1),
            'queries_per_second': round(queries_per_second, 2),
            'cache_enabled': self._get_cache_status(method)
        }
    
    
    def _get_cache_status(self, method: str) -> str:
        """Get cache status for each method"""
        cache_status = {
            'RT1': 'Enabled (10min TTL)',
            'RT2': 'Disabled (real-time network)',
            'RT3': 'Enabled (5min TTL)'
        }
        return cache_status.get(method, 'Unknown')
    
    def get_all_stats(self, time_window_minutes: int = 5) -> Dict[str, Any]:
        """Get performance statistics for all methods"""
        return {
            'rt1': self.get_rt1_stats(time_window_minutes),
            'rt2': self.get_rt2_stats(time_window_minutes),
            'rt3': self.get_rt3_stats(time_window_minutes),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_recent_timeline_data(self, minutes: int = 5) -> Dict[str, List[Dict[str, Any]]]:
        """Get timeline data for charts"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        rt1_timeline = [
            {'timestamp': m['timestamp'].isoformat(), 'execution_time': m['execution_time'], 'method': 'RT1'}
            for m in self.rt1_metrics if m['timestamp'] >= cutoff_time
        ]
        
        rt2_timeline = [
            {'timestamp': m['timestamp'].isoformat(), 'execution_time': m['execution_time'], 'method': 'RT2'}
            for m in self.rt2_metrics if m['timestamp'] >= cutoff_time
        ]
        
        rt3_timeline = [
            {'timestamp': m['timestamp'].isoformat(), 'execution_time': m['execution_time'], 'method': 'RT3'}
            for m in self.rt3_metrics if m['timestamp'] >= cutoff_time
        ]
        
        return {
            'rt1': rt1_timeline,
            'rt2': rt2_timeline,
            'rt3': rt3_timeline
        }
    
    def reset_metrics(self):
        """Reset all performance metrics"""
        self.rt1_metrics.clear()
        self.rt2_metrics.clear()
        self.rt3_metrics.clear()
        
        self.rt1_counter = 0
        self.rt2_counter = 0
        self.rt3_counter = 0
        
        self.rt1_success = 0
        self.rt1_failure = 0
        self.rt2_success = 0
        self.rt2_failure = 0
        self.rt3_success = 0
        self.rt3_failure = 0
        
        logger.info("🔄 Performance metrics reset")

# Global performance monitor instance
performance_monitor = PerformanceMonitor()
