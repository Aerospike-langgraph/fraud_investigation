#!/usr/bin/env python3
"""
Test script for performance monitoring system
"""

import asyncio
import time
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from services.performance_monitor import performance_monitor

async def test_performance_monitoring():
    """Test the performance monitoring system"""
    print("🧪 Testing Performance Monitoring System")
    print("=" * 50)
    
    # Test RT1 performance recording
    print("\n📊 Testing RT1 Performance Recording...")
    for i in range(5):
        execution_time = 30 + (i * 10)  # Simulate varying execution times
        success = i < 4  # Simulate some failures
        performance_monitor.record_rt1_performance(execution_time, success=success)
        print(f"  RT1 Test {i+1}: {execution_time}ms, Success: {success}")
        await asyncio.sleep(0.1)
    
    # Test RT2 performance recording
    print("\n📊 Testing RT2 Performance Recording...")
    for i in range(5):
        execution_time = 80 + (i * 15)  # RT2 is typically slower
        success = i < 4
        performance_monitor.record_rt2_performance(execution_time, success=success)
        print(f"  RT2 Test {i+1}: {execution_time}ms, Success: {success}")
        await asyncio.sleep(0.1)
    
    # Test RT3 performance recording
    print("\n📊 Testing RT3 Performance Recording...")
    for i in range(5):
        execution_time = 150 + (i * 20)  # RT3 is typically slowest
        success = i < 4
        performance_monitor.record_rt3_performance(execution_time, success=success)
        print(f"  RT3 Test {i+1}: {execution_time}ms, Success: {success}")
        await asyncio.sleep(0.1)
    
    # Get performance statistics
    print("\n📈 Performance Statistics (5-minute window):")
    print("-" * 50)
    
    stats = performance_monitor.get_all_stats(5)
    
    for method in ['rt1', 'rt2', 'rt3']:
        data = stats[method]
        print(f"\n{method.upper()}:")
        print(f"  Average Execution Time: {data['avg_execution_time']}ms")
        print(f"  Max Execution Time: {data['max_execution_time']}ms")
        print(f"  Min Execution Time: {data['min_execution_time']}ms")
        print(f"  Total Queries: {data['total_queries']}")
        print(f"  Success Rate: {data['success_rate']}%")
        print(f"  Queries Per Second: {data['queries_per_second']}")
        print(f"  Query Complexity: {data['query_complexity']}")
        print(f"  Cache Status: {data['cache_enabled']}")
    
    # Get timeline data
    print("\n📊 Timeline Data (5-minute window):")
    print("-" * 50)
    
    timeline = performance_monitor.get_recent_timeline_data(5)
    
    for method in ['rt1', 'rt2', 'rt3']:
        data = timeline[method]
        print(f"\n{method.upper()}: {len(data)} data points")
        if data:
            print(f"  Latest: {data[-1]['execution_time']}ms at {data[-1]['timestamp']}")
    
    print("\n✅ Performance monitoring test completed successfully!")
    print("\n🚀 Ready to demonstrate Gremlin query latencies!")
    print("   - Start the backend server: python3 main.py")
    print("   - Start the frontend: npm run dev")
    print("   - Navigate to /admin and click the 'Performance' tab")

if __name__ == "__main__":
    asyncio.run(test_performance_monitoring())
