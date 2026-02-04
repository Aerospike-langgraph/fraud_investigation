#!/usr/bin/env python3
"""
Debug script for testing data loading functionality.
This script can be run with the VS Code debugger to set breakpoints
and debug the data loading process.
"""

import asyncio
import sys
import os

# Add the backend directory to the Python path
sys.path.append(os.path.dirname(__file__))

from services.graph_service import GraphService

async def debug_data_loading():
    """Debug the data loading process"""
    print("🐛 Starting data loading debug session...")
    
    # Initialize the graph service
    graph_service = GraphService()
    
    # Connect to the graph
    print("🔗 Connecting to Aerospike Graph...")
    await graph_service.connect()
    
    if graph_service.client:
        print("✅ Connected to Aerospike Graph")
        
        # Test data loading
        print("🌱 Testing data loading...")
        result = await graph_service.seed_sample_data()
        
        print(f"📊 Data loading result: {result}")
        
        # Close connection
        await graph_service.close()
        print("🔌 Disconnected from Aerospike Graph")
    else:
        print("❌ Failed to connect to Aerospike Graph")

if __name__ == "__main__":
    # Run the debug function
    asyncio.run(debug_data_loading()) 