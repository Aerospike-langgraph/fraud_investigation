#!/usr/bin/env python3
import asyncio
import sys
import os

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.graph_service import GraphService

async def debug_transactions():
    gs = GraphService()
    try:
        await gs.connect()
        print("Connected to graph service")
        
        # Check what transaction vertices exist
        print("\n=== Checking Transaction Vertices ===")
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            
            def get_transaction_vertices():
                return gs.client.V().has_label("Transaction").limit(3).to_list()
            
            transaction_vertices = await loop.run_in_executor(None, get_transaction_vertices)
            print(f"Found {len(transaction_vertices)} transaction vertices")
            
            for i, vertex in enumerate(transaction_vertices):
                print(f"\nTransaction Vertex {i+1}:")
                
                def get_vertex_props():
                    return gs.client.V(vertex).value_map().next()
                
                props = await loop.run_in_executor(None, get_vertex_props)
                for key, value in props.items():
                    print(f"  {key}: {value}")
                
                # Check incoming edges
                def get_incoming_edges():
                    return gs.client.V(vertex).in_().to_list()
                
                incoming_edges = await loop.run_in_executor(None, get_incoming_edges)
                print(f"  Incoming edges: {len(incoming_edges)}")
                for edge in incoming_edges:
                    print(f"    Edge label: {edge.label}")
                    print(f"    Edge properties: {edge.value_map().next()}")
                    
        except Exception as e:
            print(f"Error checking transaction vertices: {e}")
        
        # Check what edges exist
        print("\n=== Checking Transaction Edges ===")
        try:
            def get_transaction_edges():
                return gs.client.E().has_label("HAS_TRANSACTION").limit(3).to_list()
            
            transaction_edges = await loop.run_in_executor(None, get_transaction_edges)
            print(f"Found {len(transaction_edges)} HAS_TRANSACTION edges")
            
            for i, edge in enumerate(transaction_edges):
                print(f"\nTransaction Edge {i+1}:")
                print(f"  Edge label: {edge.label}")
                
                def get_edge_props():
                    return gs.client.E(edge).value_map().next()
                
                props = await loop.run_in_executor(None, get_edge_props)
                for key, value in props.items():
                    print(f"  {key}: {value}")
                
                # Check source and destination vertices
                def get_source_vertex():
                    return gs.client.E(edge).out_vertex().next()
                
                def get_dest_vertex():
                    return gs.client.E(edge).in_vertex().next()
                
                source_vertex = await loop.run_in_executor(None, get_source_vertex)
                dest_vertex = await loop.run_in_executor(None, get_dest_vertex)
                
                print(f"  Source vertex label: {source_vertex.label}")
                print(f"  Destination vertex label: {dest_vertex.label}")
                
        except Exception as e:
            print(f"Error checking transaction edges: {e}")
        
        # Check total counts
        print("\n=== Checking Total Counts ===")
        try:
            def count_transaction_vertices():
                return len(gs.client.V().has_label("Transaction").to_list())
            
            def count_transaction_edges():
                return len(gs.client.E().has_label("HAS_TRANSACTION").to_list())
            
            vertex_count = await loop.run_in_executor(None, count_transaction_vertices)
            edge_count = await loop.run_in_executor(None, count_transaction_edges)
            
            print(f"Total Transaction vertices: {vertex_count}")
            print(f"Total HAS_TRANSACTION edges: {edge_count}")
            
        except Exception as e:
            print(f"Error checking counts: {e}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await gs.close()

if __name__ == "__main__":
    asyncio.run(debug_transactions()) 