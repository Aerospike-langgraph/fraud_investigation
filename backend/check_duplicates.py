#!/usr/bin/env python3

import os
import sys
sys.path.append('.')

from gremlin_python.driver import client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P
from collections import Counter

def check_duplicate_transactions():
    """Check for duplicate transactions in the graph database"""
    
    # Connect to Aerospike Graph
    connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = traversal().withRemote(connection)
    
    try:
        print("🔍 Checking for duplicate transactions...")
        
        # Get all transaction vertices
        all_transactions = g.V().has_label('transaction').to_list()
        print(f"📊 Total transaction vertices: {len(all_transactions)}")
        
        # Get all transaction IDs
        transaction_ids = []
        print("🔄 Collecting transaction IDs...")
        
        for i, tx in enumerate(all_transactions):
            if i % 10 == 0:
                print(f"   Processing {i}/{len(all_transactions)}...")
            
            try:
                props = g.V(tx).value_map().next()
                tx_id = props.get('transaction_id', ['unknown'])[0] if 'transaction_id' in props else 'unknown'
                transaction_ids.append(tx_id)
            except Exception as e:
                print(f"   Error getting properties for transaction {i}: {e}")
                transaction_ids.append('error')
        
        print(f"📋 Total transaction IDs collected: {len(transaction_ids)}")
        
        # Check for duplicates
        unique_ids = set(transaction_ids)
        print(f"🔗 Unique transaction IDs: {len(unique_ids)}")
        
        if len(transaction_ids) != len(unique_ids):
            print("🚨 DUPLICATES FOUND!")
            # Find which IDs are duplicated
            id_counts = Counter(transaction_ids)
            duplicates = {tx_id: count for tx_id, count in id_counts.items() if count > 1}
            print(f"📄 Duplicate transaction IDs:")
            for tx_id, count in duplicates.items():
                print(f"   {tx_id}: appears {count} times")
            
            # Show some examples of the duplicate properties
            print("\n🔍 Examining first few duplicates:")
            for tx_id, count in list(duplicates.items())[:3]:
                print(f"\n   Transaction ID: {tx_id} (appears {count} times)")
                matching_vertices = [tx for i, tx in enumerate(all_transactions) 
                                   if i < len(transaction_ids) and transaction_ids[i] == tx_id]
                
                for j, vertex in enumerate(matching_vertices):
                    try:
                        props = g.V(vertex).value_map().next()
                        print(f"     Copy {j+1}: {dict(props)}")
                    except Exception as e:
                        print(f"     Copy {j+1}: Error getting properties - {e}")
        else:
            print("✅ No duplicates found - all transaction IDs are unique")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    check_duplicate_transactions() 