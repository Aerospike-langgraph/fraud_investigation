#!/usr/bin/env python3
"""
Test script to verify Aerospike behavior with None values.

This demonstrates that Aerospike Python client cannot store None values
without a serializer, which is the root cause of the AerospikeSaver error.

Usage:
    python test_aerospike_none.py
    
Or from docker:
    docker exec asgraph-backend python /app/test_aerospike_none.py
"""

import aerospike
import os

# Use localhost when running directly, or Docker hostname when in container
AEROSPIKE_HOST = os.environ.get('AEROSPIKE_HOST', 'localhost')
AEROSPIKE_PORT = int(os.environ.get('AEROSPIKE_PORT', 3000))
AEROSPIKE_NAMESPACE = os.environ.get('AEROSPIKE_NAMESPACE', 'test')

def test_with_none_value():
    """Test storing a record with a None bin value - expected to FAIL"""
    print("=" * 60)
    print("TEST 1: Store record WITH None value")
    print("=" * 60)
    
    config = {'hosts': [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
    client = aerospike.client(config).connect()
    
    key = (AEROSPIKE_NAMESPACE, 'null_test', 'test_with_none')
    
    record = {
        'name': 'John Doe',
        'age': 30,
        'parent_id': None,  # This will cause the error
        'active': True
    }
    
    print(f"Record to store: {record}")
    print()
    
    try:
        client.put(key, record)
        print("✅ SUCCESS: Record stored!")
        
        # Read it back
        _, _, bins = client.get(key)
        print(f"Retrieved: {bins}")
        
        # Cleanup
        #client.remove(key)
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    finally:
        client.close()


def test_without_none_value():
    """Test storing a record without None values - expected to SUCCEED"""
    print()
    print("=" * 60)
    print("TEST 2: Store record WITHOUT None value (excluded from dict)")
    print("=" * 60)
    
    config = {'hosts': [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
    client = aerospike.client(config).connect()
    
    key = (AEROSPIKE_NAMESPACE, 'null_test', 'test_without_none')
    
    record = {
        'name': 'John Doe',
        'age': 30,
        'parent_id': '123',  # NOT included in dict
        'active': True
    }
    
    print(f"Record to store: {record}")
    print()
    
    try:
        client.put(key, record)
        print("✅ SUCCESS: Record stored!")
        
        # Read it back
        _, _, bins = client.get(key)
        print(f"Retrieved: {bins}")
        
        # Cleanup
        #client.remove(key)
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    finally:
        client.close()


def test_filtered_none_values():
    """Test storing a record with None values filtered out - expected to SUCCEED"""
    print()
    print("=" * 60)
    print("TEST 3: Store record with None values FILTERED out")
    print("=" * 60)
    
    config = {'hosts': [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
    client = aerospike.client(config).connect()
    
    key = (AEROSPIKE_NAMESPACE, 'null_test', 'test_filtered')
    
    # Original record with None
    original_record = {
        'name': 'John Doe',
        'age': 30,
        'parent_id': None,  # Will be filtered out
        'email': None,      # Will be filtered out
        'active': True
    }
    
    # Filter out None values
    filtered_record = {k: v for k, v in original_record.items() if v is not None}
    
    print(f"Original record: {original_record}")
    print(f"Filtered record: {filtered_record}")
    print()
    
    try:
        client.put(key, filtered_record)
        print("✅ SUCCESS: Filtered record stored!")
        
        # Read it back
        _, _, bins = client.get(key)
        print(f"Retrieved: {bins}")
        
        # Cleanup
        client.remove(key)
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    finally:
        client.close()


def test_empty_string_instead_of_none():
    """Test using empty string instead of None - expected to SUCCEED"""
    print()
    print("=" * 60)
    print("TEST 4: Store record with empty string instead of None")
    print("=" * 60)
    
    config = {'hosts': [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
    client = aerospike.client(config).connect()
    
    key = (AEROSPIKE_NAMESPACE, 'null_test', 'test_empty_string')
    
    record = {
        'name': 'John Doe',
        'age': 30,
        'parent_id': '',  # Empty string instead of None
        'active': True
    }
    
    print(f"Record to store: {record}")
    print()
    
    try:
        client.put(key, record)
        print("✅ SUCCESS: Record stored!")
        
        # Read it back
        _, _, bins = client.get(key)
        print(f"Retrieved: {bins}")
        
        # Cleanup
        client.remove(key)
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    finally:
        client.close()


def main():
    print()
    print("🔬 AEROSPIKE NONE VALUE BEHAVIOR TEST")
    print("=" * 60)
    print()
    print("This test demonstrates why AerospikeSaver fails on first")
    print("checkpoint: it includes 'p_checkpoint_id: None' in the record.")
    print()
    
    results = []
    
    # Run all tests
    results.append(("WITH None value", test_with_none_value()))
    results.append(("WITHOUT None value", test_without_none_value()))
    results.append(("Filtered None values", test_filtered_none_values()))
    results.append(("Empty string instead", test_empty_string_instead_of_none()))
    
    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {test_name}")
    
    print()
    print("CONCLUSION:")
    print("  The AerospikeSaver should filter out None values before")
    print("  calling client.put(). This is a bug in langgraph-checkpoint-aerospike.")
    print()


if __name__ == "__main__":
    main()
