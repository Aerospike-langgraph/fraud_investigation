#!/usr/bin/env python3
"""
Clear ALL data from both Aerospike KV store and Graph DB.

Usage (from project root):
    python scripts/clear_all_data.py

By default connects to:
    - Aerospike KV:  localhost:3000  (namespace: test)
    - Aerospike Graph: localhost:8182 (Gremlin endpoint)

Override with environment variables:
    AEROSPIKE_HOST=localhost AEROSPIKE_PORT=3000 GRAPH_HOST=localhost GRAPH_PORT=8182
"""

import os
import sys
import time

# ── Config ──────────────────────────────────────────────────────────────────

AS_HOST = os.environ.get("AEROSPIKE_HOST", "localhost")
AS_PORT = int(os.environ.get("AEROSPIKE_PORT", "3000"))
AS_NAMESPACE = os.environ.get("AEROSPIKE_NAMESPACE", "test")

GRAPH_HOST = os.environ.get("GRAPH_HOST", "localhost")
GRAPH_PORT = int(os.environ.get("GRAPH_PORT", "8182"))

# All KV sets used by the application
KV_SETS = [
    "users",
    "evaluations",
    "flagged_accounts",
    "workflow",
    "config",
    "detection_history",
    "transactions",
    "account_fact",
    "device_fact",
    "investigations",
    # LangGraph checkpoint sets
    "lg_cp",
    "lg_cp_w",
    "lg_cp_meta",
]


def clear_kv():
    """Truncate all Aerospike KV sets."""
    try:
        import aerospike
    except ImportError:
        print("  [!] aerospike Python client not installed — skipping KV clear")
        print("      Install with: pip install aerospike")
        return False

    print(f"\n{'='*60}")
    print(f"  Aerospike KV Store  ({AS_HOST}:{AS_PORT}, ns={AS_NAMESPACE})")
    print(f"{'='*60}")

    try:
        client = aerospike.client({"hosts": [(AS_HOST, AS_PORT)]}).connect()
    except Exception as e:
        print(f"  [✗] Failed to connect: {e}")
        return False

    success = 0
    failed = 0

    for set_name in KV_SETS:
        try:
            client.truncate(AS_NAMESPACE, set_name, 0)
            print(f"  [✓] Truncated  {set_name}")
            success += 1
        except Exception as e:
            # Some sets may not exist yet — that's fine
            err_str = str(e)
            if "not found" in err_str.lower() or "2" in err_str:
                print(f"  [–] Skipped    {set_name}  (set not found)")
                success += 1
            else:
                print(f"  [✗] Failed     {set_name}  ({e})")
                failed += 1

    client.close()
    print(f"\n  Summary: {success} truncated, {failed} failed")
    return failed == 0


def clear_graph():
    """Drop all vertices (and edges) from Aerospike Graph via Gremlin."""
    try:
        from gremlin_python.driver.client import Client
    except ImportError:
        print("  [!] gremlinpython not installed — skipping Graph clear")
        print("      Install with: pip install gremlinpython")
        return False

    ws_url = f"ws://{GRAPH_HOST}:{GRAPH_PORT}/gremlin"

    print(f"\n{'='*60}")
    print(f"  Aerospike Graph DB  ({ws_url})")
    print(f"{'='*60}")

    try:
        client = Client(ws_url, "g")
    except Exception as e:
        print(f"  [✗] Failed to connect: {e}")
        return False

    try:
        # Count before (submit raw Gremlin scripts to avoid bytecode version issues)
        v_count = client.submit("g.V().count()").all().result()[0]
        e_count = client.submit("g.E().count()").all().result()[0]
        print(f"  Before:  {v_count} vertices, {e_count} edges")

        # Drop all vertices (edges are removed automatically)
        if v_count > 0:
            print("  Dropping all vertices...")
            client.submit("g.V().drop().iterate()").all().result()
            time.sleep(1)

        # Verify
        v_after = client.submit("g.V().count()").all().result()[0]
        e_after = client.submit("g.E().count()").all().result()[0]
        print(f"  After:   {v_after} vertices, {e_after} edges")
        print(f"\n  Summary: Removed {v_count} vertices and {e_count} edges")

        client.close()
        return True

    except Exception as e:
        print(f"  [✗] Failed to clear graph: {e}")
        try:
            client.close()
        except:
            pass
        return False


def main():
    print("\n╔══════════════════════════════════════════════════════════╗")
    print("║          CLEAR ALL DATA — KV Store + Graph DB          ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # Confirmation
    if "--yes" not in sys.argv and "-y" not in sys.argv:
        answer = input("\n  ⚠  This will DELETE ALL DATA. Continue? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            print("  Aborted.")
            sys.exit(0)

    start = time.time()

    kv_ok = clear_kv()
    graph_ok = clear_graph()

    elapsed = time.time() - start

    print(f"\n{'='*60}")
    print(f"  Done in {elapsed:.1f}s")
    print(f"  KV Store: {'✓ cleared' if kv_ok else '✗ issues'}")
    print(f"  Graph DB: {'✓ cleared' if graph_ok else '✗ issues'}")
    print(f"{'='*60}\n")

    sys.exit(0 if (kv_ok and graph_ok) else 1)


if __name__ == "__main__":
    main()
