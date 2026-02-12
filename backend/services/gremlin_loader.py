"""
Gremlin-based Data Loader

This loader bypasses the buggy Aerospike Graph bulk loader by using 
direct Gremlin queries to insert vertices and edges with correct labels
and property names.

Key features:
- Drops fraud_flag on load (set to False) - flags are computed by ML
- Syncs data to KV store (users with nested accounts/devices maps)
"""

import csv
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Graph
from gremlin_python.process.traversal import T

from services.progress_service import progress_service

logger = logging.getLogger('fraud_detection.gremlin_loader')


class GremlinDataLoader:
    """Load graph data using Gremlin queries instead of bulk loader."""
    
    # Class-level operation ID for progress tracking
    OPERATION_ID = "bulk_load"
    
    def __init__(self, graph_service, aerospike_service=None):
        self.graph = graph_service
        self.kv = aerospike_service  # For KV sync
        self.batch_size = 100
        self.kv_batch_size = 500  # Larger batch size for KV writes
        
        # Track loaded data for KV sync
        self._loaded_users = {}      # {user_id: user_data}
        self._loaded_accounts = {}   # {account_id: account_data}
        self._loaded_devices = {}    # {device_id: device_data}
        self._owns_edges = []        # [(user_id, account_id), ...]
        self._uses_edges = []        # [(user_id, device_id), ...]
        
        # Optimization flags
        self._skip_exists_check = False  # Skip vertex existence checks for empty DB
        
    def _check_data_exists(self) -> Dict[str, int]:
        """Check if data already exists in the graph database."""
        try:
            user_count = self.graph.client.V().hasLabel("user").count().next()
            account_count = self.graph.client.V().hasLabel("account").count().next()
            device_count = self.graph.client.V().hasLabel("device").count().next()
            owns_count = self.graph.client.E().hasLabel("OWNS").count().next()
            uses_count = self.graph.client.E().hasLabel("USES").count().next()
            
            return {
                "users": user_count,
                "accounts": account_count,
                "devices": device_count,
                "owns_edges": owns_count,
                "uses_edges": uses_count
            }
        except Exception as e:
            logger.warning(f"Error checking existing data: {e}")
            return {"users": 0, "accounts": 0, "devices": 0, "owns_edges": 0, "uses_edges": 0}
    
    def load_all_data(self, vertices_path: str = "/data/graph_csv/vertices", 
                      edges_path: str = "/data/graph_csv/edges",
                      sync_kv: bool = True) -> Dict[str, Any]:
        """
        Load all vertices and edges from CSV files.
        
        Args:
            vertices_path: Path to vertices CSV directory
            edges_path: Path to edges CSV directory
            sync_kv: If True, sync loaded data to KV store
            
        Returns:
            Result dict with counts and status
        """
        result = {
            "success": True,
            "vertices": {"users": 0, "accounts": 0, "devices": 0},
            "edges": {"owns": 0, "uses": 0},
            "kv_sync": {"users": 0, "accounts_linked": 0, "devices_linked": 0},
            "errors": [],
            "skipped": False
        }
        
        # Clear tracking data
        self._loaded_users = {}
        self._loaded_accounts = {}
        self._loaded_devices = {}
        self._owns_edges = []
        self._uses_edges = []
        
        # Initialize progress tracking (6 stages: users, accounts, devices, owns, uses, kv_sync)
        total_stages = 6
        progress_service.start_operation(self.OPERATION_ID, total_stages, "Initializing bulk load...")
        
        try:
            # Check if data already exists
            progress_service.update_progress(self.OPERATION_ID, 0, "Checking existing data...")
            existing = self._check_data_exists()
            total_existing = sum(existing.values())
            
            # OPTIMIZATION: Skip existence checks if database is empty
            self._skip_exists_check = (total_existing == 0)
            if self._skip_exists_check:
                logger.info("🚀 Database is empty - enabling fast insert mode (skipping existence checks)")
            
            if total_existing > 0:
                logger.info(f"📊 Data already exists in graph: {existing}")
                
                # If we have significant data, skip loading to prevent duplicates
                if existing["users"] >= 1000:
                    logger.info("✅ Skipping data load - graph already has sufficient data")
                    result["skipped"] = True
                    result["message"] = f"Data already exists: {existing['users']} users, {existing['accounts']} accounts, {existing['devices']} devices"
                    progress_service.complete_operation(
                        self.OPERATION_ID, 
                        f"Skipped - data exists: {existing['users']} users",
                        extra={"skipped": True}
                    )
                    return result
            
            # Load vertices (these have upsert logic)
            progress_service.update_progress(self.OPERATION_ID, 0, "Loading users...")
            logger.info("Loading users...")
            result["vertices"]["users"] = self._load_users(f"{vertices_path}/users/users.csv")
            progress_service.update_progress(self.OPERATION_ID, 1, f"Loaded {result['vertices']['users']} users")
            
            progress_service.update_progress(self.OPERATION_ID, 1, "Loading accounts...")
            logger.info("Loading accounts (fraud_flag=False)...")
            result["vertices"]["accounts"] = self._load_accounts(f"{vertices_path}/accounts/accounts.csv")
            progress_service.update_progress(self.OPERATION_ID, 2, f"Loaded {result['vertices']['accounts']} accounts")
            
            progress_service.update_progress(self.OPERATION_ID, 2, "Loading devices...")
            logger.info("Loading devices (fraud_flag=False)...")
            result["vertices"]["devices"] = self._load_devices(f"{vertices_path}/devices/devices.csv")
            progress_service.update_progress(self.OPERATION_ID, 3, f"Loaded {result['vertices']['devices']} devices")
            
            # Load edges (with duplicate checking)
            progress_service.update_progress(self.OPERATION_ID, 3, "Loading OWNS edges...")
            logger.info("Loading OWNS edges...")
            result["edges"]["owns"] = self._load_owns_edges(f"{edges_path}/ownership/owns.csv")
            progress_service.update_progress(self.OPERATION_ID, 4, f"Loaded {result['edges']['owns']} OWNS edges")
            
            progress_service.update_progress(self.OPERATION_ID, 4, "Loading USES edges...")
            logger.info("Loading USES edges...")
            result["edges"]["uses"] = self._load_uses_edges(f"{edges_path}/usage/uses.csv")
            progress_service.update_progress(self.OPERATION_ID, 5, f"Loaded {result['edges']['uses']} USES edges")
            
            # Sync to KV store if enabled
            if sync_kv and self.kv and self.kv.is_connected():
                progress_service.update_progress(self.OPERATION_ID, 5, "Syncing to KV store...")
                logger.info("Syncing to KV store...")
                kv_result = self._sync_to_kv()
                result["kv_sync"] = kv_result
            
            progress_service.complete_operation(
                self.OPERATION_ID,
                f"Complete! {result['vertices']['users']} users, {result['vertices']['accounts']} accounts, {result['vertices']['devices']} devices",
                extra={
                    "users": result["vertices"]["users"],
                    "accounts": result["vertices"]["accounts"],
                    "devices": result["vertices"]["devices"],
                }
            )
            
            logger.info(f"✅ Data loading complete: {result}")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            result["success"] = False
            result["errors"].append(str(e))
            progress_service.fail_operation(self.OPERATION_ID, str(e), "Bulk load failed")
            
        return result
    
    def _load_users(self, csv_path: str) -> int:
        """Load users from CSV with correct schema."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                batch = []
                
                for row in reader:
                    vertex_data = {
                        "id": row.get("~id"),
                        "name": row.get("name:String", ""),
                        "email": row.get("email:String", ""),
                        "phone": row.get("phone:String", ""),
                        "age": int(row.get("age:Int", 0)) if row.get("age:Int") else 0,
                        "location": row.get("location:String", ""),
                        "occupation": row.get("occupation:String", ""),
                        "risk_score": 0.0,  # Initial risk score (will be computed by ML)
                        "signup_date": row.get("signup_date:Date", "")
                    }
                    batch.append(vertex_data)
                    
                    # Track for KV sync
                    self._loaded_users[vertex_data["id"]] = vertex_data
                    
                    if len(batch) >= self.batch_size:
                        self._insert_user_batch(batch)
                        count += len(batch)
                        batch = []
                        if count % 1000 == 0:
                            logger.info(f"Loaded {count} users...")
                
                # Insert remaining
                if batch:
                    self._insert_user_batch(batch)
                    count += len(batch)
                    
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            raise
            
        return count
    
    def _insert_user_batch(self, batch: List[Dict]):
        """Insert a batch of users (upsert - skip if exists)."""
        for user in batch:
            try:
                # OPTIMIZATION: Skip existence check if DB was empty at start
                if not self._skip_exists_check:
                    exists = self.graph.client.V(user["id"]).hasNext()
                    if exists:
                        continue  # Skip existing vertices
                    
                # Use T.id to set the actual vertex ID
                self.graph.client.addV("user") \
                    .property(T.id, user["id"]) \
                    .property("name", user["name"]) \
                    .property("email", user["email"]) \
                    .property("phone", user["phone"]) \
                    .property("age", user["age"]) \
                    .property("location", user["location"]) \
                    .property("occupation", user["occupation"]) \
                    .property("risk_score", user["risk_score"]) \
                    .property("signup_date", user["signup_date"]) \
                    .iterate()
            except Exception as e:
                logger.warning(f"Error inserting user {user['id']}: {e}")
    
    def _load_accounts(self, csv_path: str) -> int:
        """Load accounts from CSV with correct schema. fraud_flag is always set to False."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                batch = []
                
                for row in reader:
                    vertex_data = {
                        "id": row.get("~id"),
                        "type": row.get("type:String", ""),
                        "balance": float(row.get("balance:Double", 0)) if row.get("balance:Double") else 0.0,
                        "bank_name": row.get("bank_name:String", ""),
                        "status": row.get("status:String", "active"),
                        "created_date": row.get("created_date:Date", ""),
                        "fraud_flag": False  # Always False - flags are computed by ML
                    }
                    batch.append(vertex_data)
                    
                    # Track for KV sync
                    self._loaded_accounts[vertex_data["id"]] = vertex_data
                    
                    if len(batch) >= self.batch_size:
                        self._insert_account_batch(batch)
                        count += len(batch)
                        batch = []
                        if count % 1000 == 0:
                            logger.info(f"Loaded {count} accounts...")
                
                if batch:
                    self._insert_account_batch(batch)
                    count += len(batch)
                    
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            raise
            
        return count
    
    def _insert_account_batch(self, batch: List[Dict]):
        """Insert a batch of accounts (upsert - skip if exists)."""
        for account in batch:
            try:
                # OPTIMIZATION: Skip existence check if DB was empty at start
                if not self._skip_exists_check:
                    exists = self.graph.client.V(account["id"]).hasNext()
                    if exists:
                        continue  # Skip existing vertices
                    
                # Use T.id to set the actual vertex ID
                self.graph.client.addV("account") \
                    .property(T.id, account["id"]) \
                    .property("type", account["type"]) \
                    .property("balance", account["balance"]) \
                    .property("bank_name", account["bank_name"]) \
                    .property("status", account["status"]) \
                    .property("created_date", account["created_date"]) \
                    .property("fraud_flag", account["fraud_flag"]) \
                    .iterate()
            except Exception as e:
                logger.warning(f"Error inserting account {account['id']}: {e}")
    
    def _load_devices(self, csv_path: str) -> int:
        """Load devices from CSV with correct schema. fraud_flag is always set to False."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                batch = []
                
                for row in reader:
                    vertex_data = {
                        "id": row.get("~id"),
                        "type": row.get("type:String", ""),
                        "os": row.get("os:String", ""),
                        "browser": row.get("browser:String", ""),
                        "fingerprint": row.get("fingerprint:String", ""),
                        "first_seen": row.get("first_seen:Date", ""),
                        "last_login": row.get("last_login:Date", ""),
                        "login_count": int(row.get("login_count:Int", 0)) if row.get("login_count:Int") else 0,
                        "fraud_flag": False  # Always False - flags are computed by ML
                    }
                    batch.append(vertex_data)
                    
                    # Track for KV sync
                    self._loaded_devices[vertex_data["id"]] = vertex_data
                    
                    if len(batch) >= self.batch_size:
                        self._insert_device_batch(batch)
                        count += len(batch)
                        batch = []
                        if count % 1000 == 0:
                            logger.info(f"Loaded {count} devices...")
                
                if batch:
                    self._insert_device_batch(batch)
                    count += len(batch)
                    
        except Exception as e:
            logger.error(f"Error loading devices: {e}")
            raise
            
        return count
    
    def _insert_device_batch(self, batch: List[Dict]):
        """Insert a batch of devices (upsert - skip if exists)."""
        for device in batch:
            try:
                # OPTIMIZATION: Skip existence check if DB was empty at start
                if not self._skip_exists_check:
                    exists = self.graph.client.V(device["id"]).hasNext()
                    if exists:
                        continue  # Skip existing vertices
                    
                # Use T.id to set the actual vertex ID
                self.graph.client.addV("device") \
                    .property(T.id, device["id"]) \
                    .property("type", device["type"]) \
                    .property("os", device["os"]) \
                    .property("browser", device["browser"]) \
                    .property("fingerprint", device["fingerprint"]) \
                    .property("first_seen", device["first_seen"]) \
                    .property("last_login", device["last_login"]) \
                    .property("login_count", device["login_count"]) \
                    .property("fraud_flag", device["fraud_flag"]) \
                    .iterate()
            except Exception as e:
                logger.warning(f"Error inserting device {device['id']}: {e}")
    
    def _load_owns_edges(self, csv_path: str) -> int:
        """Load OWNS edges from CSV (skip if edge already exists)."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        skipped = 0
        errors = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    from_id = row.get("~from")  # user_id
                    to_id = row.get("~to")      # account_id
                    
                    if from_id and to_id:
                        # Track for KV sync (all edges, including existing)
                        self._owns_edges.append((from_id, to_id))
                        
                        try:
                            # OPTIMIZATION: Skip edge existence check if DB was empty at start
                            if not self._skip_exists_check:
                                edge_exists = self.graph.client.V(from_id) \
                                    .outE("OWNS") \
                                    .where(__.inV().hasId(to_id)) \
                                    .hasNext()
                                
                                if edge_exists:
                                    skipped += 1
                                    continue  # Skip duplicate edge
                            
                            # Create edge
                            self.graph.client.V(from_id) \
                                .addE("OWNS") \
                                .to(__.V(to_id)) \
                                .iterate()
                            count += 1
                            if count % 5000 == 0:
                                logger.info(f"Loaded {count} OWNS edges...")
                        except Exception as e:
                            errors += 1
                            if errors <= 5:
                                logger.warning(f"Error creating OWNS edge {from_id}->{to_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error loading OWNS edges: {e}")
            raise
        
        if skipped > 0:
            logger.info(f"OWNS edges: {count} created, {skipped} skipped (already existed)")
        if errors > 0:
            logger.warning(f"OWNS edges: {count} loaded, {errors} errors")
            
        return count
    
    def _load_uses_edges(self, csv_path: str) -> int:
        """Load USES edges from CSV (skip if edge already exists)."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        skipped = 0
        errors = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    from_id = row.get("~from")  # user_id
                    to_id = row.get("~to")      # device_id
                    
                    if from_id and to_id:
                        # Track for KV sync (all edges, including existing)
                        self._uses_edges.append((from_id, to_id))
                        
                        try:
                            # OPTIMIZATION: Skip edge existence check if DB was empty at start
                            if not self._skip_exists_check:
                                edge_exists = self.graph.client.V(from_id) \
                                    .outE("USES") \
                                    .where(__.inV().hasId(to_id)) \
                                    .hasNext()
                                
                                if edge_exists:
                                    skipped += 1
                                    continue  # Skip duplicate edge
                            
                            # Create edge
                            self.graph.client.V(from_id) \
                                .addE("USES") \
                                .to(__.V(to_id)) \
                                .iterate()
                            count += 1
                            if count % 5000 == 0:
                                logger.info(f"Loaded {count} USES edges...")
                        except Exception as e:
                            errors += 1
                            if errors <= 5:
                                logger.warning(f"Error creating USES edge {from_id}->{to_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error loading USES edges: {e}")
            raise
        
        if skipped > 0:
            logger.info(f"USES edges: {count} created, {skipped} skipped (already existed)")
        if errors > 0:
            logger.warning(f"USES edges: {count} loaded, {errors} errors")
            
        return count
    
    # ----------------------------------------------------------------------------------------------------------
    # KV Store Sync
    # ----------------------------------------------------------------------------------------------------------
    
    def _sync_to_kv(self) -> Dict[str, int]:
        """
        Sync loaded data to KV store using batch writes for better performance.
        Creates users with nested accounts and devices maps.
        """
        result = {
            "users": 0,
            "accounts_linked": 0,
            "devices_linked": 0,
            "errors": 0
        }
        
        if not self.kv or not self.kv.is_connected():
            logger.warning("KV service not available for sync")
            return result
        
        try:
            # Build user -> accounts mapping
            user_accounts = {}
            for user_id, account_id in self._owns_edges:
                if user_id not in user_accounts:
                    user_accounts[user_id] = []
                user_accounts[user_id].append(account_id)
            
            # Build user -> devices mapping
            user_devices = {}
            for user_id, device_id in self._uses_edges:
                if user_id not in user_devices:
                    user_devices[user_id] = []
                user_devices[user_id].append(device_id)
            
            # Prepare all user records for batch writing
            batch_records = []
            
            for user_id, user_data in self._loaded_users.items():
                try:
                    # Build accounts map
                    accounts_map = {}
                    for account_id in user_accounts.get(user_id, []):
                        account_data = self._loaded_accounts.get(account_id, {})
                        accounts_map[account_id] = {
                            'type': account_data.get('type', ''),
                            'balance': account_data.get('balance', 0),
                            'bank_name': account_data.get('bank_name', ''),
                            'status': account_data.get('status', 'active'),
                            'created_date': account_data.get('created_date', ''),
                            'is_fraud': False,  # Initialize at creation time
                        }
                        result["accounts_linked"] += 1
                    
                    # Build devices map
                    devices_map = {}
                    for device_id in user_devices.get(user_id, []):
                        device_data = self._loaded_devices.get(device_id, {})
                        devices_map[device_id] = {
                            'type': device_data.get('type', ''),
                            'os': device_data.get('os', ''),
                            'browser': device_data.get('browser', ''),
                            'fingerprint': device_data.get('fingerprint', ''),
                            'first_seen': device_data.get('first_seen', ''),
                            'last_login': device_data.get('last_login', ''),
                            'is_fraud': False,  # Initialize at creation time
                        }
                        result["devices_linked"] += 1
                    
                    # Create KV record
                    kv_user_data = {
                        "user_id": user_id,
                        "name": user_data.get("name", ""),
                        "email": user_data.get("email", ""),
                        "phone": user_data.get("phone", ""),
                        "age": user_data.get("age", 0),
                        "location": user_data.get("location", ""),
                        "occupation": user_data.get("occupation", ""),
                        "risk_score": 0.0,  # Will be computed by ML
                        "signup_date": user_data.get("signup_date", ""),
                        "created_at": datetime.now().isoformat(),
                        "accounts": accounts_map,
                        "devices": devices_map,
                        "last_eval": "",  # Use empty string instead of None
                        "eval_count": 0,
                        "curr_risk": 0.0,  # Use 0.0 instead of None
                    }
                    
                    batch_records.append((user_id, kv_user_data))
                    
                    # OPTIMIZATION: Write in batches of kv_batch_size
                    if len(batch_records) >= self.kv_batch_size:
                        batch_result = self.kv.batch_put('users', batch_records)
                        result["users"] += batch_result.get("success", 0)
                        result["errors"] += batch_result.get("failed", 0)
                        batch_records = []
                        logger.info(f"KV sync progress: {result['users']} users written...")
                        
                except Exception as e:
                    result["errors"] += 1
                    logger.warning(f"Error preparing user {user_id} for KV: {e}")
            
            # Write any remaining records
            if batch_records:
                batch_result = self.kv.batch_put('users', batch_records)
                result["users"] += batch_result.get("success", 0)
                result["errors"] += batch_result.get("failed", 0)
            
            logger.info(f"🚀 KV sync complete (batch mode): {result['users']} users, "
                       f"{result['accounts_linked']} accounts linked, "
                       f"{result['devices_linked']} devices linked")
            
        except Exception as e:
            logger.error(f"Error in KV sync: {e}")
            result["errors"] += 1
        
        return result
    
    def set_aerospike_service(self, aerospike_service):
        """Set the Aerospike service for KV sync."""
        self.kv = aerospike_service


# Singleton instance creator
def create_gremlin_loader(graph_service, aerospike_service=None):
    return GremlinDataLoader(graph_service, aerospike_service)
