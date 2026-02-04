"""
Gremlin-based Data Loader

This loader bypasses the buggy Aerospike Graph bulk loader by using 
direct Gremlin queries to insert vertices and edges with correct labels
and property names.
"""

import csv
import logging
from typing import Dict, Any, List
from datetime import datetime
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Graph
from gremlin_python.process.traversal import T

logger = logging.getLogger('fraud_detection.gremlin_loader')


class GremlinDataLoader:
    """Load graph data using Gremlin queries instead of bulk loader."""
    
    def __init__(self, graph_service):
        self.graph = graph_service
        self.batch_size = 100
        
    def load_all_data(self, vertices_path: str = "/data/graph_csv/vertices", 
                      edges_path: str = "/data/graph_csv/edges") -> Dict[str, Any]:
        """Load all vertices and edges from CSV files."""
        result = {
            "success": True,
            "vertices": {"users": 0, "accounts": 0, "devices": 0},
            "edges": {"owns": 0, "uses": 0},
            "errors": []
        }
        
        try:
            # Load vertices
            logger.info("Loading users...")
            result["vertices"]["users"] = self._load_users(f"{vertices_path}/users/users.csv")
            
            logger.info("Loading accounts...")
            result["vertices"]["accounts"] = self._load_accounts(f"{vertices_path}/accounts/accounts.csv")
            
            logger.info("Loading devices...")
            result["vertices"]["devices"] = self._load_devices(f"{vertices_path}/devices/devices.csv")
            
            # Load edges
            logger.info("Loading OWNS edges...")
            result["edges"]["owns"] = self._load_owns_edges(f"{edges_path}/ownership/owns.csv")
            
            logger.info("Loading USES edges...")
            result["edges"]["uses"] = self._load_uses_edges(f"{edges_path}/usage/uses.csv")
            
            logger.info(f"Data loading complete: {result}")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            result["success"] = False
            result["errors"].append(str(e))
            
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
                        "risk_score": float(row.get("risk_score:Double", 0)) if row.get("risk_score:Double") else 0.0,
                        "signup_date": row.get("signup_date:Date", "")
                    }
                    batch.append(vertex_data)
                    
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
                # Check if vertex exists first
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
        """Load accounts from CSV with correct schema."""
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
                        "fraud_flag": row.get("fraud_flag:Boolean", "False").lower() == "true"
                    }
                    batch.append(vertex_data)
                    
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
                # Check if vertex exists first
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
        """Load devices from CSV with correct schema."""
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
                        "fraud_flag": row.get("fraud_flag:Boolean", "False").lower() == "true"
                    }
                    batch.append(vertex_data)
                    
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
                # Check if vertex exists first
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
        """Load OWNS edges from CSV."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        errors = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    from_id = row.get("~from")
                    to_id = row.get("~to")
                    
                    if from_id and to_id:
                        try:
                            # Use __.V() for the target vertex in addE().to()
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
        
        if errors > 0:
            logger.warning(f"OWNS edges: {count} loaded, {errors} errors")
            
        return count
    
    def _load_uses_edges(self, csv_path: str) -> int:
        """Load USES edges from CSV."""
        if not self.graph.client:
            raise Exception("Graph client not available")
            
        count = 0
        errors = 0
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    from_id = row.get("~from")
                    to_id = row.get("~to")
                    
                    if from_id and to_id:
                        try:
                            # Use __.V() for the target vertex in addE().to()
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
        
        if errors > 0:
            logger.warning(f"USES edges: {count} loaded, {errors} errors")
            
        return count


# Singleton instance creator
def create_gremlin_loader(graph_service):
    return GremlinDataLoader(graph_service)
