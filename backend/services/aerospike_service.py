"""
Aerospike Key-Value Service

This service provides key-value storage operations using Aerospike.
Used for:
- User data storage for risk evaluation
- Risk score and cooldown tracking
- Analyst workflow stage tracking
- Flagged accounts storage
"""

import logging
import os
import csv
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

try:
    import aerospike
    from aerospike import exception as ex
    AEROSPIKE_AVAILABLE = True
except ImportError:
    AEROSPIKE_AVAILABLE = False
    aerospike = None

logger = logging.getLogger('fraud_detection.aerospike')

# Aerospike configuration
AEROSPIKE_HOST = os.environ.get('AEROSPIKE_HOST', 'localhost')
AEROSPIKE_PORT = int(os.environ.get('AEROSPIKE_KV_PORT', '3000'))
AEROSPIKE_NAMESPACE = os.environ.get('AEROSPIKE_NAMESPACE', 'test')

# Set names
SET_USERS = 'users'
SET_EVALUATIONS = 'evaluations'
SET_FLAGGED_ACCOUNTS = 'flagged_accounts'
SET_WORKFLOW = 'workflow'
SET_CONFIG = 'config'
SET_HISTORY = 'detection_history'

# Aerospike bin name limit is 15 characters
# Map long bin names to short versions
BIN_NAME_MAP = {
    'schedule_enabled': 'sched_enabled',
    'accounts_processed': 'accts_proc',
    'accounts_flagged': 'accts_flagged',
    'new_flagged_count': 'new_flag_cnt',
    'cooldown_days': 'cooldown_days',
    'risk_threshold': 'risk_thresh',
    'workflow_status': 'wf_status',
    'flagged_at': 'flagged_at',
    'last_evaluated': 'last_eval',
    'evaluation_count': 'eval_count',
    # Flagged account fields
    'suspicious_transactions': 'suspicious_txn',
    'total_flagged_amount': 'total_flag_amt',
    'transaction_count': 'txn_count',
    'investigation_started': 'invest_started',
    'resolution_notes': 'resol_notes',
    'current_risk_score': 'curr_risk_score',
    'assigned_analyst': 'assigned_anlst',
    'account_holder': 'acct_holder',
}
# Reverse map for reading
BIN_NAME_REVERSE = {v: k for k, v in BIN_NAME_MAP.items()}


class AerospikeService:
    """
    Service for Aerospike key-value operations.
    """
    
    def __init__(self):
        self.client = None
        self.connected = False
        self.namespace = AEROSPIKE_NAMESPACE
        
    def connect(self) -> bool:
        """Connect to Aerospike cluster."""
        if not AEROSPIKE_AVAILABLE:
            logger.warning("Aerospike Python client not available. Using fallback storage.")
            return False
            
        try:
            config = {
                'hosts': [(AEROSPIKE_HOST, AEROSPIKE_PORT)]
            }
            self.client = aerospike.client(config).connect()
            self.connected = True
            logger.info(f"✅ Connected to Aerospike at {AEROSPIKE_HOST}:{AEROSPIKE_PORT}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Aerospike: {e}")
            self.connected = False
            return False
    
    def close(self):
        """Close Aerospike connection."""
        if self.client and self.connected:
            try:
                self.client.close()
                self.connected = False
                logger.info("✅ Disconnected from Aerospike")
            except Exception as e:
                logger.warning(f"Error closing Aerospike connection: {e}")
    
    def is_connected(self) -> bool:
        """Check if connected to Aerospike."""
        return self.connected and self.client is not None
    
    # ----------------------------------------------------------------------------------------------------------
    # Generic Key-Value Operations
    # ----------------------------------------------------------------------------------------------------------
    
    def _shorten_bin_names(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Shorten bin names to fit Aerospike's 15-character limit and filter None values."""
        shortened = {}
        for k, v in data.items():
            # Skip None values - Aerospike can't serialize them
            if v is None:
                continue
            new_key = BIN_NAME_MAP.get(k, k)
            # If still too long, truncate
            if len(new_key) > 15:
                new_key = new_key[:15]
            shortened[new_key] = v
        return shortened
    
    def _expand_bin_names(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Expand shortened bin names back to original."""
        if data is None:
            return None
        expanded = {}
        for k, v in data.items():
            expanded[BIN_NAME_REVERSE.get(k, k)] = v
        return expanded
    
    def put(self, set_name: str, key: str, data: Dict[str, Any], ttl: int = 0) -> bool:
        """
        Store a record in Aerospike.
        
        Args:
            set_name: The set name
            key: Record key
            data: Dictionary of data to store
            ttl: Time-to-live in seconds (0 = never expire)
        """
        if not self.is_connected():
            return False
            
        try:
            record_key = (self.namespace, set_name, key)
            meta = {'ttl': ttl} if ttl > 0 else {}
            # Shorten bin names to fit Aerospike's 15-char limit
            shortened_data = self._shorten_bin_names(data)
            self.client.put(record_key, shortened_data, meta=meta)
            return True
        except Exception as e:
            logger.error(f"Error putting record {key} in {set_name}: {e}")
            return False
    
    def get(self, set_name: str, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a record from Aerospike.
        
        Args:
            set_name: The set name
            key: Record key
            
        Returns:
            Record data or None if not found
        """
        if not self.is_connected():
            return None
            
        try:
            record_key = (self.namespace, set_name, key)
            _, _, bins = self.client.get(record_key)
            # Expand shortened bin names back to original
            return self._expand_bin_names(bins)
        except ex.RecordNotFound:
            return None
        except Exception as e:
            logger.error(f"Error getting record {key} from {set_name}: {e}")
            return None
    
    def delete(self, set_name: str, key: str) -> bool:
        """Delete a record from Aerospike."""
        if not self.is_connected():
            return False
            
        try:
            record_key = (self.namespace, set_name, key)
            self.client.remove(record_key)
            return True
        except ex.RecordNotFound:
            return True  # Already deleted
        except Exception as e:
            logger.error(f"Error deleting record {key} from {set_name}: {e}")
            return False
    
    def exists(self, set_name: str, key: str) -> bool:
        """Check if a record exists."""
        if not self.is_connected():
            return False
            
        try:
            record_key = (self.namespace, set_name, key)
            _, meta = self.client.exists(record_key)
            return meta is not None
        except Exception as e:
            logger.error(f"Error checking existence of {key} in {set_name}: {e}")
            return False
    
    def scan_all(self, set_name: str, limit: int = 10000) -> List[Dict[str, Any]]:
        """
        Scan all records in a set.
        
        Args:
            set_name: The set name
            limit: Maximum records to return
            
        Returns:
            List of records
        """
        if not self.is_connected():
            return []
            
        try:
            records = []
            scan = self.client.scan(self.namespace, set_name)
            
            def callback(record):
                if len(records) < limit:
                    _, _, bins = record
                    # Expand shortened bin names back to original
                    records.append(self._expand_bin_names(bins))
            
            scan.foreach(callback)
            return records
        except Exception as e:
            logger.error(f"Error scanning {set_name}: {e}")
            return []
    
    def truncate_set(self, set_name: str) -> bool:
        """Delete all records in a set."""
        if not self.is_connected():
            return False
            
        try:
            self.client.truncate(self.namespace, set_name, 0)
            logger.info(f"Truncated set {set_name}")
            return True
        except Exception as e:
            logger.error(f"Error truncating {set_name}: {e}")
            return False
    
    # ----------------------------------------------------------------------------------------------------------
    # User Operations
    # ----------------------------------------------------------------------------------------------------------
    
    def load_users_from_csv(self, csv_path: str = None, clear_existing: bool = True) -> Dict[str, Any]:
        """
        Load users from CSV file into Aerospike.
        
        Args:
            csv_path: Path to users CSV file
            clear_existing: If True, truncate existing users first (clears evaluation timestamps)
            
        Returns:
            Result dict with count and status
        """
        if csv_path is None:
            csv_path = "/data/graph_csv/vertices/users/users.csv"
        
        result = {
            "success": False,
            "loaded": 0,
            "errors": 0,
            "message": ""
        }
        
        # Clear existing users to reset evaluation timestamps
        if clear_existing:
            self.truncate_set(SET_USERS)
            logger.info("Cleared existing users before reload")
        
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    user_id = row.get('~id', '')
                    if not user_id:
                        result["errors"] += 1
                        continue
                    
                    # Parse user data
                    user_data = {
                        "user_id": user_id,
                        "name": row.get('name:String', ''),
                        "email": row.get('email:String', ''),
                        "phone": row.get('phone:String', ''),
                        "age": int(row.get('age:Int', 0)) if row.get('age:Int') else 0,
                        "location": row.get('location:String', ''),
                        "occupation": row.get('occupation:String', ''),
                        "risk_score": float(row.get('risk_score:Double', 0)) if row.get('risk_score:Double') else 0.0,
                        "signup_date": row.get('signup_date:Date', ''),
                        "created_at": datetime.now().isoformat(),
                        # Evaluation tracking
                        "last_evaluated": None,
                        "evaluation_count": 0,
                        "current_risk_score": None,
                        # Workflow tracking
                        "workflow_status": None,
                        "flagged_date": None,
                        "assigned_analyst": None,
                        "resolution": None,
                        "resolution_date": None,
                        "resolution_notes": None
                    }
                    
                    if self.put(SET_USERS, user_id, user_data):
                        result["loaded"] += 1
                    else:
                        result["errors"] += 1
            
            result["success"] = True
            result["message"] = f"Loaded {result['loaded']} users into Aerospike"
            logger.info(result["message"])
            
        except FileNotFoundError:
            result["message"] = f"CSV file not found: {csv_path}"
            logger.error(result["message"])
        except Exception as e:
            result["message"] = f"Error loading users: {str(e)}"
            logger.error(result["message"])
        
        return result
    
    def load_sample_flagged_accounts(self) -> Dict[str, Any]:
        """
        Load sample flagged accounts for demo purposes.
        Uses existing users from Aerospike and marks some as flagged.
        
        Returns:
            Result dict with count and status
        """
        import random
        
        result = {
            "success": False,
            "loaded": 0,
            "message": ""
        }
        
        try:
            # Get existing users
            users = self.get_all_users(limit=100)
            if not users:
                result["message"] = "No users found to flag"
                return result
            
            # Select random users to flag (about 15-20 users)
            num_to_flag = min(20, len(users))
            users_to_flag = random.sample(users, num_to_flag)
            
            # Different statuses and risk levels for variety
            statuses = [
                ("pending_review", 8),     # Most common
                ("under_investigation", 4),
                ("confirmed_fraud", 2),
                ("cleared", 4)
            ]
            
            risk_reasons = [
                "Multiple failed transactions in short period",
                "Unusual transaction pattern detected",
                "Connection to known fraudulent accounts",
                "High-value transactions from new account",
                "Rapid succession of international transfers",
                "Multiple device changes in 24 hours",
                "IP address associated with fraud ring",
                "Behavioral anomaly in spending pattern",
                "Account linked to flagged device",
                "Suspicious login location changes"
            ]
            
            risk_factors = [
                ["velocity_spike", "unusual_amount"],
                ["network_connection", "flagged_contact"],
                ["device_fingerprint", "ip_reputation"],
                ["behavioral_anomaly", "time_pattern"],
                ["geographic_anomaly", "velocity_spike"],
                ["new_account_risk", "high_value_transaction"]
            ]
            
            status_idx = 0
            status_count = 0
            current_status, max_count = statuses[status_idx]
            
            for i, user in enumerate(users_to_flag):
                user_id = user.get("user_id", f"user_{i}")
                
                # Cycle through statuses
                if status_count >= max_count:
                    status_idx = (status_idx + 1) % len(statuses)
                    current_status, max_count = statuses[status_idx]
                    status_count = 0
                
                # Generate risk score based on status
                if current_status == "pending_review":
                    risk_score = random.randint(70, 85)
                elif current_status == "under_investigation":
                    risk_score = random.randint(75, 90)
                elif current_status == "confirmed_fraud":
                    risk_score = random.randint(85, 98)
                else:  # cleared
                    risk_score = random.randint(50, 69)
                
                # Calculate dates
                now = datetime.now()
                days_ago = random.randint(1, 30)
                flagged_date = now - timedelta(days=days_ago)
                
                suspicious_txns = random.randint(3, 25)
                flagged_amount = round(random.uniform(5000, 150000), 2)
                
                flagged_account = {
                    "account_id": user_id,
                    "user_id": user_id,
                    "account_holder": user.get("name", f"User {user_id}"),
                    "email": user.get("email", f"{user_id}@example.com"),
                    "risk_score": risk_score,
                    "status": current_status,
                    "flag_reason": random.choice(risk_reasons),
                    "reason": random.choice(risk_reasons),  # Keep for backwards compat
                    "risk_factors": random.choice(risk_factors),
                    "flagged_date": flagged_date.isoformat(),
                    "last_activity": (now - timedelta(hours=random.randint(1, 72))).isoformat(),
                    "suspicious_transactions": suspicious_txns,
                    "total_flagged_amount": flagged_amount,
                    "transaction_count": random.randint(50, 300),
                    "total_amount": round(random.uniform(10000, 500000), 2),
                    "model_version": "v1.0-mock",
                    "confidence": round(random.uniform(0.75, 0.95), 2),
                    "created_at": flagged_date.isoformat()
                }
                
                # Add resolution data for confirmed_fraud and cleared
                if current_status in ["confirmed_fraud", "cleared"]:
                    resolution_date = flagged_date + timedelta(days=random.randint(1, 7))
                    flagged_account["resolution"] = "fraud" if current_status == "confirmed_fraud" else "safe"
                    flagged_account["resolution_date"] = resolution_date.isoformat()
                    flagged_account["resolution_notes"] = (
                        "Account confirmed as fraudulent after investigation" 
                        if current_status == "confirmed_fraud" 
                        else "Investigation found no evidence of fraud"
                    )
                    flagged_account["resolved_by"] = "analyst@demo.com"
                
                # Add investigation data for under_investigation
                if current_status == "under_investigation":
                    flagged_account["assigned_analyst"] = "analyst@demo.com"
                    flagged_account["investigation_started"] = (flagged_date + timedelta(hours=random.randint(1, 24))).isoformat()
                
                if self.flag_account(flagged_account):
                    result["loaded"] += 1
                
                status_count += 1
            
            result["success"] = True
            result["message"] = f"Loaded {result['loaded']} sample flagged accounts"
            logger.info(result["message"])
            
        except Exception as e:
            result["message"] = f"Error loading sample flagged accounts: {str(e)}"
            logger.error(result["message"])
        
        return result
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get a user by ID."""
        return self.get(SET_USERS, user_id)
    
    def update_user(self, user_id: str, updates: Dict[str, Any]) -> bool:
        """Update user fields."""
        user = self.get_user(user_id)
        if not user:
            return False
        
        user.update(updates)
        user["updated_at"] = datetime.now().isoformat()
        return self.put(SET_USERS, user_id, user)
    
    def get_all_users(self, limit: int = 10000) -> List[Dict[str, Any]]:
        """Get all users."""
        return self.scan_all(SET_USERS, limit)
    
    def get_users_for_evaluation(self, cooldown_days: int = 7, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get users that need risk evaluation (not in cooldown).
        
        Args:
            cooldown_days: Days before re-evaluation
            limit: Maximum users to return
        """
        all_users = self.scan_all(SET_USERS, limit=10000)
        eligible_users = []
        cooldown_threshold = datetime.now() - timedelta(days=cooldown_days)
        
        for user in all_users:
            last_evaluated = user.get('last_evaluated')
            
            # Include if never evaluated or cooldown expired
            if last_evaluated is None:
                eligible_users.append(user)
            else:
                try:
                    eval_date = datetime.fromisoformat(last_evaluated)
                    if eval_date < cooldown_threshold:
                        eligible_users.append(user)
                except:
                    eligible_users.append(user)
            
            if len(eligible_users) >= limit:
                break
        
        return eligible_users
    
    def update_user_evaluation(self, user_id: str, risk_score: float) -> bool:
        """Update user's evaluation data."""
        user = self.get_user(user_id)
        if not user:
            return False
        
        user["last_evaluated"] = datetime.now().isoformat()
        user["current_risk_score"] = risk_score
        user["evaluation_count"] = user.get("evaluation_count", 0) + 1
        
        return self.put(SET_USERS, user_id, user)
    
    # ----------------------------------------------------------------------------------------------------------
    # Flagged Accounts Operations
    # ----------------------------------------------------------------------------------------------------------
    
    def flag_account(self, account_data: Dict[str, Any]) -> bool:
        """Store a flagged account record."""
        account_id = account_data.get('account_id')
        if not account_id:
            return False
        
        account_data["flagged_date"] = datetime.now().isoformat()
        account_data["status"] = account_data.get("status", "pending_review")
        
        return self.put(SET_FLAGGED_ACCOUNTS, account_id, account_data)
    
    def get_flagged_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get a flagged account by ID."""
        return self.get(SET_FLAGGED_ACCOUNTS, account_id)
    
    def get_all_flagged_accounts(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get all flagged accounts."""
        return self.scan_all(SET_FLAGGED_ACCOUNTS, limit)
    
    def update_flagged_account(self, account_id: str, updates: Dict[str, Any]) -> bool:
        """Update a flagged account."""
        account = self.get_flagged_account(account_id)
        if not account:
            return False
        
        account.update(updates)
        account["updated_at"] = datetime.now().isoformat()
        return self.put(SET_FLAGGED_ACCOUNTS, account_id, account)
    
    def delete_flagged_account(self, account_id: str) -> bool:
        """Delete a flagged account."""
        return self.delete(SET_FLAGGED_ACCOUNTS, account_id)
    
    def clear_all_flagged_accounts(self) -> bool:
        """Clear all flagged accounts."""
        return self.truncate_set(SET_FLAGGED_ACCOUNTS)
    
    # ----------------------------------------------------------------------------------------------------------
    # Workflow Operations
    # ----------------------------------------------------------------------------------------------------------
    
    def update_workflow_status(self, user_id: str, status: str, analyst: str = None, notes: str = None) -> bool:
        """
        Update the workflow status for a user/account.
        
        Status values: pending_review, under_investigation, confirmed_fraud, cleared
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        user["workflow_status"] = status
        user["workflow_updated_at"] = datetime.now().isoformat()
        
        if analyst:
            user["assigned_analyst"] = analyst
        if notes:
            user["workflow_notes"] = notes
        
        if status in ["confirmed_fraud", "cleared"]:
            user["resolution"] = status
            user["resolution_date"] = datetime.now().isoformat()
            if notes:
                user["resolution_notes"] = notes
        
        return self.put(SET_USERS, user_id, user)
    
    def get_users_by_workflow_status(self, status: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get users with a specific workflow status."""
        all_users = self.scan_all(SET_USERS, limit=10000)
        return [u for u in all_users if u.get('workflow_status') == status][:limit]
    
    # ----------------------------------------------------------------------------------------------------------
    # Configuration Operations
    # ----------------------------------------------------------------------------------------------------------
    
    def get_config(self, config_key: str = "detection_config") -> Optional[Dict[str, Any]]:
        """Get configuration."""
        return self.get(SET_CONFIG, config_key)
    
    def save_config(self, config: Dict[str, Any], config_key: str = "detection_config") -> bool:
        """Save configuration."""
        config["updated_at"] = datetime.now().isoformat()
        return self.put(SET_CONFIG, config_key, config)
    
    # ----------------------------------------------------------------------------------------------------------
    # History Operations
    # ----------------------------------------------------------------------------------------------------------
    
    def add_detection_history(self, job_result: Dict[str, Any]) -> bool:
        """Add a detection job result to history."""
        job_id = job_result.get("job_id", f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        return self.put(SET_HISTORY, job_id, job_result)
    
    def get_detection_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get detection job history."""
        records = self.scan_all(SET_HISTORY, limit=100)
        # Sort by start_time descending
        records.sort(key=lambda x: x.get('start_time', ''), reverse=True)
        return records[:limit]
    
    # ----------------------------------------------------------------------------------------------------------
    # Statistics
    # ----------------------------------------------------------------------------------------------------------
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about stored data."""
        return {
            "users_count": len(self.scan_all(SET_USERS, limit=100000)),
            "flagged_accounts_count": len(self.scan_all(SET_FLAGGED_ACCOUNTS, limit=100000)),
            "pending_review": len(self.get_users_by_workflow_status("pending_review")),
            "under_investigation": len(self.get_users_by_workflow_status("under_investigation")),
            "confirmed_fraud": len(self.get_users_by_workflow_status("confirmed_fraud")),
            "cleared": len(self.get_users_by_workflow_status("cleared")),
            "connected": self.is_connected()
        }


# Singleton instance
aerospike_service = AerospikeService()
