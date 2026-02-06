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
# New sets for enhanced data model
SET_TRANSACTIONS = 'transactions'      # PK = {account_id}:{year_month}
SET_ACCOUNT_FACT = 'account_fact'      # PK = account_id
SET_DEVICE_FACT = 'device_fact'        # PK = device_id

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
    # Account-fact bins (15 char limit)
    'txn_out_count_7d': 'txn_out_7d',
    'txn_out_count_24h_peak': 'txn_24h_peak',
    'avg_txn_per_day_7d': 'avg_txn_day',
    'max_txn_per_hour_7d': 'max_txn_hr',
    'transaction_zscore': 'txn_zscore',
    'total_out_amount_7d': 'out_amt_7d',
    'avg_out_amount_7d': 'avg_out_amt',
    'max_out_amount_7d': 'max_out_amt',
    'amount_zscore_7d': 'amt_zscore',
    'unique_recipients_7d': 'uniq_recip',
    'new_recipient_ratio_7d': 'new_recip_rat',
    'recipient_entropy_7d': 'recip_entropy',
    'device_count_7d': 'dev_count',
    'shared_device_account_count_7d': 'shared_dev_ct',
    'account_age_days': 'acct_age_days',
    'first_txn_delay_days': 'first_txn_dly',
    'historical_txn_mean': 'hist_txn_mean',
    'historical_amt_mean': 'hist_amt_mean',
    'historical_amt_std': 'hist_amt_std',
    'last_computed': 'last_computed',
    # Device-fact bins
    'shared_account_count_7d': 'shared_acct_ct',
    'flagged_account_count': 'flag_acct_ct',
    'avg_account_risk_score': 'avg_acct_risk',
    'max_account_risk_score': 'max_acct_risk',
    'new_account_rate_7d': 'new_acct_7d',
    # Transaction bins
    'counterparty': 'counterparty',
    'direction': 'direction',
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
                    
                    # Parse user data with nested accounts/devices maps
                    user_data = {
                        "user_id": user_id,
                        "name": row.get('name:String', ''),
                        "email": row.get('email:String', ''),
                        "phone": row.get('phone:String', ''),
                        "age": int(row.get('age:Int', 0)) if row.get('age:Int') else 0,
                        "location": row.get('location:String', ''),
                        "occupation": row.get('occupation:String', ''),
                        "risk_score": 0.0,  # Initial risk score (will be computed)
                        "signup_date": row.get('signup_date:Date', ''),
                        "created_at": datetime.now().isoformat(),
                        # Nested maps for accounts and devices
                        "accounts": {},  # {account_id: {type, balance, bank_name, ...}}
                        "devices": {},   # {device_id: {type, os, browser, ...}}
                        # Evaluation tracking
                        "last_eval": None,
                        "eval_count": 0,
                        "curr_risk": None,  # Current computed risk score
                        # Workflow tracking
                        "wf_status": None,
                        "flagged_date": None,
                        "analyst": None,
                        "resolution": None,
                        "resol_date": None,
                        "resol_notes": None
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
        
        user["last_eval"] = datetime.now().isoformat()
        user["curr_risk"] = risk_score
        user["eval_count"] = user.get("eval_count", 0) + 1
        
        return self.put(SET_USERS, user_id, user)
    
    def add_account_to_user(self, user_id: str, account_data: Dict[str, Any]) -> bool:
        """
        Add an account to a user's accounts map.
        
        Args:
            user_id: The user ID
            account_data: Account data including account_id, type, balance, bank_name, etc.
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        account_id = account_data.get('account_id')
        if not account_id:
            return False
        
        accounts = user.get('accounts', {})
        accounts[account_id] = {
            'type': account_data.get('type', ''),
            'balance': float(account_data.get('balance', 0)),
            'bank_name': account_data.get('bank_name', ''),
            'status': account_data.get('status', 'active'),
            'created_date': account_data.get('created_date', ''),
        }
        user['accounts'] = accounts
        
        return self.put(SET_USERS, user_id, user)
    
    def add_device_to_user(self, user_id: str, device_data: Dict[str, Any]) -> bool:
        """
        Add a device to a user's devices map.
        
        Args:
            user_id: The user ID
            device_data: Device data including device_id, type, os, browser, etc.
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        device_id = device_data.get('device_id')
        if not device_id:
            return False
        
        devices = user.get('devices', {})
        devices[device_id] = {
            'type': device_data.get('type', ''),
            'os': device_data.get('os', ''),
            'browser': device_data.get('browser', ''),
            'fingerprint': device_data.get('fingerprint', ''),
            'first_seen': device_data.get('first_seen', ''),
            'last_login': device_data.get('last_login', ''),
        }
        user['devices'] = devices
        
        return self.put(SET_USERS, user_id, user)
    
    def get_user_accounts(self, user_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all accounts for a user."""
        user = self.get_user(user_id)
        return user.get('accounts', {}) if user else {}
    
    def get_user_devices(self, user_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all devices for a user."""
        user = self.get_user(user_id)
        return user.get('devices', {}) if user else {}
    
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
    # Transaction Operations (KV storage for feature computation)
    # ----------------------------------------------------------------------------------------------------------
    
    def _get_transaction_key(self, account_id: str, timestamp: str = None) -> str:
        """Generate transaction record key: {account_id}:{year_month}"""
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                dt = datetime.now()
        else:
            dt = datetime.now()
        return f"{account_id}:{dt.strftime('%Y-%m')}"
    
    def store_transaction(self, account_id: str, txn_data: Dict[str, Any], direction: str = "out") -> bool:
        """
        Store a transaction in the KV transactions set.
        
        Args:
            account_id: The account ID (sender or receiver)
            txn_data: Transaction data including txn_id, amount, type, counterparty, etc.
            direction: "out" for outgoing (sent), "in" for incoming (received)
        
        The transaction is stored in a map keyed by timestamp within a record
        partitioned by account_id and year-month.
        """
        if not self.is_connected():
            return False
        
        try:
            timestamp = txn_data.get('timestamp', datetime.now().isoformat())
            record_key = self._get_transaction_key(account_id, timestamp)
            
            # Build transaction entry
            txn_entry = {
                'txn_id': txn_data.get('txn_id', ''),
                'amount': float(txn_data.get('amount', 0)),
                'type': txn_data.get('type', 'transfer'),
                'counterparty': txn_data.get('counterparty', ''),
                'direction': direction,
                'method': txn_data.get('method', 'electronic'),
                'location': txn_data.get('location', ''),
                'status': txn_data.get('status', 'completed'),
            }
            
            # Get existing record or create new
            key = (self.namespace, SET_TRANSACTIONS, record_key)
            try:
                _, _, bins = self.client.get(key)
                txs_map = bins.get('txs', {}) if bins else {}
            except ex.RecordNotFound:
                txs_map = {}
            
            # Add transaction to map (key = timestamp)
            txs_map[timestamp] = txn_entry
            
            # Store updated record
            data = {
                'txs': txs_map,
                'account_id': account_id,
                'year_month': record_key.split(':')[1]
            }
            self.client.put(key, data)
            return True
            
        except Exception as e:
            logger.error(f"Error storing transaction for {account_id}: {e}")
            return False
    
    def get_transactions_for_account(self, account_id: str, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get transactions for an account within a sliding window.
        Uses batch read + map filtering for efficiency.
        
        Args:
            account_id: The account ID
            days: Number of days to look back (configurable cooldown)
            
        Returns:
            List of transactions within the window
        """
        if not self.is_connected():
            return []
        
        try:
            now = datetime.now()
            cutoff = (now - timedelta(days=days)).isoformat()
            
            # Generate record keys for relevant months
            keys = []
            for i in range(max(1, (days // 30) + 2)):  # Cover enough months
                month_date = now - timedelta(days=30 * i)
                record_key = f"{account_id}:{month_date.strftime('%Y-%m')}"
                keys.append((self.namespace, SET_TRANSACTIONS, record_key))
            
            # Batch read records
            records = self.client.get_many(keys)
            
            # Filter transactions by timestamp
            transactions = []
            for record in records:
                if record and record[2]:  # bins exist
                    txs_map = record[2].get('txs', {})
                    for ts, txn in txs_map.items():
                        if ts >= cutoff:
                            transactions.append({**txn, 'timestamp': ts})
            
            # Sort by timestamp descending
            transactions.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return transactions
            
        except Exception as e:
            logger.error(f"Error getting transactions for {account_id}: {e}")
            return []
    
    def batch_get_transactions(self, account_ids: List[str], days: int = 7) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch read transactions for multiple accounts.
        
        Args:
            account_ids: List of account IDs
            days: Number of days to look back
            
        Returns:
            Dict mapping account_id to list of transactions
        """
        result = {aid: [] for aid in account_ids}
        
        if not self.is_connected() or not account_ids:
            return result
        
        try:
            now = datetime.now()
            cutoff = (now - timedelta(days=days)).isoformat()
            
            # Generate all keys for all accounts
            keys = []
            key_to_account = {}
            
            for account_id in account_ids:
                for i in range(max(1, (days // 30) + 2)):
                    month_date = now - timedelta(days=30 * i)
                    record_key = f"{account_id}:{month_date.strftime('%Y-%m')}"
                    key = (self.namespace, SET_TRANSACTIONS, record_key)
                    keys.append(key)
                    key_to_account[record_key] = account_id
            
            # Batch read
            records = self.client.get_many(keys)
            
            # Process results
            for record in records:
                if record and record[2]:
                    bins = record[2]
                    account_id = bins.get('account_id', '')
                    txs_map = bins.get('txs', {})
                    
                    if account_id in result:
                        for ts, txn in txs_map.items():
                            if ts >= cutoff:
                                result[account_id].append({**txn, 'timestamp': ts})
            
            # Sort each account's transactions
            for account_id in result:
                result[account_id].sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            return result
            
        except Exception as e:
            logger.error(f"Error batch getting transactions: {e}")
            return result
    
    # ----------------------------------------------------------------------------------------------------------
    # Account-Fact Operations (Pre-computed features for ML)
    # ----------------------------------------------------------------------------------------------------------
    
    def get_account_fact(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get computed features for an account."""
        return self.get(SET_ACCOUNT_FACT, account_id)
    
    def update_account_fact(self, account_id: str, features: Dict[str, Any]) -> bool:
        """
        Update computed features for an account.
        
        Args:
            account_id: The account ID
            features: Dict of computed features (uses short bin names internally)
        """
        features['account_id'] = account_id
        features['last_computed'] = datetime.now().isoformat()
        return self.put(SET_ACCOUNT_FACT, account_id, features)
    
    def batch_get_account_facts(self, account_ids: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Batch read account facts for multiple accounts.
        
        Args:
            account_ids: List of account IDs
            
        Returns:
            Dict mapping account_id to features (or None if not found)
        """
        result = {aid: None for aid in account_ids}
        
        if not self.is_connected() or not account_ids:
            return result
        
        try:
            keys = [(self.namespace, SET_ACCOUNT_FACT, aid) for aid in account_ids]
            records = self.client.get_many(keys)
            
            for i, record in enumerate(records):
                if record and record[2]:
                    result[account_ids[i]] = self._expand_bin_names(record[2])
            
            return result
            
        except Exception as e:
            logger.error(f"Error batch getting account facts: {e}")
            return result
    
    def get_all_account_facts(self, limit: int = 10000) -> List[Dict[str, Any]]:
        """Get all account facts."""
        return self.scan_all(SET_ACCOUNT_FACT, limit)
    
    # ----------------------------------------------------------------------------------------------------------
    # Device-Fact Operations (Pre-computed features for device flagging)
    # ----------------------------------------------------------------------------------------------------------
    
    def get_device_fact(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Get computed features for a device."""
        return self.get(SET_DEVICE_FACT, device_id)
    
    def update_device_fact(self, device_id: str, features: Dict[str, Any]) -> bool:
        """
        Update computed features for a device.
        
        Args:
            device_id: The device ID
            features: Dict of computed features
        """
        features['device_id'] = device_id
        features['last_computed'] = datetime.now().isoformat()
        return self.put(SET_DEVICE_FACT, device_id, features)
    
    def batch_get_device_facts(self, device_ids: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Batch read device facts for multiple devices.
        
        Args:
            device_ids: List of device IDs
            
        Returns:
            Dict mapping device_id to features (or None if not found)
        """
        result = {did: None for did in device_ids}
        
        if not self.is_connected() or not device_ids:
            return result
        
        try:
            keys = [(self.namespace, SET_DEVICE_FACT, did) for did in device_ids]
            records = self.client.get_many(keys)
            
            for i, record in enumerate(records):
                if record and record[2]:
                    result[device_ids[i]] = self._expand_bin_names(record[2])
            
            return result
            
        except Exception as e:
            logger.error(f"Error batch getting device facts: {e}")
            return result
    
    def get_all_device_facts(self, limit: int = 10000) -> List[Dict[str, Any]]:
        """Get all device facts."""
        return self.scan_all(SET_DEVICE_FACT, limit)
    
    # ----------------------------------------------------------------------------------------------------------
    # Statistics
    # ----------------------------------------------------------------------------------------------------------
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about stored data."""
        return {
            "users_count": len(self.scan_all(SET_USERS, limit=100000)),
            "flagged_accounts_count": len(self.scan_all(SET_FLAGGED_ACCOUNTS, limit=100000)),
            "account_facts_count": len(self.scan_all(SET_ACCOUNT_FACT, limit=100000)),
            "device_facts_count": len(self.scan_all(SET_DEVICE_FACT, limit=100000)),
            "transaction_records_count": len(self.scan_all(SET_TRANSACTIONS, limit=100000)),
            "pending_review": len(self.get_users_by_workflow_status("pending_review")),
            "under_investigation": len(self.get_users_by_workflow_status("under_investigation")),
            "confirmed_fraud": len(self.get_users_by_workflow_status("confirmed_fraud")),
            "cleared": len(self.get_users_by_workflow_status("cleared")),
            "connected": self.is_connected()
        }
    
    def truncate_all_data(self) -> Dict[str, bool]:
        """Truncate all data sets (for fresh bulk load)."""
        return {
            "users": self.truncate_set(SET_USERS),
            "flagged_accounts": self.truncate_set(SET_FLAGGED_ACCOUNTS),
            "account_facts": self.truncate_set(SET_ACCOUNT_FACT),
            "device_facts": self.truncate_set(SET_DEVICE_FACT),
            "transactions": self.truncate_set(SET_TRANSACTIONS),
            "evaluations": self.truncate_set(SET_EVALUATIONS),
            "history": self.truncate_set(SET_HISTORY),
        }


# Singleton instance
aerospike_service = AerospikeService()
