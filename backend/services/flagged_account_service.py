"""
Flagged Account Detection Service

This service handles the detection and management of flagged accounts.
It uses pre-computed features from account-fact KV set and rule-based ML scoring.

Detection Flow:
1. Compute features for all accounts (via feature_service)
2. Score each account using account-fact features
3. Calculate user risk = max(account_risks)
4. Flag users above threshold
"""

import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from threading import Lock

from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P

from services.ml_service import ml_model_service
from services.graph_service import GraphService

logger = logging.getLogger('fraud_detection.flagged_accounts')

# Storage file paths (for persistence across restarts)
DATA_DIR = os.environ.get('DATA_DIR', '/tmp/fraud_detection')
FLAGGED_ACCOUNTS_FILE = os.path.join(DATA_DIR, 'flagged_accounts.json')
EVALUATIONS_FILE = os.path.join(DATA_DIR, 'account_evaluations.json')
CONFIG_FILE = os.path.join(DATA_DIR, 'detection_config.json')
HISTORY_FILE = os.path.join(DATA_DIR, 'detection_history.json')


class FlaggedAccountService:
    """
    Service for managing flagged account detection with cooldown support.
    Uses account-fact KV set for pre-computed features.
    Requires Aerospike KV storage for risk evaluation features.
    """
    
    def __init__(self, graph_service: GraphService):
        self.graph_service = graph_service
        self._lock = Lock()
        self._aerospike = None  # Will be set if Aerospike is available
        self._feature_service = None  # Will be set for feature computation
        
        # In-memory storage (used as fallback when Aerospike not available)
        self._flagged_accounts: Dict[str, Dict[str, Any]] = {}
        self._evaluations: Dict[str, Dict[str, Any]] = {}
        self._detection_history: List[Dict[str, Any]] = []
        
        # Default configuration
        self._config = {
            "schedule_enabled": True,
            "schedule_time": "21:30",
            "cooldown_days": 7,
            "risk_threshold": 50  # Lowered from 70 for demo data
        }
        
        # Ensure data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Load persisted data (file-based fallback)
        self._load_data()
    
    def set_aerospike_service(self, aerospike_service):
        """Set the Aerospike service for storage operations."""
        self._aerospike = aerospike_service
        logger.info("Aerospike service configured for flagged account storage")
    
    def set_feature_service(self, feature_service):
        """Set the feature service for computing account features."""
        self._feature_service = feature_service
        logger.info("Feature service configured for detection")
        
        # Load config from Aerospike if available
        if self._aerospike and self._aerospike.is_connected():
            stored_config = self._aerospike.get_config()
            if stored_config:
                self._config.update(stored_config)
                logger.info("Loaded config from Aerospike")
    
    def _use_aerospike(self) -> bool:
        """Check if we should use Aerospike for storage."""
        return self._aerospike is not None and self._aerospike.is_connected()
    
    def _load_data(self):
        """Load persisted data from files."""
        try:
            if os.path.exists(FLAGGED_ACCOUNTS_FILE):
                with open(FLAGGED_ACCOUNTS_FILE, 'r') as f:
                    self._flagged_accounts = json.load(f)
                logger.info(f"Loaded {len(self._flagged_accounts)} flagged accounts")
            
            if os.path.exists(EVALUATIONS_FILE):
                with open(EVALUATIONS_FILE, 'r') as f:
                    self._evaluations = json.load(f)
                logger.info(f"Loaded {len(self._evaluations)} evaluation records")
            
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    loaded_config = json.load(f)
                    self._config.update(loaded_config)
                logger.info("Loaded detection config")
            
            if os.path.exists(HISTORY_FILE):
                with open(HISTORY_FILE, 'r') as f:
                    self._detection_history = json.load(f)
                logger.info(f"Loaded {len(self._detection_history)} history records")
                
        except Exception as e:
            logger.error(f"Error loading persisted data: {e}")
    
    def _save_data(self):
        """Save data to files for persistence (and Aerospike if available)."""
        # Save to Aerospike if available
        if self._use_aerospike():
            try:
                # Save config
                self._aerospike.save_config(self._config)
                
                # Save flagged accounts
                for account_id, account_data in self._flagged_accounts.items():
                    self._aerospike.flag_account(account_data)
                
                # Save history
                for job in self._detection_history[-100:]:
                    self._aerospike.add_detection_history(job)
                    
            except Exception as e:
                logger.error(f"Error saving to Aerospike: {e}")
        
        # Always save to files as backup
        try:
            with open(FLAGGED_ACCOUNTS_FILE, 'w') as f:
                json.dump(self._flagged_accounts, f, indent=2)
            
            with open(EVALUATIONS_FILE, 'w') as f:
                json.dump(self._evaluations, f, indent=2)
            
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self._config, f, indent=2)
            
            with open(HISTORY_FILE, 'w') as f:
                json.dump(self._detection_history[-100:], f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving data to files: {e}")
    
    # ----------------------------------------------------------------------------------------------------------
    # Configuration Management
    # ----------------------------------------------------------------------------------------------------------
    
    def get_config(self) -> Dict[str, Any]:
        """Get current detection configuration."""
        return self._config.copy()
    
    def update_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Update detection configuration."""
        with self._lock:
            if "schedule_enabled" in config:
                self._config["schedule_enabled"] = bool(config["schedule_enabled"])
            if "schedule_time" in config:
                self._config["schedule_time"] = config["schedule_time"]
            if "cooldown_days" in config:
                self._config["cooldown_days"] = max(1, int(config["cooldown_days"]))
            if "risk_threshold" in config:
                self._config["risk_threshold"] = max(0, min(100, float(config["risk_threshold"])))
            
            self._save_data()
            return self._config.copy()
    
    # ----------------------------------------------------------------------------------------------------------
    # Cooldown Management
    # ----------------------------------------------------------------------------------------------------------
    
    def _is_in_cooldown(self, account_id: str) -> bool:
        """Check if an account is still in cooldown period."""
        if account_id not in self._evaluations:
            return False
        
        eval_record = self._evaluations[account_id]
        last_evaluated = datetime.fromisoformat(eval_record["last_evaluated"])
        cooldown_expiry = last_evaluated + timedelta(days=self._config["cooldown_days"])
        
        return datetime.now() < cooldown_expiry
    
    def _update_evaluation(self, account_id: str, risk_score: float):
        """Update evaluation record for an account."""
        now = datetime.now().isoformat()
        
        if account_id in self._evaluations:
            self._evaluations[account_id]["last_evaluated"] = now
            self._evaluations[account_id]["last_risk_score"] = risk_score
            self._evaluations[account_id]["evaluation_count"] += 1
        else:
            self._evaluations[account_id] = {
                "account_id": account_id,
                "last_evaluated": now,
                "last_risk_score": risk_score,
                "evaluation_count": 1
            }
    
    # ----------------------------------------------------------------------------------------------------------
    # Feature Extraction
    # ----------------------------------------------------------------------------------------------------------
    
    def _extract_account_features(self, account_id: str) -> Dict[str, Any]:
        """Extract features for an account from the graph database."""
        try:
            if not self.graph_service.client:
                return self._get_default_features()
            
            g = self.graph_service.client
            
            # Get account vertex
            account_exists = g.V().has("account", "account_id", account_id).has_next()
            if not account_exists:
                return self._get_default_features()
            
            # Get transaction count and amounts
            txn_stats = g.V().has("account", "account_id", account_id).outE("TRANSACTS").fold().project(
                "count", "total_amount"
            ).by(__.unfold().count()).by(
                __.unfold().values("amount").fold().coalesce(__.unfold().sum(), __.constant(0))
            ).next()
            
            txn_count = txn_stats.get("count", 0)
            total_amount = float(txn_stats.get("total_amount", 0))
            avg_amount = total_amount / txn_count if txn_count > 0 else 0
            
            # Get device count (via owner user)
            device_count = g.V().has("account", "account_id", account_id).in_("owns").out("uses").dedup().count().next()
            
            # Get unique recipients
            unique_recipients = g.V().has("account", "account_id", account_id).outE("TRANSACTS").inV().dedup().count().next()
            
            # Get connections to flagged accounts
            flagged_connections = g.V().has("account", "account_id", account_id).both("TRANSACTS").has("fraud_flag", True).dedup().count().next()
            
            # Get high value transaction count (> $5000)
            high_value_txn_count = g.V().has("account", "account_id", account_id).outE("TRANSACTS").has("amount", P.gt(5000)).count().next()
            
            # Get account age (if created_at exists)
            try:
                created_at = g.V().has("account", "account_id", account_id).values("created_at").next()
                account_age_days = (datetime.now() - datetime.fromisoformat(str(created_at))).days
            except:
                account_age_days = 365  # Default to 1 year if not available
            
            return {
                "transaction_count": txn_count,
                "total_amount": total_amount,
                "avg_amount": avg_amount,
                "device_count": device_count,
                "unique_recipients": unique_recipients,
                "flagged_connections": flagged_connections,
                "account_age_days": account_age_days,
                "high_value_txn_count": high_value_txn_count
            }
            
        except Exception as e:
            logger.error(f"Error extracting features for account {account_id}: {e}")
            return self._get_default_features()
    
    def _get_default_features(self) -> Dict[str, Any]:
        """Return default features when extraction fails."""
        return {
            "transaction_count": 0,
            "total_amount": 0,
            "avg_amount": 0,
            "device_count": 1,
            "unique_recipients": 0,
            "flagged_connections": 0,
            "account_age_days": 365,
            "high_value_txn_count": 0
        }
    
    # ----------------------------------------------------------------------------------------------------------
    # Detection Job
    # ----------------------------------------------------------------------------------------------------------
    
    def run_detection(self, skip_cooldown: bool = False, compute_features: bool = True) -> Dict[str, Any]:
        """
        Run the flagged account detection job using account-level scoring.
        
        This method:
        1. Optionally computes features for all accounts (via feature_service)
        2. Gets users from Aerospike (respecting cooldown unless skip_cooldown=True)
        3. For each user, gets their accounts' features from account-fact
        4. Scores each account using ML rules
        5. User risk = max(account_risks)
        6. Flags users above the risk threshold
        7. Updates evaluation records and device flags
        
        Args:
            skip_cooldown: If True, evaluate all users regardless of cooldown period
            compute_features: If True, run feature computation before detection
        
        Requires Aerospike KV to be connected.
        """
        start_time = datetime.now()
        
        # Check if Aerospike is available - required for detection
        if not self._use_aerospike():
            return {
                "job_id": f"detection_{start_time.strftime('%Y%m%d_%H%M%S')}",
                "start_time": start_time.isoformat(),
                "end_time": start_time.isoformat(),
                "status": "failed",
                "error": "Aerospike KV service is not available. Risk evaluation requires Aerospike to be connected.",
                "accounts_evaluated": 0,
                "accounts_skipped_cooldown": 0,
                "newly_flagged": 0,
                "total_accounts": 0
            }
        
        job_result = {
            "job_id": f"detection_{start_time.strftime('%Y%m%d_%H%M%S')}",
            "start_time": start_time.isoformat(),
            "status": "running",
            "accounts_evaluated": 0,
            "users_evaluated": 0,
            "accounts_skipped_cooldown": 0,
            "newly_flagged": 0,
            "total_users": 0,
            "feature_computation": None,
            "errors": [],
            "source": "account-fact"
        }
        
        try:
            with self._lock:
                # Step 1: Optionally compute features first
                if compute_features and self._feature_service:
                    logger.info("Computing account and device features...")
                    feature_result = self._feature_service.run_feature_computation_job(
                        window_days=self._config["cooldown_days"]
                    )
                    job_result["feature_computation"] = {
                        "accounts_processed": feature_result.get("accounts_processed", 0),
                        "devices_processed": feature_result.get("devices_processed", 0),
                    }
                
                # Step 2: Get users from Aerospike for evaluation
                effective_cooldown = 0 if skip_cooldown else self._config["cooldown_days"]
                all_users = self._aerospike.get_users_for_evaluation(
                    cooldown_days=effective_cooldown,
                    limit=10000
                )
                total_users = len(self._aerospike.get_all_users(limit=100000))
                job_result["total_users"] = total_users
                job_result["accounts_skipped_cooldown"] = total_users - len(all_users) if not skip_cooldown else 0
                
                cooldown_msg = " (cooldown skipped)" if skip_cooldown else ""
                logger.info(f"Starting detection job for {len(all_users)} users{cooldown_msg}")
                
                # Step 3: Evaluate each user
                for user in all_users:
                    user_id = user.get("user_id")
                    if not user_id:
                        continue
                    
                    try:
                        # Get user's accounts from the nested accounts map
                        accounts_map = user.get("accounts", {})
                        if not accounts_map:
                            # Fallback: try to get from graph
                            accounts_map = self._get_user_accounts_from_graph(user_id)
                        
                        if not accounts_map:
                            continue
                        
                        # Score each account using account-fact features
                        account_predictions = []
                        for account_id in accounts_map.keys():
                            # Get pre-computed features from account-fact
                            account_fact = self._aerospike.get_account_fact(account_id)
                            
                            if account_fact:
                                # Score using ML rules
                                prediction = ml_model_service.predict_account_risk(account_fact)
                                prediction["account_id"] = account_id
                                account_predictions.append(prediction)
                                
                                # Update account-fact with risk score
                                account_fact["risk_score"] = prediction["risk_score"]
                                self._aerospike.update_account_fact(account_id, account_fact)
                                
                                job_result["accounts_evaluated"] += 1
                            else:
                                # No features computed yet - use legacy extraction
                                features = self._extract_user_features(user_id, user)
                                prediction = ml_model_service.predict_risk(features)
                                prediction["account_id"] = account_id
                                account_predictions.append(prediction)
                                job_result["accounts_evaluated"] += 1
                        
                        # User risk = max(account_risks)
                        if account_predictions:
                            user_prediction = ml_model_service.predict_user_risk(account_predictions)
                            risk_score = user_prediction["risk_score"]
                            
                            # Update user evaluation in Aerospike
                            self._aerospike.update_user_evaluation(user_id, risk_score)
                            job_result["users_evaluated"] += 1
                            
                            # Flag if above threshold
                            if risk_score >= self._config["risk_threshold"]:
                                self._flag_user_with_accounts(user, user_prediction, account_predictions)
                                job_result["newly_flagged"] += 1
                            
                    except Exception as e:
                        logger.error(f"Error evaluating user {user_id}: {e}")
                        job_result["errors"].append(f"User {user_id}: {str(e)}")
                
                # Step 4: Update device flags based on computed device-facts
                self._update_device_flags()
                
                # Update job result
                end_time = datetime.now()
                job_result["end_time"] = end_time.isoformat()
                job_result["duration_seconds"] = (end_time - start_time).total_seconds()
                job_result["status"] = "completed"
                
                # Add to history
                self._detection_history.append(job_result)
                
                # Save data
                self._save_data()
                
                logger.info(f"Detection job completed: users={job_result['users_evaluated']}, "
                           f"accounts={job_result['accounts_evaluated']}, "
                           f"flagged={job_result['newly_flagged']}")
                
                return job_result
                
        except Exception as e:
            logger.error(f"Detection job failed: {e}")
            job_result["status"] = "failed"
            job_result["error"] = str(e)
            job_result["end_time"] = datetime.now().isoformat()
            self._detection_history.append(job_result)
            self._save_data()
            return job_result
    
    def _get_user_accounts_from_graph(self, user_id: str) -> Dict[str, Dict]:
        """Get user's accounts from graph if not in KV."""
        if not self.graph_service or not self.graph_service.client:
            return {}
        
        try:
            g = self.graph_service.client
            account_ids = g.V(user_id).out("OWNS").id_().toList()
            return {aid: {} for aid in account_ids}
        except:
            return {}
    
    def _flag_user_with_accounts(self, user: Dict, user_prediction: Dict, account_predictions: List[Dict]):
        """Flag a user with account-level details."""
        user_id = user.get("user_id")
        now = datetime.now().isoformat()
        
        # Find highest risk account
        highest_account = user_prediction.get("highest_risk_account", {})
        
        flagged_record = {
            "account_id": user_id,  # Keep backwards compatible naming
            "user_id": user_id,
            "account_holder": user.get("name", "Unknown"),
            "email": user.get("email", ""),
            "risk_score": user_prediction["risk_score"],
            "flag_reason": user_prediction.get("reason", ""),
            "risk_factors": user_prediction.get("risk_factors", []),
            "flagged_date": now,
            "status": "pending_review",
            "account_count": user_prediction.get("account_count", 0),
            "highest_risk_account_id": highest_account.get("account_id", ""),
            "account_predictions": [
                {"account_id": p.get("account_id"), "risk_score": p.get("risk_score")}
                for p in account_predictions
            ],
            "model_version": user_prediction.get("model_version", "unknown"),
            "confidence": user_prediction.get("confidence", 0),
        }
        
        # Store in Aerospike and local cache
        if self._use_aerospike():
            self._aerospike.flag_account(flagged_record)
        
        self._flagged_accounts[user_id] = flagged_record
        logger.info(f"Flagged user {user_id} with risk score {user_prediction['risk_score']} "
                   f"({len(account_predictions)} accounts evaluated)")
    
    def _update_device_flags(self):
        """Update device fraud flags based on device-fact features."""
        if not self._use_aerospike():
            return
        
        try:
            device_facts = self._aerospike.get_all_device_facts(limit=100000)
            
            for device_fact in device_facts:
                device_id = device_fact.get("device_id")
                if not device_id:
                    continue
                
                # Apply device flagging rules
                flagging = ml_model_service.evaluate_device_flagging(device_fact)
                
                # Update device-fact with watchlist/fraud status
                device_fact["watchlist"] = flagging["watchlist"]
                device_fact["fraud"] = flagging["fraud"]
                self._aerospike.update_device_fact(device_id, device_fact)
                
                # Update graph vertex fraud_flag if device is flagged
                if flagging["fraud"] and self.graph_service and self.graph_service.client:
                    try:
                        self.graph_service.client.V(device_id) \
                            .property("fraud_flag", True) \
                            .iterate()
                    except:
                        pass
                        
        except Exception as e:
            logger.warning(f"Error updating device flags: {e}")
    
    def _get_all_accounts(self) -> List[Dict[str, Any]]:
        """Get all accounts from the graph database."""
        try:
            if not self.graph_service.client:
                return []
            
            g = self.graph_service.client
            accounts = []
            
            # Get all account vertices with their properties
            account_vertices = g.V().has_label("account").limit(10000).to_list()
            
            for vertex in account_vertices:
                try:
                    props = g.V(vertex).value_map().next()
                    account = {
                        "account_id": props.get("account_id", [""])[0] if isinstance(props.get("account_id"), list) else props.get("account_id", ""),
                        "type": props.get("type", [""])[0] if isinstance(props.get("type"), list) else props.get("type", ""),
                        "balance": props.get("balance", [0])[0] if isinstance(props.get("balance"), list) else props.get("balance", 0),
                    }
                    
                    # Get owner user info
                    try:
                        owner = g.V(vertex).in_("owns").value_map().next()
                        account["user_id"] = owner.get("user_id", [""])[0] if isinstance(owner.get("user_id"), list) else owner.get("user_id", "")
                        account["account_holder"] = owner.get("name", ["Unknown"])[0] if isinstance(owner.get("name"), list) else owner.get("name", "Unknown")
                    except:
                        account["user_id"] = ""
                        account["account_holder"] = "Unknown"
                    
                    accounts.append(account)
                except Exception as e:
                    logger.warning(f"Error getting account properties: {e}")
                    continue
            
            return accounts
            
        except Exception as e:
            logger.error(f"Error getting all accounts: {e}")
            return []
    
    def _flag_account(self, account: Dict[str, Any], prediction: Dict[str, Any], features: Dict[str, Any]):
        """Flag an account as high risk."""
        account_id = account.get("account_id")
        now = datetime.now().isoformat()
        
        flagged_record = {
            "account_id": account_id,
            "user_id": account.get("user_id", ""),
            "account_holder": account.get("account_holder", "Unknown"),
            "account_type": account.get("type", ""),
            "risk_score": prediction["risk_score"],
            "flag_reason": prediction["reason"],
            "risk_factors": prediction.get("risk_factors", []),
            "flagged_date": now,
            "status": "pending_review",
            "features": features,
            "suspicious_transactions": features.get("transaction_count", 0),
            "total_flagged_amount": features.get("total_amount", 0),
            "model_version": prediction.get("model_version", "unknown"),
            "confidence": prediction.get("confidence", 0)
        }
        
        self._flagged_accounts[account_id] = flagged_record
        logger.info(f"Flagged account {account_id} with risk score {prediction['risk_score']}")
    
    def _extract_user_features(self, user_id: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features for a user from their Aerospike data and graph relationships."""
        try:
            # Get the user's pre-existing risk score from their profile
            base_risk_score = user_data.get("risk_score", 0)
            if base_risk_score is None:
                base_risk_score = 0
            
            # Start with user's stored data
            features = {
                "transaction_count": 0,
                "total_amount": 0,
                "avg_amount": 0,
                "device_count": 1,
                "unique_recipients": 0,
                "flagged_connections": 0,
                "account_age_days": 365,
                "high_value_txn_count": 0,
                "base_risk_score": float(base_risk_score)  # Pass profile risk to ML model
            }
            
            # Calculate account age from signup date
            signup_date = user_data.get("signup_date", "")
            if signup_date:
                try:
                    signup = datetime.fromisoformat(signup_date.replace("Z", "+00:00"))
                    features["account_age_days"] = (datetime.now(signup.tzinfo) - signup).days
                except:
                    pass
            
            # Get additional features from graph if available
            if self.graph_service.client:
                g = self.graph_service.client
                
                try:
                    # Get user's accounts IDs (not Vertex objects)
                    # Note: user_id is the actual vertex ID (e.g., "U0001")
                    account_ids = g.V(user_id).out("OWNS").id_().toList()
                    
                    for acc_id in account_ids:
                        # Get transaction counts
                        out_count = g.V(acc_id).outE("TRANSACTS").count().next()
                        in_count = g.V(acc_id).inE("TRANSACTS").count().next()
                        features["transaction_count"] += out_count + in_count
                        
                        # Get transaction amounts using fold() (sum() doesn't work with Aerospike Graph)
                        try:
                            out_amounts = g.V(acc_id).outE("TRANSACTS").values("amount").fold().next()
                            features["total_amount"] += sum(float(a) for a in out_amounts) if out_amounts else 0
                        except Exception as e:
                            logger.debug(f"Error getting out amounts for {acc_id}: {e}")
                        try:
                            in_amounts = g.V(acc_id).inE("TRANSACTS").values("amount").fold().next()
                            features["total_amount"] += sum(float(a) for a in in_amounts) if in_amounts else 0
                        except Exception as e:
                            logger.debug(f"Error getting in amounts for {acc_id}: {e}")
                        
                        # Count high-value transactions (> $10,000)
                        try:
                            all_amounts = g.V(acc_id).bothE("TRANSACTS").values("amount").fold().next()
                            features["high_value_txn_count"] += sum(1 for a in all_amounts if float(a) > 10000)
                        except:
                            pass
                    
                    # Calculate average
                    if features["transaction_count"] > 0:
                        features["avg_amount"] = features["total_amount"] / features["transaction_count"]
                    
                    # Get device count
                    features["device_count"] = g.V(user_id).out("USES").count().next()
                    
                    # Count unique recipients (accounts this user sent money to)
                    try:
                        recipients = set()
                        for acc_id in account_ids:
                            recipient_ids = g.V(acc_id).outE("TRANSACTS").inV().id_().toList()
                            recipients.update(recipient_ids)
                        features["unique_recipients"] = len(recipients)
                    except:
                        pass
                    
                except Exception as e:
                    logger.warning(f"Error getting graph features for user {user_id}: {e}")
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting features for user {user_id}: {e}")
            return self._get_default_features()
    
    def _flag_user(self, user: Dict[str, Any], prediction: Dict[str, Any], features: Dict[str, Any]):
        """Flag a user as high risk (stores in both Aerospike and local cache)."""
        user_id = user.get("user_id")
        now = datetime.now().isoformat()
        
        flagged_record = {
            "account_id": user_id,  # Use user_id as account_id for compatibility
            "user_id": user_id,
            "account_holder": user.get("name", "Unknown"),
            "email": user.get("email", ""),
            "account_type": "user",
            "risk_score": prediction["risk_score"],
            "flag_reason": prediction["reason"],
            "risk_factors": prediction.get("risk_factors", []),
            "flagged_date": now,
            "status": "pending_review",
            "features": features,
            "suspicious_transactions": features.get("transaction_count", 0),
            "total_flagged_amount": features.get("total_amount", 0),
            "model_version": prediction.get("model_version", "unknown"),
            "confidence": prediction.get("confidence", 0)
        }
        
        # Store in local cache
        self._flagged_accounts[user_id] = flagged_record
        
        # Also store in Aerospike if available
        if self._use_aerospike():
            self._aerospike.flag_account(flagged_record)
            # Update user's workflow status
            self._aerospike.update_workflow_status(user_id, "pending_review")
        
        logger.info(f"Flagged user {user_id} with risk score {prediction['risk_score']}")
    
    # ----------------------------------------------------------------------------------------------------------
    # Flagged Accounts Management
    # ----------------------------------------------------------------------------------------------------------
    
    def get_flagged_accounts(
        self, 
        page: int = 1, 
        page_size: int = 20,
        status: Optional[str] = None,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get paginated list of flagged accounts."""
        # Get from Aerospike if available
        if self._use_aerospike():
            accounts = self._aerospike.get_all_flagged_accounts(limit=10000)
        else:
            accounts = list(self._flagged_accounts.values())
        
        # Filter by status
        if status and status != "all":
            accounts = [a for a in accounts if a.get("status") == status]
        
        # Filter by search query
        if search:
            search_lower = search.lower()
            accounts = [
                a for a in accounts 
                if search_lower in a.get("account_holder", "").lower() or
                   search_lower in a.get("account_id", "").lower() or
                   search_lower in a.get("user_id", "").lower()
            ]
        
        # Sort by risk score (highest first)
        accounts.sort(key=lambda x: x.get("risk_score", 0), reverse=True)
        
        # Paginate
        total = len(accounts)
        start = (page - 1) * page_size
        end = start + page_size
        paginated = accounts[start:end]
        
        return {
            "accounts": paginated,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "source": "aerospike" if self._use_aerospike() else "local"
        }
    
    def get_flagged_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific flagged account."""
        if self._use_aerospike():
            return self._aerospike.get_flagged_account(account_id)
        return None
    
    def resolve_flagged_account(self, account_id: str, resolution: str, notes: str = "") -> Optional[Dict[str, Any]]:
        """
        Resolve a flagged account.
        
        Args:
            account_id: The account ID
            resolution: "confirmed_fraud" or "cleared"
            notes: Optional notes about the resolution
        """
        if not self._use_aerospike():
            return None
        
        account = self._aerospike.get_flagged_account(account_id)
        if not account:
            return None
        
        with self._lock:
            updates = {
                "status": resolution,
                "resolution": resolution,
                "resolution_date": datetime.now().isoformat(),
                "resolution_notes": notes,
                "resolved_by": "analyst@demo.com"
            }
            
            if self._aerospike.update_flagged_account(account_id, updates):
                return self._aerospike.get_flagged_account(account_id)
            return None
    
    def get_flagged_stats(self) -> Dict[str, Any]:
        """Get statistics for flagged accounts."""
        if self._use_aerospike():
            accounts = self._aerospike.get_all_flagged_accounts(limit=10000)
        else:
            return {
                "total_flagged": 0,
                "pending_review": 0,
                "under_investigation": 0,
                "confirmed_fraud": 0,
                "cleared": 0,
                "avg_risk_score": 0,
                "total_flagged_amount": 0
            }
        
        return {
            "total_flagged": len(accounts),
            "pending_review": len([a for a in accounts if a.get("status") == "pending_review"]),
            "under_investigation": len([a for a in accounts if a.get("status") == "under_investigation"]),
            "confirmed_fraud": len([a for a in accounts if a.get("status") == "confirmed_fraud"]),
            "cleared": len([a for a in accounts if a.get("status") == "cleared"]),
            "avg_risk_score": sum(a.get("risk_score", 0) for a in accounts) / len(accounts) if accounts else 0,
            "total_flagged_amount": sum(a.get("total_flagged_amount", 0) for a in accounts)
        }
    
    # ----------------------------------------------------------------------------------------------------------
    # History Management
    # ----------------------------------------------------------------------------------------------------------
    
    def get_detection_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get detection job history."""
        return sorted(
            self._detection_history[-limit:],
            key=lambda x: x.get("start_time", ""),
            reverse=True
        )
    
    def clear_flagged_accounts(self):
        """Clear all flagged accounts (for testing/demo purposes)."""
        with self._lock:
            if self._use_aerospike():
                self._aerospike.clear_all_flagged_accounts()
                logger.info("Cleared all flagged accounts from Aerospike")
