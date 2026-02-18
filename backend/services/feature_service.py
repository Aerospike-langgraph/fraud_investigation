"""
Feature Computation Service (KV-Only)

Computes features for accounts and devices entirely from Aerospike KV store.
No Graph DB queries required. All relationship data is derived from user records.

Uses a configurable sliding window (default 7 days) for cooldown compatibility.

Account Features (15):
  A. Transaction behavior: txn_out_count, txn_24h_peak, avg_txn_per_day, max_txn_per_hour, txn_zscore
  B. Amount behavior: total_out_amount, avg_out_amount, max_out_amount, amount_zscore
  C. Counterparty spread: unique_recipients, new_recipient_ratio, recipient_entropy
  D. Device exposure: device_count, shared_device_account_count
  E. Lifecycle: account_age_days, first_txn_delay_days

Device Features (5):
  shared_account_count, flagged_account_count, avg_account_risk, max_account_risk, new_account_rate

DB Call Summary (for full job):
  1. kv.scan_all(users)                    - 1 scan
  2. kv.batch_get_transactions(all_accts)  - 1 batch read
  3. kv.batch_get_account_facts(all_accts) - 1 batch read
  4. kv.batch_put(account_fact, all)       - 1 batch write
  5. kv.batch_put(device_fact, all)        - 1 batch write
  Total: 5 KV calls, 0 Graph queries
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set
from collections import defaultdict

from services.progress_service import progress_service

logger = logging.getLogger('fraud_detection.features')


class FeatureService:
    """
    Computes account and device features from KV transaction data.
    Uses batch operations for efficiency. No Graph DB dependency.
    """
    
    # Class-level operation ID for progress tracking
    OPERATION_ID = "compute_features"
    
    def __init__(self, aerospike_service, graph_service=None):
        self.kv = aerospike_service
        self.graph = graph_service  # Kept for API compatibility but not used
        self.default_window_days = 7
    
    # ----------------------------------------------------------------------------------------------------------
    # Relationship Cache (built from KV user scan)
    # ----------------------------------------------------------------------------------------------------------
    
    def _build_relationship_cache(self, users: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Build in-memory relationship maps from a list of user records.
        Replaces all Graph DB queries with pure dict lookups.
        
        Args:
            users: List of user records from kv.get_all_users()
            
        Returns:
            Cache dict with:
              account_to_user:  {account_id -> user_id}
              user_to_accounts: {user_id -> [account_ids]}
              user_to_devices:  {user_id -> [device_ids]}
              device_to_users:  {device_id -> [user_ids]}
              account_created:  {account_id -> created_date_str}
              account_is_fraud: {account_id -> bool}
              all_account_ids:  set of all account IDs
              all_device_ids:   set of all device IDs
        """
        cache = {
            'account_to_user': {},
            'user_to_accounts': defaultdict(list),
            'user_to_devices': defaultdict(list),
            'device_to_users': defaultdict(list),
            'account_created': {},
            'account_is_fraud': {},
            'all_account_ids': set(),
            'all_device_ids': set(),
        }
        
        for user in users:
            user_id = user.get('user_id', '')
            if not user_id:
                continue
            
            # Process accounts
            accounts = user.get('accounts', {})
            if isinstance(accounts, dict):
                for account_id, account_data in accounts.items():
                    cache['account_to_user'][account_id] = user_id
                    cache['user_to_accounts'][user_id].append(account_id)
                    cache['all_account_ids'].add(account_id)
                    
                    # Extract created_date for lifecycle features
                    created = account_data.get('created_date', '') if isinstance(account_data, dict) else ''
                    if created:
                        cache['account_created'][account_id] = created
                    
                    # Extract is_fraud flag
                    is_fraud = account_data.get('is_fraud', False) if isinstance(account_data, dict) else False
                    cache['account_is_fraud'][account_id] = is_fraud
            
            # Process devices
            devices = user.get('devices', {})
            if isinstance(devices, dict):
                for device_id in devices.keys():
                    cache['user_to_devices'][user_id].append(device_id)
                    cache['device_to_users'][device_id].append(user_id)
                    cache['all_device_ids'].add(device_id)
        
        logger.info(
            f"Built relationship cache: {len(cache['all_account_ids'])} accounts, "
            f"{len(cache['all_device_ids'])} devices, "
            f"{len(cache['account_to_user'])} account->user mappings, "
            f"{len(cache['device_to_users'])} device->user mappings"
        )
        
        return cache
    
    # ----------------------------------------------------------------------------------------------------------
    # Cached Lookup Methods (replace Graph queries)
    # ----------------------------------------------------------------------------------------------------------
    
    def _get_device_exposure_cached(self, account_id: str, cache: Dict) -> tuple:
        """
        Get device count and shared device account count using KV cache.
        Zero DB calls — pure dict lookups.
        
        Replaces _get_device_exposure() which made 3+ Gremlin queries per account.
        """
        user_id = cache['account_to_user'].get(account_id)
        if not user_id:
            return (1, 0)
        
        devices = cache['user_to_devices'].get(user_id, [])
        device_count = max(len(devices), 1)
        
        # Find other accounts sharing the same devices
        shared_accounts = set()
        for did in devices:
            for uid in cache['device_to_users'].get(did, []):
                if uid != user_id:
                    shared_accounts.update(cache['user_to_accounts'].get(uid, []))
        
        return (device_count, len(shared_accounts))
    
    def _get_lifecycle_cached(self, account_id: str, transactions: List[Dict], cache: Dict) -> tuple:
        """
        Get account age and first transaction delay using KV cache.
        Zero DB calls — reads created_date from cache.
        
        Replaces _get_lifecycle_features() which made 1 Gremlin query per account.
        """
        account_age_days = 365  # Default
        first_txn_delay = 0
        
        created_str = cache['account_created'].get(account_id)
        if created_str:
            try:
                created = datetime.fromisoformat(str(created_str).replace('Z', '+00:00'))
                account_age_days = (datetime.now(created.tzinfo) - created).days
                
                # Find first transaction timestamp
                if transactions:
                    timestamps = [t.get('timestamp', '') for t in transactions if t.get('timestamp')]
                    if timestamps:
                        first_txn_ts = min(timestamps)
                        first_txn = datetime.fromisoformat(first_txn_ts.replace('Z', '+00:00'))
                        first_txn_delay = (first_txn - created).days
            except Exception:
                pass
        
        return (account_age_days, first_txn_delay)
    
    def _get_device_accounts_cached(self, device_id: str, cache: Dict) -> Set[str]:
        """
        Get all accounts connected to a device using KV cache.
        Zero DB calls.
        
        Replaces _get_device_accounts() which made 2+ Gremlin queries per device.
        """
        accounts = set()
        for uid in cache['device_to_users'].get(device_id, []):
            accounts.update(cache['user_to_accounts'].get(uid, []))
        return accounts
    
    # ----------------------------------------------------------------------------------------------------------
    # Account Feature Computation
    # ----------------------------------------------------------------------------------------------------------
    
    def compute_account_features(self, account_id: str, window_days: int = None) -> Dict[str, Any]:
        """
        Compute all 15 account features for a single account.
        Falls back to individual KV reads when no cache is available.
        
        Args:
            account_id: The account ID
            window_days: Sliding window in days (default: 7)
            
        Returns:
            Dict of computed features
        """
        window_days = window_days or self.default_window_days
        
        # Get transactions from KV
        transactions = self.kv.get_transactions_for_account(account_id, days=window_days)
        
        # Get historical stats for z-score calculation
        existing_fact = self.kv.get_account_fact(account_id) or {}
        
        return self._compute_features_from_data(
            account_id, transactions, existing_fact, window_days
        )
    
    def _compute_features_from_data(self, account_id: str, transactions: List[Dict], 
                                     existing_fact: Dict, window_days: int,
                                     cache: Dict = None) -> Dict[str, Any]:
        """
        Compute features from pre-fetched transaction data.
        
        Args:
            account_id: The account ID
            transactions: Pre-fetched transactions
            existing_fact: Pre-fetched existing account fact (historical stats)
            window_days: Sliding window in days
            cache: Optional relationship cache. If provided, uses cached lookups
                   for device exposure and lifecycle (zero DB calls).
        """
        hist_txn_mean = existing_fact.get('hist_txn_mean', 0)
        hist_amt_mean = existing_fact.get('hist_amt_mean', 0)
        hist_amt_std = existing_fact.get('hist_amt_std', 1)
        
        out_txns = [t for t in transactions if t.get('direction') == 'out']
        
        # A. Transaction behavior
        txn_out_count = len(out_txns)
        txn_24h_peak = self._compute_24h_peak(out_txns)
        avg_txn_per_day = txn_out_count / window_days if window_days > 0 else 0
        max_txn_per_hour = self._compute_max_hourly(out_txns)
        txn_zscore = self._compute_zscore(txn_out_count, hist_txn_mean, max(1, hist_txn_mean * 0.5))
        
        # B. Amount behavior
        out_amounts = [t.get('amount', 0) for t in out_txns]
        total_out_amount = sum(out_amounts)
        avg_out_amount = total_out_amount / txn_out_count if txn_out_count > 0 else 0
        max_out_amount = max(out_amounts) if out_amounts else 0
        amount_zscore = self._compute_zscore(max_out_amount, hist_amt_mean, hist_amt_std)
        
        # C. Counterparty spread
        recipients = [t.get('counterparty') for t in out_txns if t.get('counterparty')]
        unique_recipients = len(set(recipients))
        new_recipient_ratio = self._compute_new_recipient_ratio(account_id, recipients, existing_fact)
        recipient_entropy = self._compute_entropy(recipients)
        
        # D. Device exposure — use cache if available, else fallback
        if cache:
            device_count, shared_device_account_count = self._get_device_exposure_cached(account_id, cache)
        else:
            device_count, shared_device_account_count = (1, 0)
        
        # E. Lifecycle — use cache if available, else fallback
        if cache:
            account_age_days, first_txn_delay = self._get_lifecycle_cached(account_id, transactions, cache)
        else:
            account_age_days, first_txn_delay = (365, 0)
        
        # Update historical means (exponential moving average)
        new_hist_txn_mean = (hist_txn_mean * 0.9 + txn_out_count * 0.1) if hist_txn_mean > 0 else txn_out_count
        new_hist_amt_mean = (hist_amt_mean * 0.9 + avg_out_amount * 0.1) if hist_amt_mean > 0 else avg_out_amount
        new_hist_amt_std = max(1, abs(avg_out_amount - new_hist_amt_mean)) if hist_amt_std <= 1 else \
            math.sqrt(0.9 * hist_amt_std**2 + 0.1 * (avg_out_amount - new_hist_amt_mean)**2)
        
        return {
            'txn_out_7d': txn_out_count,
            'txn_24h_peak': txn_24h_peak,
            'avg_txn_day': round(avg_txn_per_day, 2),
            'max_txn_hr': max_txn_per_hour,
            'txn_zscore': round(txn_zscore, 2),
            'out_amt_7d': round(total_out_amount, 2),
            'avg_out_amt': round(avg_out_amount, 2),
            'max_out_amt': round(max_out_amount, 2),
            'amt_zscore': round(amount_zscore, 2),
            'uniq_recip': unique_recipients,
            'new_recip_rat': round(new_recipient_ratio, 2),
            'recip_entropy': round(recipient_entropy, 2),
            'dev_count': device_count,
            'shared_dev_ct': shared_device_account_count,
            'acct_age_days': account_age_days,
            'first_txn_dly': first_txn_delay,
            'hist_txn_mean': round(new_hist_txn_mean, 2),
            'hist_amt_mean': round(new_hist_amt_mean, 2),
            'hist_amt_std': round(new_hist_amt_std, 2),
        }
    
    # ----------------------------------------------------------------------------------------------------------
    # Device Feature Computation (KV-only)
    # ----------------------------------------------------------------------------------------------------------
    
    def _compute_device_features_cached(self, device_id: str, cache: Dict,
                                         account_facts: Dict[str, Dict[str, Any]],
                                         window_days: int) -> Dict[str, Any]:
        """
        Compute all 5 device features using pre-fetched cache and account facts.
        Zero DB calls — pure dict lookups.
        
        Args:
            device_id: The device ID
            cache: Relationship cache from _build_relationship_cache()
            account_facts: Pre-computed account facts (from the account features phase)
            window_days: Sliding window in days
            
        Returns:
            Dict of computed features
        """
        # Get connected accounts from cache
        connected_accounts = self._get_device_accounts_cached(device_id, cache)
        
        shared_account_count = len(connected_accounts)
        
        risk_scores = []
        flagged_count = 0
        new_account_count = 0
        
        for account_id in connected_accounts:
            # Use the freshly-computed account facts (already in memory)
            fact = account_facts.get(account_id)
            if fact:
                risk = fact.get('risk_score', 0)
                if risk is not None:
                    risk_scores.append(risk)
                # Check is_fraud from the cache (user record data)
                if cache['account_is_fraud'].get(account_id, False):
                    flagged_count += 1
                # Check if account is new (< window_days old)
                age = fact.get('acct_age_days', 365)
                if age is not None and age < window_days:
                    new_account_count += 1
        
        avg_account_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0
        max_account_risk = max(risk_scores) if risk_scores else 0
        
        return {
            'shared_acct_ct': shared_account_count,
            'flag_acct_ct': flagged_count,
            'avg_acct_risk': round(avg_account_risk, 2),
            'max_acct_risk': round(max_account_risk, 2),
            'new_acct_7d': new_account_count,
            'fraud': False,  # Will be set by device flagging rules
            'watchlist': False,
        }
    
    # ----------------------------------------------------------------------------------------------------------
    # Standalone methods (for single-entity use outside batch job)
    # ----------------------------------------------------------------------------------------------------------
    
    def compute_device_features(self, device_id: str, window_days: int = None) -> Dict[str, Any]:
        """
        Compute device features for a single device (standalone, outside batch job).
        Falls back to reading account facts from KV individually.
        """
        window_days = window_days or self.default_window_days
        
        # Without cache, we need to scan users to find device connections
        # This is slow but only used for single-device lookups outside the batch job
        users = self.kv.get_all_users(limit=100000)
        cache = self._build_relationship_cache(users)
        
        connected_accounts = self._get_device_accounts_cached(device_id, cache)
        account_facts = self.kv.batch_get_account_facts(list(connected_accounts))
        
        return self._compute_device_features_cached(device_id, cache, account_facts, window_days)
    
    def batch_compute_account_features(self, account_ids: List[str], window_days: int = None,
                                        cache: Dict = None) -> Dict[str, Dict[str, Any]]:
        """
        Compute features for multiple accounts using batch KV operations.
        
        Args:
            account_ids: List of account IDs
            window_days: Sliding window in days
            cache: Optional relationship cache for device/lifecycle features
            
        Returns:
            Dict mapping account_id to features
        """
        window_days = window_days or self.default_window_days
        
        # Batch read transactions for all accounts
        all_transactions = self.kv.batch_get_transactions(account_ids, days=window_days)
        
        # Batch read existing facts
        existing_facts = self.kv.batch_get_account_facts(account_ids)
        
        results = {}
        for account_id in account_ids:
            transactions = all_transactions.get(account_id, [])
            existing_fact = existing_facts.get(account_id) or {}
            
            results[account_id] = self._compute_features_from_data(
                account_id, transactions, existing_fact, window_days, cache=cache
            )
        
        return results
    
    # ----------------------------------------------------------------------------------------------------------
    # Helper Methods (pure computation, no DB calls)
    # ----------------------------------------------------------------------------------------------------------
    
    def _compute_24h_peak(self, transactions: List[Dict]) -> int:
        """Compute max transactions in any 24h window."""
        if not transactions:
            return 0
        
        daily_counts = defaultdict(int)
        for txn in transactions:
            ts = txn.get('timestamp', '')
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    day_key = dt.strftime('%Y-%m-%d')
                    daily_counts[day_key] += 1
                except Exception:
                    pass
        
        return max(daily_counts.values()) if daily_counts else 0
    
    def _compute_max_hourly(self, transactions: List[Dict]) -> int:
        """Compute max transactions in any single hour."""
        if not transactions:
            return 0
        
        hourly_counts = defaultdict(int)
        for txn in transactions:
            ts = txn.get('timestamp', '')
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    hour_key = dt.strftime('%Y-%m-%d-%H')
                    hourly_counts[hour_key] += 1
                except Exception:
                    pass
        
        return max(hourly_counts.values()) if hourly_counts else 0
    
    def _compute_zscore(self, value: float, mean: float, std: float) -> float:
        """Compute z-score."""
        if std <= 0:
            return 0
        return (value - mean) / std
    
    def _compute_entropy(self, items: List[str]) -> float:
        """Compute Shannon entropy of a distribution."""
        if not items:
            return 0
        
        counts = defaultdict(int)
        for item in items:
            counts[item] += 1
        
        total = len(items)
        entropy = 0
        for count in counts.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log2(p)
        
        return entropy
    
    def _compute_new_recipient_ratio(self, account_id: str, current_recipients: List[str], 
                                      existing_fact: Dict) -> float:
        """Compute ratio of new recipients vs total unique recipients."""
        if not current_recipients:
            return 0
        
        known_recipients = set(existing_fact.get('known_recipients', []))
        current_unique = set(current_recipients)
        
        if not current_unique:
            return 0
        
        new_recipients = current_unique - known_recipients
        return len(new_recipients) / len(current_unique)
    
    # ----------------------------------------------------------------------------------------------------------
    # Batch Feature Update Job (KV-Only)
    # ----------------------------------------------------------------------------------------------------------
    
    def run_feature_computation_job(self, window_days: int = None) -> Dict[str, Any]:
        """
        Run a batch job to compute and update features for all accounts and devices.
        Entirely KV-based: 1 scan + 2 batch reads + 2 batch writes = 5 total DB calls.
        
        Flow:
          1. Scan all users -> build relationship cache
          2. Batch read transactions for all accounts
          3. Batch read existing account facts
          4. Compute 15 features per account (pure Python)
          5. Batch write all account facts
          6. Compute 5 features per device (pure Python, using cache + account facts)
          7. Batch write all device facts
        
        Args:
            window_days: Sliding window in days
            
        Returns:
            Job result with counts
        """
        window_days = window_days or self.default_window_days
        start_time = datetime.now()
        
        result = {
            "job_id": f"features_{start_time.strftime('%Y%m%d_%H%M%S')}",
            "start_time": start_time.isoformat(),
            "accounts_processed": 0,
            "devices_processed": 0,
            "errors": []
        }
        
        try:
            # ================================================================
            # Step 1: Scan all users and build relationship cache
            # ================================================================
            progress_service.start_operation(self.OPERATION_ID, 100, "Scanning users...")
            
            users = self.kv.get_all_users(limit=100000)
            cache = self._build_relationship_cache(users)
            
            all_account_ids = cache['all_account_ids']
            all_device_ids = cache['all_device_ids']
            
            total_items = len(all_account_ids) + len(all_device_ids)
            progress_service.start_operation(
                self.OPERATION_ID, 
                total_items, 
                f"Computing features for {len(all_account_ids)} accounts and {len(all_device_ids)} devices"
            )
            
            logger.info(f"Computing features for {len(all_account_ids)} accounts and {len(all_device_ids)} devices")
            
            # ================================================================
            # Step 2 & 3: Batch read transactions + existing facts for ALL accounts
            # ================================================================
            account_list = list(all_account_ids)
            
            progress_service.update_progress(
                self.OPERATION_ID, 0,
                f"Batch reading transactions for {len(account_list)} accounts..."
            )
            
            logger.info(f"Batch reading transactions for {len(account_list)} accounts (window={window_days}d)...")
            all_transactions = self.kv.batch_get_transactions(account_list, days=window_days)
            
            logger.info(f"Batch reading existing account facts for {len(account_list)} accounts...")
            existing_facts = self.kv.batch_get_account_facts(account_list)
            
            # ================================================================
            # Step 4: Compute 15 features per account (pure Python)
            # ================================================================
            progress_service.update_progress(
                self.OPERATION_ID, 0,
                f"Computing account features..."
            )
            
            all_account_features = {}
            for i, account_id in enumerate(account_list):
                try:
                    transactions = all_transactions.get(account_id, [])
                    existing_fact = existing_facts.get(account_id) or {}
                    
                    features = self._compute_features_from_data(
                        account_id, transactions, existing_fact, window_days, cache=cache
                    )
                    all_account_features[account_id] = features
                except Exception as e:
                    result["errors"].append(f"Account {account_id}: {str(e)}")
                
                # Update progress every 500 accounts
                if (i + 1) % 500 == 0:
                    progress_service.update_progress(
                        self.OPERATION_ID, i + 1,
                        f"Account features: {i + 1}/{len(account_list)}"
                    )
            
            logger.info(f"Computed features for {len(all_account_features)} accounts")
            
            # ================================================================
            # Step 5: Batch write ALL account features in one call
            # ================================================================
            progress_service.update_progress(
                self.OPERATION_ID, len(account_list),
                f"Batch writing {len(all_account_features)} account facts..."
            )
            
            now_ts = datetime.now().isoformat()
            account_records = []
            for account_id, features in all_account_features.items():
                features['account_id'] = account_id
                features['last_computed'] = now_ts
                account_records.append((account_id, features))
            
            if account_records:
                batch_result = self.kv.batch_put("account_fact", account_records)
                result["accounts_processed"] = batch_result.get("success", 0)
                failed = batch_result.get("failed", 0)
                if failed > 0:
                    result["errors"].append(f"Account batch_put: {failed} failed")
                logger.info(f"Batch wrote account facts: {result['accounts_processed']} success, {failed} failed")
            
            # ================================================================
            # Step 6: Compute 5 features per device (pure Python, using cache)
            # ================================================================
            progress_service.update_progress(
                self.OPERATION_ID,
                len(account_list),
                f"Computing device features for {len(all_device_ids)} devices..."
            )
            
            all_device_features = {}
            device_list = list(all_device_ids)
            
            for i, device_id in enumerate(device_list):
                try:
                    features = self._compute_device_features_cached(
                        device_id, cache, all_account_features, window_days
                    )
                    all_device_features[device_id] = features
                except Exception as e:
                    result["errors"].append(f"Device {device_id}: {str(e)}")
                
                if (i + 1) % 500 == 0:
                    progress_service.update_progress(
                        self.OPERATION_ID,
                        len(account_list) + i + 1,
                        f"Device features: {i + 1}/{len(device_list)}"
                    )
            
            logger.info(f"Computed features for {len(all_device_features)} devices")
            
            # ================================================================
            # Step 7: Batch write ALL device features in one call
            # ================================================================
            progress_service.update_progress(
                self.OPERATION_ID,
                len(account_list) + len(device_list),
                f"Batch writing {len(all_device_features)} device facts..."
            )
            
            device_records = []
            for device_id, features in all_device_features.items():
                features['device_id'] = device_id
                features['last_computed'] = now_ts
                device_records.append((device_id, features))
            
            if device_records:
                batch_result = self.kv.batch_put("device_fact", device_records)
                result["devices_processed"] = batch_result.get("success", 0)
                failed = batch_result.get("failed", 0)
                if failed > 0:
                    result["errors"].append(f"Device batch_put: {failed} failed")
                logger.info(f"Batch wrote device facts: {result['devices_processed']} success, {failed} failed")
            
            result["status"] = "completed"
            
            # Complete progress tracking
            duration = (datetime.now() - start_time).total_seconds()
            progress_service.complete_operation(
                self.OPERATION_ID,
                f"Completed in {duration:.1f}s! {result['accounts_processed']} accounts, {result['devices_processed']} devices",
                extra={
                    "accounts_processed": result["accounts_processed"],
                    "devices_processed": result["devices_processed"],
                    "duration_seconds": duration,
                }
            )
            
        except Exception as e:
            logger.error(f"Feature computation job failed: {e}", exc_info=True)
            result["status"] = "failed"
            result["error"] = str(e)
            progress_service.fail_operation(self.OPERATION_ID, str(e), "Feature computation failed")
        
        result["end_time"] = datetime.now().isoformat()
        result["duration_seconds"] = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Feature computation completed in {result['duration_seconds']:.1f}s: "
                    f"{result['accounts_processed']} accounts, {result['devices_processed']} devices")
        
        return result


# Factory function
def create_feature_service(aerospike_service, graph_service=None):
    """Create a FeatureService instance."""
    return FeatureService(aerospike_service, graph_service)
