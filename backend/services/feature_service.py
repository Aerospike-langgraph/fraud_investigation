"""
Feature Computation Service

Computes features for accounts and devices from KV transaction data.
All features use a configurable sliding window (default 7 days) for cooldown compatibility.

Account Features (15):
  A. Transaction behavior: txn_out_count, txn_24h_peak, avg_txn_per_day, max_txn_per_hour, txn_zscore
  B. Amount behavior: total_out_amount, avg_out_amount, max_out_amount, amount_zscore
  C. Counterparty spread: unique_recipients, new_recipient_ratio, recipient_entropy
  D. Device exposure: device_count, shared_device_account_count
  E. Lifecycle: account_age_days, first_txn_delay_days

Device Features (5):
  shared_account_count, flagged_account_count, avg_account_risk, max_account_risk, new_account_rate
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
    Uses batch operations for efficiency.
    """
    
    # Class-level operation ID for progress tracking
    OPERATION_ID = "compute_features"
    
    def __init__(self, aerospike_service, graph_service=None):
        self.kv = aerospike_service
        self.graph = graph_service  # Optional: for device relationships
        self.default_window_days = 7
    
    # ----------------------------------------------------------------------------------------------------------
    # Account Feature Computation
    # ----------------------------------------------------------------------------------------------------------
    
    def compute_account_features(self, account_id: str, window_days: int = None) -> Dict[str, Any]:
        """
        Compute all 15 account features from KV transaction data.
        
        Args:
            account_id: The account ID
            window_days: Sliding window in days (default: 7)
            
        Returns:
            Dict of computed features
        """
        window_days = window_days or self.default_window_days
        
        # Get transactions from KV
        transactions = self.kv.get_transactions_for_account(account_id, days=window_days)
        
        # Get historical stats for z-score calculation (from existing account-fact if available)
        existing_fact = self.kv.get_account_fact(account_id) or {}
        hist_txn_mean = existing_fact.get('hist_txn_mean', 0)
        hist_amt_mean = existing_fact.get('hist_amt_mean', 0)
        hist_amt_std = existing_fact.get('hist_amt_std', 1)  # Avoid division by zero
        
        # Separate outgoing vs incoming transactions
        out_txns = [t for t in transactions if t.get('direction') == 'out']
        in_txns = [t for t in transactions if t.get('direction') == 'in']
        
        # A. Transaction behavior features
        txn_out_count = len(out_txns)
        txn_24h_peak = self._compute_24h_peak(out_txns)
        avg_txn_per_day = txn_out_count / window_days if window_days > 0 else 0
        max_txn_per_hour = self._compute_max_hourly(out_txns)
        txn_zscore = self._compute_zscore(txn_out_count, hist_txn_mean, max(1, hist_txn_mean * 0.5))
        
        # B. Amount behavior features
        out_amounts = [t.get('amount', 0) for t in out_txns]
        total_out_amount = sum(out_amounts)
        avg_out_amount = total_out_amount / txn_out_count if txn_out_count > 0 else 0
        max_out_amount = max(out_amounts) if out_amounts else 0
        amount_zscore = self._compute_zscore(max_out_amount, hist_amt_mean, hist_amt_std)
        
        # C. Counterparty spread features
        recipients = [t.get('counterparty') for t in out_txns if t.get('counterparty')]
        unique_recipients = len(set(recipients))
        new_recipient_ratio = self._compute_new_recipient_ratio(account_id, recipients, existing_fact)
        recipient_entropy = self._compute_entropy(recipients)
        
        # D. Device exposure features (requires graph for device relationships)
        device_count, shared_device_account_count = self._get_device_exposure(account_id)
        
        # E. Lifecycle features (requires account metadata)
        account_age_days, first_txn_delay = self._get_lifecycle_features(account_id, transactions)
        
        # Update historical means for next computation
        new_hist_txn_mean = (hist_txn_mean * 0.9 + txn_out_count * 0.1) if hist_txn_mean > 0 else txn_out_count
        new_hist_amt_mean = (hist_amt_mean * 0.9 + avg_out_amount * 0.1) if hist_amt_mean > 0 else avg_out_amount
        
        # Compute running std for amount
        if hist_amt_std > 1 and avg_out_amount > 0:
            new_hist_amt_std = math.sqrt(0.9 * hist_amt_std**2 + 0.1 * (avg_out_amount - new_hist_amt_mean)**2)
        else:
            new_hist_amt_std = max(1, abs(avg_out_amount - new_hist_amt_mean))
        
        return {
            # A. Transaction behavior
            'txn_out_7d': txn_out_count,
            'txn_24h_peak': txn_24h_peak,
            'avg_txn_day': round(avg_txn_per_day, 2),
            'max_txn_hr': max_txn_per_hour,
            'txn_zscore': round(txn_zscore, 2),
            
            # B. Amount behavior
            'out_amt_7d': round(total_out_amount, 2),
            'avg_out_amt': round(avg_out_amount, 2),
            'max_out_amt': round(max_out_amount, 2),
            'amt_zscore': round(amount_zscore, 2),
            
            # C. Counterparty spread
            'uniq_recip': unique_recipients,
            'new_recip_rat': round(new_recipient_ratio, 2),
            'recip_entropy': round(recipient_entropy, 2),
            
            # D. Device exposure
            'dev_count': device_count,
            'shared_dev_ct': shared_device_account_count,
            
            # E. Lifecycle
            'acct_age_days': account_age_days,
            'first_txn_dly': first_txn_delay,
            
            # Historical stats for next computation
            'hist_txn_mean': round(new_hist_txn_mean, 2),
            'hist_amt_mean': round(new_hist_amt_mean, 2),
            'hist_amt_std': round(new_hist_amt_std, 2),
        }
    
    def batch_compute_account_features(self, account_ids: List[str], window_days: int = None) -> Dict[str, Dict[str, Any]]:
        """
        Compute features for multiple accounts efficiently using batch operations.
        
        Args:
            account_ids: List of account IDs
            window_days: Sliding window in days
            
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
            
            # Compute features using the fetched data
            results[account_id] = self._compute_features_from_data(
                account_id, transactions, existing_fact, window_days
            )
        
        return results
    
    def _compute_features_from_data(self, account_id: str, transactions: List[Dict], 
                                     existing_fact: Dict, window_days: int) -> Dict[str, Any]:
        """Compute features from pre-fetched transaction data."""
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
        
        # D. Device exposure
        device_count, shared_device_account_count = self._get_device_exposure(account_id)
        
        # E. Lifecycle
        account_age_days, first_txn_delay = self._get_lifecycle_features(account_id, transactions)
        
        # Update historical means
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
    # Device Feature Computation
    # ----------------------------------------------------------------------------------------------------------
    
    def compute_device_features(self, device_id: str, window_days: int = None) -> Dict[str, Any]:
        """
        Compute all 5 device features.
        
        Args:
            device_id: The device ID
            window_days: Sliding window in days
            
        Returns:
            Dict of computed features
        """
        window_days = window_days or self.default_window_days
        
        # Get accounts using this device (from graph)
        connected_accounts = self._get_device_accounts(device_id)
        
        # Get account facts for risk scores
        account_facts = self.kv.batch_get_account_facts(list(connected_accounts))
        
        # Compute features
        shared_account_count = len(connected_accounts)
        
        risk_scores = []
        flagged_count = 0
        new_account_count = 0
        
        for account_id, fact in account_facts.items():
            if fact:
                risk = fact.get('risk_score', 0)
                if risk is not None:
                    risk_scores.append(risk)
                if fact.get('fraud', False):
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
    
    def batch_compute_device_features(self, device_ids: List[str], window_days: int = None) -> Dict[str, Dict[str, Any]]:
        """
        Compute features for multiple devices.
        
        Args:
            device_ids: List of device IDs
            window_days: Sliding window in days
            
        Returns:
            Dict mapping device_id to features
        """
        results = {}
        for device_id in device_ids:
            results[device_id] = self.compute_device_features(device_id, window_days)
        return results
    
    # ----------------------------------------------------------------------------------------------------------
    # Helper Methods
    # ----------------------------------------------------------------------------------------------------------
    
    def _compute_24h_peak(self, transactions: List[Dict]) -> int:
        """Compute max transactions in any 24h window."""
        if not transactions:
            return 0
        
        # Group by 24h windows
        hourly_counts = defaultdict(int)
        for txn in transactions:
            ts = txn.get('timestamp', '')
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    # Use date as key for 24h window
                    day_key = dt.strftime('%Y-%m-%d')
                    hourly_counts[day_key] += 1
                except:
                    pass
        
        return max(hourly_counts.values()) if hourly_counts else 0
    
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
                except:
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
        
        # Get known recipients from historical data
        known_recipients = set(existing_fact.get('known_recipients', []))
        current_unique = set(current_recipients)
        
        if not current_unique:
            return 0
        
        new_recipients = current_unique - known_recipients
        return len(new_recipients) / len(current_unique)
    
    def _get_device_exposure(self, account_id: str) -> tuple:
        """Get device count and shared device account count for an account."""
        if not self.graph or not self.graph.client:
            return (1, 0)
        
        try:
            g = self.graph.client
            
            # Get user who owns this account
            user_id = g.V(account_id).in_("OWNS").id_().next()
            
            # Get devices used by this user
            devices = g.V(user_id).out("USES").id_().toList()
            device_count = len(devices)
            
            # Get count of other accounts sharing these devices
            if devices:
                shared_accounts = set()
                for device_id in devices:
                    users_on_device = g.V(device_id).in_("USES").id_().toList()
                    for uid in users_on_device:
                        if uid != user_id:
                            accounts = g.V(uid).out("OWNS").id_().toList()
                            shared_accounts.update(accounts)
                shared_device_account_count = len(shared_accounts)
            else:
                shared_device_account_count = 0
            
            return (device_count, shared_device_account_count)
            
        except Exception as e:
            logger.debug(f"Error getting device exposure for {account_id}: {e}")
            return (1, 0)
    
    def _get_lifecycle_features(self, account_id: str, transactions: List[Dict]) -> tuple:
        """Get account age and first transaction delay."""
        account_age_days = 365  # Default
        first_txn_delay = 0
        
        if self.graph and self.graph.client:
            try:
                g = self.graph.client
                
                # Get account creation date
                created_date = g.V(account_id).values("created_date").next()
                if created_date:
                    try:
                        created = datetime.fromisoformat(str(created_date).replace('Z', '+00:00'))
                        account_age_days = (datetime.now(created.tzinfo) - created).days
                        
                        # Find first transaction timestamp
                        if transactions:
                            first_txn_ts = min(t.get('timestamp', '') for t in transactions if t.get('timestamp'))
                            if first_txn_ts:
                                first_txn = datetime.fromisoformat(first_txn_ts.replace('Z', '+00:00'))
                                first_txn_delay = (first_txn - created).days
                    except:
                        pass
            except Exception as e:
                logger.debug(f"Error getting lifecycle features for {account_id}: {e}")
        
        return (account_age_days, first_txn_delay)
    
    def _get_device_accounts(self, device_id: str) -> Set[str]:
        """Get all accounts connected to a device."""
        if not self.graph or not self.graph.client:
            return set()
        
        try:
            g = self.graph.client
            
            # Device <- USES - User - OWNS -> Accounts
            users = g.V(device_id).in_("USES").id_().toList()
            accounts = set()
            for user_id in users:
                user_accounts = g.V(user_id).out("OWNS").id_().toList()
                accounts.update(user_accounts)
            
            return accounts
            
        except Exception as e:
            logger.debug(f"Error getting device accounts for {device_id}: {e}")
            return set()
    
    # ----------------------------------------------------------------------------------------------------------
    # Batch Feature Update Job
    # ----------------------------------------------------------------------------------------------------------
    
    def run_feature_computation_job(self, window_days: int = None) -> Dict[str, Any]:
        """
        Run a batch job to compute and update features for all accounts and devices.
        Should be scheduled before ML detection runs.
        
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
            # Start progress tracking with initial estimate
            progress_service.start_operation(self.OPERATION_ID, 100, "Fetching entities...")
            
            # Get all users and their accounts
            users = self.kv.get_all_users(limit=100000)
            
            all_account_ids = set()
            all_device_ids = set()
            
            for user in users:
                accounts = user.get('accounts', {})
                devices = user.get('devices', {})
                all_account_ids.update(accounts.keys())
                all_device_ids.update(devices.keys())
            
            # Also get accounts from graph if available
            if self.graph and self.graph.client:
                try:
                    graph_accounts = self.graph.client.V().hasLabel("account").id_().toList()
                    all_account_ids.update(graph_accounts)
                    
                    graph_devices = self.graph.client.V().hasLabel("device").id_().toList()
                    all_device_ids.update(graph_devices)
                except:
                    pass
            
            # Now we know total items - update progress with accurate count
            total_items = len(all_account_ids) + len(all_device_ids)
            progress_service.start_operation(
                self.OPERATION_ID, 
                total_items, 
                f"Computing features for {len(all_account_ids)} accounts and {len(all_device_ids)} devices"
            )
            
            logger.info(f"Computing features for {len(all_account_ids)} accounts and {len(all_device_ids)} devices")
            
            # Compute and store account features in batches
            account_list = list(all_account_ids)
            batch_size = 100
            current_progress = 0
            
            for i in range(0, len(account_list), batch_size):
                batch = account_list[i:i+batch_size]
                features_batch = self.batch_compute_account_features(batch, window_days)
                
                for account_id, features in features_batch.items():
                    try:
                        self.kv.update_account_fact(account_id, features)
                        result["accounts_processed"] += 1
                    except Exception as e:
                        result["errors"].append(f"Account {account_id}: {str(e)}")
                
                # Update progress after each batch
                current_progress += len(batch)
                progress_service.update_progress(
                    self.OPERATION_ID, 
                    current_progress,
                    f"Accounts: {result['accounts_processed']}/{len(all_account_ids)}"
                )
            
            # Compute and store device features
            progress_service.update_progress(
                self.OPERATION_ID,
                current_progress,
                f"Computing device features ({len(all_device_ids)} devices)..."
            )
            
            device_progress = 0
            for device_id in all_device_ids:
                try:
                    features = self.compute_device_features(device_id, window_days)
                    self.kv.update_device_fact(device_id, features)
                    result["devices_processed"] += 1
                except Exception as e:
                    result["errors"].append(f"Device {device_id}: {str(e)}")
                
                device_progress += 1
                if device_progress % 100 == 0:
                    progress_service.update_progress(
                        self.OPERATION_ID,
                        current_progress + device_progress,
                        f"Devices: {result['devices_processed']}/{len(all_device_ids)}"
                    )
            
            result["status"] = "completed"
            
            # Complete progress tracking
            progress_service.complete_operation(
                self.OPERATION_ID,
                f"Completed! {result['accounts_processed']} accounts, {result['devices_processed']} devices",
                extra={
                    "accounts_processed": result["accounts_processed"],
                    "devices_processed": result["devices_processed"],
                }
            )
            
        except Exception as e:
            logger.error(f"Feature computation job failed: {e}")
            result["status"] = "failed"
            result["error"] = str(e)
            progress_service.fail_operation(self.OPERATION_ID, str(e), "Feature computation failed")
        
        result["end_time"] = datetime.now().isoformat()
        result["duration_seconds"] = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Feature computation completed: {result['accounts_processed']} accounts, "
                   f"{result['devices_processed']} devices")
        
        return result


# Factory function
def create_feature_service(aerospike_service, graph_service=None):
    """Create a FeatureService instance."""
    return FeatureService(aerospike_service, graph_service)
