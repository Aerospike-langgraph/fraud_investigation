"""
Transaction Injector Service

Generates historical transactions for testing and demo purposes.
Writes to both Graph (TRANSACTS edges) AND KV (transactions set).

Features:
- Spreads transactions over configurable period (default 30 days)
- Injects ~15% fraudulent patterns:
  - Fraud rings: Concentrated inter-connected accounts
  - Velocity anomalies: Single accounts with burst activity
  - Amount anomalies: High-value outlier transactions
  - New account fraud: Immediate high activity after creation
"""

import logging
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set, Tuple
from collections import defaultdict

from gremlin_python.process.graph_traversal import __

logger = logging.getLogger('fraud_detection.transaction_injector')


class TransactionInjector:
    """
    Generates historical transactions with fraud patterns.
    Dual-writes to Graph and KV store.
    """
    
    def __init__(self, graph_service, aerospike_service):
        self.graph = graph_service
        self.kv = aerospike_service
        
        # Transaction locations
        self.locations = [
            'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
            'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
            'Dallas, TX', 'San Jose, CA', 'Austin, TX', 'Jacksonville, FL',
            'Fort Worth, TX', 'Columbus, OH', 'Charlotte, NC', 'San Francisco, CA',
            'Indianapolis, IN', 'Seattle, WA', 'Denver, CO', 'Washington, DC',
        ]
        
        # Transaction types
        self.txn_types = ['transfer', 'payment', 'purchase', 'withdrawal', 'deposit']
        
        # Fraud pattern configuration
        self.fraud_config = {
            'fraud_ring_count': 3,           # Number of fraud rings
            'fraud_ring_size': 5,            # Accounts per ring
            'fraud_ring_txn_count': 50,      # Transactions within each ring
            'velocity_anomaly_count': 10,    # Accounts with burst activity
            'velocity_burst_size': 30,       # Transactions per burst
            'amount_anomaly_count': 20,      # High-value outlier transactions
            'new_account_fraud_count': 5,    # New accounts with immediate activity
        }
    
    def inject_historical_transactions(
        self,
        transaction_count: int = 10000,
        spread_days: int = 30,
        fraud_percentage: float = 0.15
    ) -> Dict[str, Any]:
        """
        Inject historical transactions with fraud patterns.
        
        Args:
            transaction_count: Total number of transactions to generate
            spread_days: Days to spread transactions over (should cover cooldown)
            fraud_percentage: Percentage of fraudulent transactions (default 15%)
            
        Returns:
            Result dict with counts and details
        """
        start_time = datetime.now()
        
        result = {
            "job_id": f"inject_{start_time.strftime('%Y%m%d_%H%M%S')}",
            "start_time": start_time.isoformat(),
            "config": {
                "transaction_count": transaction_count,
                "spread_days": spread_days,
                "fraud_percentage": fraud_percentage,
            },
            "normal_transactions": 0,
            "fraud_transactions": 0,
            "fraud_patterns": {
                "fraud_rings": 0,
                "velocity_anomalies": 0,
                "amount_anomalies": 0,
                "new_account_fraud": 0,
            },
            "errors": [],
            "kv_writes": 0,
            "graph_writes": 0,
        }
        
        try:
            # Get all accounts
            accounts = self._get_all_accounts()
            if len(accounts) < 10:
                raise Exception(f"Not enough accounts ({len(accounts)}). Need at least 10.")
            
            logger.info(f"Injecting {transaction_count} transactions over {spread_days} days "
                       f"with {fraud_percentage*100:.0f}% fraud rate")
            
            # Calculate fraud transaction counts
            fraud_txn_count = int(transaction_count * fraud_percentage)
            normal_txn_count = transaction_count - fraud_txn_count
            
            # Generate fraud patterns first (they're more structured)
            fraud_result = self._generate_fraud_patterns(accounts, fraud_txn_count, spread_days)
            result["fraud_transactions"] = fraud_result["total"]
            result["fraud_patterns"] = fraud_result["patterns"]
            result["graph_writes"] += fraud_result["graph_writes"]
            result["kv_writes"] += fraud_result["kv_writes"]
            
            # Generate normal transactions
            normal_result = self._generate_normal_transactions(accounts, normal_txn_count, spread_days)
            result["normal_transactions"] = normal_result["count"]
            result["graph_writes"] += normal_result["graph_writes"]
            result["kv_writes"] += normal_result["kv_writes"]
            
            result["status"] = "completed"
            
        except Exception as e:
            logger.error(f"Transaction injection failed: {e}")
            result["status"] = "failed"
            result["errors"].append(str(e))
        
        result["end_time"] = datetime.now().isoformat()
        result["duration_seconds"] = (datetime.now() - start_time).total_seconds()
        
        total = result["normal_transactions"] + result["fraud_transactions"]
        logger.info(f"Transaction injection complete: {total} transactions "
                   f"({result['fraud_transactions']} fraud, {result['normal_transactions']} normal)")
        
        return result
    
    def _get_all_accounts(self) -> List[str]:
        """Get all account IDs from the graph."""
        if not self.graph or not self.graph.client:
            raise Exception("Graph service not available")
        
        return self.graph.client.V().hasLabel("account").id_().toList()
    
    def _get_new_accounts(self, days: int = 30) -> List[str]:
        """Get accounts created in the last N days."""
        if not self.graph or not self.graph.client:
            return []
        
        try:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            # Get accounts with created_date > cutoff
            return self.graph.client.V().hasLabel("account") \
                .has("created_date", lambda x: x > cutoff if x else False) \
                .id_().toList()
        except:
            # Fallback: just return first 20 accounts (assume some are new)
            all_accounts = self._get_all_accounts()
            return all_accounts[:min(20, len(all_accounts))]
    
    def _generate_timestamp(self, days_back_max: int) -> str:
        """Generate a random timestamp within the specified days."""
        days_back = random.randint(0, days_back_max)
        hours_back = random.randint(0, 23)
        minutes_back = random.randint(0, 59)
        
        dt = datetime.now() - timedelta(days=days_back, hours=hours_back, minutes=minutes_back)
        return dt.isoformat()
    
    def _create_transaction(
        self,
        sender_id: str,
        receiver_id: str,
        amount: float,
        timestamp: str,
        txn_type: str = "transfer",
        is_fraud: bool = False
    ) -> Tuple[bool, bool]:
        """
        Create a transaction in both Graph and KV.
        
        Returns:
            Tuple of (graph_success, kv_success)
        """
        txn_id = str(uuid.uuid4())
        location = random.choice(self.locations)
        
        graph_success = False
        kv_success = False
        
        # Write to Graph
        try:
            self.graph.client.V(sender_id) \
                .addE("TRANSACTS") \
                .to(__.V(receiver_id)) \
                .property("txn_id", txn_id) \
                .property("amount", round(amount, 2)) \
                .property("currency", "USD") \
                .property("type", txn_type) \
                .property("method", "electronic_transfer") \
                .property("location", location) \
                .property("timestamp", timestamp) \
                .property("status", "completed") \
                .property("gen_type", "HISTORICAL_FRAUD" if is_fraud else "HISTORICAL") \
                .iterate()
            graph_success = True
        except Exception as e:
            logger.warning(f"Error writing transaction to graph: {e}")
        
        # Write to KV (dual-write for both sender and receiver)
        try:
            txn_data = {
                "txn_id": txn_id,
                "amount": round(amount, 2),
                "type": txn_type,
                "method": "electronic_transfer",
                "location": location,
                "timestamp": timestamp,
                "status": "completed",
            }
            
            # Sender's outgoing transaction
            sender_success = self.kv.store_transaction(
                sender_id,
                {**txn_data, "counterparty": receiver_id},
                direction="out"
            )
            
            # Receiver's incoming transaction
            receiver_success = self.kv.store_transaction(
                receiver_id,
                {**txn_data, "counterparty": sender_id},
                direction="in"
            )
            
            kv_success = sender_success and receiver_success
            
        except Exception as e:
            logger.warning(f"Error writing transaction to KV: {e}")
        
        return (graph_success, kv_success)
    
    def _generate_normal_transactions(
        self,
        accounts: List[str],
        count: int,
        spread_days: int
    ) -> Dict[str, Any]:
        """Generate normal (non-fraudulent) transactions."""
        result = {"count": 0, "graph_writes": 0, "kv_writes": 0}
        
        for _ in range(count):
            # Random sender and receiver
            sender, receiver = random.sample(accounts, 2)
            
            # Normal amount distribution: $50 - $5000
            amount = random.uniform(50, 5000)
            
            # Random timestamp within spread_days
            timestamp = self._generate_timestamp(spread_days)
            
            # Random transaction type
            txn_type = random.choice(self.txn_types)
            
            graph_ok, kv_ok = self._create_transaction(
                sender, receiver, amount, timestamp, txn_type, is_fraud=False
            )
            
            if graph_ok:
                result["graph_writes"] += 1
            if kv_ok:
                result["kv_writes"] += 1
            if graph_ok or kv_ok:
                result["count"] += 1
            
            if result["count"] % 1000 == 0:
                logger.info(f"Generated {result['count']} normal transactions...")
        
        return result
    
    def _generate_fraud_patterns(
        self,
        accounts: List[str],
        fraud_txn_count: int,
        spread_days: int
    ) -> Dict[str, Any]:
        """Generate various fraud patterns."""
        result = {
            "total": 0,
            "patterns": {
                "fraud_rings": 0,
                "velocity_anomalies": 0,
                "amount_anomalies": 0,
                "new_account_fraud": 0,
            },
            "graph_writes": 0,
            "kv_writes": 0,
        }
        
        # Allocate fraud transactions to different patterns
        ring_txns = int(fraud_txn_count * 0.4)      # 40% fraud rings
        velocity_txns = int(fraud_txn_count * 0.25) # 25% velocity anomalies
        amount_txns = int(fraud_txn_count * 0.20)   # 20% amount anomalies
        new_acct_txns = fraud_txn_count - ring_txns - velocity_txns - amount_txns  # 15% new account fraud
        
        # 1. Fraud Rings: Tight-knit groups transacting heavily among themselves
        ring_result = self._generate_fraud_rings(accounts, ring_txns, spread_days)
        result["patterns"]["fraud_rings"] = ring_result["count"]
        result["graph_writes"] += ring_result["graph_writes"]
        result["kv_writes"] += ring_result["kv_writes"]
        
        # 2. Velocity Anomalies: Single accounts with burst activity
        velocity_result = self._generate_velocity_anomalies(accounts, velocity_txns, spread_days)
        result["patterns"]["velocity_anomalies"] = velocity_result["count"]
        result["graph_writes"] += velocity_result["graph_writes"]
        result["kv_writes"] += velocity_result["kv_writes"]
        
        # 3. Amount Anomalies: Unusually high-value transactions
        amount_result = self._generate_amount_anomalies(accounts, amount_txns, spread_days)
        result["patterns"]["amount_anomalies"] = amount_result["count"]
        result["graph_writes"] += amount_result["graph_writes"]
        result["kv_writes"] += amount_result["kv_writes"]
        
        # 4. New Account Fraud: New accounts with immediate high activity
        new_acct_result = self._generate_new_account_fraud(accounts, new_acct_txns, spread_days)
        result["patterns"]["new_account_fraud"] = new_acct_result["count"]
        result["graph_writes"] += new_acct_result["graph_writes"]
        result["kv_writes"] += new_acct_result["kv_writes"]
        
        result["total"] = sum(result["patterns"].values())
        
        return result
    
    def _generate_fraud_rings(
        self,
        accounts: List[str],
        target_count: int,
        spread_days: int
    ) -> Dict[str, int]:
        """Generate fraud ring transactions - tight-knit groups."""
        result = {"count": 0, "graph_writes": 0, "kv_writes": 0}
        
        ring_count = self.fraud_config['fraud_ring_count']
        ring_size = min(self.fraud_config['fraud_ring_size'], len(accounts) // ring_count)
        txns_per_ring = target_count // ring_count
        
        for ring_idx in range(ring_count):
            # Select accounts for this ring
            ring_accounts = random.sample(accounts, ring_size)
            
            # Concentrated time window (1-3 days)
            ring_start_days = random.randint(1, max(1, spread_days - 3))
            
            for _ in range(txns_per_ring):
                sender, receiver = random.sample(ring_accounts, 2)
                
                # Structured amounts (common in money laundering)
                amount = random.choice([
                    random.uniform(9000, 9999),    # Just under $10K threshold
                    random.uniform(4500, 5000),   # Half of threshold
                    random.uniform(2000, 3000),   # Structured smaller amounts
                ])
                
                # Concentrated timestamps
                days_back = random.randint(ring_start_days, ring_start_days + 3)
                dt = datetime.now() - timedelta(days=days_back, hours=random.randint(0, 23))
                timestamp = dt.isoformat()
                
                graph_ok, kv_ok = self._create_transaction(
                    sender, receiver, amount, timestamp, "transfer", is_fraud=True
                )
                
                if graph_ok:
                    result["graph_writes"] += 1
                if kv_ok:
                    result["kv_writes"] += 1
                if graph_ok or kv_ok:
                    result["count"] += 1
        
        logger.info(f"Generated {result['count']} fraud ring transactions across {ring_count} rings")
        return result
    
    def _generate_velocity_anomalies(
        self,
        accounts: List[str],
        target_count: int,
        spread_days: int
    ) -> Dict[str, int]:
        """Generate velocity anomaly transactions - burst activity."""
        result = {"count": 0, "graph_writes": 0, "kv_writes": 0}
        
        anomaly_count = self.fraud_config['velocity_anomaly_count']
        burst_size = target_count // anomaly_count
        
        # Select accounts for velocity anomalies
        anomaly_accounts = random.sample(accounts, min(anomaly_count, len(accounts) // 2))
        
        for anomaly_account in anomaly_accounts:
            # Pick a specific day for the burst
            burst_day = random.randint(1, max(1, spread_days - 1))
            
            for _ in range(burst_size):
                # All transactions in a single day (burst)
                receiver = random.choice([a for a in accounts if a != anomaly_account])
                
                # Small to medium amounts (rapid small transfers)
                amount = random.uniform(100, 2000)
                
                # Same day, different hours (high velocity)
                dt = datetime.now() - timedelta(days=burst_day, hours=random.randint(0, 23), minutes=random.randint(0, 59))
                timestamp = dt.isoformat()
                
                graph_ok, kv_ok = self._create_transaction(
                    anomaly_account, receiver, amount, timestamp, "transfer", is_fraud=True
                )
                
                if graph_ok:
                    result["graph_writes"] += 1
                if kv_ok:
                    result["kv_writes"] += 1
                if graph_ok or kv_ok:
                    result["count"] += 1
        
        logger.info(f"Generated {result['count']} velocity anomaly transactions")
        return result
    
    def _generate_amount_anomalies(
        self,
        accounts: List[str],
        target_count: int,
        spread_days: int
    ) -> Dict[str, int]:
        """Generate amount anomaly transactions - unusually high values."""
        result = {"count": 0, "graph_writes": 0, "kv_writes": 0}
        
        for _ in range(target_count):
            sender, receiver = random.sample(accounts, 2)
            
            # High-value amounts (outliers)
            amount = random.choice([
                random.uniform(15000, 50000),    # Very high
                random.uniform(50000, 100000),   # Extremely high
                random.uniform(10000, 15000),    # High
            ])
            
            timestamp = self._generate_timestamp(spread_days)
            
            graph_ok, kv_ok = self._create_transaction(
                sender, receiver, amount, timestamp, "transfer", is_fraud=True
            )
            
            if graph_ok:
                result["graph_writes"] += 1
            if kv_ok:
                result["kv_writes"] += 1
            if graph_ok or kv_ok:
                result["count"] += 1
        
        logger.info(f"Generated {result['count']} amount anomaly transactions")
        return result
    
    def _generate_new_account_fraud(
        self,
        accounts: List[str],
        target_count: int,
        spread_days: int
    ) -> Dict[str, int]:
        """Generate new account fraud - immediate activity after creation."""
        result = {"count": 0, "graph_writes": 0, "kv_writes": 0}
        
        # Use first few accounts (assuming they're newer) or random selection
        new_account_candidates = accounts[:min(20, len(accounts))]
        txns_per_account = target_count // max(1, len(new_account_candidates))
        
        for new_account in new_account_candidates[:self.fraud_config['new_account_fraud_count']]:
            # All activity within first few days (immediate)
            for _ in range(txns_per_account):
                receiver = random.choice([a for a in accounts if a != new_account])
                
                # Mix of amounts
                amount = random.uniform(500, 8000)
                
                # Within first 2 days of the spread
                dt = datetime.now() - timedelta(days=random.randint(0, 2), hours=random.randint(0, 23))
                timestamp = dt.isoformat()
                
                graph_ok, kv_ok = self._create_transaction(
                    new_account, receiver, amount, timestamp, "transfer", is_fraud=True
                )
                
                if graph_ok:
                    result["graph_writes"] += 1
                if kv_ok:
                    result["kv_writes"] += 1
                if graph_ok or kv_ok:
                    result["count"] += 1
        
        logger.info(f"Generated {result['count']} new account fraud transactions")
        return result


# Factory function
def create_transaction_injector(graph_service, aerospike_service):
    """Create a TransactionInjector instance."""
    return TransactionInjector(graph_service, aerospike_service)
