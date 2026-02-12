import asyncio
import random
import pickle
import math
import time
from datetime import datetime
from typing import List, Dict, Any
from enum import Enum
import json
import uuid
from gremlin_python.process.graph_traversal import __
from concurrent.futures import ThreadPoolExecutor

# Import local modules
from services.fraud_service import FraudService
from services.graph_service import GraphService
from logging_config import get_logger

logger = get_logger('fraud_detection.transaction_generator')
stats_logger = get_logger('fraud_detection.stats')

class FraudScenario(Enum):
    SCENARIO_A = "Multiple Small Credits Followed by Large Debit"
    SCENARIO_B = "Large Credit Followed by Structured Equal Debits"
    SCENARIO_C = "Multiple Large ATM Withdrawals"
    SCENARIO_D = "High-Frequency Transfers Between Mule Accounts"
    SCENARIO_E = "Salary-Like Deposits Followed by Suspicious Transfers"
    SCENARIO_F = "Dormant Account Sudden Activity"
    SCENARIO_G = "International Transfers to High-Risk Jurisdictions"
    SCENARIO_H = "Region-Specific Fraud (Indian Context)"

def get_stored_max_transaction_rate():
    try:
        file = open('store.pckl', 'rb')
        obj = pickle.load(file)
        file.close()
        return obj.get("rate", 50)
    except:
        return 50

class TransactionGeneratorService:
    def __init__(self, graph_service: GraphService, fraud_service: FraudService, aerospike_service=None):
        self.graph_service = graph_service
        self.fraud_service = fraud_service
        self.kv = aerospike_service  # KV store for dual-write
        self.is_running = False
        self.max_generation_rate = get_stored_max_transaction_rate()
        self.generation_rate = 1  # transactions per second
        self.generated_transactions = []
        self.transaction_counter = 0
        self.task = None
        self.account_vertices = []
        self.start_time = None

        # High-risk jurisdictions for international transfers
        self.high_risk_jurisdictions = ['Dubai', 'Bahrain', 'Thailand', 'Cayman Islands', 'Panama']
        
        # Indian fraud locations
        self.indian_fraud_locations = ['Jamtara', 'Bharatpur', 'Alwar', 'Mewat', 'Nuh']
        
        # Normal locations (Indian cities)
        # self.normal_locations = [
        #     'Mumbai, Maharashtra', 'Delhi, Delhi', 'Bangalore, Karnataka', 'Hyderabad, Telangana', 
        #     'Chennai, Tamil Nadu', 'Kolkata, West Bengal', 'Pune, Maharashtra', 'Ahmedabad, Gujarat',
        #     'Jaipur, Rajasthan', 'Surat, Gujarat', 'Lucknow, Uttar Pradesh', 'Kanpur, Uttar Pradesh',
        #     'Nagpur, Maharashtra', 'Visakhapatnam, Andhra Pradesh', 'Indore, Madhya Pradesh',
        #     'Thane, Maharashtra', 'Bhopal, Madhya Pradesh', 'Patna, Bihar', 'Vadodara, Gujarat',
        #     'Ghaziabad, Uttar Pradesh', 'Ludhiana, Punjab', 'Agra, Uttar Pradesh', 'Nashik, Maharashtra'
        # ]
        
        self.normal_locations = [
            'New York, New York', 'Los Angeles, California', 'Chicago, Illinois', 'Houston, Texas',
            'Phoenix, Arizona', 'Philadelphia, Pennsylvania', 'San Antonio, Texas', 'San Diego, California',
            'Dallas, Texas', 'San Jose, California', 'Austin, Texas', 'Jacksonville, Florida',
            'Fort Worth, Texas', 'Columbus, Ohio', 'Charlotte, North Carolina', 'San Francisco, California',
            'Indianapolis, Indiana', 'Seattle, Washington', 'Denver, Colorado', 'Washington, District of Columbia',
            'Boston, Massachusetts', 'El Paso, Texas', 'Nashville, Tennessee', 'Detroit, Michigan',
            'Oklahoma City, Oklahoma', 'Portland, Oregon', 'Las Vegas, Nevada', 'Memphis, Tennessee',
            'Louisville, Kentucky', 'Baltimore, Maryland', 'Milwaukee, Wisconsin', 'Albuquerque, New Mexico',
            'Tucson, Arizona', 'Fresno, California', 'Sacramento, California', 'Mesa, Arizona',
            'Kansas City, Missouri', 'Atlanta, Georgia', 'Long Beach, California', 'Colorado Springs, Colorado',
            'Raleigh, North Carolina', 'Miami, Florida', 'Virginia Beach, Virginia', 'Omaha, Nebraska',
            'Oakland, California', 'Minneapolis, Minnesota', 'Tulsa, Oklahoma', 'Arlington, Texas'
        ]
        
        # Transaction types
        self.transaction_types = ['purchase', 'transfer', 'withdrawal', 'deposit', 'payment']


    # ----------------------------------------------------------------------------------------------------------
    # Transaction generation control
    # ----------------------------------------------------------------------------------------------------------

    
    def get_max_transaction_rate(self):
        """Get the max rate for transactions genreated per second"""
        return self.max_generation_rate

    def set_max_transaction_rate(self, new_rate):
        """Set the max rate for transactions genreated per second"""
        try:
            old_rate = self.max_generation_rate
            
            file = open('store.pckl', 'wb')
            pickle.dump({ "rate": new_rate }, file)
            file.close()

            self.max_generation_rate = new_rate
            
            logger.info(f"📊 Max generation rate updated from {old_rate} to {new_rate} transactions/second")
            return True
        
        except Exception as e:
            logger.error(f"Unable to set new max generation rate: {e}")
            return False

    def start_generation(self, rate: int = 1, start: str = ""):
        """Start transaction generation at specified rate"""
        if self.is_running:
            logger.warning("Transaction generation is already running")
            return False
        try:
            self.account_vertices = self.graph_service.client.V().has_label("account").id_().to_list()
            if len(self.account_vertices) < 1:
                raise Exception("No accounts available")
        except Exception as e:
            logger.error(f"Unable to start transaction generator: {e}")
            return False

        self.generation_rate = rate
        self.is_running = True
        self.transaction_counter = 0
        self.start_time = start
        
        logger.info(f"🚀 Starting transaction generation at {self.generation_rate} transactions/second")
        stats_logger.info(f"START: Generation started at {self.generation_rate} txn/sec")
        
        # self.task = asyncio.create_task(self._generation_loop())

        return True

    def stop_generation(self):
        """Stop transaction generation"""
        if not self.is_running:
            logger.warning("Transaction generation is not running")
            return False
            
        self.is_running = False
        self.start_time = None
        self.transaction_counter = 0

        # if self.task:
        #     self.task.cancel()
        #     try:
        #         await self.task
        #     except asyncio.CancelledError:
        #         pass
        # self.task = None
        
        logger.info("🛑 Transaction generation stopped")
        logger.info(f"📊 Generated {self.transaction_counter} transactions")
        
        stats_logger.info(f"STOP: Generation stopped. Total: {self.transaction_counter} transactions")
        self._log_statistics()
        
        return True

    async def _generation_loop(self):
        """Main generation loop"""
        while self.is_running:
            try:    
                asyncio.create_task(self._generate_transaction())
                await asyncio.sleep(.0005)
        
            except Exception as e:
                logger.error(f"❌ Error in generation loop: {e}")
                await asyncio.sleep(1)



    # ----------------------------------------------------------------------------------------------------------
    # Transaction generation functions
    # ----------------------------------------------------------------------------------------------------------

       
    def create_manual_transaction(self, from_id: str, to_id: str, amount: float, type: str = "transfer", gen_type: str = "MANUAL") -> Dict[str, Any]:
        """Create a manual transaction between specified accounts"""
        try:
            logger.info(f"Creating {gen_type.lower()} transaction from {from_id} to {to_id} amount {amount}")
            # Validate accounts exist
            if not self._validate_account_exists(from_id):
                raise Exception(f"Source account {from_id} not found")
            if not self._validate_account_exists(to_id):
                raise Exception(f"Destination account {to_id} not found")
            # Prevent self-transactions
            if from_id == to_id:
                raise Exception("Source and destination accounts cannot be the same")

            # Get sender's and receiver's user IDs and sender's devices for tracking
            device_id = None
            sender_user_id = None
            receiver_user_id = None
            try:
                # Account -> User (via reverse OWNS edge) for sender
                user_ids = self.graph_service.client.V(from_id).in_("OWNS").id_().toList()
                if user_ids:
                    sender_user_id = user_ids[0]
                    # User -> Devices (via USES edge)
                    user_devices = self.graph_service.client.V(sender_user_id).out("USES").id_().toList()
                    if user_devices:
                        device_id = random.choice(user_devices)
                
                # Get receiver's user_id
                receiver_user_ids = self.graph_service.client.V(to_id).in_("OWNS").id_().toList()
                if receiver_user_ids:
                    receiver_user_id = receiver_user_ids[0]
            except Exception as e:
                logger.debug(f"Could not get user/device for transaction: {e}")

            # Create transaction
            txn_id = str(uuid.uuid4())
            edge_builder = (self.graph_service.client.V(from_id)
                .addE("TRANSACTS")
                .to(__.V(to_id))
                .property("txn_id", txn_id)
                .property("amount", round(amount, 2))
                .property("currency", "USD")
                .property("type", type)
                .property("method", "electronic_transfer")
                .property("location", random.choice(self.normal_locations))
                .property("timestamp", datetime.now().isoformat())
                .property("status", "completed")
                .property("gen_type", gen_type))
            
            # Add device_id if available
            if device_id:
                edge_builder = edge_builder.property("device_id", device_id)
            
            edge_id = edge_builder.id_().next()
            timestamp = datetime.now().isoformat()
            
            self.transaction_counter += 1
            logger.info(f"✅ Transaction {txn_id} stored in graph database with both sender and receiver edges")
            
            # Write to KV store (dual-write)
            if self.kv and self.kv.is_connected():
                try:
                    txn_data = {
                        "txn_id": txn_id,
                        "amount": round(amount, 2),
                        "type": type,
                        "method": "electronic_transfer",
                        "location": random.choice(self.normal_locations),
                        "timestamp": timestamp,
                        "status": "completed",
                        "device_id": device_id,
                    }
                    
                    # Sender's outgoing transaction
                    self.kv.store_transaction(
                        from_id,
                        {**txn_data, "counterparty": to_id, "user_id": sender_user_id, "counterparty_user_id": receiver_user_id},
                        direction="out"
                    )
                    
                    # Receiver's incoming transaction
                    self.kv.store_transaction(
                        to_id,
                        {**txn_data, "counterparty": from_id, "user_id": receiver_user_id, "counterparty_user_id": sender_user_id},
                        direction="in"
                    )
                    logger.info(f"✅ Transaction {txn_id} also stored in KV store")
                except Exception as e:
                    logger.warning(f"Failed to write transaction to KV: {e}")
                       
            # Run fraud detection
            try:
                self.fraud_service.run_fraud_detection(edge_id, txn_id)
                logger.info(f"✅ {gen_type} transaction created: {txn_id} from {from_id} to {to_id} amount {amount}")
            except Exception as e:
                raise Exception(f"Error running fraud detection: {e}")
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Error creating manual transaction: {e}")
            return {"success": False, "error": f"❌ Error creating manual transaction: {e}"}

    def generate_transaction(self) -> Dict[str, Any]:
        """Generate a normal transaction between real users and accounts"""
        try:
            if len(self.account_vertices) < 1:
                self.account_vertices = self.graph_service.client.V().has_label("account").id_().to_list()
            if len(self.account_vertices) < 1:
                raise Exception("No accounts available")
            
            # Get 2 random accounts from the graph database
            sender_account_id, receiver_account_id = random.sample(self.account_vertices, 2)
            
            if not sender_account_id or not receiver_account_id:
                logger.error("Could not get accounts from graph database. Cannot generate transaction without valid accounts.")
                raise Exception("No valid accounts available in graph database for transaction generation")
            
            # Generate transaction data
            amount = random.uniform(100.0, 15000.0)
            transaction_type = random.choice(["transfer", "payment", "deposit", "withdrawal"])
            
            self.create_manual_transaction(sender_account_id, receiver_account_id, amount, transaction_type, "AUTO")
        
        except Exception as e:
            logger.error(f"Error generating normal transaction: {e}")
            raise e
        
    def _validate_account_exists(self, account_id: str) -> bool:
        """Validate that an account exists in the graph database"""
        try:
            if self.graph_service.client:
                accounts = self.graph_service.client.V(str(account_id)).to_list()
                return len(accounts) > 0
            return False
        
        except Exception as e:
            logger.error(f"Error validating account {account_id}: {e}")
            return False


    # ----------------------------------------------------------------------------------------------------------
    # Transaction generation statistics
    # ----------------------------------------------------------------------------------------------------------


    def get_recent_transactions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent transactions generated by this service"""
        return self.generated_transactions[-limit:]

    def get_status(self) -> Dict[str, Any]:
        """Get current status of transaction generation"""
        return {
            "status": "running" if self.is_running else "stopped",
            "generation_rate": self.generation_rate,
            "total_generated": len(self.generated_transactions),
            "transaction_count": self.transaction_counter,
            "last_10_transactions": self.get_recent_transactions(10),
            "start_time": self.start_time
        }

    def get_generation_stats(self) -> Dict[str, Any]:
        """Get detailed generation statistics"""
        return {
            "is_running": self.is_running,
            "generation_rate": self.generation_rate,
            "total_generated": len(self.generated_transactions),
            "transaction_count": self.transaction_counter,
            "start_time": self.start_time
        }


    # ----------------------------------------------------------------------------------------------------------
    # Logging functions
    # ----------------------------------------------------------------------------------------------------------


    def _log_transaction(self, transaction: Dict[str, Any], transaction_type: str = "TRANSACTION"):
        """Log transaction details to appropriate log files"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create detailed log message
        log_data = {
            "timestamp": timestamp,
            "transaction_id": transaction['id'],
            "user_id": transaction.get('user_id', ''),
            "account_id": transaction['sndr_id'],
            "receiver_user_id": transaction.get('recr_user_id'),
            "receiver_account_id": transaction.get('receiver_account_id'),
            "amount": transaction['amount'],
            "currency": transaction['currency'],
            "transaction_type": transaction['txn_type'],

            "location": transaction['location'],
            "status": transaction['status']
        }
        
        # Log to main transaction log
        logger.info(f"{transaction_type}: {json.dumps(log_data, indent=2)}")
        
        # Log basic transaction info
        transaction_log_msg = f"ID: {transaction['id']} | Amount: ₹{transaction['amount']} | Type: {transaction['transaction_type']} | Location: {transaction['location']}"
        logger.info(f"TRANSACTION: {transaction_log_msg}")

    def _log_statistics(self):
        """Log current statistics"""
        stats_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_transactions": self.transaction_counter,
            "generation_rate": self.generation_rate,
            "is_running": self.is_running
        }
        
        stats_logger.info(f"STATISTICS: {json.dumps(stats_data, indent=2)}")