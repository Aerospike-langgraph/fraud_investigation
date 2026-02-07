from fastapi import FastAPI, HTTPException, Query, Path, Body, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime
import urllib.parse
import tempfile
import zipfile
import os
import shutil
import json
import asyncio

from sse_starlette.sse import EventSourceResponse

from services.fraud_service import FraudService
from services.graph_service import GraphService
from services.transaction_generator import TransactionGeneratorService
from services.performance_monitor import performance_monitor
from services.flagged_account_service import FlaggedAccountService
from services.scheduler_service import scheduler_service
from services.aerospike_service import aerospike_service
from services.investigation_service import InvestigationService
from services.gremlin_loader import GremlinDataLoader
from services.feature_service import FeatureService
from services.transaction_injector import TransactionInjector

from logging_config import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger('fraud_detection.api')

# Initialize services
graph_service = GraphService()
fraud_service = FraudService(graph_service)
transaction_generator = TransactionGeneratorService(graph_service, fraud_service)
flagged_account_service = FlaggedAccountService(graph_service)
investigation_service: Optional[InvestigationService] = None
feature_service: Optional[FeatureService] = None
transaction_injector: Optional[TransactionInjector] = None

# Configuration variables
max_generation_rate = 50  # Default max rate, can be changed via API

@asynccontextmanager
async def lifespan(app: FastAPI):
    global investigation_service, feature_service, transaction_injector
    
    # Startup
    logger.info("Starting Fraud Detection API")
    graph_service.connect()
    
    # Connect to Aerospike KV store
    if aerospike_service.connect():
        logger.info("Aerospike KV service connected")
        # Update flagged account service to use Aerospike
        flagged_account_service.set_aerospike_service(aerospike_service)
        
        # Initialize feature service
        feature_service = FeatureService(aerospike_service, graph_service)
        flagged_account_service.set_feature_service(feature_service)
        logger.info("Feature service initialized")
        
        # Initialize transaction injector
        transaction_injector = TransactionInjector(graph_service, aerospike_service)
        logger.info("Transaction injector initialized")
    else:
        logger.warning("Aerospike KV service not available, using file-based storage")
    
    # Initialize investigation service
    ollama_base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    ollama_model = os.environ.get("OLLAMA_MODEL", "mistral")
    
    investigation_service = InvestigationService(
        aerospike_service=aerospike_service,
        graph_service=graph_service,
        ollama_base_url=ollama_base_url,
        ollama_model=ollama_model
    )
    
    try:
        await investigation_service.initialize()
        logger.info("Investigation service initialized")
    except Exception as e:
        logger.warning(f"Investigation service initialization warning: {e}")
    
    # Setup scheduler with detection callback
    scheduler_service.set_detection_callback(flagged_account_service.run_detection)
    scheduler_service.start()
    
    # Schedule detection job based on config
    config = flagged_account_service.get_config()
    if config.get("schedule_enabled", True):
        try:
            schedule_time = config.get("schedule_time", "21:30")
            hour, minute = map(int, schedule_time.split(":"))
            scheduler_service.schedule_detection_job(hour, minute)
            logger.info(f"Detection job scheduled for {schedule_time}")
        except Exception as e:
            logger.error(f"Failed to schedule detection job: {e}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Fraud Detection API")
    scheduler_service.shutdown()
    
    if investigation_service:
        await investigation_service.close()
    
    aerospike_service.close()
    graph_service.close()

app = FastAPI(
    title="Fraud Detection API",
    description="REST API for fraud detection using Aerospike Graph",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------------------------------------------
# Health check endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/")
def root():
    """Health check endpoint"""
    return {"message": "Fraud Detection API is running", "status": "healthy"}


@app.head("/health")
def docker_health_check():
    """Docker health check endpoint"""
    return True


@app.get("/health")
def health_check():
    """Detailed health check endpoint"""
    graph_status = "connected" if graph_service.client else "error"
    return {
        "status": "healthy",
        "graph_connection": graph_status,
        "timestamp": datetime.now().isoformat()
    }


# ----------------------------------------------------------------------------------------------------------
# Dashboard endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/dashboard/stats")
def get_dashboard_stats():
    """Get dashboard statistics"""
    try:
        return graph_service.get_dashboard_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard stats: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# User endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/users")
def get_users(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int | None = Query(None, ge=1, le=100, description="Number of users per page"),
    order_by: str = Query('name', description="Field to order results by"),
    order: str = Query('asc', description="Direction to order results"),
    query: str | None = Query(None, description="Search term for user name or ID")
):
    """Get paginated list of all users"""
    try:
        return graph_service.search("user", page, page_size, order_by, order, query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get users: {str(e)}")


@app.get("/users/stats")
def get_users_stats():
    """Get user stats"""
    try:
        return graph_service.get_user_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get users: {str(e)}")


@app.get("/users/{user_id}")
def get_user(user_id: str):
    """Get user's profile, connected accounts, and transaction summary"""
    try:
        user_summary = graph_service.get_user_summary(user_id)
        if not user_summary:
            raise HTTPException(status_code=404, detail="User not found")
        return user_summary
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user summary: {str(e)}")


@app.get("/users/{user_id}/accounts")
def get_user_accounts(user_id: str):
    """Get all accounts for a specific user"""
    try:
        accounts = graph_service.get_user_accounts(user_id)
        if not accounts:
            raise HTTPException(status_code=404, detail="User not found")
        return {"user_id": user_id, "accounts": accounts}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user accounts: {str(e)}")


@app.get("/users/{user_id}/devices")
def get_user_devices(user_id: str):
    """Get all devices for a specific user"""
    try:
        devices = graph_service.get_user_devices(user_id)
        if not devices:
            raise HTTPException(status_code=404, detail="User not found")
        return {"user_id": user_id, "devices": devices}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user devices: {str(e)}")


@app.get("/users/{user_id}/transactions")
def get_user_transactions(
    user_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of transactions per page")
):
    """Get paginated list of transactions for a specific user"""
    try:
        transactions = graph_service.get_user_transactions(user_id, page, page_size)
        if not transactions:
            raise HTTPException(status_code=404, detail="User not found")
        return transactions
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user transactions: {str(e)}")


@app.get("/users/{user_id}/connected-devices")
def get_user_connected_devices(user_id: str = Path(..., description="User ID")):
    """Get users who share devices with the specified user"""
    try:
        connected_users = graph_service.get_user_connected_devices(user_id)
        return {
            "user_id": user_id,
            "connected_users": connected_users,
            "total_connections": len(connected_users)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get connected device users: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Transaction endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/transactions")
def get_transactions(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(12, ge=1, le=100, description="Number of transactions per page"),
    order_by: str = Query('name', description="Field to order results by"),
    order: str = Query('asc', description="Direction to order results"),
    query: str | None = Query(None, description="Search term for user name or ID")
):
    """Get paginated list of all transactions"""
    try:
        results = graph_service.search("txns", page, page_size, order_by, order, query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get transactions: {str(e)}")


@app.delete("/transactions")
def delete_all_transactions():
    """Delete all transactions from the graph"""
    try:
        result = graph_service.drop_all_transactions()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop all transactions: {str(e)}")


@app.get("/transactions/stats")
def get_transaction_stats():
    """Get transaction stats"""
    try:
        results = graph_service.get_transaction_stats()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get users: {str(e)}")


@app.get("/transactions/flagged")
def get_flagged_transactions(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(12, ge=1, le=100, description="Number of transactions per page")
):
    """Get paginated list of transactions that have been flagged by fraud detection"""
    try:
        results = graph_service.get_flagged_transactions_paginated(page, page_size)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get flagged transactions: {str(e)}")


@app.get("/transaction/{transaction_id}")
def get_transaction_detail(transaction_id: str):
    """Get transaction details and related entities"""
    try:
        transaction_detail = graph_service.get_transaction_summary(urllib.parse.unquote(transaction_id))
        if not transaction_detail:
            raise HTTPException(status_code=404, detail="Transaction not found")
        return transaction_detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get transaction detail: {str(e)}")
        

# ----------------------------------------------------------------------------------------------------------
# Transaction generation endpoints
# ----------------------------------------------------------------------------------------------------------


@app.post("/transaction-generation/generate")
def generate_random_transaction():
    try:
        transaction_generator.generate_transaction()
        return True
    
    except Exception as e:
        logger.error(f"❌ Failed to generate transaction: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate transaction: {str(e)}")


@app.post("/transaction-generation/start")
def start_transaction_generation(
    rate: int = Query(1, ge=1, description="Generation rate (transactions per second)"),
    start: str = Query("", description="Generation start time")
):
    """Start transaction generation at specified rate"""
    try:
        max_generation_rate = transaction_generator.get_max_transaction_rate()
        
        # Validate rate against dynamic max
        if rate > max_generation_rate:
            raise HTTPException(
                status_code=400,
                detail=f"Generation rate {rate} exceeds maximum allowed rate of {max_generation_rate}"
            )
        
        success = transaction_generator.start_generation(rate, start)
        if success:
            logger.info(f"🎯 Transaction generation started at {rate} transactions/second")
            return {
                "message": f"Transaction generation started at {rate} transactions/second",
                "status": "started",
                "rate": rate,
                "max_rate": max_generation_rate
            }
        else:
            raise HTTPException(status_code=400, detail="Transaction generation is already running")
    except Exception as e:
        logger.error(f"❌ Failed to start transaction generation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start transaction generation: {str(e)}")


@app.post("/transaction-generation/stop")
def stop_transaction_generation():
    """Stop transaction generation"""
    try:
        success = transaction_generator.stop_generation()
        if success:
            logger.info("🛑 Transaction generation stopped")
            return {
                "message": "Transaction generation stopped",
                "status": "stopped"
            }
        else:
            raise HTTPException(status_code=400, detail="Transaction generation is not running")
    except Exception as e:
        logger.error(f"❌ Failed to stop transaction generation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop transaction generation: {str(e)}")


@app.post("/transaction-generation/manual")
def create_manual_transaction(
    from_account_id: str = Query(..., description="Source account ID"),
    to_account_id: str = Query(..., description="Destination account ID"), 
    amount: float = Query(..., gt=0, description="Transaction amount"),
    transaction_type: str = Query("transfer", description="Transaction type")
):
    """Create a manual transaction between specific accounts"""
    try:
        logger.info(f"Attempting to create manual transaction from {from_account_id} to {to_account_id} amount {amount}")
        result = transaction_generator.create_manual_transaction(
            from_id=from_account_id,
            to_id=to_account_id,
            amount=amount,
            type=transaction_type,
            gen_type="MANUAL"
        )
        
        if result:
            logger.info(f"✅ Transaction created")
            return {
                "message": "Transaction created successfully",
            }
        else:
            logger.error("❌ Failed to create manual transaction")
            raise HTTPException(status_code=400, detail="Failed to create transaction")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to create manual transaction: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create manual transaction: {str(e)}")


# Max Rate Configuration Endpoints
@app.get("/transaction-generation/max-rate")
def get_max_generation_rate():
    """Get the current maximum transaction generation rate"""
    max_generation_rate = transaction_generator.get_max_transaction_rate()
    return {
        "max_rate": max_generation_rate,
        "message": f"Maximum allowed transaction generation rate: {max_generation_rate} transactions/second"
    }


@app.post("/transaction-generation/max-rate")
def set_max_generation_rate(
    new_max_rate: int = Query(..., ge=1, description="New maximum generation rate (minimum 1)")
):
    """Set the maximum transaction generation rate"""
    try:
        success = transaction_generator.set_max_transaction_rate(new_max_rate)
        if success:
            return {
                "max_rate": new_max_rate,
                "message": f"Maximum generation rate updated to {new_max_rate} transactions/second"
            }
        else:
            return {
                "message": f"Maximum generation rate unable to be updated to {new_max_rate} transactions/second"
            }
        
    except Exception as e:
        logger.error(f"❌ Failed to update max generation rate: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update max generation rate: {str(e)}")


@app.get("/transaction-generation/status")
async def get_transaction_generation_status():
    """Get current transaction generation status"""
    try:
        status = transaction_generator.get_status()
        return status
    except Exception as e:
        logger.error(f"❌ Failed to get transaction generation status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Account endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/accounts")
def get_all_accounts():
    """Get all accounts for manual transaction dropdowns"""
    try:
        accounts = graph_service.get_all_accounts()
        return { "accounts": accounts }
    except Exception as e:
        logger.error(f"❌ Failed to get accounts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get accounts: {str(e)}")


@app.get("/accounts/flagged")
async def get_flagged_accounts():
    """Get list of all flagged accounts"""
    try:
        flagged_accounts = await graph_service.get_flagged_accounts()
        return {
            "flagged_accounts": flagged_accounts,
            "count": len(flagged_accounts)
        }
    except Exception as e:
        logger.error(f"❌ Failed to get flagged accounts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get flagged accounts: {str(e)}")
    

@app.post("/accounts/{account_id}/flag")
async def flag_account(account_id: str, reason: str = "Manual flag for testing"):
    """Flag an account as fraudulent for RT1 testing"""
    try:
        result = await graph_service.flag_account(account_id, reason)
        if result:
            return {
                "message": f"Account {account_id} flagged successfully",
                "account_id": account_id,
                "reason": reason,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=404, detail="Account not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to flag account {account_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to flag account: {str(e)}")


@app.delete("/accounts/{account_id}/flag")
async def unflag_account(account_id: str):
    """Remove fraud flag from an account"""
    try:
        result = await graph_service.unflag_account(account_id)
        if result:
            return {
                "message": f"Account {account_id} unflagged successfully",
                "account_id": account_id,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=404, detail="Account not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to unflag account {account_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to unflag account: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Performance monitoring endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/performance/stats")
def get_performance_stats(time_window: int = Query(5, ge=1, le=60, description="Time window in minutes")):
    """Get performance statistics for all fraud detection methods"""
    try:
        stats = performance_monitor.get_all_stats(time_window)
        return {
            "performance_stats": stats,
            "time_window_minutes": time_window,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"❌ Failed to get performance stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get performance stats: {str(e)}")


@app.get("/performance/timeline")
def get_performance_timeline(minutes: int = Query(5, ge=1, le=60, description="Timeline window in minutes")):
    """Get timeline data for performance charts"""
    try:
        timeline_data = performance_monitor.get_recent_timeline_data(minutes)
        return {
            "timeline_data": timeline_data,
            "time_window_minutes": minutes,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"❌ Failed to get performance timeline: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get performance timeline: {str(e)}")


@app.post("/performance/reset")
def reset_performance_metrics():
    """Reset all performance metrics"""
    try:
        performance_monitor.reset_metrics()
        return {
            "message": "Performance metrics reset successfully",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"❌ Failed to reset performance metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reset performance metrics: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Bulk loading endpoints
# ----------------------------------------------------------------------------------------------------------


@app.post("/bulk-load-csv")
def bulk_load_csv_data(
    vertices_path: Optional[str] = None, 
    edges_path: Optional[str] = None,
    load_graph: bool = True,
    load_aerospike: bool = True
):
    """
    Bulk load data from CSV files.
    
    By default loads to both:
    - Aerospike Graph (vertices and edges)
    - Aerospike KV (users for risk evaluation tracking)
    
    This simulates receiving account data from a third-party system.
    
    Args:
        vertices_path: Path to vertices CSV directory
        edges_path: Path to edges CSV directory
        load_graph: Load data into Aerospike Graph (default: True)
        load_aerospike: Load users into Aerospike KV for tracking (default: True)
    """
    result = {
        "success": True,
        "graph": None,
        "aerospike": None,
        "message": ""
    }
    
    try:
        # Load to Graph DB
        if load_graph:
            graph_result = graph_service.bulk_load_csv_data(vertices_path, edges_path)
            result["graph"] = graph_result
            
            if not graph_result["success"]:
                result["success"] = False
                result["message"] = f"Graph load failed: {graph_result.get('error', 'Unknown error')}"
        
        # Load users to Aerospike KV
        if load_aerospike:
            if aerospike_service.is_connected():
                # Determine the CSV path for users
                users_csv_path = None
                if vertices_path:
                    users_csv_path = f"{vertices_path}/users/users.csv"
                
                aerospike_result = aerospike_service.load_users_from_csv(users_csv_path)
                result["aerospike"] = aerospike_result
                
                if not aerospike_result["success"]:
                    # Don't fail entire operation, just note the error
                    logger.warning(f"Aerospike load warning: {aerospike_result.get('message', 'Unknown error')}")
            else:
                result["aerospike"] = {
                    "success": False,
                    "message": "Aerospike KV service not available - skipped",
                    "loaded": 0
                }
                logger.warning("Aerospike KV not available, skipping user load")
        
        # Build summary message
        messages = []
        if load_graph and result["graph"]:
            if result["graph"]["success"]:
                stats = result["graph"].get("statistics", {})
                messages.append(f"Graph: {stats.get('users', 0)} users, {stats.get('accounts', 0)} accounts")
            else:
                messages.append(f"Graph: Failed")
        
        if load_aerospike and result["aerospike"]:
            if result["aerospike"]["success"]:
                messages.append(f"Aerospike KV: {result['aerospike'].get('loaded', 0)} users")
            else:
                messages.append(f"Aerospike KV: {result['aerospike'].get('message', 'Failed')}")
        
        result["message"] = " | ".join(messages) if messages else "No operations performed"
        
        return result
        
    except Exception as e:
        logger.error(f"Bulk load failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to bulk load data: {str(e)}")

@app.get("/bulk-load-status")
def get_bulk_load_status():
    """Get the status of the current bulk load operation"""
    try:
        result = graph_service.get_bulk_load_status()
        
        if result["success"]:
            return result
        else:
            return {
                "message": result["message"],
                "error": result["error"],
                "status": None
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get bulk load status: {str(e)}")


@app.post("/bulk-load-gremlin")
def bulk_load_gremlin_data(
    vertices_path: Optional[str] = None,
    edges_path: Optional[str] = None,
    sync_kv: bool = True
):
    """
    Load data using Gremlin queries (bypasses buggy Aerospike Graph bulk loader).
    
    This method:
    - Loads vertices and edges to graph with correct labels
    - Drops fraud_flag on load (flags are computed by ML)
    - Syncs data to KV store (users with nested accounts/devices maps)
    """
    result = {
        "success": True,
        "graph": None,
        "kv_sync": None,
        "message": ""
    }
    
    try:
        # Default paths
        if not vertices_path:
            vertices_path = "/data/graph_csv/vertices"
        if not edges_path:
            edges_path = "/data/graph_csv/edges"
        
        # Use Gremlin loader with KV sync
        kv_service = aerospike_service if aerospike_service.is_connected() else None
        loader = GremlinDataLoader(graph_service, kv_service)
        graph_result = loader.load_all_data(vertices_path, edges_path, sync_kv=sync_kv)
        result["graph"] = graph_result
        result["kv_sync"] = graph_result.get("kv_sync")
        
        if not graph_result["success"]:
            result["success"] = False
            result["message"] = f"Gremlin load failed: {graph_result.get('errors', [])}"
        
        # Build message
        if result["success"]:
            v = graph_result.get("vertices", {})
            kv = graph_result.get("kv_sync", {})
            result["message"] = (f"Graph: {v.get('users', 0)} users, {v.get('accounts', 0)} accounts, "
                               f"{v.get('devices', 0)} devices. KV: {kv.get('users', 0)} users synced.")
        
        return result
        
    except Exception as e:
        logger.error(f"Gremlin bulk load failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load data: {str(e)}")


@app.post("/inject-historical-transactions")
def inject_historical_transactions(
    transaction_count: int = Query(10000, ge=100, le=100000, description="Total transactions to generate"),
    spread_days: int = Query(30, ge=1, le=365, description="Days to spread transactions over"),
    fraud_percentage: float = Query(0.15, ge=0.0, le=0.5, description="Percentage of fraudulent transactions")
):
    """
    Inject historical transactions with fraud patterns for testing.
    
    Transactions are written to both Graph (TRANSACTS edges) and KV (transactions set).
    Includes fraud patterns:
    - Fraud rings (40%): Tight-knit groups with high velocity
    - Velocity anomalies (25%): Single accounts with burst activity  
    - Amount anomalies (20%): High-value outlier transactions
    - New account fraud (15%): Immediate activity after creation
    """
    if not transaction_injector:
        raise HTTPException(status_code=503, detail="Transaction injector not available. Aerospike may not be connected.")
    
    try:
        result = transaction_injector.inject_historical_transactions(
            transaction_count=transaction_count,
            spread_days=spread_days,
            fraud_percentage=fraud_percentage
        )
        return result
    except Exception as e:
        logger.error(f"Transaction injection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to inject transactions: {str(e)}")


@app.post("/compute-features")
def compute_features(
    window_days: int = Query(7, ge=1, le=90, description="Sliding window in days")
):
    """
    Compute account and device features from transaction data.
    
    This runs the feature computation job which:
    - Reads transactions from KV transactions set
    - Computes 15 account features and 5 device features
    - Stores results in account-fact and device-fact sets
    
    Should be run before ML detection for accurate scoring.
    """
    if not feature_service:
        raise HTTPException(status_code=503, detail="Feature service not available. Aerospike may not be connected.")
    
    try:
        result = feature_service.run_feature_computation_job(window_days=window_days)
        return result
    except Exception as e:
        logger.error(f"Feature computation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute features: {str(e)}")


@app.delete("/delete-all-data")
def delete_all_data(confirm: bool = Query(False, description="Must be True to confirm deletion")):
    """
    Delete all data from both Graph and KV stores.
    
    This is a destructive operation that:
    - Truncates all KV sets (users, transactions, account-fact, device-fact, flagged_accounts)
    - Drops all graph vertices (which also removes edges)
    
    Use with caution!
    """
    if not confirm:
        raise HTTPException(status_code=400, detail="Must set confirm=True to delete all data")
    
    result = {
        "kv_truncated": {},
        "graph_cleared": False,
        "errors": []
    }
    
    try:
        # Truncate KV sets
        if aerospike_service.is_connected():
            result["kv_truncated"] = aerospike_service.truncate_all_data()
            logger.info(f"KV sets truncated: {result['kv_truncated']}")
        else:
            result["errors"].append("Aerospike not connected")
        
        # Clear graph
        if graph_service.client:
            try:
                graph_service.client.V().drop().iterate()
                result["graph_cleared"] = True
                logger.info("Graph vertices dropped")
            except Exception as e:
                result["errors"].append(f"Graph drop failed: {str(e)}")
        else:
            result["errors"].append("Graph not connected")
        
        return result
        
    except Exception as e:
        logger.error(f"Delete all data failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete data: {str(e)}")


@app.get("/kv-stats")
def get_kv_stats():
    """
    Get statistics about data stored in KV sets.
    
    Returns counts for:
    - users
    - flagged_accounts
    - account_facts
    - device_facts
    - transaction_records
    """
    if not aerospike_service.is_connected():
        raise HTTPException(status_code=503, detail="Aerospike not connected")
    
    return aerospike_service.get_stats()


@app.post("/bulk-load-upload")
async def bulk_load_upload(
    file: UploadFile = File(...),
    load_graph: bool = Form(True),
    load_aerospike: bool = Form(True)
):
    """
    Upload and bulk load data from a ZIP file.
    
    The ZIP file should contain:
    - vertices/users/users.csv
    - vertices/accounts/accounts.csv  
    - vertices/devices/devices.csv
    - edges/ownership/owns.csv
    - edges/usage/uses.csv
    
    Args:
        file: ZIP file containing CSV data
        load_graph: Load data into Aerospike Graph (default: True)
        load_aerospike: Load users into Aerospike KV for tracking (default: True)
    """
    result = {
        "success": True,
        "graph": None,
        "aerospike": None,
        "message": ""
    }
    
    temp_dir = None
    
    try:
        # Validate file type
        if not file.filename or not file.filename.endswith('.zip'):
            raise HTTPException(status_code=400, detail="File must be a ZIP archive")
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix="bulk_load_")
        zip_path = os.path.join(temp_dir, "upload.zip")
        extract_dir = os.path.join(temp_dir, "extracted")
        
        # Save uploaded file
        with open(zip_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Extract ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        logger.info(f"Extracted uploaded ZIP to {extract_dir}")
        
        # Find the vertices and edges directories
        vertices_path = None
        edges_path = None
        
        for root, dirs, files in os.walk(extract_dir):
            if 'vertices' in dirs:
                vertices_path = os.path.join(root, 'vertices')
            if 'edges' in dirs:
                edges_path = os.path.join(root, 'edges')
        
        if not vertices_path or not edges_path:
            raise HTTPException(
                status_code=400, 
                detail="ZIP file must contain 'vertices' and 'edges' directories"
            )
        
        # Load to Graph DB
        if load_graph:
            graph_result = graph_service.bulk_load_csv_data(vertices_path, edges_path)
            result["graph"] = graph_result
            
            if not graph_result["success"]:
                result["success"] = False
                result["message"] = f"Graph load failed: {graph_result.get('error', 'Unknown error')}"
        
        # Load users to Aerospike KV
        if load_aerospike:
            if aerospike_service.is_connected():
                users_csv_path = os.path.join(vertices_path, "users", "users.csv")
                
                if os.path.exists(users_csv_path):
                    aerospike_result = aerospike_service.load_users_from_csv(users_csv_path)
                    result["aerospike"] = aerospike_result
                    
                else:
                    result["aerospike"] = {
                        "success": False,
                        "message": "users.csv not found in uploaded data",
                        "loaded": 0
                    }
            else:
                result["aerospike"] = {
                    "success": False,
                    "message": "Aerospike KV service not available - skipped",
                    "loaded": 0
                }
        
        # Build summary message
        messages = []
        if load_graph and result["graph"]:
            if result["graph"]["success"]:
                stats = result["graph"].get("statistics", {})
                messages.append(f"Graph: {stats.get('users', 0)} users, {stats.get('accounts', 0)} accounts")
            else:
                messages.append(f"Graph: Failed")
        
        if load_aerospike and result["aerospike"]:
            if result["aerospike"]["success"]:
                messages.append(f"Aerospike KV: {result['aerospike'].get('loaded', 0)} users")
            else:
                messages.append(f"Aerospike KV: {result['aerospike'].get('message', 'Failed')}")
        
        result["message"] = " | ".join(messages) if messages else "No operations performed"
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bulk load upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process uploaded file: {str(e)}")
    finally:
        # Cleanup temporary directory
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")


@app.post("/load-users-aerospike")
def load_users_to_aerospike(csv_path: Optional[str] = None):
    """Load users from CSV into Aerospike key-value store for risk evaluation"""
    try:
        if not aerospike_service.is_connected():
            raise HTTPException(status_code=503, detail="Aerospike KV service not available")
        
        result = aerospike_service.load_users_from_csv(csv_path)
        
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=500, detail=result["message"])
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to load users to Aerospike: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load users: {str(e)}")


@app.get("/aerospike/stats")
def get_aerospike_stats():
    """Get statistics about data stored in Aerospike"""
    try:
        if not aerospike_service.is_connected():
            return {
                "connected": False,
                "message": "Aerospike KV service not available"
            }
        
        stats = aerospike_service.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Failed to get Aerospike stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Flagged Accounts Detection endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/flagged-accounts")
def get_flagged_accounts_list(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of accounts per page"),
    status: Optional[str] = Query(None, description="Filter by status (pending_review, under_investigation, confirmed_fraud, cleared)"),
    search: Optional[str] = Query(None, description="Search by account holder name or ID")
):
    """Get paginated list of flagged accounts detected by the ML model"""
    try:
        result = flagged_account_service.get_flagged_accounts(page, page_size, status, search)
        return result
    except Exception as e:
        logger.error(f"❌ Failed to get flagged accounts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get flagged accounts: {str(e)}")


@app.get("/flagged-accounts/stats")
def get_flagged_accounts_stats():
    """Get statistics for flagged accounts"""
    try:
        stats = flagged_account_service.get_flagged_stats()
        return stats
    except Exception as e:
        logger.error(f"❌ Failed to get flagged accounts stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@app.get("/flagged-accounts/{account_id}")
def get_flagged_account_detail(account_id: str = Path(..., description="Account ID")):
    """Get details of a specific flagged account"""
    try:
        account = flagged_account_service.get_flagged_account(account_id)
        if not account:
            raise HTTPException(status_code=404, detail="Flagged account not found")
        return account
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get flagged account {account_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get flagged account: {str(e)}")


@app.post("/flagged-accounts/{account_id}/resolve")
def resolve_flagged_account(
    account_id: str = Path(..., description="Account ID"),
    resolution: str = Query(..., description="Resolution: confirmed_fraud or cleared"),
    notes: str = Query("", description="Resolution notes")
):
    """Resolve a flagged account as confirmed fraud or cleared"""
    try:
        if resolution not in ["confirmed_fraud", "cleared", "under_investigation"]:
            raise HTTPException(status_code=400, detail="Invalid resolution. Must be 'confirmed_fraud', 'cleared', or 'under_investigation'")
        
        result = flagged_account_service.resolve_flagged_account(account_id, resolution, notes)
        if not result:
            raise HTTPException(status_code=404, detail="Flagged account not found")
        
        return {
            "message": f"Account {account_id} resolved as {resolution}",
            "account": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to resolve flagged account {account_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to resolve account: {str(e)}")


@app.post("/accounts/{account_id}/resolve")
def resolve_individual_account(
    account_id: str = Path(..., description="Account ID (e.g., A000401)"),
    resolution: str = Query(..., description="Resolution: confirmed_fraud or cleared"),
    notes: str = Query("", description="Resolution notes")
):
    """
    Resolve an individual account (account-level, not user-level).
    
    When confirmed_fraud:
    - Updates account's fraud_flag in Graph DB
    - Updates account-fact in KV with fraud=True
    - Flags all devices used in this account's transactions in both Graph and KV
    
    When cleared:
    - Updates account's fraud_flag=False in Graph DB
    - Updates account-fact in KV with fraud=False
    
    This endpoint is used by the fraud investigation review workflow to make
    per-account fraud decisions after AI investigation.
    """
    try:
        if resolution not in ["confirmed_fraud", "cleared"]:
            raise HTTPException(
                status_code=400, 
                detail="Invalid resolution. Must be 'confirmed_fraud' or 'cleared'"
            )
        
        result = flagged_account_service.resolve_account(account_id, resolution, notes)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to resolve account: {', '.join(result.get('errors', ['Unknown error']))}"
            )
        
        return {
            "message": f"Account {account_id} resolved as {resolution}",
            "result": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to resolve account {account_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to resolve account: {str(e)}")


@app.post("/flagged-accounts/detect")
def trigger_detection_job(
    skip_cooldown: bool = Query(False, description="Skip cooldown period and evaluate all users")
):
    """
    Manually trigger the flagged account detection job.
    
    By default, users evaluated within the cooldown period (7 days) are skipped.
    Set skip_cooldown=true to force evaluation of ALL users regardless of when they were last evaluated.
    """
    try:
        # Check if Aerospike KV is connected - required for risk evaluation
        if not aerospike_service.is_connected():
            raise HTTPException(
                status_code=503, 
                detail="Aerospike KV service is not available. Risk evaluation requires Aerospike to be connected."
            )
        
        cooldown_msg = " (skip cooldown)" if skip_cooldown else ""
        logger.info(f"Manual detection job triggered via API{cooldown_msg}")
        result = scheduler_service.run_detection_now(skip_cooldown=skip_cooldown)
        return {
            "message": "Detection job completed",
            "skip_cooldown": skip_cooldown,
            "result": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to run detection job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to run detection job: {str(e)}")


@app.delete("/flagged-accounts")
def clear_flagged_accounts():
    """Clear all flagged accounts (for testing/demo purposes)"""
    try:
        flagged_account_service.clear_flagged_accounts()
        return {"message": "All flagged accounts cleared"}
    except Exception as e:
        logger.error(f"❌ Failed to clear flagged accounts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear flagged accounts: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Detection Configuration endpoints
# ----------------------------------------------------------------------------------------------------------


@app.get("/detection/config")
def get_detection_config():
    """Get current detection configuration (schedule, cooldown, threshold)"""
    try:
        config = flagged_account_service.get_config()
        scheduler_status = scheduler_service.get_status()
        
        return {
            "config": config,
            "scheduler": scheduler_status
        }
    except Exception as e:
        logger.error(f"❌ Failed to get detection config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get config: {str(e)}")


@app.post("/detection/config")
def update_detection_config(
    schedule_enabled: Optional[bool] = Body(None, description="Enable/disable scheduled detection"),
    schedule_time: Optional[str] = Body(None, description="Schedule time in HH:MM format (24-hour)"),
    cooldown_days: Optional[int] = Body(None, ge=1, description="Cooldown period in days"),
    risk_threshold: Optional[float] = Body(None, ge=0, le=100, description="Risk score threshold for flagging")
):
    """Update detection configuration"""
    try:
        # Build config update dict
        config_update = {}
        if schedule_enabled is not None:
            config_update["schedule_enabled"] = schedule_enabled
        if schedule_time is not None:
            config_update["schedule_time"] = schedule_time
        if cooldown_days is not None:
            config_update["cooldown_days"] = cooldown_days
        if risk_threshold is not None:
            config_update["risk_threshold"] = risk_threshold
        
        # Update config
        new_config = flagged_account_service.update_config(config_update)
        
        # Update scheduler if schedule changed
        if schedule_time is not None or schedule_enabled is not None:
            if new_config.get("schedule_enabled"):
                try:
                    time_str = new_config.get("schedule_time", "21:30")
                    hour, minute = map(int, time_str.split(":"))
                    scheduler_service.schedule_detection_job(hour, minute)
                    logger.info(f"Detection job rescheduled for {time_str}")
                except Exception as e:
                    logger.error(f"Failed to reschedule detection job: {e}")
            else:
                scheduler_service.remove_detection_job()
                logger.info("Detection job disabled")
        
        return {
            "message": "Configuration updated",
            "config": new_config,
            "scheduler": scheduler_service.get_status()
        }
    except Exception as e:
        logger.error(f"❌ Failed to update detection config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update config: {str(e)}")


@app.get("/detection/history")
def get_detection_history(
    limit: int = Query(20, ge=1, le=100, description="Number of history records to return")
):
    """Get detection job run history"""
    try:
        history = flagged_account_service.get_detection_history(limit)
        return {
            "history": history,
            "total": len(history)
        }
    except Exception as e:
        logger.error(f"❌ Failed to get detection history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")


# ----------------------------------------------------------------------------------------------------------
# Investigation endpoints (LangGraph-powered fraud investigation)
# ----------------------------------------------------------------------------------------------------------


@app.get("/investigation/steps")
def get_investigation_steps():
    """Get list of investigation workflow steps"""
    try:
        if not investigation_service:
            raise HTTPException(status_code=503, detail="Investigation service not initialized")
        
        steps = investigation_service.get_workflow_steps()
        return {
            "steps": steps,
            "total": len(steps)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get investigation steps: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get steps: {str(e)}")


@app.get("/investigation/{user_id}/stream")
async def stream_investigation(
    user_id: str = Path(..., description="User ID to investigate"),
    investigation_id: Optional[str] = Query(None, description="Optional existing investigation ID")
):
    """
    SSE endpoint that streams investigation progress.
    
    Events:
    - start: Investigation started with workflow steps
    - trace: Node execution trace events
    - progress: State updates from each node
    - complete: Investigation completed
    - error: Error occurred
    """
    if not investigation_service:
        raise HTTPException(status_code=503, detail="Investigation service not initialized")
    
    def json_serializer(obj):
        """Custom JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
    
    async def event_generator():
        try:
            async for event in investigation_service.stream_investigation(user_id, investigation_id):
                event_type = event.get("event", "message")
                event_data = event.get("data", event)
                
                yield {
                    "event": event_type,
                    "data": json.dumps(event_data, default=json_serializer)
                }
        except Exception as e:
            logger.error(f"Investigation stream error: {e}")
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e)})
            }
    
    return EventSourceResponse(event_generator())


@app.post("/investigation/{user_id}/start")
async def start_investigation(
    user_id: str = Path(..., description="User ID to investigate"),
    triggered_by: str = Query("manual", description="What triggered the investigation")
):
    """Start a new investigation and return the investigation ID"""
    try:
        if not investigation_service:
            raise HTTPException(status_code=503, detail="Investigation service not initialized")
        
        investigation_id = await investigation_service.start_investigation(user_id, triggered_by)
        
        return {
            "investigation_id": investigation_id,
            "user_id": user_id,
            "triggered_by": triggered_by,
            "stream_url": f"/investigation/{user_id}/stream?investigation_id={investigation_id}"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to start investigation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start investigation: {str(e)}")


@app.get("/investigation/{investigation_id}/status")
def get_investigation_status(
    investigation_id: str = Path(..., description="Investigation ID")
):
    """Get current investigation status (for reconnection)"""
    try:
        if not investigation_service:
            raise HTTPException(status_code=503, detail="Investigation service not initialized")
        
        status = investigation_service.get_investigation_status(investigation_id)
        
        if not status:
            raise HTTPException(status_code=404, detail="Investigation not found")
        
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get investigation status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@app.get("/investigation/{investigation_id}/result")
def get_investigation_result(
    investigation_id: str = Path(..., description="Investigation ID")
):
    """Get completed investigation result"""
    try:
        if not investigation_service:
            raise HTTPException(status_code=503, detail="Investigation service not initialized")
        
        result = investigation_service.get_investigation_result(investigation_id)
        
        if not result:
            raise HTTPException(status_code=404, detail="Investigation result not found")
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get investigation result: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get result: {str(e)}")


@app.get("/investigation/{investigation_id}/report")
async def get_investigation_report(
    investigation_id: str = Path(..., description="Investigation ID")
):
    """Get the markdown report for an investigation"""
    try:
        if not investigation_service:
            raise HTTPException(status_code=503, detail="Investigation service not initialized")
        
        report = await investigation_service.get_investigation_report(investigation_id)
        
        if not report:
            raise HTTPException(status_code=404, detail="Investigation report not found")
        
        return {
            "investigation_id": investigation_id,
            "report": report
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get investigation report: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get report: {str(e)}")


@app.get("/investigation/user/{user_id}/history")
def get_user_investigation_history(
    user_id: str = Path(..., description="User ID")
):
    """Get investigation history for a user"""
    try:
        if not investigation_service:
            raise HTTPException(status_code=503, detail="Investigation service not initialized")
        
        history = investigation_service.get_user_investigation_history(user_id)
        
        return {
            "user_id": user_id,
            "investigations": history,
            "total": len(history)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get user investigation history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")


# ============================================================================
# DEMO / SUSPICIOUS ACTIVITY INJECTION
# ============================================================================

@app.post("/demo/inject-suspicious-activity")
def inject_suspicious_activity(
    transaction_count: int = Query(100, ge=10, le=10000, description="Number of transactions to create"),
    spread_days: int = Query(30, ge=1, le=365, description="Days to spread transactions over"),
    high_value_percentage: float = Query(10.0, ge=0, le=100, description="Percentage of high-value transactions"),
    seed_flagged_accounts: int = Query(3, ge=0, le=20, description="Number of accounts to flag as seed")
):
    """
    Inject suspicious demo activity to generate realistic fraud patterns.
    This creates transactions spread over time with high-value outliers.
    Separate from normal generator - does not affect regular transaction generation.
    """
    import random
    import uuid
    from datetime import datetime, timedelta
    from gremlin_python.process.graph_traversal import __
    
    try:
        logger.info(f"🚨 DEMO: Injecting suspicious activity - {transaction_count} transactions over {spread_days} days")
        
        # Get all accounts
        accounts = graph_service.client.V().hasLabel("account").id_().toList()
        if len(accounts) < 10:
            raise HTTPException(status_code=400, detail="Not enough accounts. Need at least 10 accounts.")
        
        created_txns = []
        high_value_count = int(transaction_count * high_value_percentage / 100)
        normal_count = transaction_count - high_value_count
        
        # Define time spread
        now = datetime.now()
        
        # Create normal transactions spread over time
        for i in range(normal_count):
            sender, receiver = random.sample(accounts, 2)
            
            # Spread timestamp over the past N days
            days_ago = random.uniform(0, spread_days)
            hours_offset = random.uniform(0, 24)
            timestamp = now - timedelta(days=days_ago, hours=hours_offset)
            
            # Normal amount ($100 - $5,000)
            amount = round(random.uniform(100, 5000), 2)
            txn_type = random.choice(["transfer", "payment", "deposit", "withdrawal"])
            
            txn_id = str(uuid.uuid4())
            try:
                graph_service.client.V(sender) \
                    .addE("TRANSACTS") \
                    .to(__.V(receiver)) \
                    .property("txn_id", txn_id) \
                    .property("amount", amount) \
                    .property("currency", "USD") \
                    .property("type", txn_type) \
                    .property("method", "electronic_transfer") \
                    .property("location", random.choice(["New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ"])) \
                    .property("timestamp", timestamp.isoformat()) \
                    .property("status", "completed") \
                    .property("gen_type", "DEMO_NORMAL") \
                    .iterate()
                created_txns.append({"txn_id": txn_id, "amount": amount, "type": "normal"})
            except Exception as e:
                logger.warning(f"Error creating normal txn: {e}")
        
        logger.info(f"✅ Created {len(created_txns)} normal transactions")
        
        # Create high-value suspicious transactions (recent, high amount)
        suspicious_accounts = random.sample(accounts, min(20, len(accounts)))
        for i in range(high_value_count):
            sender = random.choice(suspicious_accounts)
            receiver = random.choice(accounts)
            if sender == receiver:
                continue
            
            # Recent timestamp (last 3 days for high-value = suspicious velocity)
            days_ago = random.uniform(0, 3)
            hours_offset = random.uniform(0, 12)
            timestamp = now - timedelta(days=days_ago, hours=hours_offset)
            
            # High-value amount ($15,000 - $100,000)
            amount = round(random.uniform(15000, 100000), 2)
            txn_type = random.choice(["transfer", "wire_transfer"])
            
            txn_id = str(uuid.uuid4())
            try:
                graph_service.client.V(sender) \
                    .addE("TRANSACTS") \
                    .to(__.V(receiver)) \
                    .property("txn_id", txn_id) \
                    .property("amount", amount) \
                    .property("currency", "USD") \
                    .property("type", txn_type) \
                    .property("method", "wire_transfer") \
                    .property("location", random.choice(["Miami, FL", "Las Vegas, NV", "Atlantic City, NJ"])) \
                    .property("timestamp", timestamp.isoformat()) \
                    .property("status", "completed") \
                    .property("gen_type", "DEMO_HIGH_VALUE") \
                    .iterate()
                created_txns.append({"txn_id": txn_id, "amount": amount, "type": "high_value"})
            except Exception as e:
                logger.warning(f"Error creating high-value txn: {e}")
        
        logger.info(f"✅ Created {high_value_count} high-value transactions")
        
        # Seed some flagged accounts (bootstrap the detection)
        flagged_seeds = []
        if seed_flagged_accounts > 0 and aerospike_service.is_connected():
            # Find users associated with suspicious accounts
            for acc_id in random.sample(suspicious_accounts, min(seed_flagged_accounts, len(suspicious_accounts))):
                try:
                    # Get user who owns this account
                    user_id = graph_service.client.V(acc_id).in_("OWNS").id_().next()
                    
                    # Create flagged account record
                    flagged_record = {
                        "user_id": str(user_id),
                        "flagged_at": datetime.now().isoformat(),
                        "risk_score": random.randint(75, 95),
                        "risk_factors": ["high_value_transaction", "demo_seed"],
                        "status": "pending_review",
                        "detection_method": "demo_injection"
                    }
                    
                    aerospike_service.save_flagged_account(flagged_record)
                    flagged_seeds.append(user_id)
                    logger.info(f"✅ Seeded flagged account: {user_id}")
                except Exception as e:
                    logger.warning(f"Error seeding flagged account for {acc_id}: {e}")
        
        result = {
            "success": True,
            "message": f"Injected {len(created_txns)} demo transactions",
            "details": {
                "total_transactions": len(created_txns),
                "normal_transactions": sum(1 for t in created_txns if t["type"] == "normal"),
                "high_value_transactions": sum(1 for t in created_txns if t["type"] == "high_value"),
                "spread_days": spread_days,
                "flagged_seeds": len(flagged_seeds),
                "flagged_user_ids": flagged_seeds
            },
            "next_steps": [
                "Run 'Manual Detection' in Fraud Detection tab to detect flagged accounts",
                "High-value transactions will increase risk scores",
                "Seeded flagged accounts enable the 'flagged_connections' factor (30 points)"
            ]
        }
        
        logger.info(f"✅ DEMO: Injection complete - {result}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to inject suspicious activity: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to inject: {str(e)}")