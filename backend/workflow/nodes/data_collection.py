"""
Data Collection Node

Combined node that gathers initial evidence before the LLM agent takes over.
Merges functionality from account_deep_dive and timeline_reconstruction.
Collects minimal baseline data - the LLM agent decides if more is needed via tools.

Data Sources: Aerospike KV + Aerospike Graph
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from workflow.state import InvestigationState, InitialEvidence, TraceEvent

logger = logging.getLogger('investigation.data_collection')


def data_collection_node(
    state: InvestigationState,
    aerospike_service: Any,
    graph_service: Any
) -> Dict[str, Any]:
    """
    Collect initial evidence for LLM reasoning agent.
    
    Gathers baseline data:
    - User profile from KV
    - Account list from Graph
    - Device list (basic info)
    - Last 7 days transactions
    - 1-hop connections
    
    The LLM agent will request more data via tools if needed.
    
    Args:
        state: Current investigation state
        aerospike_service: Aerospike KV service instance
        graph_service: Aerospike Graph service instance
        
    Returns:
        Updated state with initial evidence
    """
    user_id = state["user_id"]
    node_name = "data_collection"
    
    logger.info(f"[{node_name}] Collecting initial data for user {user_id}")
    
    trace_events = []
    
    # Emit start event
    trace_events.append(TraceEvent(
        type="node_start",
        node=node_name,
        timestamp=datetime.now().isoformat(),
        data={"user_id": user_id}
    ))
    
    try:
        # 1. User Profile from KV
        user_profile = _get_user_profile(aerospike_service, user_id)
        
        # 2. Account list from Graph
        accounts = _get_user_accounts(graph_service, user_id)
        
        # 3. Device list (basic)
        devices = _get_user_devices(graph_service, user_id)
        
        # 4. Recent transactions (last 7 days - baseline)
        recent_transactions = _get_recent_transactions(graph_service, user_id, days=7)
        
        # 5. Direct connections (1-hop only)
        direct_connections = _get_direct_connections(graph_service, user_id)
        
        # 6. Calculate basic metrics
        account_metrics = _calculate_account_metrics(user_profile, accounts, devices)
        
        # Build initial evidence structure
        initial_evidence = InitialEvidence(
            user_id=user_id,
            profile=user_profile,
            accounts=accounts,
            devices=devices,
            recent_transactions=recent_transactions,
            direct_connections=direct_connections,
            account_metrics=account_metrics,
            alert_evidence=state.get("alert_evidence", {})
        )
        
        # Emit evidence event
        trace_events.append(TraceEvent(
            type="evidence",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={
                "profile_found": bool(user_profile.get("name")),
                "account_count": len(accounts),
                "device_count": len(devices),
                "recent_txn_count": len(recent_transactions),
                "connection_count": len(direct_connections),
                "account_age_days": account_metrics.get("account_age_days", 0),
                "total_balance": account_metrics.get("total_balance", 0),
                "has_flagged_device": account_metrics.get("has_flagged_device", False)
            }
        ))
        
        # Emit complete event
        trace_events.append(TraceEvent(
            type="node_complete",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={"status": "success"}
        ))
        
        logger.info(
            f"[{node_name}] Data collection complete - "
            f"{len(accounts)} accounts, {len(devices)} devices, "
            f"{len(recent_transactions)} transactions"
        )
        
        return {
            "initial_evidence": initial_evidence,
            "current_node": "llm_agent",
            "current_phase": "llm_reasoning",
            "trace_events": trace_events
        }
        
    except Exception as e:
        logger.error(f"[{node_name}] Error during data collection: {e}")
        
        trace_events.append(TraceEvent(
            type="error",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={"error": str(e)}
        ))
        
        # Return minimal evidence on error
        return {
            "initial_evidence": InitialEvidence(
                user_id=user_id,
                profile={},
                accounts=[],
                devices=[],
                recent_transactions=[],
                direct_connections=[],
                account_metrics={},
                alert_evidence=state.get("alert_evidence", {})
            ),
            "current_node": "llm_agent",
            "current_phase": "llm_reasoning",
            "error_message": str(e),
            "trace_events": trace_events
        }


def _get_user_profile(aerospike_service: Any, user_id: str) -> Dict[str, Any]:
    """Get user profile from Aerospike KV."""
    try:
        user_data = aerospike_service.get_user(user_id)
        if user_data:
            return {
                "name": user_data.get("name", ""),
                "email": user_data.get("email", ""),
                "phone": user_data.get("phone"),
                "location": user_data.get("location", "Unknown"),
                "occupation": user_data.get("occupation", "Unknown"),
                "age": user_data.get("age", 0),
                "signup_date": user_data.get("signup_date", ""),
                "workflow_status": user_data.get("workflow_status", "pending_review"),
                "current_risk_score": user_data.get("current_risk_score", 0)
            }
        return {}
    except Exception as e:
        logger.warning(f"Error getting user profile: {e}")
        return {}


def _get_user_accounts(graph_service: Any, user_id: str) -> List[Dict[str, Any]]:
    """Get user's accounts from Graph."""
    try:
        if not graph_service.client:
            return []
        
        from gremlin_python.process.graph_traversal import __
        
        accounts = (graph_service.client.V(user_id)
            .out("OWNS")
            .project("id", "type", "balance", "status", "created_at")
            .by(__.id_())
            .by(__.coalesce(__.values("type"), __.constant("unknown")))
            .by(__.coalesce(__.values("balance"), __.constant(0)))
            .by(__.coalesce(__.values("status"), __.constant("active")))
            .by(__.coalesce(__.values("created_at"), __.constant("")))
            .to_list()
        )
        return accounts
    except Exception as e:
        logger.warning(f"Error getting accounts: {e}")
        return []


def _get_user_devices(graph_service: Any, user_id: str) -> List[Dict[str, Any]]:
    """Get user's devices from Graph."""
    try:
        if not graph_service.client:
            return []
        
        from gremlin_python.process.graph_traversal import __
        
        devices = (graph_service.client.V(user_id)
            .out("USES")
            .project("id", "type", "os", "fraud_flag", "first_seen", "user_count")
            .by(__.id_())
            .by(__.coalesce(__.values("type"), __.constant("unknown")))
            .by(__.coalesce(__.values("os"), __.constant("unknown")))
            .by(__.coalesce(__.values("fraud_flag"), __.constant(False)))
            .by(__.coalesce(__.values("first_seen"), __.constant("")))
            .by(__.in_("USES").count())
            .to_list()
        )
        return devices
    except Exception as e:
        logger.warning(f"Error getting devices: {e}")
        return []


def _get_recent_transactions(
    graph_service: Any, 
    user_id: str, 
    days: int = 7
) -> List[Dict[str, Any]]:
    """Get recent transactions from Graph."""
    try:
        if not graph_service.client:
            return []
        
        from gremlin_python.process.graph_traversal import __
        from gremlin_python.process.traversal import Order
        
        transactions = (graph_service.client.V(user_id)
            .out("OWNS")
            .bothE("TRANSACTS")
            .order().by("timestamp", Order.desc)
            .limit(50)
            .project("id", "timestamp", "amount", "fraud_score", "type", "status")
            .by(__.id_())
            .by(__.coalesce(__.values("timestamp"), __.constant("")))
            .by(__.coalesce(__.values("amount"), __.constant(0)))
            .by(__.coalesce(__.values("fraud_score"), __.constant(0)))
            .by(__.coalesce(__.values("type"), __.constant("transfer")))
            .by(__.coalesce(__.values("fraud_status"), __.constant("clean")))
            .to_list()
        )
        return transactions
    except Exception as e:
        logger.warning(f"Error getting transactions: {e}")
        return []


def _get_direct_connections(graph_service: Any, user_id: str) -> List[Dict[str, Any]]:
    """Get 1-hop connections (users connected via devices or transactions)."""
    try:
        if not graph_service.client:
            return []
        
        from gremlin_python.process.graph_traversal import __
        
        connections = []
        seen_ids = {user_id}
        
        # Device connections
        device_connections = (graph_service.client.V(user_id)
            .out("USES")
            .in_("USES")
            .where(__.not_(__.hasId(user_id)))
            .dedup()
            .limit(20)
            .project("user_id", "name", "risk_score", "connection_type")
            .by(__.id_())
            .by(__.coalesce(__.values("name"), __.constant("Unknown")))
            .by(__.coalesce(__.values("risk_score"), __.constant(0)))
            .by(__.constant("device"))
            .to_list()
        )
        
        for conn in device_connections:
            if conn["user_id"] not in seen_ids:
                connections.append(conn)
                seen_ids.add(conn["user_id"])
        
        # Transaction connections
        txn_connections = (graph_service.client.V(user_id)
            .out("OWNS")
            .bothE("TRANSACTS")
            .bothV()
            .in_("OWNS")
            .where(__.not_(__.hasId(user_id)))
            .dedup()
            .limit(20)
            .project("user_id", "name", "risk_score", "connection_type")
            .by(__.id_())
            .by(__.coalesce(__.values("name"), __.constant("Unknown")))
            .by(__.coalesce(__.values("risk_score"), __.constant(0)))
            .by(__.constant("transaction"))
            .to_list()
        )
        
        for conn in txn_connections:
            if conn["user_id"] not in seen_ids:
                connections.append(conn)
                seen_ids.add(conn["user_id"])
        
        return connections
        
    except Exception as e:
        logger.warning(f"Error getting connections: {e}")
        return []


def _calculate_account_metrics(
    profile: Dict[str, Any],
    accounts: List[Dict[str, Any]],
    devices: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Calculate basic account metrics."""
    
    # Account age
    account_age_days = 0
    signup_date = profile.get("signup_date", "")
    if signup_date:
        try:
            signup_dt = datetime.fromisoformat(signup_date.replace("Z", "+00:00"))
            account_age_days = (datetime.now(signup_dt.tzinfo) - signup_dt).days
        except Exception:
            pass
    
    # Total balance
    total_balance = sum(a.get("balance", 0) for a in accounts)
    
    # Device analysis
    has_flagged_device = any(d.get("fraud_flag") for d in devices)
    shared_device_count = sum(1 for d in devices if d.get("user_count", 1) > 1)
    
    # KYC completeness
    kyc_completeness = "none"
    if profile.get("email") and profile.get("name"):
        kyc_completeness = "basic"
        if profile.get("phone") and profile.get("location"):
            kyc_completeness = "full"
    
    return {
        "account_age_days": account_age_days,
        "total_balance": round(total_balance, 2),
        "account_count": len(accounts),
        "device_count": len(devices),
        "has_flagged_device": has_flagged_device,
        "shared_device_count": shared_device_count,
        "kyc_completeness": kyc_completeness,
        "profile_risk_score": profile.get("current_risk_score", 0)
    }
