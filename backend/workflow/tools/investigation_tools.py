"""
Investigation Tools

Tools that the LLM agent can invoke to gather evidence during fraud investigations.
Each tool queries Aerospike KV or Graph for specific information.
The LLM decides which tools to call and with what parameters.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger('investigation.tools')


class InvestigationTools:
    """
    Tools available to the LLM reasoning agent.
    Each tool queries Aerospike KV or Graph for specific information.
    """
    
    # Tool schemas for LLM
    TOOL_SCHEMAS = [
        {
            "name": "get_transactions",
            "description": "Get user's transactions for a time period. Use to analyze spending patterns, velocity, or find suspicious amounts.",
            "parameters": {
                "days": {"type": "integer", "description": "Days to look back (1-90)", "default": 30},
                "min_amount": {"type": "number", "description": "Minimum transaction amount to include", "default": 0},
                "max_results": {"type": "integer", "description": "Maximum transactions to return", "default": 100}
            }
        },
        {
            "name": "traverse_graph",
            "description": "Traverse the graph to find connected entities. Use to explore network connections and find related suspicious accounts.",
            "parameters": {
                "hops": {"type": "integer", "description": "Number of hops to traverse (1-4)", "default": 2},
                "edge_types": {"type": "array", "description": "Types of edges to follow: device, transaction, account", "default": ["device", "transaction"]},
                "include_flagged_only": {"type": "boolean", "description": "Only return flagged entities", "default": False}
            }
        },
        {
            "name": "get_shared_device_users",
            "description": "Find all users who share devices with the target user. Critical for fraud ring detection.",
            "parameters": {}
        },
        {
            "name": "get_device_history",
            "description": "Get detailed history for devices. Shows all users who have used the device(s).",
            "parameters": {
                "device_id": {"type": "string", "description": "Specific device ID to query, or omit for all user devices", "default": None}
            }
        },
        {
            "name": "get_transaction_partners",
            "description": "Get users who have transacted with the target user. Shows transaction frequencies and amounts.",
            "parameters": {
                "limit": {"type": "integer", "description": "Maximum number of partners to return", "default": 20},
                "min_txn_count": {"type": "integer", "description": "Minimum transactions to include partner", "default": 1}
            }
        },
        {
            "name": "check_fraud_ring",
            "description": "Analyze if user is part of a coordinated fraud ring. Combines device sharing, transaction patterns, and risk scores.",
            "parameters": {}
        },
        {
            "name": "custom_gremlin",
            "description": """Execute a custom Gremlin query on the fraud graph for flexible exploration.

Graph Schema:
- Vertices: User (id, name, email, risk_score, location, signup_date), Account (id, type, balance, status), Device (id, type, os, browser, fraud_flag, first_seen), Transaction (id, amount, timestamp, fraud_score)
- Edges: OWNS (User->Account), USES (User->Device), TRANSACTS (Account<->Account with amount, timestamp, fraud_score)

Example queries:
- Get user's total balance: g.V('USER_ID').out('OWNS').values('balance').sum()
- Find users sharing devices: g.V('USER_ID').out('USES').in('USES').dedup().valueMap()
- Find high-risk connections: g.V('USER_ID').out('OWNS').both('TRANSACTS').in('OWNS').has('risk_score', gte(70)).dedup()
- Count transactions by day: g.V('USER_ID').out('OWNS').bothE('TRANSACTS').count()

Use this when predefined tools don't meet your specific query needs.""",
            "parameters": {
                "query": {"type": "string", "description": "Gremlin query string. Use 'USER_ID' as placeholder for the investigated user.", "required": True},
                "description": {"type": "string", "description": "Brief description of what you're trying to find", "default": ""}
            }
        },
        {
            "name": "submit_assessment",
            "description": "Submit final fraud assessment. Call this when you have enough evidence to make a decision.",
            "parameters": {
                "typology": {"type": "string", "description": "Fraud type: account_takeover, money_mule, synthetic_identity, promo_abuse, friendly_fraud, card_testing, fraud_ring, legitimate", "required": True},
                "risk_level": {"type": "string", "description": "Risk level: low, medium, high, critical", "required": True},
                "risk_score": {"type": "integer", "description": "Risk score 0-100", "required": True},
                "decision": {"type": "string", "description": "Recommended action: allow_monitor, step_up_auth, temporary_freeze, full_block, escalate_compliance", "required": True},
                "reasoning": {"type": "string", "description": "Detailed reasoning for the assessment", "required": True}
            }
        }
    ]
    
    def __init__(self, aerospike_service: Any, graph_service: Any, user_id: str):
        """
        Initialize tools with service dependencies.
        
        Args:
            aerospike_service: Aerospike KV service instance
            graph_service: Aerospike Graph service instance
            user_id: User ID being investigated
        """
        self.aerospike = aerospike_service
        self.graph = graph_service
        self.user_id = user_id
        self.tool_calls: List[Dict[str, Any]] = []  # Audit trail
    
    def execute_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a tool by name with given parameters.
        
        Args:
            tool_name: Name of the tool to execute
            params: Parameters for the tool
            
        Returns:
            Tool result as dictionary
        """
        # Record the tool call
        call_record = {
            "tool": tool_name,
            "params": params,
            "timestamp": datetime.now().isoformat()
        }
        
        # Route to appropriate tool
        try:
            if tool_name == "get_transactions":
                result = self.get_transactions(**params)
            elif tool_name == "traverse_graph":
                result = self.traverse_graph(**params)
            elif tool_name == "get_shared_device_users":
                result = self.get_shared_device_users()
            elif tool_name == "get_device_history":
                result = self.get_device_history(**params)
            elif tool_name == "get_transaction_partners":
                result = self.get_transaction_partners(**params)
            elif tool_name == "check_fraud_ring":
                result = self.check_fraud_ring()
            elif tool_name == "custom_gremlin":
                result = self.custom_gremlin(**params)
            elif tool_name == "submit_assessment":
                result = self.submit_assessment(**params)
            else:
                result = {"success": False, "error": f"Unknown tool: {tool_name}"}
            
            call_record["success"] = result.get("success", True)
            call_record["result_summary"] = self._summarize_result(tool_name, result)
            
        except Exception as e:
            logger.error(f"Tool {tool_name} failed: {e}")
            result = {"success": False, "error": str(e)}
            call_record["success"] = False
            call_record["error"] = str(e)
        
        self.tool_calls.append(call_record)
        return result
    
    def _summarize_result(self, tool_name: str, result: Dict) -> str:
        """Create a brief summary of tool result for logging."""
        if not result.get("success", True):
            return f"Error: {result.get('error', 'Unknown')}"
        
        if tool_name == "get_transactions":
            return f"{result.get('count', 0)} transactions found"
        elif tool_name == "traverse_graph":
            return f"{result.get('node_count', 0)} nodes, {result.get('edge_count', 0)} edges"
        elif tool_name == "get_shared_device_users":
            return f"{result.get('count', 0)} shared device users"
        elif tool_name == "get_device_history":
            return f"{len(result.get('devices', []))} devices"
        elif tool_name == "get_transaction_partners":
            return f"{result.get('count', 0)} partners"
        elif tool_name == "check_fraud_ring":
            return f"Ring: {result.get('is_fraud_ring', False)}, confidence: {result.get('ring_confidence', 0)}%"
        elif tool_name == "custom_gremlin":
            return f"Query returned {result.get('result_count', 0)} results"
        elif tool_name == "submit_assessment":
            return f"Assessment: {result.get('typology', 'unknown')} - {result.get('risk_level', 'unknown')}"
        return "OK"
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 1: Get Transactions
    # ─────────────────────────────────────────────────────────────
    def get_transactions(
        self, 
        days: int = 30, 
        min_amount: float = 0,
        max_results: int = 100
    ) -> Dict[str, Any]:
        """
        Get user's transactions for the specified time period.
        
        Args:
            days: Number of days to look back (default: 30, max: 90)
            min_amount: Minimum transaction amount to include (default: 0)
            max_results: Maximum number of transactions (default: 100)
            
        Returns:
            List of transactions with amounts, timestamps, risk scores
        """
        logger.info(f"[Tool] get_transactions(days={days}, min_amount={min_amount})")
        
        # Enforce limits
        days = min(90, max(1, days))
        max_results = min(200, max(1, max_results))
        
        try:
            if not self.graph.client:
                return {"success": False, "error": "Graph service not connected"}
            
            from gremlin_python.process.graph_traversal import __
            from gremlin_python.process.traversal import Order, P
            
            cutoff = datetime.now() - timedelta(days=days)
            
            transactions = (self.graph.client.V(self.user_id)
                .out("OWNS")
                .bothE("TRANSACTS")
                .order().by("timestamp", Order.desc)
                .limit(max_results)
                .project("id", "timestamp", "amount", "fraud_score", "type", "status")
                .by(__.id_())
                .by(__.coalesce(__.values("timestamp"), __.constant("")))
                .by(__.coalesce(__.values("amount"), __.constant(0)))
                .by(__.coalesce(__.values("fraud_score"), __.constant(0)))
                .by(__.coalesce(__.values("type"), __.constant("transfer")))
                .by(__.coalesce(__.values("fraud_status"), __.constant("clean")))
                .to_list()
            )
            
            # Filter by min_amount
            if min_amount > 0:
                transactions = [t for t in transactions if t.get("amount", 0) >= min_amount]
            
            # Calculate velocity metrics
            total_amount = sum(t.get("amount", 0) for t in transactions)
            high_risk_count = sum(1 for t in transactions if t.get("fraud_score", 0) > 50)
            
            return {
                "success": True,
                "count": len(transactions),
                "transactions": transactions[:50],  # Limit response size
                "total_amount": round(total_amount, 2),
                "high_risk_count": high_risk_count,
                "query_params": {"days": days, "min_amount": min_amount}
            }
            
        except Exception as e:
            logger.error(f"get_transactions error: {e}")
            return {"success": False, "error": str(e)}
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 2: Traverse Graph
    # ─────────────────────────────────────────────────────────────
    def traverse_graph(
        self, 
        hops: int = 2,
        edge_types: List[str] = None,
        include_flagged_only: bool = False
    ) -> Dict[str, Any]:
        """
        Traverse the graph to find connected entities.
        
        Args:
            hops: Number of hops to traverse (1-4, default: 2)
            edge_types: Types of edges to follow ["device", "transaction", "account"]
            include_flagged_only: Only return flagged entities
            
        Returns:
            Connected users, devices, and their risk levels
        """
        logger.info(f"[Tool] traverse_graph(hops={hops}, edge_types={edge_types})")
        
        # Enforce limits
        hops = min(4, max(1, hops))
        edge_types = edge_types or ["device", "transaction"]
        
        try:
            if not self.graph.client:
                return {"success": False, "error": "Graph service not connected"}
            
            from gremlin_python.process.graph_traversal import __
            from gremlin_python.process.traversal import P
            
            nodes = []
            edges = []
            seen_ids = {self.user_id}
            
            # Build traversal based on edge types
            for hop in range(1, hops + 1):
                hop_nodes = []
                
                if "device" in edge_types:
                    # User -> Device -> Other Users
                    device_connections = (self.graph.client.V(self.user_id)
                        .out("USES")
                        .in_("USES")
                        .where(__.not_(__.hasId(self.user_id)))
                        .dedup()
                        .limit(50)
                        .project("id", "type", "name", "risk_score", "is_flagged")
                        .by(__.id_())
                        .by(__.constant("user"))
                        .by(__.coalesce(__.values("name"), __.constant("Unknown")))
                        .by(__.coalesce(__.values("risk_score"), __.constant(0)))
                        .by(__.coalesce(__.values("fraud_flag"), __.constant(False)))
                        .to_list()
                    )
                    hop_nodes.extend(device_connections)
                
                if "transaction" in edge_types:
                    # User -> Account -> Transaction -> Account -> Other Users
                    txn_connections = (self.graph.client.V(self.user_id)
                        .out("OWNS")
                        .bothE("TRANSACTS")
                        .bothV()
                        .in_("OWNS")
                        .where(__.not_(__.hasId(self.user_id)))
                        .dedup()
                        .limit(50)
                        .project("id", "type", "name", "risk_score", "is_flagged")
                        .by(__.id_())
                        .by(__.constant("user"))
                        .by(__.coalesce(__.values("name"), __.constant("Unknown")))
                        .by(__.coalesce(__.values("risk_score"), __.constant(0)))
                        .by(__.coalesce(__.values("fraud_flag"), __.constant(False)))
                        .to_list()
                    )
                    hop_nodes.extend(txn_connections)
                
                # Deduplicate and add
                for node in hop_nodes:
                    node_id = node.get("id", "")
                    if node_id and node_id not in seen_ids:
                        node["hop"] = hop
                        nodes.append(node)
                        seen_ids.add(node_id)
            
            # Filter flagged only if requested
            if include_flagged_only:
                nodes = [n for n in nodes if n.get("is_flagged") or n.get("risk_score", 0) >= 70]
            
            # Count by risk level
            high_risk = [n for n in nodes if n.get("risk_score", 0) >= 70]
            medium_risk = [n for n in nodes if 40 <= n.get("risk_score", 0) < 70]
            
            return {
                "success": True,
                "node_count": len(nodes),
                "edge_count": len(edges),
                "nodes": nodes[:30],  # Limit response
                "high_risk_count": len(high_risk),
                "medium_risk_count": len(medium_risk),
                "hops_traversed": hops,
                "edge_types_used": edge_types
            }
            
        except Exception as e:
            logger.error(f"traverse_graph error: {e}")
            return {"success": False, "error": str(e)}
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 3: Get Shared Device Users
    # ─────────────────────────────────────────────────────────────
    def get_shared_device_users(self) -> Dict[str, Any]:
        """
        Find all users who share devices with the target user.
        Critical for fraud ring detection.
        
        Returns:
            List of users sharing devices, their risk scores, shared device count
        """
        logger.info(f"[Tool] get_shared_device_users()")
        
        try:
            if not self.graph.client:
                return {"success": False, "error": "Graph service not connected"}
            
            from gremlin_python.process.graph_traversal import __
            
            shared_users = (self.graph.client.V(self.user_id)
                .out("USES")  # User → Device
                .in_("USES")  # Device ← Other Users
                .where(__.not_(__.hasId(self.user_id)))
                .dedup()
                .project("user_id", "name", "risk_score", "shared_device_count")
                .by(__.id_())
                .by(__.coalesce(__.values("name"), __.constant("Unknown")))
                .by(__.coalesce(__.values("risk_score"), __.constant(0)))
                .by(__.out("USES").where(__.in_("USES").hasId(self.user_id)).count())
                .to_list()
            )
            
            # Determine fraud ring likelihood
            high_risk_shared = [u for u in shared_users if u.get("risk_score", 0) >= 60]
            
            return {
                "success": True,
                "count": len(shared_users),
                "users": shared_users,
                "high_risk_shared_count": len(high_risk_shared),
                "fraud_ring_likely": len(shared_users) > 3 or len(high_risk_shared) > 0
            }
            
        except Exception as e:
            logger.error(f"get_shared_device_users error: {e}")
            return {"success": False, "error": str(e)}
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 4: Get Device History
    # ─────────────────────────────────────────────────────────────
    def get_device_history(self, device_id: str = None) -> Dict[str, Any]:
        """
        Get detailed history for a specific device or all user devices.
        
        Args:
            device_id: Specific device to query (optional, defaults to all)
            
        Returns:
            Device details, all users who have used it, login history
        """
        logger.info(f"[Tool] get_device_history(device_id={device_id})")
        
        try:
            if not self.graph.client:
                return {"success": False, "error": "Graph service not connected"}
            
            from gremlin_python.process.graph_traversal import __
            
            if device_id:
                # Get specific device
                try:
                    device_info = (self.graph.client.V(device_id)
                        .project("id", "type", "os", "browser", "fraud_flag", "first_seen", "user_count")
                        .by(__.id_())
                        .by(__.coalesce(__.values("type"), __.constant("unknown")))
                        .by(__.coalesce(__.values("os"), __.constant("unknown")))
                        .by(__.coalesce(__.values("browser"), __.constant("unknown")))
                        .by(__.coalesce(__.values("fraud_flag"), __.constant(False)))
                        .by(__.coalesce(__.values("first_seen"), __.constant("")))
                        .by(__.in_("USES").count())
                        .next()
                    )
                    
                    # Get users of this device
                    users = (self.graph.client.V(device_id)
                        .in_("USES")
                        .project("user_id", "name", "risk_score")
                        .by(__.id_())
                        .by(__.coalesce(__.values("name"), __.constant("Unknown")))
                        .by(__.coalesce(__.values("risk_score"), __.constant(0)))
                        .to_list()
                    )
                    device_info["users"] = users
                    
                    return {"success": True, "devices": [device_info]}
                except Exception:
                    return {"success": False, "error": f"Device {device_id} not found"}
            else:
                # Get all user devices
                devices = (self.graph.client.V(self.user_id)
                    .out("USES")
                    .project("id", "type", "fraud_flag", "user_count", "first_seen")
                    .by(__.id_())
                    .by(__.coalesce(__.values("type"), __.constant("unknown")))
                    .by(__.coalesce(__.values("fraud_flag"), __.constant(False)))
                    .by(__.in_("USES").count())
                    .by(__.coalesce(__.values("first_seen"), __.constant("")))
                    .to_list()
                )
                
                flagged_count = sum(1 for d in devices if d.get("fraud_flag"))
                shared_count = sum(1 for d in devices if d.get("user_count", 1) > 1)
                
                return {
                    "success": True,
                    "devices": devices,
                    "total_devices": len(devices),
                    "flagged_devices": flagged_count,
                    "shared_devices": shared_count
                }
                
        except Exception as e:
            logger.error(f"get_device_history error: {e}")
            return {"success": False, "error": str(e)}
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 5: Get Transaction Partners
    # ─────────────────────────────────────────────────────────────
    def get_transaction_partners(
        self, 
        limit: int = 20,
        min_txn_count: int = 1
    ) -> Dict[str, Any]:
        """
        Get users who have transacted with the target user.
        
        Args:
            limit: Maximum number of partners to return
            min_txn_count: Minimum number of transactions to include
            
        Returns:
            Transaction partners with amounts and frequencies
        """
        logger.info(f"[Tool] get_transaction_partners(limit={limit})")
        
        # Enforce limits
        limit = min(50, max(1, limit))
        
        try:
            if not self.graph.client:
                return {"success": False, "error": "Graph service not connected"}
            
            from gremlin_python.process.graph_traversal import __
            from gremlin_python.process.traversal import Order
            
            # Get transaction partners
            partners = (self.graph.client.V(self.user_id)
                .out("OWNS")
                .bothE("TRANSACTS")
                .bothV()
                .in_("OWNS")
                .where(__.not_(__.hasId(self.user_id)))
                .dedup()
                .limit(limit)
                .project("user_id", "name", "risk_score")
                .by(__.id_())
                .by(__.coalesce(__.values("name"), __.constant("Unknown")))
                .by(__.coalesce(__.values("risk_score"), __.constant(0)))
                .to_list()
            )
            
            # Count high risk partners
            high_risk = [p for p in partners if p.get("risk_score", 0) >= 70]
            
            return {
                "success": True,
                "count": len(partners),
                "partners": partners,
                "high_risk_partners": len(high_risk)
            }
            
        except Exception as e:
            logger.error(f"get_transaction_partners error: {e}")
            return {"success": False, "error": str(e)}
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 6: Check Fraud Ring
    # ─────────────────────────────────────────────────────────────
    def check_fraud_ring(self) -> Dict[str, Any]:
        """
        Analyze if user is part of a coordinated fraud ring.
        Looks for triangle patterns and shared infrastructure.
        
        Returns:
            Fraud ring analysis with member list and confidence
        """
        logger.info(f"[Tool] check_fraud_ring()")
        
        try:
            # Get shared device users
            shared_result = self.get_shared_device_users()
            shared_users = shared_result.get("users", [])
            
            # Get transaction partners
            partners_result = self.get_transaction_partners(limit=50)
            partners = partners_result.get("partners", [])
            
            # Find overlap (users who share devices AND transact)
            shared_ids = {u["user_id"] for u in shared_users}
            partner_ids = {p["user_id"] for p in partners}
            overlap = shared_ids & partner_ids
            
            # Calculate ring score
            ring_score = 0
            ring_members = []
            evidence = []
            
            # Each overlapping user adds 20 points
            for uid in overlap:
                ring_members.append(uid)
                ring_score += 20
                evidence.append(f"User {uid} shares device AND has transactions")
            
            # High-risk shared device users add 15 points each
            high_risk_shared = [u for u in shared_users if u.get("risk_score", 0) >= 60]
            for u in high_risk_shared:
                if u["user_id"] not in ring_members:
                    ring_members.append(u["user_id"])
                ring_score += 15
                evidence.append(f"High-risk user {u['user_id']} (score: {u['risk_score']}) shares device")
            
            # Many shared devices indicates coordinated activity
            if len(shared_users) >= 3:
                ring_score += 10
                evidence.append(f"Device shared with {len(shared_users)} users")
            
            return {
                "success": True,
                "is_fraud_ring": ring_score >= 40,
                "ring_confidence": min(100, ring_score),
                "ring_member_count": len(ring_members),
                "ring_members": ring_members[:10],
                "shared_device_users": len(shared_users),
                "overlap_with_txn_partners": len(overlap),
                "high_risk_connections": len(high_risk_shared),
                "evidence": evidence[:5]
            }
            
        except Exception as e:
            logger.error(f"check_fraud_ring error: {e}")
            return {"success": False, "error": str(e)}
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 7: Custom Gremlin Query (Flexible Exploration)
    # ─────────────────────────────────────────────────────────────
    def custom_gremlin(
        self,
        query: str,
        description: str = ""
    ) -> Dict[str, Any]:
        """
        Execute a custom Gremlin query for flexible graph exploration.
        
        Args:
            query: Gremlin query string. 'USER_ID' will be replaced with actual user ID.
            description: Brief description of what the query is looking for.
            
        Returns:
            Query results
        """
        logger.info(f"[Tool] custom_gremlin: {query[:100]}... | {description}")
        
        try:
            if not self.graph.client:
                return {"success": False, "error": "Graph service not connected"}
            
            # Replace USER_ID placeholder with actual user ID
            processed_query = query.replace("USER_ID", self.user_id)
            processed_query = processed_query.replace("user_id", self.user_id)
            
            # Safety checks
            dangerous_patterns = ["drop", "remove", "delete", "clear", "addV", "addE", "property("]
            query_lower = processed_query.lower()
            for pattern in dangerous_patterns:
                if pattern in query_lower:
                    return {
                        "success": False, 
                        "error": f"Query contains forbidden operation: {pattern}. Only read operations allowed."
                    }
            
            # Add limit if not present to prevent huge result sets
            if ".limit(" not in processed_query.lower() and ".count()" not in processed_query.lower():
                # Try to add limit before terminal step
                if processed_query.rstrip().endswith(".toList()"):
                    processed_query = processed_query.rstrip()[:-9] + ".limit(100).toList()"
                elif processed_query.rstrip().endswith(".to_list()"):
                    processed_query = processed_query.rstrip()[:-10] + ".limit(100).to_list()"
                elif not processed_query.rstrip().endswith(")"):
                    processed_query = processed_query + ".limit(100)"
            
            # Execute the query
            from gremlin_python.process.graph_traversal import __
            from gremlin_python.process.traversal import P, Order
            
            # Build a safe execution context
            g = self.graph.client
            
            # Execute - handle both g.V() style and raw traversal
            if processed_query.strip().startswith("g."):
                # Remove 'g.' prefix and execute on our client
                traversal_query = processed_query.strip()[2:]
                result = eval(f"g.{traversal_query}", {
                    "g": g,
                    "__": __,
                    "P": P,
                    "Order": Order
                })
            else:
                # Execute as-is
                result = eval(processed_query, {
                    "g": g,
                    "__": __,
                    "P": P,
                    "Order": Order
                })
            
            # Handle different result types
            if hasattr(result, "toList"):
                result = result.toList()
            elif hasattr(result, "to_list"):
                result = result.to_list()
            elif hasattr(result, "next"):
                try:
                    result = result.next()
                except StopIteration:
                    result = None
            
            # Format result for return
            if isinstance(result, list):
                return {
                    "success": True,
                    "result_count": len(result),
                    "results": result[:50],  # Limit returned results
                    "query": processed_query,
                    "description": description,
                    "truncated": len(result) > 50
                }
            else:
                return {
                    "success": True,
                    "result_count": 1 if result is not None else 0,
                    "results": result,
                    "query": processed_query,
                    "description": description
                }
                
        except Exception as e:
            logger.error(f"custom_gremlin error: {e}")
            return {
                "success": False, 
                "error": str(e),
                "query": query,
                "hint": "Check query syntax. Common issues: missing quotes around IDs, invalid traversal steps."
            }
    
    # ─────────────────────────────────────────────────────────────
    # TOOL 8: Submit Assessment (Exit Tool)
    # ─────────────────────────────────────────────────────────────
    def submit_assessment(
        self,
        typology: str,
        risk_level: str,
        risk_score: int,
        decision: str,
        reasoning: str
    ) -> Dict[str, Any]:
        """
        Submit final fraud assessment. This is the exit tool that ends the agent loop.
        
        Args:
            typology: Fraud type classification
            risk_level: Risk level (low, medium, high, critical)
            risk_score: Numeric risk score (0-100)
            decision: Recommended action
            reasoning: Detailed reasoning
            
        Returns:
            Assessment confirmation
        """
        logger.info(f"[Tool] submit_assessment(typology={typology}, risk_level={risk_level})")
        
        # Validate inputs
        valid_typologies = [
            "account_takeover", "money_mule", "synthetic_identity", 
            "promo_abuse", "friendly_fraud", "card_testing", 
            "fraud_ring", "legitimate", "unknown"
        ]
        valid_risk_levels = ["low", "medium", "high", "critical"]
        valid_decisions = [
            "allow_monitor", "step_up_auth", "temporary_freeze", 
            "full_block", "escalate_compliance"
        ]
        
        if typology not in valid_typologies:
            typology = "unknown"
        if risk_level not in valid_risk_levels:
            risk_level = "medium"
        if decision not in valid_decisions:
            decision = "allow_monitor"
        
        risk_score = min(100, max(0, risk_score))
        
        return {
            "success": True,
            "is_final_assessment": True,
            "typology": typology,
            "risk_level": risk_level,
            "risk_score": risk_score,
            "decision": decision,
            "reasoning": reasoning,
            "tool_calls_made": len(self.tool_calls)
        }
    
    @classmethod
    def get_tool_descriptions(cls) -> str:
        """Get formatted tool descriptions for LLM prompt."""
        descriptions = []
        for tool in cls.TOOL_SCHEMAS:
            params_str = ""
            if tool.get("parameters"):
                params_list = []
                for name, info in tool["parameters"].items():
                    default = info.get("default", "required")
                    params_list.append(f"{name}={default}")
                params_str = f"({', '.join(params_list)})"
            else:
                params_str = "()"
            
            descriptions.append(f"- {tool['name']}{params_str}: {tool['description']}")
        
        return "\n".join(descriptions)
