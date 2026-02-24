from typing import List, Dict, Any, Optional
import logging
import os
import re
import time

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T

# Get logger for graph service
logger = logging.getLogger('fraud_detection.graph')

class GraphService:
    def __init__(self, host: str = os.environ.get('GRAPH_HOST_ADDRESS') or 'localhost', port: int = 8182):
        self.host = host
        self.port = port
        self.client = None
        self.connection = None
    

    # ----------------------------------------------------------------------------------------------------------
    # Connection maintenance
    # ----------------------------------------------------------------------------------------------------------


    def connect(self):
        """Synchronous connection to Aerospike Graph (to be called outside async context)"""
        try:
            url = f'ws://{self.host}:{self.port}/gremlin'
            logger.info(f"🔄 Connecting to Aerospike Graph: {url}")
            
            # Use the same approach as the working sample
            self.connection = DriverRemoteConnection(url, "g", transport_factory=lambda:AiohttpTransport(call_from_event_loop=True))
            self.client = traversal().with_remote(self.connection)
            
            # Test connection using the same method as the sample
            test_result = self.client.inject(0).next()
            if test_result != 0:
                raise Exception("Failed to connect to graph instance")
            
            logger.info("✅ Connected to Aerospike Graph Service")
            return True
                
        except Exception as e:
            logger.error(f"❌ Could not connect to Aerospike Graph: {e}")
            logger.error("Graph database connection is required. Please ensure Aerospike Graph is running on port 8182")
            self.client = None
            self.connection = None
            raise Exception(f"Failed to connect to Aerospike Graph: {e}")

    def close(self):
        """Synchronous close of graph connection"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("✅ Disconnected from Aerospike Graph")
            except Exception as e:
                logger.warning(f"⚠️  Error closing connection: {e}")


    # ----------------------------------------------------------------------------------------------------------
    # Helper functions
    # ----------------------------------------------------------------------------------------------------------


    def get_property_value(self, vertex, key, default=None):
        """Helper function to get property value from vertex"""
        for prop in vertex.properties:
            if prop.key == key:
                return prop.value
        return default
    
    # ----------------------------------------------------------------------------------------------------------
    # Dashboard functions
    # ----------------------------------------------------------------------------------------------------------


    def get_graph_summary(self) -> Dict[str, Any]:
        """Get graph summary using Aerospike Graph admin API - reusable method"""
        try:
            if not self.client:
                logger.warning("No graph client available for summary")
                return {}
                
            logger.info("Getting graph summary using Aerospike Graph admin API")
            summary_result = self.client.call("aerospike.graph.admin.metadata.summary").next()
            logger.debug(f"Raw graph summary result: {summary_result}")
            
            # Parse and structure the summary data
            parsed_summary = {
                'total_vertex_count': summary_result.get('Total vertex count', 0),
                'total_edge_count': summary_result.get('Total edge count', 0),
                'total_supernode_count': summary_result.get('Total supernode count', 0),
                'vertex_counts': summary_result.get('Vertex count by label', {}),
                'edge_counts': summary_result.get('Edge count by label', {}),
                'supernode_counts': summary_result.get('Supernode count by label', {}),
                'vertex_properties': summary_result.get('Vertex properties by label', {}),
                'edge_properties': summary_result.get('Edge properties by label', {}),
                'raw_summary': summary_result  # Include raw data for advanced use cases
            }
            
            logger.info(f"Parsed graph summary - Vertices: {parsed_summary['total_vertex_count']}, Edges: {parsed_summary['total_edge_count']}")
            return parsed_summary
            
        except Exception as e:
            logger.error(f"Error getting graph summary: {e}")
            return {}
        
    # ----------------------------------------------------------------------------------------------------------
    # User functions
    # ----------------------------------------------------------------------------------------------------------


    def update_user_risk_score(self, user_id: str, risk_score: float) -> bool:
        """Update the risk_score property on a user vertex in the graph."""
        try:
            if not self.client:
                logger.warning("Graph client not available — cannot update risk score")
                return False
            
            self.client.V(user_id).property("risk_score", risk_score).iterate()
            return True
        except Exception as e:
            logger.error(f"Error updating risk score for user {user_id} in graph: {e}")
            return False

    def get_user_connected_devices(self, user_id: str) -> List[Dict[str, Any]]:
        """Get users who share devices with the specified user"""
        try:
            if not self.client:
                return []
            
            connected_users = (self.client.V(user_id)
                .out("USES")
                .in_("USES")
                .where(__.not_(__.hasId(user_id)))
                .dedup()
                .project("user_id", "name", "risk_score", "shared_device_count")
                .by(__.id_())
                .by(__.coalesce(__.values("name"), __.constant("Unknown")))
                .by(__.coalesce(__.values("risk_score"), __.constant(0)))
                .by(__.out("USES").where(__.in_("USES").hasId(user_id)).count())
                .to_list()
            )
            return connected_users
        except Exception as e:
            logger.error(f"Error finding connected device users: {e}")
            return []
    
    # ----------------------------------------------------------------------------------------------------------
    # Transaction functions
    # ----------------------------------------------------------------------------------------------------------


    def get_transaction_summary(self, txn_id_or_edge_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed transaction information by txn_id or edge ID"""
        try:
            if self.client:
                # First try to find by txn_id property (for KV-sourced transaction links)
                edges = self.client.E().has("txn_id", txn_id_or_edge_id).toList()
                
                if not edges:
                    # Not found by txn_id - transaction may not exist in Graph
                    logger.warning(f"Transaction with txn_id '{txn_id_or_edge_id}' not found in Graph DB")
                    return None
                
                # Found by txn_id property - use the edge ID
                edge_id = edges[0].id
                
                txn_detail = (self.client.E(edge_id)
                    .project("txn", "src", "dest")
                    .by(__.elementMap())
                    .by(__.outV()
                        .project("account", "user")
                        .by(__.elementMap())
                        .by(__.in_("OWNS").elementMap()))
                    .by(__.inV()
                        .project("account", "user")
                        .by(__.elementMap())
                        .by(__.in_("OWNS").elementMap()))
                    .next())

                return {
                    "txn": txn_detail.get("txn"),
                    "src": txn_detail.get("src"),
                    "dest": txn_detail.get("dest")
                }

            else:
                # No graph client available
                raise Exception("Graph client not available. Cannot get transaction detail without graph database connection.")
                
        except Exception as e:
            logger.error(f"Error getting transaction detail for {txn_id_or_edge_id}: {e}")
            return None


    def drop_all_transactions(self):
        if self.client:
            try:
                self.client.with_('evaluationTimeout', 0).E().has_label("TRANSACTS").drop().iterate()
                
                edges = 1
                while edges > 0:
                    edges = self.client.E().has_label("TRANSACTS").count()
                    time.sleep(.5)
                
                return True
            
            except Exception as e:
                logger.error(f"An error occured while dropping all transactions: {e}")
                return False
        logger.error("No graph client available. Cannot drop all transactions without graph database connection.")
        return False


    # ----------------------------------------------------------------------------------------------------------
    # Account functions
    # ----------------------------------------------------------------------------------------------------------


    def get_all_accounts(self) -> List[Dict[str, Any]]:
        """Get all accounts with their associated user information"""
        try:
            if self.client:
                accounts = self.client.V().has_label("account").project("account_id", "account_type").by(T.id).by("type").to_list()
                logger.info(f"Found {len(accounts)} account vertices")
                return accounts
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting all accounts: {e}")
            return []
        
    def get_fraud_details_by_txn_id(self, txn_id: str) -> Optional[Dict[str, Any]]:
        """
        Get fraud detection details from Graph DB by transaction ID.
        
        Returns fraud details including RT1, RT2, RT3 rule results if the transaction was flagged.
        Details are returned as JSON strings (as stored in Graph) for frontend to parse.
        """
        try:
            if not self.client:
                return None
            
            # Find the TRANSACTS edge by txn_id property
            edges = self.client.E().has_label('TRANSACTS').has('txn_id', txn_id).valueMap(True).toList()
            
            if not edges:
                return None
            
            edge = edges[0]
            
            # Check if this transaction has fraud details
            is_fraud = edge.get('is_fraud', [False])
            is_fraud = is_fraud[0] if isinstance(is_fraud, list) else is_fraud
            
            if not is_fraud:
                return None
            
            # Get the details array (contains JSON strings for each RT rule)
            # Return as-is since frontend expects JSON strings to parse
            details_raw = edge.get('details', [])
            
            # Details is already a list of JSON strings from Graph
            # No need to unwrap - just ensure it's a list
            if not isinstance(details_raw, list):
                details_raw = [details_raw] if details_raw else []
            
            # Get other fraud properties
            fraud_score = edge.get('fraud_score', [0])
            fraud_score = fraud_score[0] if isinstance(fraud_score, list) else fraud_score
            
            fraud_status = edge.get('fraud_status', [''])
            fraud_status = fraud_status[0] if isinstance(fraud_status, list) else fraud_status
            
            eval_timestamp = edge.get('eval_timestamp', [''])
            eval_timestamp = eval_timestamp[0] if isinstance(eval_timestamp, list) else eval_timestamp
            
            return {
                'is_fraud': True,
                'fraud_score': fraud_score,
                'fraud_status': fraud_status,
                'eval_timestamp': eval_timestamp,
                'details': details_raw  # Return as JSON strings for frontend to parse
            }
            
        except Exception as e:
            logger.error(f"Error getting fraud details for txn_id {txn_id}: {e}")
            return None
        

    # ----------------------------------------------------------------------------------------------------------
    # Utility functions
    # ----------------------------------------------------------------------------------------------------------

    def bulk_load_csv_data(self, vertices_path: str = None, edges_path: str = None) -> Dict[str, Any]:
        """Bulk load data from CSV files using Aerospike Graph bulk loader.
        
        Note: Aerospike Graph bulk loader REQUIRES vertices path - it's not optional.
        
        Args:
            vertices_path: Path to vertices CSV directory (required by bulk loader)
            edges_path: Path to edges CSV directory
        """
        try:
            if not self.client:
                raise Exception("Graph client not available. Cannot bulk load data without graph database connection.")
            
            # Default paths if not provided
            if not vertices_path:
                vertices_path = "/data/graph_csv/vertices"
            if not edges_path:
                edges_path = "/data/graph_csv/edges"
            
            logger.info(f"🚀 bulk_load_csv_data called:")
            logger.info(f"   vertices_path: {vertices_path}")
            logger.info(f"   edges_path: {edges_path}")
            
            # Check what files exist in the edges path (os imported at module level)
            if edges_path and os.path.exists(edges_path):
                logger.info(f"📂 Checking edges directory: {edges_path}")
                if os.path.isdir(edges_path):
                    for root, dirs, files in os.walk(edges_path):
                        for f in files:
                            file_path = os.path.join(root, f)
                            file_size = os.path.getsize(file_path)
                            logger.info(f"   Found: {file_path} ({file_size} bytes)")
                            # Read first 2 lines of CSV files
                            if f.endswith('.csv'):
                                try:
                                    with open(file_path, 'r') as csvf:
                                        lines = csvf.readlines()[:3]
                                        logger.info(f"      Header: {lines[0].strip() if lines else 'EMPTY'}")
                                        if len(lines) > 1:
                                            logger.info(f"      Row 1: {lines[1].strip()}")
                                        logger.info(f"      Total lines: {len(open(file_path).readlines())}")
                                except Exception as read_e:
                                    logger.warning(f"      Could not read file: {read_e}")
                else:
                    logger.info(f"   Path is a file: {edges_path}")
            else:
                logger.warning(f"⚠️ Edges path does not exist: {edges_path}")
            
            bulk_load_result = {}
            try:
                # Execute bulk load using Aerospike Graph loader
                # Note: Both vertices and edges paths are REQUIRED by Aerospike Graph bulk loader
                logger.info("   Executing bulk load Gremlin call...")
                
                bulk_load_result["result"] = (self.client
                    .with_("evaluationTimeout", 2000000)
                    .call("aerospike.graphloader.admin.bulk-load.load")
                    .with_("aerospike.graphloader.vertices", vertices_path)
                    .with_("aerospike.graphloader.edges", edges_path)
                    .next())
                
                logger.info(f"   Bulk load Gremlin call returned: {bulk_load_result['result']}")
                bulk_load_result["success"] = True
                
                # Poll for bulk load status (time imported at module level)
                max_polls = 30
                for i in range(max_polls):
                    try:
                        status = self.client.call("aerospike.graphloader.admin.bulk-load.status").next()
                        logger.info(f"   Bulk load status (poll {i+1}): {status}")
                        status_str = str(status)
                        if "complete=true" in status_str or "step=done" in status_str:
                            logger.info(f"✅ Bulk load completed!")
                            if "bad-edges=" in status_str:
                                # Extract bad-edges count (re imported at module level)
                                match = re.search(r'bad-edges=(\d+)', status_str)
                                if match:
                                    bad_edges = int(match.group(1))
                                    if bad_edges > 0:
                                        logger.warning(f"⚠️ Bulk load had {bad_edges} bad edges!")
                            break
                        time.sleep(1)
                    except Exception as status_e:
                        logger.warning(f"   Could not get status: {status_e}")
                        break

            except Exception as e:
                logger.error(f"❌ Bulk load Gremlin call failed: {e}")
                import traceback
                logger.error(traceback.format_exc())
                bulk_load_result["success"] = False 
                bulk_load_result["error"] = str(e)
                      
            if bulk_load_result["success"]:
                logger.info("Bulk load completed successfully")
                
                # Get statistics about loaded data
                stats = self._get_bulk_load_statistics()
                
                return {
                    "success": True,
                    "message": "Data bulk loaded successfully from CSV files",
                    "vertices_path": vertices_path,
                    "edges_path": edges_path,
                    "statistics": stats,
                    "bulk_load_result": bulk_load_result["result"]
                }
            else:
                return {
                    "success": False,
                    "error": bulk_load_result["error"],
                    "vertices_path": vertices_path,
                    "edges_path": edges_path
                }
                
        except Exception as e:
            logger.error(f"Error during bulk load: {e}")
            return {
                "success": False,
                "error": str(e),
                "vertices_path": vertices_path if vertices_path else "default",
                "edges_path": edges_path if edges_path else "default"
            }

    def _get_bulk_load_statistics(self) -> Dict[str, Any]:
        """Get statistics about the loaded data after bulk load using the reusable get_graph_summary method"""
        try:
            # Reuse the existing get_graph_summary() which correctly parses the dict-based API response
            summary = self.get_graph_summary()
            
            if not summary:
                logger.warning("Could not retrieve graph summary after bulk load")
                return {
                    "total_vertices": 0, "total_edges": 0,
                    "users": 0, "accounts": 0, "devices": 0,
                    "owns_edges": 0, "uses_edges": 0,
                    "vertex_counts_by_label": {}, "edge_counts_by_label": {},
                    "supernode_count": 0, "supernode_counts_by_label": {}
                }
            
            vertices = summary.get('vertex_counts', {})
            edges = summary.get('edge_counts', {})
            
            stats = {
                "total_vertices": summary.get('total_vertex_count', 0),
                "total_edges": summary.get('total_edge_count', 0),
                "users": vertices.get('user', 0),
                "accounts": vertices.get('account', 0),
                "devices": vertices.get('device', 0),
                "owns_edges": edges.get('OWNS', 0),
                "uses_edges": edges.get('USES', 0),
                "vertex_counts_by_label": vertices,
                "edge_counts_by_label": edges,
                "supernode_count": summary.get('total_supernode_count', 0),
                "supernode_counts_by_label": summary.get('supernode_counts', {})
            }
            
            logger.info(f"Graph summary retrieved: {stats['total_vertices']} vertices, {stats['total_edges']} edges, "
                        f"{stats['users']} users, {stats['accounts']} accounts, {stats['devices']} devices")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting graph summary: {e}")
            return {
                "total_vertices": 0,
                "total_edges": 0,
                "users": 0,
                "accounts": 0,
                "devices": 0,
                "owns_edges": 0,
                "uses_edges": 0,
                "error": str(e)
            }

    def get_bulk_load_status(self) -> Dict[str, Any]:
        """Get the status of the current bulk load operation using Aerospike Graph Status API"""
        try:
            if not self.client:
                raise Exception("Graph client not available. Cannot check bulk load status without graph database connection.")
            logger.info("Checking bulk load status using Aerospike Graph Status API...")

            # Use Aerospike Graph Status API to check bulk load progress
            status_result = self.client.call("aerospike.graphloader.admin.bulk-load.status").next()
            
            logger.info(f"Raw bulk load status result: {status_result}")
            
            # Parse the status result
            status_info = {
                "step": status_result.get("step", "unknown"),
                "complete": status_result.get("complete", False),
                "status": status_result.get("status", "unknown"),
                "elements_written": status_result.get("elements-written"),
                "complete_partitions_percentage": status_result.get("complete-partitions-percentage"),
                "duplicate_vertex_ids": status_result.get("duplicate-vertex-ids"),
                "bad_entries": status_result.get("bad-entries"),
                "bad_edges": status_result.get("bad-edges"),
                "message": status_result.get("message"),
                "stacktrace": status_result.get("stacktrace")
            }
            
            # Clean up None values
            status_info = {k: v for k, v in status_info.items() if v is not None}
            
            logger.info(f"Bulk load status: {status_info.get('status', 'unknown')} - {status_info.get('step', 'unknown')}")
            
            # Determine if bulk load is complete or still running
            is_complete = status_info.get("complete", False)
            current_status = status_info.get("status", "unknown")
            
            return {
                "success": True,
                "message": f"Bulk load {current_status}" if current_status != "unknown" else "Status retrieved",
                "status": current_status,
                "step": status_info.get("step", "unknown"),
                "complete": is_complete,
                "elements_written": status_info.get("elements_written"),
                "progress_percentage": status_info.get("complete_partitions_percentage"),
                "details": status_info
            }
                
        except Exception as e:
            logger.error(f"Error getting bulk load status: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "Error occurred while checking bulk load status",
                "status": "error"
            }