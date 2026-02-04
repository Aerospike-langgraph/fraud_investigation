"""
Investigation Workflow Nodes

Agentic 4-node architecture:
- alert_validation: Get flag context
- data_collection: Gather baseline evidence  
- llm_agent: ReAct agent with tool calling
- report_generation: Generate final report
"""

from workflow.nodes.alert_validation import alert_validation_node
from workflow.nodes.data_collection import data_collection_node
from workflow.nodes.llm_agent import llm_agent_node
from workflow.nodes.report_generation import report_generation_node

__all__ = [
    "alert_validation_node",
    "data_collection_node",
    "llm_agent_node",
    "report_generation_node",
]
