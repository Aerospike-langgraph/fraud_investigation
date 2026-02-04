"""
Investigation Workflow Package

LangGraph-based fraud investigation workflow that orchestrates
multi-dimensional evidence gathering and LLM-powered analysis.
"""

from workflow.state import InvestigationState, create_initial_state
from workflow.graph import create_investigation_workflow

__all__ = [
    "InvestigationState",
    "create_initial_state", 
    "create_investigation_workflow"
]
