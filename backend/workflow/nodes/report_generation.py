"""
Report Generation Node

Uses LLM to generate a comprehensive markdown report of the investigation.
Updated for agentic workflow - uses FinalAssessment from LLM agent.

Data Source: Ollama (Mistral)
"""

from datetime import datetime
from typing import Dict, Any
import logging
import os
import httpx

from workflow.state import InvestigationState, TraceEvent

logger = logging.getLogger('investigation.report_generation')

REPORT_PROMPT_TEMPLATE = """You are a fraud analyst AI. Generate a comprehensive investigation report in Markdown format.

## Investigation Details
- Investigation ID: {investigation_id}
- User ID: {user_id}
- Started: {started_at}

## Alert Information
- Trigger Type: {trigger_type}
- Flag Reason: {flag_reason}
- Original Risk Score: {original_score}

## User Profile
- Name: {user_name}
- Location: {location}
- Account Age: {account_age_days} days
- KYC Status: {kyc_completeness}

## Account Summary
- Total Accounts: {account_count}
- Total Balance: ${total_balance}
- Device Count: {device_count}
- Flagged Devices: {has_flagged_device}
- Shared Devices: {shared_device_count}

## Network Connections
- Direct Connections: {connection_count}
- High Risk Connections: {high_risk_connections}

## Recent Activity
- Transactions (7 days): {transaction_count}
- High Risk Transactions: {high_risk_transactions}

## AI Investigation Summary
The AI agent analyzed this case in {iterations} iterations, making {tool_calls} tool calls to gather evidence.

### Assessment
- **Fraud Typology**: {typology}
- **Risk Level**: {risk_level}
- **Risk Score**: {risk_score}/100
- **Recommended Action**: {decision}

### Agent's Reasoning
{reasoning}

## Tool Calls Made
{tool_call_summary}

Generate a professional investigation report with the following sections:
1. Executive Summary (2-3 sentences)
2. Key Risk Factors (bullet points)
3. Evidence Summary (what the AI agent found)
4. Investigation Timeline (tool calls and findings)
5. Risk Assessment Analysis
6. Recommendation and Rationale
7. Next Steps for Analyst

Use clear, professional language suitable for compliance review.
"""


async def report_generation_node(
    state: InvestigationState,
    ollama_client: Any = None  # Not used, we create our own client
) -> Dict[str, Any]:
    """
    Generate investigation report using LLM.
    
    Args:
        state: Current investigation state with FinalAssessment
        ollama_client: (Deprecated) HTTP client for Ollama
        
    Returns:
        Updated state with report markdown
    """
    user_id = state["user_id"]
    node_name = "report_generation"
    
    logger.info(f"[{node_name}] Starting report generation for user {user_id}")
    
    trace_events = []
    
    # Emit start event
    trace_events.append(TraceEvent(
        type="node_start",
        node=node_name,
        timestamp=datetime.now().isoformat(),
        data={"user_id": user_id, "llm_powered": True}
    ))
    
    try:
        # Extract evidence from new state structure
        alert = state.get("alert_evidence") or {}
        initial = state.get("initial_evidence") or {}
        assessment = state.get("final_assessment") or {}
        tool_calls = state.get("tool_calls") or []
        
        profile = initial.get("profile", {})
        metrics = initial.get("account_metrics", {})
        accounts = initial.get("accounts", [])
        devices = initial.get("devices", [])
        transactions = initial.get("recent_transactions", [])
        connections = initial.get("direct_connections", [])
        
        # Build tool call summary
        tool_call_summary = _build_tool_call_summary(tool_calls)
        
        # Calculate high risk counts
        high_risk_conn = sum(1 for c in connections if c.get("risk_score", 0) >= 70)
        high_risk_txn = sum(1 for t in transactions if t.get("fraud_score", 0) > 50)
        
        prompt = REPORT_PROMPT_TEMPLATE.format(
            investigation_id=state.get("investigation_id", "N/A"),
            user_id=user_id,
            started_at=state.get("started_at", ""),
            trigger_type=alert.get("trigger_type", "Unknown"),
            flag_reason=alert.get("flag_reason", "N/A"),
            original_score=alert.get("original_score", 0),
            user_name=profile.get("name", "Unknown"),
            location=profile.get("location", "Unknown"),
            account_age_days=metrics.get("account_age_days", 0),
            kyc_completeness=metrics.get("kyc_completeness", "unknown"),
            account_count=len(accounts),
            total_balance=metrics.get("total_balance", 0),
            device_count=len(devices),
            has_flagged_device=metrics.get("has_flagged_device", False),
            shared_device_count=metrics.get("shared_device_count", 0),
            connection_count=len(connections),
            high_risk_connections=high_risk_conn,
            transaction_count=len(transactions),
            high_risk_transactions=high_risk_txn,
            iterations=state.get("agent_iterations", 0),
            tool_calls=len(tool_calls),
            typology=assessment.get("typology", "unknown"),
            risk_level=assessment.get("risk_level", "unknown"),
            risk_score=assessment.get("risk_score", 0),
            decision=assessment.get("decision", "pending"),
            reasoning=assessment.get("reasoning", "No reasoning provided"),
            tool_call_summary=tool_call_summary
        )
        
        # Call LLM (supports both Gemini and Ollama via env config)
        response = await _call_llm(prompt)
        
        # Clean up the response
        report_markdown = _clean_report(response, state)
        
        # Emit complete event
        trace_events.append(TraceEvent(
            type="node_complete",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={"status": "success", "report_length": len(report_markdown)}
        ))
        
        logger.info(f"[{node_name}] Report generation complete - {len(report_markdown)} characters")
        
        return {
            "report_markdown": report_markdown,
            "current_phase": "report",
            "current_node": "complete",
            "workflow_status": "completed",
            "trace_events": trace_events
        }
        
    except Exception as e:
        logger.error(f"[{node_name}] Error during report generation: {e}")
        
        trace_events.append(TraceEvent(
            type="error",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={"error": str(e)}
        ))
        
        # Generate fallback report
        fallback_report = _generate_fallback_report(state)
        
        return {
            "report_markdown": fallback_report,
            "current_phase": "report",
            "current_node": "complete",
            "workflow_status": "completed",
            "error_message": str(e),
            "trace_events": trace_events
        }


async def _call_ollama(prompt: str) -> str:
    """Call Ollama API with the prompt."""
    base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    model = os.environ.get("OLLAMA_MODEL", "mistral")
    
    logger.info(f"[Report] Calling Ollama at {base_url} with model {model}")
    
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.post(
            f"{base_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.4,
                    "num_predict": 1500
                }
            }
        )
        response.raise_for_status()
        result = response.json()
        return result.get("response", "")


async def _call_gemini(prompt: str) -> str:
    """Call Google Gemini API using native generateContent endpoint."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    model = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
    
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is required for Gemini provider")
    
    logger.info(f"[Report] Calling Gemini API with model {model}")
    
    # Use native Gemini API endpoint
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.post(
            url,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key
            },
            json={
                "contents": [
                    {
                        "parts": [{"text": prompt}]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.4,
                    "maxOutputTokens": 1500
                }
            }
        )
        response.raise_for_status()
        result = response.json()
        
        # Extract response text from Gemini format
        candidates = result.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if parts:
                return parts[0].get("text", "")
        
        return ""


async def _call_llm(prompt: str) -> str:
    """Call the configured LLM provider (Gemini or Ollama)."""
    provider = os.environ.get("LLM_PROVIDER", "ollama").lower()
    
    if provider == "gemini":
        return await _call_gemini(prompt)
    else:
        return await _call_ollama(prompt)


def _build_tool_call_summary(tool_calls: list) -> str:
    """Build a summary of tool calls for the report."""
    if not tool_calls:
        return "No tool calls made - assessment based on initial evidence only."
    
    lines = []
    for i, call in enumerate(tool_calls[:10], 1):  # Limit to 10
        tool = call.get("tool", "unknown")
        params = call.get("params", {})
        timestamp = call.get("timestamp", "")
        
        # Format parameters
        params_str = ", ".join(f"{k}={v}" for k, v in params.items())[:50]
        
        lines.append(f"{i}. **{tool}**({params_str})")
    
    if len(tool_calls) > 10:
        lines.append(f"... and {len(tool_calls) - 10} more tool calls")
    
    return "\n".join(lines)


def _clean_report(response: str, state: InvestigationState) -> str:
    """Clean and format the generated report."""
    initial = state.get("initial_evidence") or {}
    profile = initial.get("profile", {})
    
    # Add header if not present
    if not response.strip().startswith("#"):
        user_name = profile.get("name", "Unknown")
        response = f"# Fraud Investigation Report\n## User: {user_name}\n\n{response}"
    
    # Add footer with metadata
    footer = f"""

---
*Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*  
*Investigation ID: {state.get('investigation_id', 'N/A')}*  
*User ID: {state.get('user_id', 'N/A')}*  
*AI Agent Iterations: {state.get('agent_iterations', 0)}*  
*Tool Calls Made: {len(state.get('tool_calls', []))}*
"""
    
    return response.strip() + footer


def _generate_fallback_report(state: InvestigationState) -> str:
    """Generate a structured report when LLM fails."""
    alert = state.get("alert_evidence") or {}
    initial = state.get("initial_evidence") or {}
    assessment = state.get("final_assessment") or {}
    tool_calls = state.get("tool_calls") or []
    
    profile = initial.get("profile", {})
    metrics = initial.get("account_metrics", {})
    accounts = initial.get("accounts", [])
    devices = initial.get("devices", [])
    transactions = initial.get("recent_transactions", [])
    connections = initial.get("direct_connections", [])
    
    high_risk_conn = sum(1 for c in connections if c.get("risk_score", 0) >= 70)
    
    report = f"""# Fraud Investigation Report

## Executive Summary

Investigation of user **{profile.get('name', 'Unknown')}** ({state.get('user_id', 'N/A')}) 
triggered by **{alert.get('trigger_type', 'Unknown')}** alert.

**AI Assessment**: {assessment.get('typology', 'Unknown').upper()} - {assessment.get('risk_level', 'Unknown').upper()} RISK ({assessment.get('risk_score', 0)}/100)

**Recommended Action**: {assessment.get('decision', 'pending').replace('_', ' ').title()}

---

## Key Risk Factors

"""
    
    # Add risk factors
    risk_factors = []
    if metrics.get("has_flagged_device"):
        risk_factors.append("- ⚠️ **Flagged device detected**")
    if metrics.get("shared_device_count", 0) > 2:
        risk_factors.append(f"- ⚠️ **Shared devices** with {metrics['shared_device_count']} other users")
    if high_risk_conn > 0:
        risk_factors.append(f"- ⚠️ **High risk connections**: {high_risk_conn} users")
    if metrics.get("account_age_days", 365) < 30:
        risk_factors.append(f"- ⚠️ **New account** ({metrics.get('account_age_days', 0)} days old)")
    if alert.get("original_score", 0) >= 70:
        risk_factors.append(f"- ⚠️ **High initial risk score**: {alert.get('original_score', 0)}")
    
    if risk_factors:
        report += "\n".join(risk_factors)
    else:
        report += "- No critical risk factors identified"
    
    report += f"""

---

## AI Investigation Summary

The AI agent analyzed this case in **{state.get('agent_iterations', 0)} iterations**, 
making **{len(tool_calls)} tool calls** to gather evidence.

### Assessment Details
| Attribute | Value |
|-----------|-------|
| Typology | {assessment.get('typology', 'Unknown')} |
| Risk Level | {assessment.get('risk_level', 'Unknown')} |
| Risk Score | {assessment.get('risk_score', 0)}/100 |
| Decision | {assessment.get('decision', 'pending')} |

### Agent's Reasoning
{assessment.get('reasoning', 'No reasoning provided')}

---

## Evidence Summary

### User Profile
| Attribute | Value |
|-----------|-------|
| Name | {profile.get('name', 'Unknown')} |
| Location | {profile.get('location', 'Unknown')} |
| Account Age | {metrics.get('account_age_days', 0)} days |
| KYC Status | {metrics.get('kyc_completeness', 'Unknown')} |

### Accounts & Devices
| Metric | Value |
|--------|-------|
| Total Accounts | {len(accounts)} |
| Total Balance | ${metrics.get('total_balance', 0):,.2f} |
| Device Count | {len(devices)} |
| Flagged Devices | {metrics.get('has_flagged_device', False)} |
| Shared Devices | {metrics.get('shared_device_count', 0)} |

### Network Connections
| Metric | Value |
|--------|-------|
| Direct Connections | {len(connections)} |
| High Risk Connections | {high_risk_conn} |
| Device-Shared Users | {sum(1 for c in connections if c.get('connection_type') == 'device')} |

### Recent Activity (7 days)
| Metric | Value |
|--------|-------|
| Transactions | {len(transactions)} |
| High Risk Transactions | {sum(1 for t in transactions if t.get('fraud_score', 0) > 50)} |
| Total Amount | ${sum(t.get('amount', 0) for t in transactions):,.2f} |

---

## Tool Calls Made

{_build_tool_call_summary(tool_calls)}

---

## Recommendation

**Recommended Action**: {assessment.get('decision', 'pending').replace('_', ' ').title()}

Based on the {assessment.get('risk_level', 'unknown')} risk level and {assessment.get('typology', 'unknown')} classification, 
{'immediate action is recommended' if assessment.get('risk_score', 0) >= 70 else 'continued monitoring is advised'}.

---

## Next Steps for Analyst

1. Review the tool call findings above
2. Verify the AI assessment against manual review
3. {'Take immediate action on high-risk items' if assessment.get('risk_score', 0) >= 70 else 'Document findings for ongoing monitoring'}
4. Update case status based on final decision
5. Complete compliance documentation

---
*Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*  
*Investigation ID: {state.get('investigation_id', 'N/A')}*  
*User ID: {state.get('user_id', 'N/A')}*  
*AI Agent Iterations: {state.get('agent_iterations', 0)}*  
*Tool Calls Made: {len(tool_calls)}*
"""
    
    return report
