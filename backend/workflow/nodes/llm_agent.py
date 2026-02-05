"""
LLM Reasoning Agent Node

ReAct-style agent that uses tools to gather evidence and make decisions.
The LLM decides what data to collect, how many hops to traverse, etc.
Loops until it calls submit_assessment or hits safety limits.

LLM: Mistral via Ollama (local or Docker)
"""

import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
import httpx

from workflow.state import InvestigationState, AgentMessage, ToolCall, FinalAssessment, TraceEvent
from workflow.tools.investigation_tools import InvestigationTools

logger = logging.getLogger('investigation.llm_agent')

# Safety limits
MAX_ITERATIONS = 8
MAX_TOOL_CALLS = 15
TIMEOUT_SECONDS = 180


def llm_agent_node(
    state: InvestigationState,
    aerospike_service: Any,
    graph_service: Any
) -> Dict[str, Any]:
    """
    LLM reasoning agent that iteratively gathers evidence and makes decisions.
    
    The agent:
    1. Receives initial evidence from data_collection
    2. Uses available tools to gather more data as needed
    3. Calls submit_assessment when it has enough evidence
    
    Args:
        state: Current investigation state
        aerospike_service: Aerospike KV service
        graph_service: Aerospike Graph service
        
    Returns:
        Updated state with final assessment
    """
    user_id = state["user_id"]
    node_name = "llm_agent"
    
    logger.info(f"[{node_name}] Starting LLM agent for user {user_id}")
    
    # Initialize tools
    tools = InvestigationTools(aerospike_service, graph_service, user_id)
    
    # Get initial evidence
    initial_evidence = state.get("initial_evidence", {})
    alert_evidence = state.get("alert_evidence", {})
    
    # Build initial context for LLM
    evidence_summary = _build_evidence_summary(initial_evidence, alert_evidence)
    
    # Agent loop state
    agent_messages: List[AgentMessage] = []
    tool_calls: List[ToolCall] = []
    accumulated_evidence: Dict[str, Any] = {"initial": evidence_summary}
    trace_events: List[TraceEvent] = []
    
    # Emit start
    trace_events.append(TraceEvent(
        type="node_start",
        node=node_name,
        timestamp=datetime.now().isoformat(),
        data={"user_id": user_id, "max_iterations": MAX_ITERATIONS}
    ))
    
    # Get Ollama configuration
    ollama_base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    ollama_model = os.environ.get("OLLAMA_MODEL", "mistral")
    
    iteration = 0
    final_assessment = None
    error_count = 0
    
    try:
        while iteration < MAX_ITERATIONS:
            iteration += 1
            
            logger.info(f"[{node_name}] Iteration {iteration}/{MAX_ITERATIONS}")
            
            # Emit iteration event
            trace_events.append(TraceEvent(
                type="agent_iteration",
                node=node_name,
                timestamp=datetime.now().isoformat(),
                data={"iteration": iteration, "tool_calls_so_far": len(tool_calls)}
            ))
            
            # Build prompt for this iteration
            prompt = _build_agent_prompt(
                evidence_summary,
                accumulated_evidence,
                agent_messages,
                tool_calls,
                iteration
            )
            
            # Call LLM
            try:
                llm_response = _call_ollama(ollama_base_url, ollama_model, prompt)
                
                # Record message
                agent_messages.append(AgentMessage(
                    role="assistant",
                    content=llm_response,
                    timestamp=datetime.now().isoformat()
                ))
                
                # Emit thinking event
                trace_events.append(TraceEvent(
                    type="agent_thinking",
                    node=node_name,
                    timestamp=datetime.now().isoformat(),
                    data={
                        "iteration": iteration,
                        "response_preview": llm_response[:200] if llm_response else ""
                    }
                ))
                
            except Exception as e:
                logger.error(f"[{node_name}] LLM call failed: {e}")
                error_count += 1
                
                if error_count >= 3:
                    # Fallback to deterministic assessment
                    logger.warning(f"[{node_name}] Falling back to deterministic assessment")
                    final_assessment = _deterministic_assessment(initial_evidence, alert_evidence)
                    break
                
                continue
            
            # Parse tool call from response
            tool_name, tool_params = _parse_tool_call(llm_response)
            
            if not tool_name:
                logger.warning(f"[{node_name}] No valid tool call in response, retrying")
                error_count += 1
                if error_count >= 3:
                    final_assessment = _deterministic_assessment(initial_evidence, alert_evidence)
                    break
                continue
            
            error_count = 0  # Reset on successful parse
            
            # Check if this is the exit tool
            if tool_name == "submit_assessment":
                logger.info(f"[{node_name}] Agent submitted assessment")
                
                final_assessment = FinalAssessment(
                    typology=tool_params.get("typology", "unknown"),
                    risk_level=tool_params.get("risk_level", "medium"),
                    risk_score=tool_params.get("risk_score", 50),
                    decision=tool_params.get("decision", "allow_monitor"),
                    reasoning=tool_params.get("reasoning", "Assessment submitted by agent"),
                    iteration=iteration,
                    tool_calls_made=len(tool_calls)
                )
                
                # Emit assessment event
                trace_events.append(TraceEvent(
                    type="assessment",
                    node=node_name,
                    timestamp=datetime.now().isoformat(),
                    data={
                        "typology": final_assessment["typology"],
                        "risk_level": final_assessment["risk_level"],
                        "risk_score": final_assessment["risk_score"],
                        "decision": final_assessment["decision"]
                    }
                ))
                
                break
            
            # Check tool call limit
            if len(tool_calls) >= MAX_TOOL_CALLS:
                logger.warning(f"[{node_name}] Hit tool call limit, forcing assessment")
                final_assessment = _deterministic_assessment(
                    initial_evidence, 
                    alert_evidence,
                    accumulated_evidence
                )
                break
            
            # Execute the tool
            logger.info(f"[{node_name}] Executing tool: {tool_name}({tool_params})")
            
            tool_result = tools.execute_tool(tool_name, tool_params)
            
            # Record tool call
            tool_call = ToolCall(
                tool=tool_name,
                params=tool_params,
                result=tool_result,
                timestamp=datetime.now().isoformat(),
                iteration=iteration
            )
            tool_calls.append(tool_call)
            
            # Add result to accumulated evidence
            accumulated_evidence[f"{tool_name}_{len(tool_calls)}"] = tool_result
            
            # Emit tool event
            trace_events.append(TraceEvent(
                type="tool_call",
                node=node_name,
                timestamp=datetime.now().isoformat(),
                data={
                    "tool": tool_name,
                    "params": tool_params,
                    "result_summary": tools.tool_calls[-1].get("result_summary", ""),
                    "iteration": iteration
                }
            ))
            
            # Add tool result to messages for next iteration
            agent_messages.append(AgentMessage(
                role="tool",
                content=json.dumps(tool_result, default=str),
                timestamp=datetime.now().isoformat(),
                tool_name=tool_name
            ))
        
        # If we hit max iterations without assessment
        if not final_assessment:
            logger.warning(f"[{node_name}] Max iterations reached, using deterministic assessment")
            final_assessment = _deterministic_assessment(
                initial_evidence, 
                alert_evidence,
                accumulated_evidence
            )
        
        # Emit complete
        trace_events.append(TraceEvent(
            type="node_complete",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={
                "iterations": iteration,
                "tool_calls": len(tool_calls),
                "typology": final_assessment["typology"],
                "risk_level": final_assessment["risk_level"]
            }
        ))
        
        logger.info(
            f"[{node_name}] Complete - {iteration} iterations, "
            f"{len(tool_calls)} tool calls, "
            f"typology: {final_assessment['typology']}"
        )
        
        return {
            "final_assessment": final_assessment,
            "agent_messages": agent_messages,
            "tool_calls": tool_calls,
            "agent_iterations": iteration,
            "current_node": "report_generation",
            "current_phase": "report",
            "trace_events": trace_events
        }
        
    except Exception as e:
        logger.error(f"[{node_name}] Agent error: {e}")
        
        trace_events.append(TraceEvent(
            type="error",
            node=node_name,
            timestamp=datetime.now().isoformat(),
            data={"error": str(e)}
        ))
        
        # Return with deterministic assessment on error
        return {
            "final_assessment": _deterministic_assessment(initial_evidence, alert_evidence),
            "agent_messages": agent_messages,
            "tool_calls": tool_calls,
            "agent_iterations": iteration,
            "current_node": "report_generation",
            "current_phase": "report",
            "error_message": str(e),
            "trace_events": trace_events
        }


def _build_evidence_summary(initial: Dict[str, Any], alert: Dict[str, Any]) -> str:
    """Build a text summary of initial evidence for LLM."""
    
    profile = initial.get("profile", {})
    accounts = initial.get("accounts", [])
    devices = initial.get("devices", [])
    transactions = initial.get("recent_transactions", [])
    connections = initial.get("direct_connections", [])
    metrics = initial.get("account_metrics", {})
    
    lines = [
        "# INVESTIGATION EVIDENCE",
        "",
        "## Alert Information",
        f"- Trigger Type: {alert.get('trigger_type', 'Unknown')}",
        f"- Original Risk Score: {alert.get('original_score', 0)}",
        f"- Flag Reason: {alert.get('flag_reason', 'Not specified')}",
        "",
        "## User Profile",
        f"- Name: {profile.get('name', 'Unknown')}",
        f"- Location: {profile.get('location', 'Unknown')}",
        f"- Account Age: {metrics.get('account_age_days', 0)} days",
        f"- Current Risk Score: {metrics.get('profile_risk_score', 0)}",
        f"- KYC Status: {metrics.get('kyc_completeness', 'unknown')}",
        "",
        "## Account Summary",
        f"- Total Accounts: {len(accounts)}",
        f"- Total Balance: ${metrics.get('total_balance', 0):,.2f}",
    ]
    
    # Add account details
    for acc in accounts[:3]:
        lines.append(f"  - {acc.get('type', 'Unknown')} ({acc.get('id', '')}): ${acc.get('balance', 0):,.2f}")
    
    lines.extend([
        "",
        "## Device Information",
        f"- Total Devices: {len(devices)}",
        f"- Flagged Devices: {metrics.get('has_flagged_device', False)}",
        f"- Shared Devices: {metrics.get('shared_device_count', 0)}",
    ])
    
    # Add device details
    for dev in devices[:3]:
        flag = "⚠️ FLAGGED" if dev.get("fraud_flag") else ""
        shared = f"(shared by {dev.get('user_count', 1)} users)" if dev.get('user_count', 1) > 1 else ""
        lines.append(f"  - {dev.get('type', 'Unknown')} {dev.get('os', '')} {flag} {shared}")
    
    lines.extend([
        "",
        "## Recent Transactions (Last 7 Days)",
        f"- Transaction Count: {len(transactions)}",
    ])
    
    if transactions:
        total = sum(t.get("amount", 0) for t in transactions)
        high_risk = sum(1 for t in transactions if t.get("fraud_score", 0) > 50)
        lines.append(f"- Total Amount: ${total:,.2f}")
        lines.append(f"- High Risk Transactions: {high_risk}")
    
    lines.extend([
        "",
        "## Direct Connections (1-hop)",
        f"- Total Connections: {len(connections)}",
    ])
    
    # Count by risk level
    high_risk_conn = sum(1 for c in connections if c.get("risk_score", 0) >= 70)
    if high_risk_conn > 0:
        lines.append(f"- High Risk Connections: {high_risk_conn}")
    
    # Device connections
    device_conn = sum(1 for c in connections if c.get("connection_type") == "device")
    if device_conn > 0:
        lines.append(f"- Device-Shared Users: {device_conn}")
    
    return "\n".join(lines)


def _build_agent_prompt(
    evidence_summary: str,
    accumulated_evidence: Dict[str, Any],
    messages: List[AgentMessage],
    tool_calls: List[ToolCall],
    iteration: int
) -> str:
    """Build the prompt for the LLM agent."""
    
    tool_descriptions = InvestigationTools.get_tool_descriptions()
    
    # Build conversation history
    history = ""
    for msg in messages[-6:]:  # Keep last 6 messages for context
        if msg["role"] == "assistant":
            history += f"\nAssistant: {msg['content'][:500]}"
        elif msg["role"] == "tool":
            tool_name = msg.get("tool_name", "unknown")
            history += f"\nTool Result ({tool_name}): {msg['content'][:800]}"
    
    # Decide what the agent should do based on iteration
    action_hint = ""
    if iteration == 1:
        action_hint = "Start by getting transaction history with get_transactions."
    elif iteration >= 3 and len(tool_calls) >= 2:
        action_hint = "You have gathered evidence. Consider submitting your assessment with submit_assessment."
    elif iteration >= MAX_ITERATIONS - 2:
        action_hint = "Time is running out. Submit your final assessment now with submit_assessment."
    
    prompt = f"""You are a fraud investigation agent. Analyze the evidence and respond with ONLY a JSON object.

{evidence_summary}

## TOOLS AVAILABLE
{tool_descriptions}

## CURRENT STATUS
- Iteration: {iteration}/{MAX_ITERATIONS}
- Tool calls made: {len(tool_calls)}/{MAX_TOOL_CALLS}
{f"- Hint: {action_hint}" if action_hint else ""}

{history}

## RESPONSE FORMAT
You MUST respond with ONLY a valid JSON object. No other text, no explanations, just JSON.

Example tool call:
{{"tool": "get_transactions", "params": {{"days": 30, "min_amount": 1000}}}}

Example assessment submission:
{{"tool": "submit_assessment", "params": {{"typology": "money_mule", "risk_level": "high", "risk_score": 85, "decision": "temporary_freeze", "reasoning": "High transaction velocity with multiple high-value transfers"}}}}

Valid values:
- typology: account_takeover, money_mule, synthetic_identity, promo_abuse, friendly_fraud, card_testing, fraud_ring, suspicious_activity, legitimate
- risk_level: low, medium, high, critical  
- risk_score: integer 0-100
- decision: allow_monitor, step_up_auth, temporary_freeze, full_block, escalate_compliance

YOUR JSON RESPONSE:"""

    return prompt


def _call_ollama(base_url: str, model: str, prompt: str) -> str:
    """Call Ollama API synchronously."""
    import time
    
    logger.info(f"[LLM] Calling Ollama at {base_url} with model {model}")
    start = time.time()
    
    try:
        with httpx.Client(timeout=300.0) as client:
            response = client.post(
                f"{base_url}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 1000
                    }
                }
            )
            response.raise_for_status()
            result = response.json()
            
            elapsed = time.time() - start
            logger.info(f"[LLM] Ollama responded in {elapsed:.1f}s")
            
            return result.get("response", "")
            
    except Exception as e:
        logger.error(f"[LLM] Ollama error: {e}")
        raise


def _parse_tool_call(response: str) -> Tuple[Optional[str], Dict[str, Any]]:
    """Parse tool call from LLM response with improved handling."""
    
    if not response:
        return None, {}
    
    # Clean response - remove markdown code blocks if present
    cleaned = response.strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()
    
    # Strategy 1: Try direct JSON parse (if response is just JSON)
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict) and "tool" in data:
            logger.debug(f"[Parse] Strategy 1 success: {data.get('tool')}")
            return data.get("tool"), data.get("params", {})
    except Exception:
        pass
    
    # Strategy 2: Find JSON object with balanced braces
    try:
        start_idx = cleaned.find('{')
        if start_idx >= 0:
            depth = 0
            end_idx = start_idx
            for i, char in enumerate(cleaned[start_idx:], start_idx):
                if char == '{':
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if depth == 0:
                        end_idx = i + 1
                        break
            
            if end_idx > start_idx:
                json_str = cleaned[start_idx:end_idx]
                data = json.loads(json_str)
                if isinstance(data, dict) and "tool" in data:
                    logger.debug(f"[Parse] Strategy 2 success: {data.get('tool')}")
                    return data.get("tool"), data.get("params", {})
    except Exception:
        pass
    
    # Strategy 3: Regex for tool pattern with nested params
    try:
        # Match {"tool": "...", "params": {...}}
        pattern = r'\{\s*"tool"\s*:\s*"([^"]+)"\s*,\s*"params"\s*:\s*(\{[^}]*\}|\{\})\s*\}'
        match = re.search(pattern, cleaned, re.DOTALL)
        if match:
            tool_name = match.group(1)
            params_str = match.group(2)
            try:
                params = json.loads(params_str)
            except:
                params = {}
            logger.debug(f"[Parse] Strategy 3 success: {tool_name}")
            return tool_name, params
    except Exception:
        pass
    
    # Strategy 4: Look for tool name without params
    try:
        pattern = r'"tool"\s*:\s*"([^"]+)"'
        match = re.search(pattern, cleaned)
        if match:
            tool_name = match.group(1)
            # Try to extract params separately
            params = {}
            params_match = re.search(r'"params"\s*:\s*(\{[^}]*\})', cleaned)
            if params_match:
                try:
                    params = json.loads(params_match.group(1))
                except:
                    pass
            logger.debug(f"[Parse] Strategy 4 success: {tool_name}")
            return tool_name, params
    except Exception:
        pass
    
    # Strategy 5: Fix common JSON issues and retry
    try:
        # Replace single quotes with double quotes
        fixed = cleaned.replace("'", '"')
        # Fix unquoted keys
        fixed = re.sub(r'(\{|\,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1 "\2":', fixed)
        
        start_idx = fixed.find('{')
        end_idx = fixed.rfind('}') + 1
        if start_idx >= 0 and end_idx > start_idx:
            data = json.loads(fixed[start_idx:end_idx])
            if isinstance(data, dict) and "tool" in data:
                logger.debug(f"[Parse] Strategy 5 success: {data.get('tool')}")
                return data.get("tool"), data.get("params", {})
    except Exception:
        pass
    
    logger.warning(f"[Parse] Failed to extract tool call from: {cleaned[:200]}...")
    return None, {}


def _deterministic_assessment(
    initial: Dict[str, Any],
    alert: Dict[str, Any],
    accumulated: Dict[str, Any] = None
) -> FinalAssessment:
    """
    Fallback deterministic assessment when LLM fails.
    Uses rule-based logic based on evidence.
    """
    
    metrics = initial.get("account_metrics", {})
    trigger_type = alert.get("trigger_type", "unknown")
    original_score = alert.get("original_score", 50)
    
    # Calculate risk score
    risk_score = original_score
    
    # Adjust based on devices
    if metrics.get("has_flagged_device"):
        risk_score += 20
    if metrics.get("shared_device_count", 0) > 2:
        risk_score += 15
    
    # Adjust based on connections
    connections = initial.get("direct_connections", [])
    high_risk_conn = sum(1 for c in connections if c.get("risk_score", 0) >= 70)
    risk_score += high_risk_conn * 5
    
    # Cap at 100
    risk_score = min(100, risk_score)
    
    # Determine risk level
    if risk_score >= 80:
        risk_level = "critical"
    elif risk_score >= 60:
        risk_level = "high"
    elif risk_score >= 40:
        risk_level = "medium"
    else:
        risk_level = "low"
    
    # Determine typology
    typology = "unknown"
    if trigger_type == "RT3":
        typology = "fraud_ring" if metrics.get("shared_device_count", 0) > 2 else "supernode_detection"
    elif trigger_type == "RT2":
        typology = "fraud_ring"
    elif trigger_type == "RT1":
        typology = "suspicious_activity"
    
    # Determine decision
    if risk_score >= 80:
        decision = "temporary_freeze"
    elif risk_score >= 60:
        decision = "step_up_auth"
    else:
        decision = "allow_monitor"
    
    return FinalAssessment(
        typology=typology,
        risk_level=risk_level,
        risk_score=risk_score,
        decision=decision,
        reasoning=f"Deterministic assessment based on {trigger_type} alert with risk score {original_score}",
        iteration=0,
        tool_calls_made=0
    )
