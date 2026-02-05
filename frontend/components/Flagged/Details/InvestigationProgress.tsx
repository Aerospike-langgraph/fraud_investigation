"use client";

import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import {
  CheckCircle,
  Circle,
  Loader2,
  AlertCircle,
  Brain,
  Database,
  Network,
  FileText,
  ShieldAlert,
  Wrench,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import type { WorkflowStep, TraceEvent } from "@/hooks/useInvestigation";

interface ToolCallInfo {
  tool: string;
  params: Record<string, unknown>;
  timestamp: string;
  result_summary?: string;
}

interface InvestigationProgressProps {
  status: "idle" | "connecting" | "running" | "completed" | "error";
  currentNode: string;
  currentPhase: string;
  progress: number;
  steps: WorkflowStep[];
  completedSteps: string[];
  error?: string;
  traceEvents?: TraceEvent[];
  getStepStatus: (stepId: string) => "pending" | "running" | "completed";
}

const stepIcons: Record<string, React.ReactNode> = {
  alert_validation: <ShieldAlert className="w-4 h-4" />,
  data_collection: <Database className="w-4 h-4" />,
  llm_agent: <Brain className="w-4 h-4" />,
  report_generation: <FileText className="w-4 h-4" />,
};

const phaseColors: Record<string, string> = {
  context: "bg-blue-500",
  evidence: "bg-cyan-500",
  reasoning: "bg-purple-500",
  report: "bg-emerald-500",
};

// Default steps for the new 4-node architecture
const defaultSteps: WorkflowStep[] = [
  { id: "alert_validation", name: "Alert Validation", description: "Extract alert trigger context", phase: "context" },
  { id: "data_collection", name: "Data Collection", description: "Gather baseline evidence", phase: "evidence" },
  { id: "llm_agent", name: "AI Investigation Agent", description: "LLM agent uses tools to investigate", phase: "reasoning" },
  { id: "report_generation", name: "Report Generation", description: "Generate investigation report", phase: "report" },
];

export function InvestigationProgress({
  status,
  currentNode,
  currentPhase,
  progress,
  steps,
  completedSteps,
  error,
  traceEvents = [],
  getStepStatus,
}: InvestigationProgressProps) {
  const [showToolCalls, setShowToolCalls] = React.useState(true);
  
  // Extract tool calls from trace events
  const toolCalls = React.useMemo(() => {
    const calls: ToolCallInfo[] = [];
    traceEvents.forEach(event => {
      if (event.type === "tool_call" && event.data) {
        calls.push({
          tool: event.data.tool || "unknown",
          params: event.data.params || {},
          timestamp: event.timestamp || event.data.timestamp,
          result_summary: event.data.result_summary,
        });
      }
    });
    return calls;
  }, [traceEvents]);

  // Extract agent iterations from trace events
  const agentIterations = React.useMemo(() => {
    let maxIteration = 0;
    traceEvents.forEach(event => {
      if (event.type === "agent_iteration" && event.data?.iteration) {
        maxIteration = Math.max(maxIteration, event.data.iteration);
      }
    });
    return maxIteration;
  }, [traceEvents]);

  // Group steps by phase
  const phases = React.useMemo(() => {
    const stepsToUse = steps.length > 0 ? steps : (status === "running" || status === "completed" ? defaultSteps : []);
    
    const grouped: Record<string, WorkflowStep[]> = {};
    stepsToUse.forEach((step) => {
      if (!grouped[step.phase]) {
        grouped[step.phase] = [];
      }
      grouped[step.phase].push(step);
    });
    return grouped;
  }, [steps, status]);

  const phaseOrder = ["context", "evidence", "reasoning", "report"];
  const phaseLabels: Record<string, string> = {
    context: "Alert Context",
    evidence: "Evidence Gathering",
    reasoning: "AI Agent Investigation",
    report: "Report",
  };

  // Calculate display steps
  const displaySteps = steps.length > 0 ? steps : (status === "running" || status === "completed" ? defaultSteps : []);
  const calculatedProgress = displaySteps.length > 0 ? Math.round((completedSteps.length / displaySteps.length) * 100) : 0;

  if (status === "idle") {
    return (
      <Card className="bg-white border-slate-200 shadow-sm">
        <CardContent className="pt-6">
          <div className="text-center text-slate-500">
            <Brain className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p className="text-slate-600">Click &quot;Start AI Investigation&quot; to begin</p>
            <p className="text-xs mt-2 text-slate-500">
              The AI agent will analyze the account using multiple tools
            </p>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (status === "connecting") {
    return (
      <Card className="bg-white border-slate-200 shadow-sm">
        <CardContent className="pt-6">
          <div className="text-center">
            <Loader2 className="w-12 h-12 mx-auto mb-4 animate-spin text-indigo-500" />
            <p className="text-slate-600">Connecting to AI investigation service...</p>
            <p className="text-slate-500 text-sm mt-2">Establishing connection to backend</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (status === "error") {
    return (
      <Card className="bg-white border-red-200 shadow-sm">
        <CardContent className="pt-6">
          <div className="text-center">
            <AlertCircle className="w-12 h-12 mx-auto mb-4 text-red-500" />
            <p className="text-red-600 font-medium">Investigation Error</p>
            <p className="text-slate-500 text-sm mt-2">{error || "An error occurred"}</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="bg-white border-slate-200 shadow-sm">
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg flex items-center gap-2 text-slate-900">
            {status === "running" && (
              <Loader2 className="w-5 h-5 animate-spin text-indigo-500" />
            )}
            {status === "completed" && (
              <CheckCircle className="w-5 h-5 text-emerald-500" />
            )}
            AI Investigation Progress
          </CardTitle>
          <div className="flex items-center gap-2">
            {toolCalls.length > 0 && (
              <Badge variant="outline" className="text-xs border-purple-200 text-purple-600">
                {toolCalls.length} tool calls
              </Badge>
            )}
            {agentIterations > 0 && (
              <Badge variant="outline" className="text-xs border-indigo-200 text-indigo-600">
                {agentIterations} iterations
              </Badge>
            )}
            <Badge
              className={status === "completed" ? "bg-emerald-100 text-emerald-700" : "bg-slate-100 text-slate-700"}
            >
              {status === "completed" ? "Complete" : `${calculatedProgress}%`}
            </Badge>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Progress bar */}
        <div className="space-y-2">
          <Progress value={calculatedProgress} className="h-2" />
          <div className="flex justify-between text-xs text-slate-500">
            <span>{completedSteps.length} of {displaySteps.length} steps</span>
            <span>
              {currentNode 
                ? `Processing: ${currentNode.replace(/_/g, ' ')}` 
                : (currentPhase && phaseLabels[currentPhase])
              }
            </span>
          </div>
        </div>

        {/* Steps by phase */}
        <div className="space-y-4">
          {phaseOrder.map((phase) => {
            const phaseSteps = phases[phase] || [];
            if (phaseSteps.length === 0) return null;

            const phaseComplete = phaseSteps.every((s) =>
              completedSteps.includes(s.id)
            );
            const phaseActive = phaseSteps.some((s) => s.id === currentNode);

            return (
              <div key={phase} className="space-y-2">
                {/* Phase header */}
                <div className="flex items-center gap-2">
                  <div
                    className={`w-2 h-2 rounded-full ${
                      phaseComplete
                        ? "bg-emerald-500"
                        : phaseActive
                        ? phaseColors[phase]
                        : "bg-slate-300"
                    }`}
                  />
                  <span
                    className={`text-sm font-medium ${
                      phaseComplete
                        ? "text-emerald-600"
                        : phaseActive
                        ? "text-slate-900"
                        : "text-slate-500"
                    }`}
                  >
                    {phaseLabels[phase]}
                  </span>
                  {phase === "reasoning" && (
                    <Badge variant="outline" className="text-xs border-purple-200 text-purple-600">
                      AI Agent
                    </Badge>
                  )}
                </div>

                {/* Steps in phase */}
                <div className="ml-4 space-y-1">
                  {phaseSteps.map((step) => {
                    const stepStatus = getStepStatus(step.id);
                    const isAgentStep = step.id === "llm_agent";

                    return (
                      <div key={step.id}>
                        <div
                          className={`flex items-center gap-3 py-1.5 px-2 rounded transition-all ${
                            stepStatus === "running"
                              ? "bg-slate-50"
                              : ""
                          }`}
                        >
                          {/* Status icon */}
                          <div className="flex-shrink-0">
                            {stepStatus === "completed" ? (
                              <CheckCircle className="w-4 h-4 text-emerald-500" />
                            ) : stepStatus === "running" ? (
                              <Loader2 className="w-4 h-4 animate-spin text-indigo-500" />
                            ) : (
                              <Circle className="w-4 h-4 text-slate-300" />
                            )}
                          </div>

                          {/* Step icon */}
                          <div
                            className={`flex-shrink-0 ${
                              stepStatus === "completed"
                                ? "text-slate-500"
                                : stepStatus === "running"
                                ? "text-indigo-500"
                                : "text-slate-400"
                            }`}
                          >
                            {stepIcons[step.id] || <Circle className="w-4 h-4" />}
                          </div>

                          {/* Step info */}
                          <div className="flex-1 min-w-0">
                            <p
                              className={`text-sm ${
                                stepStatus === "completed"
                                  ? "text-slate-600"
                                  : stepStatus === "running"
                                  ? "text-slate-900"
                                  : "text-slate-400"
                              }`}
                            >
                              {step.name}
                            </p>
                            {stepStatus === "running" && (
                              <p className="text-xs text-slate-500 truncate">
                                {step.description}
                              </p>
                            )}
                          </div>

                          {/* Tool call count for agent step */}
                          {isAgentStep && toolCalls.length > 0 && (
                            <Badge variant="outline" className="text-xs border-slate-300">
                              {toolCalls.length} tools
                            </Badge>
                          )}
                        </div>

                        {/* Tool calls section for agent step */}
                        {isAgentStep && toolCalls.length > 0 && (stepStatus === "running" || stepStatus === "completed") && (
                          <div className="ml-10 mt-2">
                            <button
                              onClick={() => setShowToolCalls(!showToolCalls)}
                              className="flex items-center gap-1 text-xs text-slate-500 hover:text-slate-700 transition-colors"
                            >
                              <Wrench className="w-3 h-3" />
                              <span>Tool Calls ({toolCalls.length})</span>
                              {showToolCalls ? (
                                <ChevronUp className="w-3 h-3" />
                              ) : (
                                <ChevronDown className="w-3 h-3" />
                              )}
                            </button>
                            
                            {showToolCalls && (
                              <div className="mt-2 space-y-1.5 max-h-48 overflow-y-auto">
                                {toolCalls.map((call, idx) => (
                                  <div
                                    key={idx}
                                    className="flex items-start gap-2 text-xs bg-slate-50 rounded px-2 py-1.5 border border-slate-100"
                                  >
                                    <span className="text-slate-400 font-mono w-4">{idx + 1}.</span>
                                    <div className="flex-1 min-w-0">
                                      <span className="font-medium text-purple-600">{call.tool}</span>
                                      {Object.keys(call.params).length > 0 && (
                                        <span className="text-slate-500">
                                          ({Object.entries(call.params)
                                            .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
                                            .join(", ")
                                            .slice(0, 50)}
                                          {Object.entries(call.params).join("").length > 50 ? "..." : ""})
                                        </span>
                                      )}
                                      {call.result_summary && (
                                        <p className="text-slate-500 mt-0.5">→ {call.result_summary}</p>
                                      )}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>

        {/* Agent reasoning summary */}
        {status === "running" && currentNode === "llm_agent" && agentIterations > 0 && (
          <div className="bg-purple-50 border border-purple-200 rounded-lg p-3">
            <div className="flex items-center gap-2 text-sm text-purple-700">
              <Brain className="w-4 h-4" />
              <span>AI Agent is analyzing...</span>
            </div>
            <p className="text-xs text-slate-500 mt-1">
              Iteration {agentIterations}/8 • {toolCalls.length} tools called
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
