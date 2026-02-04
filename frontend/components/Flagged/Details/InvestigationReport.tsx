"use client";

import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  AlertTriangle,
  CheckCircle,
  XCircle,
  Clock,
  Shield,
  Scale,
  Brain,
  FileText,
  Network,
  TrendingUp,
  User,
  Activity,
} from "lucide-react";
import type {
  TypologyAssessment,
  RiskAssessment,
  Decision,
  FinalAssessment,
  ToolCall,
} from "@/hooks/useInvestigation";

interface InvestigationReportProps {
  // New agentic workflow props
  finalAssessment?: FinalAssessment;
  toolCalls?: ToolCall[];
  agentIterations?: number;
  initialEvidence?: Record<string, any>;
  
  // Legacy props (for backwards compatibility)
  typology?: TypologyAssessment;
  risk?: RiskAssessment;
  decision?: Decision;
  report?: string;
  accountProfile?: Record<string, any>;
  networkEvidence?: Record<string, any>;
}

const riskLevelColors: Record<string, string> = {
  low: "bg-emerald-600",
  medium: "bg-amber-600",
  high: "bg-orange-600",
  critical: "bg-red-600",
};

const riskLevelIcons: Record<string, React.ReactNode> = {
  low: <CheckCircle className="w-5 h-5 text-emerald-500" />,
  medium: <AlertTriangle className="w-5 h-5 text-amber-500" />,
  high: <AlertTriangle className="w-5 h-5 text-orange-500" />,
  critical: <XCircle className="w-5 h-5 text-red-500" />,
};

const actionLabels: Record<string, string> = {
  allow_monitor: "Allow with Enhanced Monitoring",
  step_up_auth: "Require Step-up Authentication",
  temporary_freeze: "Temporary Account Freeze",
  full_block: "Full Account Block",
  escalate_compliance: "Escalate to Compliance",
};

const actionColors: Record<string, string> = {
  allow_monitor: "bg-emerald-600",
  step_up_auth: "bg-amber-600",
  temporary_freeze: "bg-orange-600",
  full_block: "bg-red-600",
  escalate_compliance: "bg-purple-600",
};

export function InvestigationReport({
  finalAssessment,
  toolCalls,
  agentIterations,
  initialEvidence,
  typology,
  risk,
  decision,
  report,
  accountProfile,
  networkEvidence,
}: InvestigationReportProps) {
  // Check if we have any data to show
  const hasAgentResults = finalAssessment || (toolCalls && toolCalls.length > 0);
  const hasLegacyResults = risk || decision || report;
  
  if (!hasAgentResults && !hasLegacyResults) {
    return (
      <Card className="bg-zinc-900 border-zinc-800">
        <CardContent className="pt-6">
          <div className="text-center text-zinc-500">
            <FileText className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p>Investigation report will appear here when analysis is complete</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      {/* NEW: Final Assessment from AI Agent */}
      {finalAssessment && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg flex items-center gap-2">
                <Brain className="w-5 h-5 text-purple-400" />
                AI Agent Assessment
              </CardTitle>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="text-xs border-purple-700 text-purple-400">
                  {finalAssessment.iteration} iterations
                </Badge>
                <Badge variant="outline" className="text-xs border-blue-700 text-blue-400">
                  {finalAssessment.tool_calls_made} tools
                </Badge>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Risk Score */}
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-zinc-400">Risk Score</span>
                <div className="flex items-center gap-2">
                  {riskLevelIcons[finalAssessment.risk_level]}
                  <span className="font-mono font-semibold text-lg">
                    {finalAssessment.risk_score}/100
                  </span>
                </div>
              </div>
              <Progress value={finalAssessment.risk_score} className="h-3" />
            </div>

            {/* Typology and Decision */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1">
                <span className="text-xs text-zinc-500">Fraud Typology</span>
                <p className="text-sm font-medium text-zinc-200 capitalize">
                  {finalAssessment.typology?.replace(/_/g, " ") || "Unknown"}
                </p>
              </div>
              <div className="space-y-1">
                <span className="text-xs text-zinc-500">Risk Level</span>
                <Badge className={`${riskLevelColors[finalAssessment.risk_level] || "bg-zinc-600"}`}>
                  {finalAssessment.risk_level?.toUpperCase()}
                </Badge>
              </div>
            </div>

            {/* Recommended Action */}
            <div className="pt-3 border-t border-zinc-800">
              <span className="text-xs text-zinc-500">Recommended Action</span>
              <div className="mt-1">
                <Badge
                  className={`text-sm py-1.5 px-3 ${
                    actionColors[finalAssessment.decision] || "bg-zinc-600"
                  }`}
                >
                  {actionLabels[finalAssessment.decision] || finalAssessment.decision?.replace(/_/g, " ")}
                </Badge>
              </div>
            </div>

            {/* Reasoning */}
            {finalAssessment.reasoning && (
              <div className="pt-3 border-t border-zinc-800">
                <span className="text-xs text-zinc-500 block mb-2">Agent's Reasoning</span>
                <p className="text-sm text-zinc-400">{finalAssessment.reasoning}</p>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* NEW: Initial Evidence from Agent */}
      {initialEvidence && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center gap-2">
              <Activity className="w-5 h-5 text-cyan-400" />
              Evidence Collected
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              {initialEvidence.profile && (
                <>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Name</span>
                    <span className="text-zinc-300">{initialEvidence.profile.name || "Unknown"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Location</span>
                    <span className="text-zinc-300">{initialEvidence.profile.location || "Unknown"}</span>
                  </div>
                </>
              )}
              {initialEvidence.account_metrics && (
                <>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Account Age</span>
                    <span className="text-zinc-300">{initialEvidence.account_metrics.account_age_days || 0} days</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Devices</span>
                    <span className="text-zinc-300">{initialEvidence.account_metrics.device_count || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Shared Devices</span>
                    <span className={initialEvidence.account_metrics.shared_device_count > 0 ? "text-amber-400" : "text-zinc-300"}>
                      {initialEvidence.account_metrics.shared_device_count || 0}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Flagged Device</span>
                    <span className={initialEvidence.account_metrics.has_flagged_device ? "text-red-400" : "text-zinc-300"}>
                      {initialEvidence.account_metrics.has_flagged_device ? "Yes" : "No"}
                    </span>
                  </div>
                </>
              )}
              <div className="flex justify-between">
                <span className="text-zinc-500">Accounts</span>
                <span className="text-zinc-300">{initialEvidence.accounts?.length || 0}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-500">Recent Transactions</span>
                <span className="text-zinc-300">{initialEvidence.recent_transactions?.length || 0}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-500">Direct Connections</span>
                <span className="text-zinc-300">{initialEvidence.direct_connections?.length || 0}</span>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Legacy: Risk Assessment Summary */}
      {risk && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg flex items-center gap-2">
                <Scale className="w-5 h-5 text-zinc-400" />
                Risk Assessment
              </CardTitle>
              <Badge
                className={`${riskLevelColors[risk.risk_level] || "bg-zinc-600"}`}
              >
                {risk.risk_level?.toUpperCase()}
              </Badge>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Overall Risk Score */}
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-zinc-400">Final Risk Score</span>
                <span className="font-mono font-semibold text-lg">
                  {risk.final_risk_score?.toFixed(0) || 0}/100
                </span>
              </div>
              <Progress
                value={risk.final_risk_score || 0}
                className="h-3"
              />
              {risk.amplification_factor > 1 && (
                <p className="text-xs text-orange-400">
                  ⚠️ Risk amplified {risk.amplification_factor.toFixed(1)}x due to compounding factors
                </p>
              )}
            </div>

            {/* Dimensional Scores */}
            <div className="grid grid-cols-2 gap-3 pt-2">
              {risk.dimension_scores && Object.entries(risk.dimension_scores).map(([key, value]) => (
                <div key={key} className="space-y-1">
                  <div className="flex items-center justify-between text-xs">
                    <span className="text-zinc-500 capitalize">
                      {key.replace(/_/g, " ")}
                    </span>
                    <span className="text-zinc-300">{(value as number).toFixed(0)}</span>
                  </div>
                  <Progress value={value as number} className="h-1.5" />
                </div>
              ))}
            </div>

            {/* Reasoning */}
            {risk.reasoning && (
              <div className="pt-2 border-t border-zinc-800">
                <p className="text-sm text-zinc-400">{risk.reasoning}</p>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Fraud Typology */}
      {typology && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg flex items-center gap-2">
                <Brain className="w-5 h-5 text-purple-400" />
                Fraud Typology
              </CardTitle>
              <Badge variant="outline" className="border-purple-700 text-purple-400">
                {(typology.confidence * 100).toFixed(0)}% confidence
              </Badge>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex items-center gap-3">
              <div className="flex-1">
                <p className="text-lg font-medium text-zinc-200 capitalize">
                  {typology.primary_typology?.replace(/_/g, " ") || "Unknown"}
                </p>
                {typology.secondary_typology && (
                  <p className="text-sm text-zinc-500">
                    Secondary: {typology.secondary_typology.replace(/_/g, " ")}
                  </p>
                )}
              </div>
            </div>

            {/* Indicators */}
            {typology.indicators && typology.indicators.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {typology.indicators.map((indicator, i) => (
                  <Badge key={i} variant="secondary" className="text-xs bg-zinc-800">
                    {indicator}
                  </Badge>
                ))}
              </div>
            )}

            {/* Reasoning */}
            {typology.reasoning && (
              <p className="text-sm text-zinc-400 pt-2 border-t border-zinc-800">
                {typology.reasoning}
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Decision Recommendation */}
      {decision && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg flex items-center gap-2">
                <Shield className="w-5 h-5 text-zinc-400" />
                Recommended Action
              </CardTitle>
              {decision.requires_human_review && (
                <Badge variant="outline" className="border-amber-700 text-amber-400">
                  <User className="w-3 h-3 mr-1" />
                  Human Review Required
                </Badge>
              )}
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center gap-4">
              <Badge
                className={`text-sm py-1.5 px-3 ${
                  actionColors[decision.recommended_action] || "bg-zinc-600"
                }`}
              >
                {actionLabels[decision.recommended_action] || decision.recommended_action}
              </Badge>
              <span className="text-sm text-zinc-500">
                {(decision.confidence * 100).toFixed(0)}% confidence
              </span>
            </div>

            {decision.alternative_action && (
              <div className="flex items-center gap-2 text-sm text-zinc-500">
                <span>Alternative:</span>
                <Badge variant="outline" className="border-zinc-700">
                  {actionLabels[decision.alternative_action] || decision.alternative_action}
                </Badge>
              </div>
            )}

            {decision.reasoning && (
              <p className="text-sm text-zinc-400 pt-2 border-t border-zinc-800">
                {decision.reasoning}
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Evidence Summary */}
      {(accountProfile || networkEvidence) && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center gap-2">
              <Activity className="w-5 h-5 text-zinc-400" />
              Evidence Summary
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Account Profile */}
            {accountProfile && (
              <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-zinc-500">Account Age</span>
                  <span className="text-zinc-300">{accountProfile.account_age_days || 0} days</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-zinc-500">Devices</span>
                  <span className="text-zinc-300">{accountProfile.device_count || 0}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-zinc-500">Flagged Device</span>
                  <span className={accountProfile.has_flagged_device ? "text-red-400" : "text-zinc-300"}>
                    {accountProfile.has_flagged_device ? "Yes" : "No"}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-zinc-500">Profile Risk</span>
                  <span className="text-zinc-300">{accountProfile.profile_risk_score?.toFixed(0) || 0}</span>
                </div>
              </div>
            )}

            {/* Network Evidence */}
            {networkEvidence && (
              <div className="pt-3 border-t border-zinc-800">
                <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Shared Devices</span>
                    <span className="text-zinc-300">{networkEvidence.shared_device_count || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Flagged Connections</span>
                    <span className={
                      networkEvidence.flagged_connections?.length > 0 ? "text-red-400" : "text-zinc-300"
                    }>
                      {networkEvidence.flagged_connections?.length || 0}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Network Type</span>
                    <span className="text-zinc-300 capitalize">{networkEvidence.network_topology || "Unknown"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Fraud Ring</span>
                    <span className={
                      networkEvidence.fraud_ring_members?.length > 0 ? "text-red-400" : "text-zinc-300"
                    }>
                      {networkEvidence.fraud_ring_members?.length > 0 
                        ? `${networkEvidence.fraud_ring_members.length} members`
                        : "None"}
                    </span>
                  </div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Full Report (Markdown) */}
      {report && (
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center gap-2">
              <FileText className="w-5 h-5 text-zinc-400" />
              Full Investigation Report
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="prose prose-sm prose-invert max-w-none">
              <pre className="whitespace-pre-wrap text-sm text-zinc-300 bg-zinc-950 p-4 rounded-lg overflow-auto max-h-96">
                {report}
              </pre>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
