"use client";

import React, { useMemo } from "react";
import { Badge } from "@/components/ui/badge";
import { Network, AlertTriangle, Shield } from "lucide-react";
import type { ToolCall } from "@/hooks/useInvestigation";

interface FraudRingGraphProps {
  toolCalls: ToolCall[];
  userId: string;
}

interface RingMember {
  user_id: string;
  name: string;
  risk_score: number;
  connection_type: string;
}

interface PotentialRing {
  triangle_count: number;
  reciprocal_partner_count: number;
  cluster_density: number;
  cluster_size: number;
  cluster_members: { user_id: string; name: string; risk_score: number }[];
  high_volume_pair_count: number;
  high_volume_pairs: { user_id: string; name: string; transaction_count: number }[];
  triangles: { members: string[] }[];
}

interface FraudRingResult {
  success: boolean;
  is_fraud_ring: boolean;
  ring_confidence: number;
  ring_members: RingMember[];
  potential_ring: PotentialRing;
  evidence: string[];
  transaction_partner_count: number;
  shared_device_user_count: number;
}

// Risk-based node colors
function getRiskColor(score: number): { fill: string; stroke: string; text: string } {
  if (score >= 70) return { fill: "#fecaca", stroke: "#dc2626", text: "#991b1b" };
  if (score >= 40) return { fill: "#fed7aa", stroke: "#ea580c", text: "#9a3412" };
  return { fill: "#d1fae5", stroke: "#16a34a", text: "#166534" };
}

function getConfidenceColor(confidence: number): string {
  if (confidence >= 70) return "bg-red-100 text-red-700 border-red-200";
  if (confidence >= 40) return "bg-amber-100 text-amber-700 border-amber-200";
  return "bg-emerald-100 text-emerald-700 border-emerald-200";
}

export function FraudRingGraph({ toolCalls, userId }: FraudRingGraphProps) {
  // Extract detect_fraud_ring result from tool calls
  const ringResult = useMemo<FraudRingResult | null>(() => {
    for (const call of toolCalls) {
      if (call.tool === "detect_fraud_ring" && call.result?.success && call.result?.is_fraud_ring) {
        return call.result as FraudRingResult;
      }
    }
    return null;
  }, [toolCalls]);

  if (!ringResult) return null;

  const { potential_ring, ring_confidence, ring_members, evidence } = ringResult;
  const clusterMembers = potential_ring?.cluster_members || [];
  const highVolPairs = potential_ring?.high_volume_pairs || [];
  const triangles = potential_ring?.triangles || [];

  // Build nodes: target user + cluster members
  const nodes = useMemo(() => {
    const result: { id: string; label: string; risk: number; isTarget: boolean }[] = [];

    // Target user in center
    result.push({ id: userId, label: userId, risk: 0, isTarget: true });

    // Add cluster members (skip target)
    for (const m of clusterMembers) {
      if (m.user_id !== userId) {
        result.push({
          id: m.user_id,
          label: m.name !== "Unknown" && m.name !== "TARGET" ? `${m.user_id}\n${m.name}` : m.user_id,
          risk: m.risk_score || 0,
          isTarget: false,
        });
      }
    }

    // If no cluster members but we have ring_members, use those
    if (result.length <= 1) {
      for (const m of ring_members) {
        if (m.user_id !== userId && !result.find(n => n.id === m.user_id)) {
          result.push({
            id: m.user_id,
            label: m.name !== "Unknown" ? `${m.user_id}\n${m.name}` : m.user_id,
            risk: m.risk_score || 0,
            isTarget: false,
          });
        }
      }
    }

    return result;
  }, [userId, clusterMembers, ring_members]);

  // Build edges from triangles (each triangle = 3 edges between members)
  const edges = useMemo(() => {
    const edgeSet = new Set<string>();
    const result: { from: string; to: string }[] = [];

    // Add edges from triangles
    for (const tri of triangles) {
      const members = tri.members || [];
      for (let i = 0; i < members.length; i++) {
        for (let j = i + 1; j < members.length; j++) {
          const key = [members[i], members[j]].sort().join("-");
          if (!edgeSet.has(key)) {
            edgeSet.add(key);
            result.push({ from: members[i], to: members[j] });
          }
        }
      }
    }

    // Ensure target connects to all ring members (even if not in triangles)
    const nodeIds = new Set(nodes.map(n => n.id));
    for (const m of ring_members) {
      if (nodeIds.has(m.user_id) && m.user_id !== userId) {
        const key = [userId, m.user_id].sort().join("-");
        if (!edgeSet.has(key)) {
          edgeSet.add(key);
          result.push({ from: userId, to: m.user_id });
        }
      }
    }

    return result;
  }, [triangles, ring_members, nodes, userId]);

  // Compute node positions: target in center, others in circle around it
  const WIDTH = 500;
  const HEIGHT = 340;
  const CX = WIDTH / 2;
  const CY = HEIGHT / 2;
  const RADIUS = Math.min(WIDTH, HEIGHT) * 0.35;

  const nodePositions = useMemo(() => {
    const positions: Record<string, { x: number; y: number }> = {};

    const nonTarget = nodes.filter(n => !n.isTarget);
    const count = nonTarget.length;

    // Target in center
    positions[userId] = { x: CX, y: CY };

    // Others in circle
    nonTarget.forEach((node, i) => {
      const angle = (2 * Math.PI * i) / count - Math.PI / 2;
      positions[node.id] = {
        x: CX + RADIUS * Math.cos(angle),
        y: CY + RADIUS * Math.sin(angle),
      };
    });

    return positions;
  }, [nodes, userId, CX, CY, RADIUS]);

  // Build high volume lookup
  const highVolMap = useMemo(() => {
    const map: Record<string, number> = {};
    for (const p of highVolPairs) {
      map[p.user_id] = p.transaction_count;
    }
    return map;
  }, [highVolPairs]);

  return (
    <div className="bg-white rounded-lg border border-slate-200 p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold flex items-center gap-2 text-slate-900">
          <Network className="w-5 h-5 text-red-500" />
          Potential Fraud Ring Detected
        </h3>
        <div className="flex items-center gap-2">
          <Badge className={getConfidenceColor(ring_confidence)}>
            Confidence: {ring_confidence}%
          </Badge>
          <Badge variant="outline" className="text-xs">
            {nodes.length} members
          </Badge>
        </div>
      </div>
        {/* SVG Graph */}
        <div className="bg-slate-50 rounded-lg border border-slate-200 p-2">
          <svg width="100%" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="select-none">
            <defs>
              <filter id="ring-shadow" x="-20%" y="-20%" width="140%" height="140%">
                <feDropShadow dx="0" dy="1" stdDeviation="2" floodOpacity="0.15" />
              </filter>
              <filter id="ring-glow" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="4" result="blur" />
                <feMerge>
                  <feMergeNode in="blur" />
                  <feMergeNode in="SourceGraphic" />
                </feMerge>
              </filter>
            </defs>

            {/* Edges */}
            {edges.map((edge, i) => {
              const from = nodePositions[edge.from];
              const to = nodePositions[edge.to];
              if (!from || !to) return null;

              // Check if either end is a high-volume pair with target
              const vol = highVolMap[edge.from] || highVolMap[edge.to] || 0;
              const isHighVol = vol >= 50;

              return (
                <g key={`edge-${i}`}>
                  <line
                    x1={from.x}
                    y1={from.y}
                    x2={to.x}
                    y2={to.y}
                    stroke={isHighVol ? "#ef4444" : "#94a3b8"}
                    strokeWidth={isHighVol ? 2.5 : 1.5}
                    strokeDasharray={isHighVol ? undefined : "4 2"}
                    opacity={0.7}
                  />
                  {/* Volume label on high-volume edges */}
                  {isHighVol && (
                    <text
                      x={(from.x + to.x) / 2}
                      y={(from.y + to.y) / 2 - 6}
                      textAnchor="middle"
                      fontSize="9"
                      fill="#dc2626"
                      fontWeight="600"
                    >
                      {vol} txns
                    </text>
                  )}
                </g>
              );
            })}

            {/* Nodes */}
            {nodes.map((node) => {
              const pos = nodePositions[node.id];
              if (!pos) return null;

              const colors = node.isTarget
                ? { fill: "#dbeafe", stroke: "#2563eb", text: "#1e40af" }
                : getRiskColor(node.risk);

              const r = node.isTarget ? 28 : 22;
              const lines = node.label.split("\n");

              return (
                <g key={node.id} filter="url(#ring-shadow)">
                  {/* Node circle */}
                  <circle
                    cx={pos.x}
                    cy={pos.y}
                    r={r}
                    fill={colors.fill}
                    stroke={colors.stroke}
                    strokeWidth={node.isTarget ? 3 : 2}
                    filter={node.isTarget ? "url(#ring-glow)" : undefined}
                  />

                  {/* Node ID */}
                  <text
                    x={pos.x}
                    y={pos.y + (lines.length > 1 ? -4 : 1)}
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fontSize={node.isTarget ? "11" : "10"}
                    fontWeight="700"
                    fill={colors.text}
                  >
                    {node.id}
                  </text>

                  {/* Name (second line) */}
                  {lines.length > 1 && (
                    <text
                      x={pos.x}
                      y={pos.y + 8}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fontSize="7"
                      fill={colors.text}
                      opacity={0.8}
                    >
                      {lines[1].length > 12 ? lines[1].slice(0, 12) + "…" : lines[1]}
                    </text>
                  )}

                  {/* Target badge */}
                  {node.isTarget && (
                    <text
                      x={pos.x}
                      y={pos.y + r + 14}
                      textAnchor="middle"
                      fontSize="9"
                      fontWeight="600"
                      fill="#2563eb"
                    >
                      TARGET
                    </text>
                  )}

                  {/* Risk score badge */}
                  {!node.isTarget && node.risk > 0 && (
                    <>
                      <circle
                        cx={pos.x + r * 0.7}
                        cy={pos.y - r * 0.7}
                        r={9}
                        fill={node.risk >= 70 ? "#dc2626" : node.risk >= 40 ? "#ea580c" : "#16a34a"}
                        stroke="white"
                        strokeWidth={1.5}
                      />
                      <text
                        x={pos.x + r * 0.7}
                        y={pos.y - r * 0.7 + 1}
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fontSize="7"
                        fontWeight="700"
                        fill="white"
                      >
                        {Math.round(node.risk)}
                      </text>
                    </>
                  )}
                </g>
              );
            })}
          </svg>
        </div>

        {/* Legend */}
        <div className="flex flex-wrap items-center gap-4 text-xs text-slate-500 px-1">
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-blue-100 border-2 border-blue-600 inline-block" />
            Target User
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-red-100 border-2 border-red-600 inline-block" />
            High Risk (&ge;70)
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-orange-100 border-2 border-orange-600 inline-block" />
            Medium Risk
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-emerald-100 border-2 border-emerald-600 inline-block" />
            Low Risk
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-6 border-t-2 border-red-500 inline-block" />
            High Volume
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-6 border-t border-dashed border-slate-400 inline-block" />
            Connected
          </span>
        </div>

        {/* Ring Stats */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div className="bg-slate-50 rounded-lg p-3 text-center border border-slate-100">
            <div className="text-lg font-bold text-slate-900">{potential_ring?.cluster_density != null ? `${Math.round(potential_ring.cluster_density * 100)}%` : "N/A"}</div>
            <div className="text-xs text-slate-500">Cluster Density</div>
          </div>
          <div className="bg-slate-50 rounded-lg p-3 text-center border border-slate-100">
            <div className="text-lg font-bold text-slate-900">{potential_ring?.triangle_count ?? 0}</div>
            <div className="text-xs text-slate-500">Triangles</div>
          </div>
          <div className="bg-slate-50 rounded-lg p-3 text-center border border-slate-100">
            <div className="text-lg font-bold text-slate-900">{potential_ring?.reciprocal_partner_count ?? 0}</div>
            <div className="text-xs text-slate-500">Reciprocal Flows</div>
          </div>
          <div className="bg-slate-50 rounded-lg p-3 text-center border border-slate-100">
            <div className="text-lg font-bold text-slate-900">{potential_ring?.high_volume_pair_count ?? 0}</div>
            <div className="text-xs text-slate-500">High Vol. Pairs</div>
          </div>
        </div>

        {/* Evidence */}
        {evidence && evidence.length > 0 && (
          <div className="space-y-1.5">
            <div className="flex items-center gap-1.5 text-sm font-semibold text-slate-700">
              <AlertTriangle className="w-4 h-4 text-amber-500" />
              Evidence
            </div>
            <ul className="space-y-1 ml-1">
              {evidence.map((e, i) => (
                <li key={i} className="text-xs text-slate-600 flex items-start gap-2">
                  <span className="w-1.5 h-1.5 bg-amber-400 rounded-full mt-1.5 flex-shrink-0" />
                  <span>{e}</span>
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Ring Members Table */}
        {ring_members.length > 0 && (
          <div className="space-y-1.5">
            <div className="flex items-center gap-1.5 text-sm font-semibold text-slate-700">
              <Shield className="w-4 h-4 text-slate-500" />
              Ring Members
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="bg-slate-100 text-slate-600">
                    <th className="px-3 py-1.5 text-left font-semibold">User ID</th>
                    <th className="px-3 py-1.5 text-left font-semibold">Name</th>
                    <th className="px-3 py-1.5 text-left font-semibold">Risk</th>
                    <th className="px-3 py-1.5 text-left font-semibold">Connection</th>
                    <th className="px-3 py-1.5 text-left font-semibold">Txn Volume</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {ring_members.map((m) => (
                    <tr key={m.user_id} className="hover:bg-slate-50">
                      <td className="px-3 py-1.5 font-mono font-medium text-slate-800">{m.user_id}</td>
                      <td className="px-3 py-1.5 text-slate-600">{m.name}</td>
                      <td className="px-3 py-1.5">
                        <Badge
                          variant="outline"
                          className={
                            m.risk_score >= 70
                              ? "bg-red-50 text-red-700 border-red-200 text-[10px]"
                              : m.risk_score >= 40
                              ? "bg-amber-50 text-amber-700 border-amber-200 text-[10px]"
                              : "bg-emerald-50 text-emerald-700 border-emerald-200 text-[10px]"
                          }
                        >
                          {Math.round(m.risk_score)}
                        </Badge>
                      </td>
                      <td className="px-3 py-1.5 text-slate-500 capitalize">
                        {m.connection_type.replace(/_/g, " ")}
                      </td>
                      <td className="px-3 py-1.5 text-slate-600 font-mono">
                        {highVolMap[m.user_id] ? `${highVolMap[m.user_id]} txns` : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
    </div>
  );
}
