'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import {
    ChevronDown,
    ChevronUp,
    Clock,
    Database,
    Network,
    Cpu,
    Activity,
    Zap,
} from 'lucide-react'
import type { PerformanceMetrics } from '@/hooks/useInvestigation'

interface PerformanceMetricsPanelProps {
    metrics: PerformanceMetrics
}

const PerformanceMetricsPanel = ({ metrics }: PerformanceMetricsPanelProps) => {
    const [expanded, setExpanded] = useState(false)

    // Auto-expand when metrics first arrive
    useEffect(() => {
        if (metrics && metrics.total_duration_ms > 0) {
            setExpanded(true)
        }
    }, [metrics])

    const formatTime = (ms: number) => {
        if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`
        return `${ms.toFixed(0)}ms`
    }

    const nodeDurations = metrics.node_durations || {}
    const nodeNames: Record<string, string> = {
        alert_validation: 'Alert Validation',
        data_collection: 'Data Collection',
        llm_agent: 'AI Agent',
        report_generation: 'Report Generation',
    }

    return (
        <Card className="bg-white border-slate-200 shadow-sm">
            <button
                onClick={() => setExpanded(!expanded)}
                className="w-full flex items-center justify-between px-6 py-4 hover:bg-slate-50 transition-colors rounded-lg"
            >
                <div className="flex items-center gap-2 text-sm font-medium text-slate-700">
                    <Activity className="w-4 h-4 text-indigo-500" />
                    Performance Metrics
                </div>
                <div className="flex items-center gap-3">
                    <span className="text-xs text-slate-500">
                        {formatTime(metrics.total_duration_ms)} total
                    </span>
                    {expanded ? (
                        <ChevronUp className="w-4 h-4 text-slate-400" />
                    ) : (
                        <ChevronDown className="w-4 h-4 text-slate-400" />
                    )}
                </div>
            </button>

            {expanded && (
                <CardContent className="pt-0 pb-5 px-6 space-y-4">
                    {/* Summary Stats Row */}
                    <div className="grid grid-cols-2 gap-3">
                        {/* Total Time */}
                        <div className="bg-slate-50 rounded-lg p-3">
                            <div className="flex items-center gap-1.5 text-xs text-slate-500">
                                <Clock className="w-3 h-3" />
                                Total Time
                            </div>
                            <p className="text-xl font-bold text-slate-900 mt-1">
                                {formatTime(metrics.total_duration_ms)}
                            </p>
                        </div>

                        {/* Total DB Calls */}
                        <div className="bg-slate-50 rounded-lg p-3">
                            <div className="flex items-center gap-1.5 text-xs text-slate-500">
                                <Zap className="w-3 h-3" />
                                Total DB Calls
                            </div>
                            <p className="text-xl font-bold text-slate-900 mt-1">
                                {metrics.total_db_calls || (metrics.kv_calls + metrics.graph_calls)}
                            </p>
                        </div>
                    </div>

                    {/* Service Breakdown */}
                    <div className="space-y-2">
                        <h4 className="text-xs font-medium text-slate-500 uppercase tracking-wide">Service Breakdown</h4>
                        <div className="space-y-2">
                            {/* KV Store */}
                            <div className="flex items-center justify-between py-2 px-3 bg-blue-50/60 rounded-lg">
                                <div className="flex items-center gap-2">
                                    <Database className="w-3.5 h-3.5 text-blue-500" />
                                    <span className="text-sm font-medium text-blue-700">Aerospike KV</span>
                                </div>
                                <div className="flex items-center gap-4 text-sm">
                                    <span className="text-blue-600 font-semibold">{metrics.kv_calls} calls</span>
                                    <span className="text-blue-500 text-xs w-16 text-right">{formatTime(metrics.kv_time_ms)}</span>
                                </div>
                            </div>

                            {/* Graph DB */}
                            <div className="flex items-center justify-between py-2 px-3 bg-cyan-50/60 rounded-lg">
                                <div className="flex items-center gap-2">
                                    <Network className="w-3.5 h-3.5 text-cyan-500" />
                                    <span className="text-sm font-medium text-cyan-700">Aerospike Graph</span>
                                </div>
                                <div className="flex items-center gap-4 text-sm">
                                    <span className="text-cyan-600 font-semibold">{metrics.graph_calls} calls</span>
                                    <span className="text-cyan-500 text-xs w-16 text-right">{formatTime(metrics.graph_time_ms)}</span>
                                </div>
                            </div>

                            {/* LLM */}
                            <div className="flex items-center justify-between py-2 px-3 bg-purple-50/60 rounded-lg">
                                <div className="flex items-center gap-2">
                                    <Cpu className="w-3.5 h-3.5 text-purple-500" />
                                    <span className="text-sm font-medium text-purple-700">LLM (Ollama)</span>
                                </div>
                                <div className="flex items-center gap-4 text-sm">
                                    <span className="text-purple-600 font-semibold">{metrics.llm_calls} calls</span>
                                    <span className="text-purple-500 text-xs w-16 text-right">{formatTime(metrics.llm_time_ms)}</span>
                                </div>
                            </div>

                            {/* Checkpoints */}
                            {metrics.checkpoint_calls > 0 && (
                                <div className="flex items-center justify-between py-2 px-3 bg-amber-50/60 rounded-lg">
                                    <div className="flex items-center gap-2">
                                        <Database className="w-3.5 h-3.5 text-amber-500" />
                                        <span className="text-sm font-medium text-amber-700">Checkpoints</span>
                                    </div>
                                    <div className="flex items-center gap-4 text-sm">
                                        <span className="text-amber-600 font-semibold">{metrics.checkpoint_calls} saves</span>
                                        <span className="text-amber-500 text-xs w-16 text-right">{formatTime(metrics.checkpoint_time_ms)}</span>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Node Durations */}
                    {Object.keys(nodeDurations).length > 0 && (
                        <div className="space-y-2">
                            <h4 className="text-xs font-medium text-slate-500 uppercase tracking-wide">Node Durations</h4>
                            <div className="space-y-1">
                                {Object.entries(nodeDurations).map(([node, durationMs]) => {
                                    const pct = metrics.total_duration_ms > 0
                                        ? ((durationMs as number) / metrics.total_duration_ms) * 100
                                        : 0
                                    return (
                                        <div key={node} className="flex items-center gap-3">
                                            <span className="text-xs text-slate-600 w-32 truncate">
                                                {nodeNames[node] || node.replace(/_/g, ' ')}
                                            </span>
                                            <div className="flex-1 bg-slate-100 rounded-full h-2 overflow-hidden">
                                                <div
                                                    className="h-full bg-indigo-400 rounded-full transition-all"
                                                    style={{ width: `${Math.min(pct, 100)}%` }}
                                                />
                                            </div>
                                            <span className="text-xs text-slate-500 w-14 text-right">
                                                {formatTime(durationMs as number)}
                                            </span>
                                        </div>
                                    )
                                })}
                            </div>
                        </div>
                    )}
                </CardContent>
            )}
        </Card>
    )
}

export default PerformanceMetricsPanel
