'use client'

import { useState, useRef, useEffect, useCallback } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { 
    Network, 
    ZoomIn, 
    ZoomOut, 
    Maximize2, 
    RotateCcw,
    User,
    CreditCard,
    Smartphone,
    Globe,
    AlertTriangle,
    X,
    RefreshCw
} from 'lucide-react'
import { useGraphData } from '@/hooks/useAccountData'

// Node types
type NodeType = 'account' | 'user' | 'device' | 'ip' | 'transaction'

interface GraphNode {
    id: string
    label: string
    type: NodeType
    x: number
    y: number
    risk?: 'high' | 'medium' | 'low'
    isFlagged?: boolean
    details?: Record<string, string>
}

interface GraphEdge {
    source: string
    target: string
    label?: string
    type?: 'transaction' | 'owns' | 'uses' | 'connected'
}

interface Props {
    accountId: string
}

const nodeColors: Record<NodeType, { bg: string, border: string, icon: string }> = {
    account: { bg: '#3b82f6', border: '#1d4ed8', icon: '#ffffff' },
    user: { bg: '#8b5cf6', border: '#6d28d9', icon: '#ffffff' },
    device: { bg: '#06b6d4', border: '#0891b2', icon: '#ffffff' },
    ip: { bg: '#64748b', border: '#475569', icon: '#ffffff' },
    transaction: { bg: '#22c55e', border: '#16a34a', icon: '#ffffff' },
}

const riskColors = {
    high: { bg: '#ef4444', border: '#dc2626' },
    medium: { bg: '#f59e0b', border: '#d97706' },
    low: { bg: '#22c55e', border: '#16a34a' },
}

const GraphVisualization = ({ accountId }: Props) => {
    const svgRef = useRef<SVGSVGElement>(null)
    const [nodes, setNodes] = useState<GraphNode[]>([])
    const [edges, setEdges] = useState<GraphEdge[]>([])
    const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
    const [draggedNode, setDraggedNode] = useState<string | null>(null)
    const [zoom, setZoom] = useState(1)
    const [pan, setPan] = useState({ x: 0, y: 0 })
    const [isPanning, setIsPanning] = useState(false)
    const [panStart, setPanStart] = useState({ x: 0, y: 0 })

    // Use the graph data hook
    const { graphData, loading, error, refetch } = useGraphData(accountId)

    // Update nodes and edges when graph data changes
    useEffect(() => {
        if (graphData) {
            setNodes(graphData.nodes)
            setEdges(graphData.edges)
        }
    }, [graphData])

    const handleMouseDown = useCallback((e: React.MouseEvent, nodeId?: string) => {
        if (nodeId) {
            setDraggedNode(nodeId)
        } else {
            setIsPanning(true)
            setPanStart({ x: e.clientX - pan.x, y: e.clientY - pan.y })
        }
    }, [pan])

    const handleMouseMove = useCallback((e: React.MouseEvent) => {
        if (draggedNode) {
            const svg = svgRef.current
            if (!svg) return
            
            const rect = svg.getBoundingClientRect()
            const x = (e.clientX - rect.left - pan.x) / zoom
            const y = (e.clientY - rect.top - pan.y) / zoom
            
            setNodes(prev => prev.map(node => 
                node.id === draggedNode ? { ...node, x, y } : node
            ))
        } else if (isPanning) {
            setPan({
                x: e.clientX - panStart.x,
                y: e.clientY - panStart.y
            })
        }
    }, [draggedNode, isPanning, panStart, pan, zoom])

    const handleMouseUp = useCallback(() => {
        setDraggedNode(null)
        setIsPanning(false)
    }, [])

    const handleZoomIn = () => setZoom(prev => Math.min(prev + 0.2, 2))
    const handleZoomOut = () => setZoom(prev => Math.max(prev - 0.2, 0.5))
    const handleReset = () => {
        setZoom(1)
        setPan({ x: 0, y: 0 })
        refetch()
    }

    const getNodePosition = (nodeId: string) => {
        const node = nodes.find(n => n.id === nodeId)
        return node ? { x: node.x, y: node.y } : { x: 0, y: 0 }
    }

    const getNodeIcon = (type: NodeType) => {
        switch (type) {
            case 'account': return CreditCard
            case 'user': return User
            case 'device': return Smartphone
            case 'ip': return Globe
            default: return CreditCard
        }
    }

    const getEdgeColor = (type?: string) => {
        switch (type) {
            case 'transaction': return '#ef4444'
            case 'owns': return '#8b5cf6'
            case 'uses': return '#06b6d4'
            default: return '#94a3b8'
        }
    }

    // Loading state
    if (loading) {
        return (
            <Card>
                <CardHeader>
                    <div className="flex items-center justify-between">
                        <div>
                            <CardTitle className="flex items-center gap-2">
                                <Network className="h-5 w-5" />
                                Connection Graph
                            </CardTitle>
                            <CardDescription>
                                Loading graph data...
                            </CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent>
                    <Skeleton className="w-full h-[500px] rounded-lg" />
                </CardContent>
            </Card>
        )
    }

    // Error state
    if (error) {
        return (
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Network className="h-5 w-5" />
                        Connection Graph
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="flex flex-col items-center justify-center h-[400px] space-y-4">
                        <div className="p-4 rounded-full bg-destructive/10 text-destructive">
                            <AlertTriangle className="h-8 w-8" />
                        </div>
                        <p className="text-muted-foreground">{error}</p>
                        <Button onClick={refetch} variant="outline">
                            <RefreshCw className="h-4 w-4 mr-2" />
                            Retry
                        </Button>
                    </div>
                </CardContent>
            </Card>
        )
    }

    // Empty state
    if (nodes.length === 0) {
        return (
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Network className="h-5 w-5" />
                        Connection Graph
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="flex flex-col items-center justify-center h-[400px] space-y-4">
                        <div className="p-4 rounded-full bg-muted text-muted-foreground">
                            <Network className="h-8 w-8" />
                        </div>
                        <p className="text-muted-foreground">No graph connections found for this user</p>
                        <Button onClick={refetch} variant="outline">
                            <RefreshCw className="h-4 w-4 mr-2" />
                            Refresh
                        </Button>
                    </div>
                </CardContent>
            </Card>
        )
    }

    return (
        <Card>
            <CardHeader>
                <div className="flex items-center justify-between">
                    <div>
                        <CardTitle className="flex items-center gap-2">
                            <Network className="h-5 w-5" />
                            Connection Graph
                        </CardTitle>
                        <CardDescription>
                            Interactive visualization of account connections ({nodes.length} nodes, {edges.length} edges)
                        </CardDescription>
                    </div>
                    <div className="flex items-center gap-2">
                        <Button variant="outline" size="icon" onClick={refetch} title="Refresh">
                            <RefreshCw className="h-4 w-4" />
                        </Button>
                        <Button variant="outline" size="icon" onClick={handleZoomOut}>
                            <ZoomOut className="h-4 w-4" />
                        </Button>
                        <span className="text-sm text-muted-foreground w-12 text-center">
                            {Math.round(zoom * 100)}%
                        </span>
                        <Button variant="outline" size="icon" onClick={handleZoomIn}>
                            <ZoomIn className="h-4 w-4" />
                        </Button>
                        <Button variant="outline" size="icon" onClick={handleReset}>
                            <RotateCcw className="h-4 w-4" />
                        </Button>
                    </div>
                </div>
            </CardHeader>
            <CardContent>
                <div className="relative">
                    {/* Legend */}
                    <div className="absolute top-2 left-2 z-10 bg-background/95 backdrop-blur border rounded-lg p-3 text-xs space-y-2">
                        <p className="font-medium text-sm">Legend</p>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded-full" style={{ backgroundColor: nodeColors.account.bg }} />
                            <span>Account</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded-full" style={{ backgroundColor: nodeColors.user.bg }} />
                            <span>User</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded-full" style={{ backgroundColor: nodeColors.device.bg }} />
                            <span>Device</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded-full" style={{ backgroundColor: nodeColors.ip.bg }} />
                            <span>IP Address</span>
                        </div>
                        <div className="border-t pt-2 mt-2">
                            <div className="flex items-center gap-2">
                                <div className="w-4 h-4 rounded border-2 border-red-500 bg-red-500/20" />
                                <span>High Risk</span>
                            </div>
                            <div className="flex items-center gap-2 mt-1">
                                <div className="w-4 h-4 rounded border-2 border-amber-500 bg-amber-500/20" />
                                <span>Medium Risk</span>
                            </div>
                        </div>
                        <div className="border-t pt-2 mt-2">
                            <div className="flex items-center gap-2">
                                <div className="w-8 h-0.5 bg-red-500" />
                                <span>Transaction</span>
                            </div>
                            <div className="flex items-center gap-2 mt-1">
                                <div className="w-8 h-0.5 bg-cyan-500" />
                                <span>Uses</span>
                            </div>
                            <div className="flex items-center gap-2 mt-1">
                                <div className="w-8 h-0.5 bg-purple-500" />
                                <span>Owns</span>
                            </div>
                        </div>
                    </div>

                    {/* Selected Node Details */}
                    {selectedNode && (
                        <div className="absolute top-2 right-2 z-10 bg-background/95 backdrop-blur border rounded-lg p-4 w-64">
                            <div className="flex items-center justify-between mb-3">
                                <h4 className="font-semibold flex items-center gap-2">
                                    {selectedNode.isFlagged && <AlertTriangle className="h-4 w-4 text-red-500" />}
                                    {selectedNode.type.charAt(0).toUpperCase() + selectedNode.type.slice(1)}
                                </h4>
                                <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => setSelectedNode(null)}>
                                    <X className="h-4 w-4" />
                                </Button>
                            </div>
                            <div className="space-y-2 text-sm">
                                {selectedNode.details && Object.entries(selectedNode.details).map(([key, value]) => (
                                    <div key={key} className="flex justify-between">
                                        <span className="text-muted-foreground">{key}</span>
                                        <span className="font-medium">{value}</span>
                                    </div>
                                ))}
                                {selectedNode.risk && (
                                    <div className="flex justify-between pt-2 border-t">
                                        <span className="text-muted-foreground">Risk Level</span>
                                        <span className={`font-medium ${
                                            selectedNode.risk === 'high' ? 'text-red-500' :
                                            selectedNode.risk === 'medium' ? 'text-amber-500' : 'text-green-500'
                                        }`}>
                                            {selectedNode.risk.toUpperCase()}
                                        </span>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {/* SVG Graph */}
                    <svg
                        ref={svgRef}
                        width="100%"
                        height="500"
                        className="border rounded-lg bg-muted/20 cursor-grab active:cursor-grabbing"
                        onMouseDown={(e) => handleMouseDown(e)}
                        onMouseMove={handleMouseMove}
                        onMouseUp={handleMouseUp}
                        onMouseLeave={handleMouseUp}
                    >
                        <defs>
                            {/* Arrow marker for edges */}
                            <marker
                                id="arrowhead"
                                markerWidth="10"
                                markerHeight="7"
                                refX="9"
                                refY="3.5"
                                orient="auto"
                            >
                                <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8" />
                            </marker>
                            <marker
                                id="arrowhead-red"
                                markerWidth="10"
                                markerHeight="7"
                                refX="9"
                                refY="3.5"
                                orient="auto"
                            >
                                <polygon points="0 0, 10 3.5, 0 7" fill="#ef4444" />
                            </marker>
                            {/* Glow filter for flagged nodes */}
                            <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
                                <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
                                <feMerge>
                                    <feMergeNode in="coloredBlur"/>
                                    <feMergeNode in="SourceGraphic"/>
                                </feMerge>
                            </filter>
                        </defs>
                        
                        <g transform={`translate(${pan.x}, ${pan.y}) scale(${zoom})`}>
                            {/* Edges */}
                            {edges.map((edge, idx) => {
                                const source = getNodePosition(edge.source)
                                const target = getNodePosition(edge.target)
                                
                                // Skip if positions are invalid
                                if (!source.x && !source.y && !target.x && !target.y) return null
                                
                                const midX = (source.x + target.x) / 2
                                const midY = (source.y + target.y) / 2
                                const color = getEdgeColor(edge.type)
                                
                                return (
                                    <g key={idx}>
                                        <line
                                            x1={source.x}
                                            y1={source.y}
                                            x2={target.x}
                                            y2={target.y}
                                            stroke={color}
                                            strokeWidth={edge.type === 'transaction' ? 2.5 : 1.5}
                                            strokeDasharray={edge.type === 'connected' ? '5,5' : undefined}
                                            opacity={0.7}
                                            markerEnd={edge.type === 'transaction' ? 'url(#arrowhead-red)' : 'url(#arrowhead)'}
                                        />
                                        {edge.label && (
                                            <text
                                                x={midX}
                                                y={midY - 5}
                                                textAnchor="middle"
                                                className="text-[10px] fill-muted-foreground font-medium"
                                            >
                                                {edge.label}
                                            </text>
                                        )}
                                    </g>
                                )
                            })}
                            
                            {/* Nodes */}
                            {nodes.map((node) => {
                                const colors = node.risk ? riskColors[node.risk] : nodeColors[node.type]
                                const Icon = getNodeIcon(node.type)
                                const isSelected = selectedNode?.id === node.id
                                
                                return (
                                    <g
                                        key={node.id}
                                        transform={`translate(${node.x}, ${node.y})`}
                                        onMouseDown={(e) => {
                                            e.stopPropagation()
                                            handleMouseDown(e, node.id)
                                        }}
                                        onClick={() => setSelectedNode(node)}
                                        className="cursor-pointer"
                                        filter={node.isFlagged ? 'url(#glow)' : undefined}
                                    >
                                        {/* Node circle */}
                                        <circle
                                            r={node.isFlagged ? 28 : 24}
                                            fill={node.risk ? colors.bg : nodeColors[node.type].bg}
                                            stroke={isSelected ? '#ffffff' : (node.risk ? colors.border : nodeColors[node.type].border)}
                                            strokeWidth={isSelected ? 4 : node.isFlagged ? 3 : 2}
                                            className="transition-all duration-150"
                                        />
                                        
                                        {/* Flagged indicator ring */}
                                        {node.isFlagged && (
                                            <circle
                                                r={34}
                                                fill="none"
                                                stroke="#ef4444"
                                                strokeWidth={2}
                                                strokeDasharray="4,4"
                                                className="animate-spin"
                                                style={{ animationDuration: '10s' }}
                                            />
                                        )}
                                        
                                        {/* Icon */}
                                        <foreignObject x="-12" y="-12" width="24" height="24">
                                            <div className="flex items-center justify-center w-full h-full">
                                                <Icon className="h-5 w-5 text-white" />
                                            </div>
                                        </foreignObject>
                                        
                                        {/* Label */}
                                        <text
                                            y={node.isFlagged ? 48 : 42}
                                            textAnchor="middle"
                                            className="text-[11px] fill-foreground font-medium"
                                        >
                                            {node.label.split('\n').map((line, i) => (
                                                <tspan key={i} x="0" dy={i === 0 ? 0 : 14}>
                                                    {line}
                                                </tspan>
                                            ))}
                                        </text>
                                        
                                        {/* Risk indicator */}
                                        {node.risk && !node.isFlagged && (
                                            <circle
                                                cx={18}
                                                cy={-18}
                                                r={8}
                                                fill={riskColors[node.risk].bg}
                                                stroke="#ffffff"
                                                strokeWidth={2}
                                            />
                                        )}
                                    </g>
                                )
                            })}
                        </g>
                    </svg>

                    {/* Instructions */}
                    <div className="mt-3 flex items-center justify-center gap-4 text-xs text-muted-foreground">
                        <span>🖱️ Drag nodes to rearrange</span>
                        <span>•</span>
                        <span>👆 Click node for details</span>
                        <span>•</span>
                        <span>✋ Drag canvas to pan</span>
                    </div>
                </div>
            </CardContent>
        </Card>
    )
}

export default GraphVisualization
