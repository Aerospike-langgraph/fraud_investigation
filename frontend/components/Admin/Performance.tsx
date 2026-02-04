'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import {  
  Zap, 
  Target, 
  RefreshCw, 
  XCircle,
  Database,
  Shield,
  LineChart,
} from 'lucide-react'

interface PerformanceStats {
  method: string
  avg_execution_time: number
  max_execution_time: number
  min_execution_time: number
  total_queries: number
  success_rate: number
  queries_per_second: number
  query_complexity: string
  cache_enabled: string
}

interface TimelineData {
  timestamp: string
  execution_time: number
  method: string
}

interface PerformanceData {
  rt1: PerformanceStats
  rt2: PerformanceStats
  rt3: PerformanceStats
  timestamp: string
}

interface TimelineResponse {
  rt1: TimelineData[]
  rt2: TimelineData[]
  rt3: TimelineData[]
}

export default function Performance() {
  const [performanceData, setPerformanceData] = useState<PerformanceData | null>(null)
  const [timelineData, setTimelineData] = useState<TimelineResponse | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [timeWindow, setTimeWindow] = useState(5)
  const [autoRefresh, setAutoRefresh] = useState(true)

  const fetchPerformanceData = async () => {
    try {
      setIsLoading(true)
      setError(null)
      
      const [statsResponse, timelineResponse] = await Promise.all([
        fetch(`/api/performance/stats?time_window=${timeWindow}`),
        fetch(`/api/performance/timeline?minutes=${timeWindow}`)
      ])

      if (!statsResponse.ok || !timelineResponse.ok) {
        throw new Error('Failed to fetch performance data')
      }

      const statsData = await statsResponse.json()
      const timelineData = await timelineResponse.json()

      setPerformanceData(statsData.performance_stats)
      setTimelineData(timelineData.timeline_data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch performance data')
    } finally {
      setIsLoading(false)
    }
  }

  const runPerformanceTest = async (method: 'rt1' | 'rt2' | 'rt3', count: number = 10) => {
    try {
      setIsLoading(true)
      setError(null)
      
      const response = await fetch(`/api/performance/test/${method}?transaction_count=${count}`, {
        method: 'POST'
      })

      if (!response.ok) {
        throw new Error(`Failed to run ${method.toUpperCase()} performance test`)
      }

      const result = await response.json()
      console.log(`${method.toUpperCase()} test result:`, result)
      
      // Refresh performance data after test
      await fetchPerformanceData()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to run performance test')
    } finally {
      setIsLoading(false)
    }
  }

  const resetMetrics = async () => {
    try {
      setIsLoading(true)
      setError(null)
      
      const response = await fetch('/api/performance/reset', {
        method: 'POST'
      })

      if (!response.ok) {
        throw new Error('Failed to reset performance metrics')
      }

      await fetchPerformanceData()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to reset metrics')
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchPerformanceData()
  }, [timeWindow])

  useEffect(() => {
    if (!autoRefresh) return

    const interval = setInterval(fetchPerformanceData, 5000) // Refresh every 20 seconds
    return () => clearInterval(interval)
  }, [autoRefresh, timeWindow])

  const getPerformanceColor = (avgTime: number) => {
    if (avgTime < 50) return 'text-green-600'
    if (avgTime < 100) return 'text-yellow-600'
    return 'text-red-600'
  }

  const getPerformanceStatus = (avgTime: number) => {
    if (avgTime < 50) return 'Excellent'
    if (avgTime < 100) return 'Good'
    if (avgTime < 200) return 'Fair'
    return 'Poor'
  }

  const renderMethodCard = (method: 'rt1' | 'rt2' | 'rt3', data: PerformanceStats) => {
    const methodInfo = {
      rt1: { name: 'RT1', description: 'Flagged Account Detection', icon: Shield, color: 'bg-blue-500' },
      rt2: { name: 'RT2', description: 'Flagged Account Multi-hop Detection', icon: Database, color: 'bg-green-500' },
      rt3: { name: 'RT3', description: 'Flagged Device Detection', icon: Target, color: 'bg-purple-500' }
    }

    const info = methodInfo[method]
    const Icon = info.icon

    return (
      <Card key={method} className="w-full">
        <CardHeader className="pb-3">
          <div className="flex items-center space-x-2">
            <div className={`p-2 rounded-lg ${info.color}`}>
              <Icon className="h-4 w-4 text-white" />
            </div>
            <div>
              <CardTitle className="text-lg">{info.name}</CardTitle>
              <CardDescription>{info.description}</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="text-center">
              <div className={`text-2xl font-bold ${getPerformanceColor(data.avg_execution_time)}`}>
                {data.avg_execution_time}ms
              </div>
              <div className="text-sm text-muted-foreground">Avg Time</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-blue-600">
                {data.queries_per_second}
              </div>
              <div className="text-sm text-muted-foreground">QPS</div>
            </div>
          </div>
          
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span>Success Rate</span>
              <span>{data.success_rate}%</span>
            </div>
            <Progress value={data.success_rate} className="h-2" />
          </div>

          <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground">
            <div>Max: {data.max_execution_time}ms</div>
            <div>Min: {data.min_execution_time}ms</div>
            <div>Total: {data.total_queries}</div>
          </div>
        </CardContent>
      </Card>
    )
  }

  const renderTimelineChart = () => {
    if (!timelineData) return null

    const methods = ['rt1', 'rt2', 'rt3'] as const
    const colors = { rt1: '#3b82f6', rt2: '#10b981', rt3: '#8b5cf6' }
    const methodNames = { rt1: 'RT1', rt2: 'RT2', rt3: 'RT3' }

    // Get all execution times across all methods for consistent scaling
    const allExecutionTimes = methods.flatMap(method => 
      timelineData[method]?.map(d => d.execution_time) || []
    )
    
    if (allExecutionTimes.length === 0) {
      return (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <LineChart className="h-5 w-5" />
              <span>Performance Timeline</span>
            </CardTitle>
            <CardDescription>Real-time execution times over the last {timeWindow} minutes</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-64 flex items-center justify-center bg-gray-50 rounded-lg">
              <div className="text-center text-muted-foreground">
                <LineChart className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>No performance data available</p>
                <p className="text-sm">Run some tests to see timeline data</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )
    }

    // Simple, reliable scaling - use the actual data range with proper padding
    const globalMaxTime = Math.max(...allExecutionTimes)
    const globalMinTime = Math.min(...allExecutionTimes)
    
    // Chart scaling: Start from 0, go to max + 10%
    const chartMinTime = 0
    const chartMaxTime = Math.max(globalMaxTime * 1.1, 100) // Ensure minimum 100ms range
    const chartRange = chartMaxTime - chartMinTime

    // Maximum number of points to show
    const maxPoints = 100
    const chartHeight = 160
    const chartWidth = 'auto' // Will be calculated dynamically

    // Create SVG path for each method
    const createLinePath = (data: TimelineData[], width: number) => {
      if (!data || data.length === 0) return ''
      
      const recentData = data.slice(-maxPoints)
      const points = recentData.map((point, index) => {
        const x = (index / (recentData.length - 1 || 1)) * width
        const y = chartHeight - ((point.execution_time - chartMinTime) / chartRange) * chartHeight
        return `${x},${y}`
      })
      
      return `M ${points.join(' L ')}`
    }

    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <LineChart className="h-5 w-5" />
            <span>Performance Timeline</span>
          </CardTitle>
          <CardDescription>Real-time execution times over the last {timeWindow} minutes</CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          {/* Compact Legend */}
          <div className="flex items-center justify-between px-6 py-3 border-b bg-gray-50/50">
            <div className="flex items-center space-x-4">
              {methods.map(method => {
                const data = timelineData[method]
                const hasData = data && data.length > 0
                return (
                  <div key={method} className="flex items-center space-x-1.5">
                    <div 
                      className={`w-2 h-2 rounded-full ${hasData ? '' : 'opacity-30'}`}
                      style={{ backgroundColor: colors[method] }}
                    />
                    <span className={`text-xs font-medium ${hasData ? '' : 'text-gray-400'}`}>
                      {methodNames[method]}
                    </span>
                    {hasData && (
                      <span className="text-xs text-muted-foreground">
                        ({data.length})
                      </span>
                    )}
                  </div>
                )
              })}
            </div>
            <div className="text-xs text-muted-foreground">
              Last {timeWindow} minutes
            </div>
          </div>

          {/* Full-width Chart Area */}
          <div className="relative w-full" style={{ height: `${chartHeight + 40}px` }}>
            {/* Chart container with responsive width */}
            <div className="absolute inset-0 pl-12 pr-4 py-3">
              {/* Y-axis labels */}
              <div className="absolute left-1 top-3 h-full flex flex-col justify-between text-xs text-gray-500" style={{ height: `${chartHeight}px` }}>
                <span className="bg-white/80 px-1 rounded text-right" style={{ transform: 'translateY(-50%)' }}>
                  {chartMaxTime.toFixed(0)}ms
                </span>
                <span className="bg-white/80 px-1 rounded text-right" style={{ transform: 'translateY(-50%)' }}>
                  {((chartMaxTime + chartMinTime) / 2).toFixed(0)}ms
                </span>
                <span className="bg-white/80 px-1 rounded text-right" style={{ transform: 'translateY(-50%)' }}>
                  {chartMinTime.toFixed(0)}ms
                </span>
              </div>

              {/* Responsive chart area */}
              <div className="ml-10 mr-2 h-full relative">
                {/* Grid and chart container */}
                <div className="w-full h-full relative" style={{ height: `${chartHeight}px`, marginTop: '12px' }}>
                  {/* Horizontal grid lines */}
                  {[0, 0.25, 0.5, 0.75, 1].map((ratio, index) => (
                    <div
                      key={`h-${index}`}
                      className="absolute w-full border-t border-gray-100"
                      style={{ top: `${ratio * chartHeight}px` }}
                    />
                  ))}

                  {/* SVG Chart that fills available width */}
                  <svg 
                    width="100%" 
                    height={chartHeight}
                    className="absolute top-0 left-0"
                    viewBox={`0 0 800 ${chartHeight}`}
                    preserveAspectRatio="none"
                  >
                    {methods.map(method => {
                      const data = timelineData[method]
                      if (!data || data.length === 0) return null

                      const path = createLinePath(data, 800) // Use viewBox width
                      if (!path) return null

                      return (
                        <g key={method}>
                          {/* Line with smooth curves */}
                          <path
                            d={path}
                            fill="none"
                            stroke={colors[method]}
                            strokeWidth="1.5"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            className="drop-shadow-sm"
                            style={{ vectorEffect: 'non-scaling-stroke' }}
                          />
                          {/* Data points - only show recent ones */}
                          {data.slice(-maxPoints).filter((_, index) => index % 3 === 0).map((point, index) => {
                            const recentData = data.slice(-maxPoints)
                            const actualIndex = index * 3
                            const x = (actualIndex / (recentData.length - 1 || 1)) * 800
                            const y = chartHeight - ((point.execution_time - chartMinTime) / chartRange) * chartHeight
                            
                            return (
                              <circle
                                key={`${method}-point-${actualIndex}`}
                                cx={x}
                                cy={y}
                                r="2"
                                fill={colors[method]}
                                stroke="white"
                                strokeWidth="1"
                                className="cursor-help hover:r-3 transition-all duration-200"
                                style={{ vectorEffect: 'non-scaling-stroke' }}
                              >
                                <title>
                                  {`${methodNames[method]}: ${point.execution_time.toFixed(2)}ms at ${new Date(point.timestamp).toLocaleTimeString()}`}
                                </title>
                              </circle>
                            )
                          })}
                        </g>
                      )
                    })}
                  </svg>
                </div>

                {/* X-axis time labels */}
                <div className="absolute bottom-0 left-0 w-full flex justify-between text-xs text-gray-500 mt-2">
                  <span>-{timeWindow}min</span>
                  <span>-{Math.floor(timeWindow * 0.75)}min</span>
                  <span>-{Math.floor(timeWindow * 0.5)}min</span>
                  <span>-{Math.floor(timeWindow * 0.25)}min</span>
                  <span>Now</span>
                </div>
              </div>
            </div>
          </div>

          {/* Compact Performance Summary */}
          <div className="px-6 py-3 border-t bg-gray-50/30">
            <div className="grid grid-cols-3 gap-4">
              {methods.map(method => {
                const data = timelineData[method]
                if (!data || data.length === 0) return (
                  <div key={method} className="text-center">
                    <div className="text-xs font-medium text-gray-400 mb-1">
                      {methodNames[method]}
                    </div>
                    <div className="text-xs text-gray-400">No recent data</div>
                  </div>
                )

                const recentData = data.slice(-10)
                const avgTime = recentData.reduce((sum, d) => sum + d.execution_time, 0) / recentData.length

                return (
                  <div key={method} className="text-center">
                    <div className="text-xs font-medium mb-1" style={{ color: colors[method] }}>
                      {methodNames[method]}
                    </div>
                    <div className="text-lg font-bold" style={{ color: colors[method] }}>
                      {avgTime.toFixed(1)}ms
                    </div>
                    <div className="text-xs text-muted-foreground">
                      avg last 10
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold tracking-tight">Fraud Detection Performance</h2>
          <p className="text-muted-foreground">
            Real-time monitoring of Gremlin query latencies for RT1, RT2, and RT3 fraud detection methods
          </p>
        </div>
        <div className="flex items-center space-x-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={autoRefresh ? 'bg-green-50 border-green-200' : ''}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${autoRefresh ? 'animate-spin' : ''}`} />
            {autoRefresh ? 'Auto' : 'Manual'}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={fetchPerformanceData}
            disabled={isLoading}
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center space-x-2">
            <XCircle className="h-5 w-5 text-red-600" />
            <span className="text-red-800">{error}</span>
          </div>
        </div>
      )}

      {/* Controls */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Zap className="h-5 w-5" />
            <span>Performance Testing</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <label className="text-sm font-medium">Time Window:</label>
              <select
                value={timeWindow}
                onChange={(e) => setTimeWindow(Number(e.target.value))}
                className="border rounded px-2 py-1 text-sm"
              >
                <option value={1}>1 minute</option>
                <option value={5}>5 minutes</option>
                <option value={10}>10 minutes</option>
                <option value={30}>30 minutes</option>
              </select>
            </div>
            
            <div className="flex items-center space-x-2">
              <Button
                variant="outline"
                size="sm"
                onClick={resetMetrics}
                disabled={isLoading}
              >
                Reset Metrics
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Performance Cards */}
      {performanceData && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {renderMethodCard('rt1', performanceData.rt1)}
          {renderMethodCard('rt2', performanceData.rt2)}
          {renderMethodCard('rt3', performanceData.rt3)}
        </div>
      )}

      {/* Timeline Chart */}
      {renderTimelineChart()}

      {/* Loading State */}
      {isLoading && (
        <div className="flex items-center justify-center py-8">
          <RefreshCw className="h-6 w-6 animate-spin mr-2" />
          <span>Loading performance data...</span>
        </div>
      )}
    </div>
  )
}
