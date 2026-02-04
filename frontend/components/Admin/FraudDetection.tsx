'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Slider } from '@/components/ui/slider'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { 
    Play, 
    Settings, 
    History, 
    Clock, 
    AlertTriangle,
    CheckCircle,
    XCircle,
    Loader2,
    RefreshCw,
    Calendar,
    Zap,
    Timer,
    Target,
    Users,
    TrendingUp,
    Shield,
    Eye
} from 'lucide-react'
import { toast } from 'sonner'
import { getRiskLevelColor } from '@/lib/utils'

interface DetectionConfig {
    schedule_enabled: boolean
    schedule_time: string
    cooldown_days: number
    risk_threshold: number
}

interface FraudScenario {
    id: string
    name: string
    description: string
    riskLevel: 'High' | 'Medium-High' | 'Medium' | 'Low'
    enabled: boolean
    priority: 'Phase 1' | 'Phase 2' | 'Phase 3'
    keyIndicators: string[]
    commonUseCase: string
    detailedDescription: string
    disabled?: boolean
}

const fraudScenarios: FraudScenario[] = [
    {
        id: 'RT1',
        name: 'Transaction to Flagged Account',
        description: 'Immediate threat detection via 1-hop lookup',
        riskLevel: 'High',
        enabled: true,
        priority: 'Phase 1',
        keyIndicators: [
            'Transaction directed to known flagged account',
            '1-hop graph lookup for immediate detection',
            'Real-time risk assessment'
        ],
        commonUseCase: 'Immediate threat detection, known fraudster connections',
        detailedDescription: 'Real-time detection system that flags transactions sent to accounts that have been previously identified as fraudulent.'
    },
    {
        id: 'RT2',
        name: 'Transaction with Users Associated with Flagged Accounts',
        description: 'Threat detection via 2-hop lookup',
        riskLevel: 'High',
        enabled: true,
        priority: 'Phase 1',
        keyIndicators: [
            'Transaction directed to users associated with flagged accounts',
            'Multi-hop neighborhood analysis',
            'Real-time risk assessment'
        ],
        commonUseCase: 'Immediate threat detection, known fraudster connections',
        detailedDescription: 'Real-time detection system that flags transactions sent to accounts that have transacted with accounts identified as fraudulent.'
    },
    {
        id: 'RT3',
        name: 'Transactions with Users Associated with Flagged Devices',
        description: 'Detect threats through flagged device usage',
        riskLevel: 'High',
        enabled: true,
        priority: 'Phase 1',
        keyIndicators: [
            'Transactions directed to users associated with fraudulent devices',
            'Multi-hop neighborhood analysis',
            'Transaction history analysis'
        ],
        commonUseCase: 'Immediate threat detection, known fraudster connections',
        detailedDescription: ''
    }
]

interface SchedulerStatus {
    scheduler_running: boolean
    detection_job_scheduled: boolean
    detection_job_running: boolean
    next_run: string | null
    last_run_result: any
}

interface JobHistoryItem {
    job_id: string
    start_time: string
    end_time?: string
    status: string
    accounts_evaluated: number
    accounts_skipped_cooldown: number
    newly_flagged: number
    total_accounts: number
    duration_seconds?: number
    error?: string
}

const FraudDetection = () => {
    const [config, setConfig] = useState<DetectionConfig>({
        schedule_enabled: true,
        schedule_time: '21:30',
        cooldown_days: 7,
        risk_threshold: 70
    })
    const [schedulerStatus, setSchedulerStatus] = useState<SchedulerStatus | null>(null)
    const [history, setHistory] = useState<JobHistoryItem[]>([])
    const [loading, setLoading] = useState(true)
    const [saving, setSaving] = useState(false)
    const [runningDetection, setRunningDetection] = useState(false)
    const [skipCooldown, setSkipCooldown] = useState(false)
    const [historyLoading, setHistoryLoading] = useState(true)
    const [scenarios, setScenarios] = useState<FraudScenario[]>(fraudScenarios)
    const [aerospikeConnected, setAerospikeConnected] = useState<boolean | null>(null)

    const toggleScenario = (scenarioId: string) => {
        setScenarios(prev => prev.map(scenario => 
            scenario.id === scenarioId 
                ? { ...scenario, enabled: !scenario.enabled }
                : scenario
        ))
    }

    // Fetch config and status
    const fetchConfig = async () => {
        try {
            const response = await fetch('/api/detection/config')
            if (response.ok) {
                const data = await response.json()
                if (data.config) {
                    setConfig(data.config)
                }
                if (data.scheduler) {
                    setSchedulerStatus(data.scheduler)
                }
            }
        } catch (error) {
            console.error('Error fetching config:', error)
        } finally {
            setLoading(false)
        }
    }

    // Fetch history
    const fetchHistory = async () => {
        setHistoryLoading(true)
        try {
            const response = await fetch('/api/detection/history?limit=10')
            if (response.ok) {
                const data = await response.json()
                setHistory(data.history || [])
            }
        } catch (error) {
            console.error('Error fetching history:', error)
        } finally {
            setHistoryLoading(false)
        }
    }

    // Check Aerospike connection
    const checkAerospikeConnection = async () => {
        try {
            const response = await fetch('/api/aerospike/stats')
            if (response.ok) {
                const data = await response.json()
                setAerospikeConnected(data.connected || false)
            } else {
                setAerospikeConnected(false)
            }
        } catch (error) {
            setAerospikeConnected(false)
        }
    }

    useEffect(() => {
        checkAerospikeConnection()
        fetchConfig()
        fetchHistory()
    }, [])

    // Save config
    const handleSaveConfig = async () => {
        setSaving(true)
        try {
            const response = await fetch('/api/detection/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
            
            if (response.ok) {
                const data = await response.json()
                setConfig(data.config)
                setSchedulerStatus(data.scheduler)
                toast.success('Configuration saved successfully')
            } else {
                toast.error('Failed to save configuration')
            }
        } catch (error) {
            toast.error('Error saving configuration')
        } finally {
            setSaving(false)
        }
    }

    // Run detection manually
    const handleRunDetection = async () => {
        setRunningDetection(true)
        toast.info(skipCooldown ? 'Starting detection job (skipping cooldown)...' : 'Starting detection job...')
        
        try {
            const url = skipCooldown 
                ? '/api/flagged-accounts/detect?skip_cooldown=true' 
                : '/api/flagged-accounts/detect'
            const response = await fetch(url, {
                method: 'POST'
            })
            
            if (response.ok) {
                const data = await response.json()
                toast.success(`Detection completed! ${data.result?.newly_flagged || 0} accounts flagged.`)
                fetchHistory()
                fetchConfig()
            } else {
                const error = await response.json()
                toast.error(error.detail || 'Detection job failed')
            }
        } catch (error) {
            toast.error('Error running detection job')
        } finally {
            setRunningDetection(false)
        }
    }

    const formatDateTime = (dateStr: string) => {
        return new Date(dateStr).toLocaleString()
    }

    const formatDuration = (seconds: number) => {
        if (seconds < 60) return `${seconds.toFixed(1)}s`
        const mins = Math.floor(seconds / 60)
        const secs = seconds % 60
        return `${mins}m ${secs.toFixed(0)}s`
    }

    if (loading) {
        return (
            <div className="grid gap-6 md:grid-cols-2">
                <Card>
                    <CardHeader>
                        <Skeleton className="h-6 w-40" />
                        <Skeleton className="h-4 w-64" />
                    </CardHeader>
                    <CardContent className="space-y-6">
                        <Skeleton className="h-10 w-full" />
                        <Skeleton className="h-10 w-full" />
                        <Skeleton className="h-10 w-full" />
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader>
                        <Skeleton className="h-6 w-40" />
                    </CardHeader>
                    <CardContent>
                        <Skeleton className="h-32 w-full" />
                    </CardContent>
                </Card>
            </div>
        )
    }

    return (
        <div className="space-y-6">
            {/* Real-Time Fraud Detection Section */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                        <span className="flex items-center space-x-2">
                            <Zap className="w-5 h-5" />
                            <span>Real-Time Fraud Detection</span>
                        </span>
                        <Badge variant="secondary">
                            {scenarios.filter(s => s.enabled).length} of {scenarios.length} enabled
                        </Badge>
                    </CardTitle>
                    <CardDescription>
                        Configure which fraud detection patterns to monitor in real-time during transactions
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                    {scenarios.map((scenario) => (
                        <Collapsible key={scenario.id} defaultOpen={false}>
                            <div className={`border rounded-lg ${scenario.disabled ? 'opacity-50 bg-gray-50 dark:bg-gray-900' : ''}`}>
                                <CollapsibleTrigger asChild>
                                    <div className="flex items-center justify-between w-full p-3 hover:bg-muted/50 cursor-pointer">
                                        <div className="flex items-center space-x-3">
                                            <div onClick={(e) => e.stopPropagation()}>
                                                <Switch
                                                    checked={scenario.enabled}
                                                    onCheckedChange={() => !scenario.disabled && toggleScenario(scenario.id)}
                                                    disabled={scenario.disabled}
                                                />
                                            </div>
                                            <div className="text-left">
                                                <div className={`font-medium ${scenario.disabled ? 'text-gray-400' : ''}`}>
                                                    <span className="text-muted-foreground mr-2">{scenario.id}</span>
                                                    {scenario.name}
                                                    {scenario.disabled && <span className="ml-2 text-xs">(Coming Soon)</span>}
                                                </div>
                                                <div className={`text-sm ${scenario.disabled ? 'text-gray-400' : 'text-muted-foreground'}`}>
                                                    {scenario.description}
                                                </div>
                                            </div>
                                        </div>
                                        <div className="flex items-center space-x-2">
                                            <Badge className={`${getRiskLevelColor(scenario.riskLevel)} ${scenario.disabled ? 'opacity-50' : ''}`}>
                                                {scenario.riskLevel}
                                            </Badge>
                                            <Eye className="w-4 h-4 text-muted-foreground" />
                                        </div>
                                    </div>
                                </CollapsibleTrigger>
                                <CollapsibleContent>
                                    <div className="space-y-3 text-sm p-3 pt-0 border-t">
                                        <div>
                                            <strong>Key Indicators:</strong>
                                            <ul className="list-disc list-inside mt-1 space-y-1">
                                                {scenario.keyIndicators.map((indicator, index) => (
                                                    <li key={index} className="text-muted-foreground">{indicator}</li>
                                                ))}
                                            </ul>
                                        </div>
                                        <div>
                                            <strong>Common Use Case:</strong>
                                            <p className="text-muted-foreground mt-1">{scenario.commonUseCase}</p>
                                        </div>
                                    </div>
                                </CollapsibleContent>
                            </div>
                        </Collapsible>
                    ))}
                </CardContent>
            </Card>

            {/* Scheduled ML Detection Section */}
            {aerospikeConnected === false ? (
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Settings className="h-5 w-5" />
                            Scheduled ML Detection
                        </CardTitle>
                        <CardDescription>
                            Configure automated ML-based account risk scoring schedule and parameters
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="p-6 bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800 rounded-lg">
                            <div className="flex items-start gap-3">
                                <XCircle className="h-6 w-6 text-red-500 flex-shrink-0 mt-0.5" />
                                <div>
                                    <h4 className="text-base font-medium text-red-800 dark:text-red-300">
                                        Aerospike KV Service Unavailable
                                    </h4>
                                    <p className="text-sm text-red-700 dark:text-red-400 mt-1">
                                        Risk evaluation features require Aerospike KV service to be connected. 
                                        Please ensure Aerospike is running and try again.
                                    </p>
                                    <Button 
                                        variant="outline" 
                                        size="sm" 
                                        className="mt-3"
                                        onClick={checkAerospikeConnection}
                                    >
                                        <RefreshCw className="h-4 w-4 mr-2" />
                                        Check Connection
                                    </Button>
                                </div>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            ) : (
            <>
            <div className="grid gap-6 md:grid-cols-2">
                {/* Configuration Card */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Settings className="h-5 w-5" />
                            Scheduled ML Detection
                        </CardTitle>
                        <CardDescription>
                            Configure automated ML-based account risk scoring schedule and parameters
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                        {/* Schedule Enable Toggle */}
                        <div className="flex items-center justify-between">
                            <div className="space-y-0.5">
                                <Label className="text-base">Scheduled Detection</Label>
                                <p className="text-sm text-muted-foreground">
                                    Automatically run detection daily
                                </p>
                            </div>
                            <Switch
                                checked={config.schedule_enabled}
                                onCheckedChange={(checked) => setConfig(prev => ({ ...prev, schedule_enabled: checked }))}
                            />
                        </div>

                        {/* Schedule Time */}
                        <div className="space-y-2">
                            <Label className="flex items-center gap-2">
                                <Clock className="h-4 w-4" />
                                Schedule Time (24-hour format)
                            </Label>
                            <Input
                                type="time"
                                value={config.schedule_time}
                                onChange={(e) => setConfig(prev => ({ ...prev, schedule_time: e.target.value }))}
                                disabled={!config.schedule_enabled}
                                className="w-40"
                            />
                            {schedulerStatus?.next_run && config.schedule_enabled && (
                                <p className="text-sm text-muted-foreground">
                                    Next run: {formatDateTime(schedulerStatus.next_run)}
                                </p>
                            )}
                        </div>

                        {/* Cooldown Period */}
                        <div className="space-y-2">
                            <Label className="flex items-center gap-2">
                                <Timer className="h-4 w-4" />
                                Cooldown Period (days)
                            </Label>
                            <div className="flex items-center gap-4">
                                <Input
                                    type="number"
                                    min={1}
                                    max={90}
                                    value={config.cooldown_days}
                                    onChange={(e) => setConfig(prev => ({ ...prev, cooldown_days: parseInt(e.target.value) || 7 }))}
                                    className="w-24"
                                />
                                <span className="text-sm text-muted-foreground">
                                    Accounts won't be re-evaluated within {config.cooldown_days} days
                                </span>
                            </div>
                        </div>

                        {/* Risk Threshold */}
                        <div className="space-y-3">
                            <Label className="flex items-center gap-2">
                                <Target className="h-4 w-4" />
                                Risk Threshold: {config.risk_threshold}
                            </Label>
                            <Slider
                                value={[config.risk_threshold]}
                                onValueChange={([value]) => setConfig(prev => ({ ...prev, risk_threshold: value }))}
                                min={0}
                                max={100}
                                step={5}
                                className="w-full"
                            />
                            <p className="text-sm text-muted-foreground">
                                Accounts with risk score ≥ {config.risk_threshold} will be flagged
                            </p>
                        </div>

                        {/* Save Button */}
                        <Button onClick={handleSaveConfig} disabled={saving} className="w-full">
                            {saving ? (
                                <>
                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                    Saving...
                                </>
                            ) : (
                                <>
                                    <CheckCircle className="h-4 w-4 mr-2" />
                                    Save Configuration
                                </>
                            )}
                        </Button>
                    </CardContent>
                </Card>

                {/* Manual Trigger Card */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Zap className="h-5 w-5" />
                            Manual Detection
                        </CardTitle>
                        <CardDescription>
                            Trigger the detection job manually to identify flagged accounts
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        {/* Status Display */}
                        <div className="p-4 bg-muted/50 rounded-lg space-y-3">
                            <div className="flex items-center justify-between">
                                <span className="text-sm font-medium">Scheduler Status</span>
                                <Badge variant={schedulerStatus?.scheduler_running ? "default" : "secondary"}>
                                    {schedulerStatus?.scheduler_running ? 'Running' : 'Stopped'}
                                </Badge>
                            </div>
                            <div className="flex items-center justify-between">
                                <span className="text-sm font-medium">Detection Job</span>
                                <Badge variant={schedulerStatus?.detection_job_scheduled ? "default" : "outline"}>
                                    {schedulerStatus?.detection_job_scheduled ? 'Scheduled' : 'Not Scheduled'}
                                </Badge>
                            </div>
                            {schedulerStatus?.detection_job_running && (
                                <div className="flex items-center gap-2 text-sm text-blue-600">
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                    Detection job is currently running...
                                </div>
                            )}
                        </div>

                        {/* What happens when you run */}
                        <div className="p-4 bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-lg">
                            <h4 className="text-sm font-medium text-amber-800 dark:text-amber-300 mb-2 flex items-center gap-2">
                                <AlertTriangle className="h-4 w-4" />
                                What happens when detection runs:
                            </h4>
                            <ul className="text-sm text-amber-700 dark:text-amber-400 space-y-1 list-disc list-inside">
                                <li>Fetches all accounts from the database</li>
                                <li>Skips accounts still in cooldown period {skipCooldown && <span className="text-green-600 font-medium">(disabled)</span>}</li>
                                <li>Extracts features and calls ML model</li>
                                <li>Flags accounts above risk threshold</li>
                            </ul>
                        </div>

                        {/* Skip Cooldown Option */}
                        <div className="flex items-center justify-between p-3 bg-muted/30 rounded-lg border">
                            <div className="space-y-0.5">
                                <Label className="text-sm font-medium">Skip Cooldown Period</Label>
                                <p className="text-xs text-muted-foreground">
                                    Evaluate all users even if recently checked (ignores {config.cooldown_days}-day cooldown)
                                </p>
                            </div>
                            <Switch 
                                checked={skipCooldown} 
                                onCheckedChange={setSkipCooldown}
                            />
                        </div>

                        {/* Run Button */}
                        <Button 
                            onClick={handleRunDetection} 
                            disabled={runningDetection || schedulerStatus?.detection_job_running}
                            className="w-full"
                            size="lg"
                        >
                            {runningDetection || schedulerStatus?.detection_job_running ? (
                                <>
                                    <Loader2 className="h-5 w-5 mr-2 animate-spin" />
                                    Running Detection...
                                </>
                            ) : (
                                <>
                                    <Play className="h-5 w-5 mr-2" />
                                    Run Detection Now
                                </>
                            )}
                        </Button>

                        {/* Refresh Status */}
                        <Button 
                            variant="outline" 
                            onClick={() => { fetchConfig(); fetchHistory(); }}
                            className="w-full"
                        >
                            <RefreshCw className="h-4 w-4 mr-2" />
                            Refresh Status
                        </Button>
                    </CardContent>
                </Card>
            </div>

            {/* History Card */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <History className="h-5 w-5" />
                        Detection History
                    </CardTitle>
                    <CardDescription>
                        Recent detection job runs and their results
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    {historyLoading ? (
                        <div className="space-y-2">
                            {[...Array(3)].map((_, i) => (
                                <Skeleton key={i} className="h-12 w-full" />
                            ))}
                        </div>
                    ) : history.length === 0 ? (
                        <div className="text-center py-8 text-muted-foreground">
                            <History className="h-12 w-12 mx-auto mb-4 opacity-50" />
                            <p>No detection jobs have been run yet.</p>
                            <p className="text-sm">Click "Run Detection Now" to start the first job.</p>
                        </div>
                    ) : (
                        <div className="overflow-x-auto">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>Run Time</TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead className="text-center">
                                            <span className="flex items-center justify-center gap-1">
                                                <Users className="h-4 w-4" />
                                                Evaluated
                                            </span>
                                        </TableHead>
                                        <TableHead className="text-center">
                                            <span className="flex items-center justify-center gap-1">
                                                <Timer className="h-4 w-4" />
                                                Skipped
                                            </span>
                                        </TableHead>
                                        <TableHead className="text-center">
                                            <span className="flex items-center justify-center gap-1">
                                                <TrendingUp className="h-4 w-4" />
                                                Flagged
                                            </span>
                                        </TableHead>
                                        <TableHead className="text-right">Duration</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {history.map((job) => (
                                        <TableRow key={job.job_id}>
                                            <TableCell className="font-medium">
                                                <div className="flex items-center gap-2">
                                                    <Calendar className="h-4 w-4 text-muted-foreground" />
                                                    {formatDateTime(job.start_time)}
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                {job.status === 'completed' ? (
                                                    <Badge variant="default" className="bg-green-500">
                                                        <CheckCircle className="h-3 w-3 mr-1" />
                                                        Completed
                                                    </Badge>
                                                ) : job.status === 'failed' ? (
                                                    <Badge variant="destructive">
                                                        <XCircle className="h-3 w-3 mr-1" />
                                                        Failed
                                                    </Badge>
                                                ) : (
                                                    <Badge variant="secondary">
                                                        <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                                                        Running
                                                    </Badge>
                                                )}
                                            </TableCell>
                                            <TableCell className="text-center">
                                                {job.accounts_evaluated.toLocaleString()}
                                            </TableCell>
                                            <TableCell className="text-center text-muted-foreground">
                                                {job.accounts_skipped_cooldown.toLocaleString()}
                                            </TableCell>
                                            <TableCell className="text-center">
                                                <span className={job.newly_flagged > 0 ? 'text-red-600 font-semibold' : ''}>
                                                    {job.newly_flagged.toLocaleString()}
                                                </span>
                                            </TableCell>
                                            <TableCell className="text-right text-muted-foreground">
                                                {job.duration_seconds ? formatDuration(job.duration_seconds) : '-'}
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </div>
                    )}
                </CardContent>
            </Card>
            </>
            )}
        </div>
    )
}

export default FraudDetection
