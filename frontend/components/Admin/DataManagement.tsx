'use client'

import { useState, useEffect, useRef } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { 
    Database, 
    RefreshCw, 
    CheckCircle, 
    XCircle, 
    Loader2,
    HardDrive,
    Users,
    CreditCard,
    Smartphone,
    GitBranch,
    AlertTriangle,
    Play,
    Server,
    Upload,
    FolderOpen,
    FileText
} from 'lucide-react'
import { toast } from 'sonner'

interface BulkLoadStatus {
    loading: boolean
    success?: boolean
    message?: string
    error?: string
    status?: string
    step?: string
    complete?: boolean
    progress_percentage?: number
    elements_written?: number
    graph?: {
        success: boolean
        statistics?: {
            users: number
            accounts: number
            devices: number
        }
    }
    aerospike?: {
        success: boolean
        loaded?: number
        message?: string
    }
}

interface GraphStats {
    users: number
    accounts: number
    devices: number
    transactions: number
    total_vertices: number
    total_edges: number
}

interface AerospikeStats {
    connected: boolean
    users_count: number
    flagged_accounts_count: number
    pending_review: number
    confirmed_fraud: number
    cleared: number
}

const DataManagement = () => {
    const [bulkLoadStatus, setBulkLoadStatus] = useState<BulkLoadStatus>({ loading: false })
    const [isLoading, setIsLoading] = useState(false)
    const [graphStats, setGraphStats] = useState<GraphStats | null>(null)
    const [aerospikeStats, setAerospikeStats] = useState<AerospikeStats | null>(null)
    const [loadingStats, setLoadingStats] = useState(false)
    
    // Load options
    const [loadGraph, setLoadGraph] = useState(true)
    const [loadAerospike, setLoadAerospike] = useState(true)
    
    // Data source options
    const [useDefaultData, setUseDefaultData] = useState(true)
    const [uploadedFile, setUploadedFile] = useState<File | null>(null)
    const fileInputRef = useRef<HTMLInputElement>(null)
    
    // Demo injection state
    const [demoLoading, setDemoLoading] = useState(false)
    const [demoTxnCount, setDemoTxnCount] = useState(500)
    const [demoSpreadDays, setDemoSpreadDays] = useState(30)
    const [demoHighValuePct, setDemoHighValuePct] = useState(15)
    const [demoSeedFlagged, setDemoSeedFlagged] = useState(5)

    // Fetch current graph statistics
    const fetchGraphStats = async () => {
        setLoadingStats(true)
        try {
            const response = await fetch('/api/dashboard/stats')
            if (response.ok) {
                const data = await response.json()
                setGraphStats({
                    users: data.users || 0,
                    accounts: 0,
                    devices: 0,
                    transactions: data.txns || 0,
                    total_vertices: 0,
                    total_edges: 0
                })
            }
        } catch (error) {
            console.error('Failed to fetch graph stats:', error)
        } finally {
            setLoadingStats(false)
        }
    }

    // Fetch Aerospike stats
    const fetchAerospikeStats = async () => {
        try {
            const response = await fetch('/api/aerospike/stats')
            if (response.ok) {
                const data = await response.json()
                setAerospikeStats(data)
            }
        } catch (error) {
            console.error('Failed to fetch Aerospike stats:', error)
        }
    }

    useEffect(() => {
        fetchGraphStats()
        fetchAerospikeStats()
    }, [])

    // Trigger bulk load
    const handleBulkLoad = async () => {
        if (!loadGraph && !loadAerospike) {
            toast.error('Please select at least one target system')
            return
        }
        
        if (!useDefaultData && !uploadedFile) {
            toast.error('Please select a file to upload or use default data')
            return
        }

        setIsLoading(true)
        setBulkLoadStatus({ loading: true, message: 'Starting bulk load...' })
        
        try {
            let response: Response
            
            if (useDefaultData) {
                // Use default data - simple query params
                const params = new URLSearchParams()
                params.set('load_graph', String(loadGraph))
                params.set('load_aerospike', String(aerospikeStats?.connected ? loadAerospike : false))
                
                response = await fetch(`/api/bulk-load-gremlin?${params.toString()}`, {
                    method: 'POST',
                })
            } else if (uploadedFile) {
                // Upload custom file
                const formData = new FormData()
                formData.append('file', uploadedFile)
                formData.append('load_graph', String(loadGraph))
                formData.append('load_aerospike', String(aerospikeStats?.connected ? loadAerospike : false))
                
                response = await fetch('/api/bulk-load-upload', {
                    method: 'POST',
                    body: formData,
                })
            } else {
                throw new Error('No data source selected')
            }
            
            const data = await response.json()
            
            if (data.success) {
                setBulkLoadStatus({
                    loading: false,
                    success: true,
                    message: data.message || 'Bulk load completed successfully',
                    graph: data.graph,
                    aerospike: data.aerospike
                })
                toast.success('Bulk load completed!')
                
                // If graph was loaded, poll for status
                if (loadGraph && data.graph?.success) {
                    pollBulkLoadStatus()
                }
                
                // Refresh stats
                fetchGraphStats()
                fetchAerospikeStats()
            } else {
                setBulkLoadStatus({
                    loading: false,
                    success: false,
                    error: data.detail || data.message || 'Failed to start bulk load',
                    graph: data.graph,
                    aerospike: data.aerospike
                })
                toast.error('Bulk load failed')
            }
        } catch (error) {
            setBulkLoadStatus({
                loading: false,
                success: false,
                error: 'Network error - failed to connect to backend'
            })
            toast.error('Failed to connect to backend')
        } finally {
            setIsLoading(false)
        }
    }

    // Poll for bulk load status
    const pollBulkLoadStatus = async () => {
        let attempts = 0
        const maxAttempts = 60
        
        const checkStatus = async (): Promise<boolean> => {
            try {
                const response = await fetch('/api/bulk-load-status')
                const data = await response.json()
                
                setBulkLoadStatus(prev => ({
                    ...prev,
                    loading: !data.complete,
                    status: data.status,
                    step: data.step,
                    complete: data.complete,
                    progress_percentage: data.progress_percentage,
                    elements_written: data.elements_written
                }))
                
                if (data.complete) {
                    toast.success('Graph bulk load completed!')
                    fetchGraphStats()
                    return true
                } else if (data.status?.toLowerCase().includes('failed') || data.status?.toLowerCase().includes('error')) {
                    setBulkLoadStatus(prev => ({
                        ...prev,
                        loading: false,
                        success: false,
                        error: data.error || 'Bulk load failed'
                    }))
                    toast.error('Graph bulk load failed')
                    return true
                }
                
                return false
            } catch (error) {
                console.error('Error polling status:', error)
                return false
            }
        }
        
        const poll = async () => {
            if (attempts >= maxAttempts) {
                setBulkLoadStatus(prev => ({
                    ...prev,
                    loading: false,
                    message: 'Bulk load is still running in the background. Click refresh to check status.'
                }))
                return
            }
            
            const done = await checkStatus()
            if (!done) {
                attempts++
                setTimeout(poll, 3000)
            }
        }
        
        setTimeout(poll, 2000)
    }

    // Check bulk load status manually
    const handleCheckStatus = async () => {
        setLoadingStats(true)
        try {
            const response = await fetch('/api/bulk-load-status')
            const data = await response.json()
            
            if (data.success) {
                setBulkLoadStatus(prev => ({
                    ...prev,
                    loading: !data.complete,
                    success: data.complete,
                    message: data.message || `Status: ${data.status}`,
                    status: data.status,
                    step: data.step,
                    complete: data.complete,
                    progress_percentage: data.progress_percentage,
                    elements_written: data.elements_written
                }))
                
                if (data.complete) {
                    toast.success('Bulk load completed!')
                } else {
                    toast.info(`Bulk load ${data.status}: ${data.step || 'processing...'}`)
                }
            } else {
                setBulkLoadStatus(prev => ({
                    ...prev,
                    loading: false,
                    success: false,
                    error: data.error || data.message || 'Unknown error'
                }))
                toast.error(data.error || 'Failed to get status')
            }
            
            await fetchGraphStats()
            await fetchAerospikeStats()
        } catch (error) {
            toast.error('Failed to check status')
        } finally {
            setLoadingStats(false)
        }
    }

    // Inject demo suspicious activity
    const handleInjectDemoActivity = async () => {
        setDemoLoading(true)
        try {
            const params = new URLSearchParams({
                transaction_count: String(demoTxnCount),
                spread_days: String(demoSpreadDays),
                high_value_percentage: String(demoHighValuePct),
                seed_flagged_accounts: String(demoSeedFlagged)
            })
            
            const response = await fetch(`/api/demo/inject-suspicious-activity?${params.toString()}`, {
                method: 'POST'
            })
            
            const data = await response.json()
            
            if (data.success) {
                toast.success(`Injected ${data.details.total_transactions} demo transactions!`)
                toast.info(`${data.details.high_value_transactions} high-value, ${data.details.flagged_seeds} flagged seeds`)
                
                // Refresh stats
                await fetchGraphStats()
                await fetchAerospikeStats()
            } else {
                toast.error(data.detail || 'Failed to inject demo activity')
            }
        } catch (error) {
            console.error('Failed to inject demo activity:', error)
            toast.error('Failed to inject demo activity')
        } finally {
            setDemoLoading(false)
        }
    }

    return (
        <div className="grid gap-6 md:grid-cols-2">
            {/* Bulk Load Controls */}
            <Card className="md:col-span-2">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Database className="h-5 w-5" />
                        Bulk Data Load
                    </CardTitle>
                    <CardDescription>
                        Simulates receiving account data from a third-party system. Load sample data into Graph DB and/or Aerospike KV store.
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="grid md:grid-cols-3 gap-4">
                        {/* Data Source Selection */}
                        <div className="p-4 bg-muted/50 rounded-lg space-y-3">
                            <p className="text-sm font-medium">Data Source:</p>
                            <div className="space-y-2">
                                <button
                                    onClick={() => {
                                        setUseDefaultData(true)
                                        setUploadedFile(null)
                                    }}
                                    className={`w-full p-3 rounded-lg border-2 transition-all text-left ${
                                        useDefaultData 
                                            ? 'border-blue-500 bg-blue-50 dark:bg-blue-950/30' 
                                            : 'border-transparent bg-background hover:bg-muted'
                                    }`}
                                >
                                    <div className="flex items-center gap-2">
                                        <FolderOpen className={`h-4 w-4 ${useDefaultData ? 'text-blue-500' : 'text-muted-foreground'}`} />
                                        <span className="text-sm font-medium">Use Default Data</span>
                                    </div>
                                    <p className="text-xs text-muted-foreground mt-1 ml-6">
                                        Pre-loaded sample data
                                    </p>
                                </button>
                                
                                <button
                                    onClick={() => {
                                        setUseDefaultData(false)
                                        fileInputRef.current?.click()
                                    }}
                                    className={`w-full p-3 rounded-lg border-2 transition-all text-left ${
                                        !useDefaultData 
                                            ? 'border-blue-500 bg-blue-50 dark:bg-blue-950/30' 
                                            : 'border-transparent bg-background hover:bg-muted'
                                    }`}
                                >
                                    <div className="flex items-center gap-2">
                                        <Upload className={`h-4 w-4 ${!useDefaultData ? 'text-blue-500' : 'text-muted-foreground'}`} />
                                        <span className="text-sm font-medium">Upload CSV Files</span>
                                    </div>
                                    <p className="text-xs text-muted-foreground mt-1 ml-6">
                                        {uploadedFile ? (
                                            <span className="flex items-center gap-1 text-blue-600">
                                                <FileText className="h-3 w-3" />
                                                {uploadedFile.name}
                                            </span>
                                        ) : (
                                            'Select a ZIP file'
                                        )}
                                    </p>
                                </button>
                                <input
                                    ref={fileInputRef}
                                    type="file"
                                    accept=".zip"
                                    className="hidden"
                                    onChange={(e) => {
                                        const file = e.target.files?.[0]
                                        if (file) {
                                            setUploadedFile(file)
                                            setUseDefaultData(false)
                                            toast.success(`Selected: ${file.name}`)
                                        }
                                    }}
                                />
                            </div>
                        </div>

                        {/* Default Data Info (only show when using default) */}
                        <div className={`p-4 bg-muted/50 rounded-lg space-y-2 ${!useDefaultData ? 'opacity-50' : ''}`}>
                            <p className="text-sm font-medium">Default Data Contains:</p>
                            <ul className="text-sm text-muted-foreground space-y-1">
                                <li className="flex items-center gap-2">
                                    <Users className="h-4 w-4" />
                                    <span>10,000 Users</span>
                                </li>
                                <li className="flex items-center gap-2">
                                    <CreditCard className="h-4 w-4" />
                                    <span>~21,000 Accounts</span>
                                </li>
                                <li className="flex items-center gap-2">
                                    <Smartphone className="h-4 w-4" />
                                    <span>~25,000 Devices</span>
                                </li>
                                <li className="flex items-center gap-2">
                                    <GitBranch className="h-4 w-4" />
                                    <span>Ownership & Usage relationships</span>
                                </li>
                            </ul>
                        </div>

                        {/* Target Systems */}
                        <div className="p-4 bg-muted/50 rounded-lg space-y-3">
                            <p className="text-sm font-medium">Target Systems:</p>
                            <div className="space-y-3">
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                        <HardDrive className="h-4 w-4 text-blue-500" />
                                        <Label htmlFor="load-graph" className="text-sm cursor-pointer">
                                            Aerospike Graph
                                        </Label>
                                    </div>
                                    <Switch 
                                        id="load-graph" 
                                        checked={loadGraph} 
                                        onCheckedChange={setLoadGraph}
                                    />
                                </div>
                                <p className="text-xs text-muted-foreground ml-6">
                                    Loads vertices and edges
                                </p>
                                
                                <div className={`flex items-center justify-between ${!aerospikeStats?.connected ? 'opacity-50' : ''}`}>
                                    <div className="flex items-center gap-2">
                                        <Server className={`h-4 w-4 ${aerospikeStats?.connected ? 'text-green-500' : 'text-gray-400'}`} />
                                        <Label 
                                            htmlFor="load-aerospike" 
                                            className={`text-sm ${aerospikeStats?.connected ? 'cursor-pointer' : 'cursor-not-allowed text-muted-foreground'}`}
                                        >
                                            Aerospike KV Store
                                        </Label>
                                    </div>
                                    <Switch 
                                        id="load-aerospike" 
                                        checked={aerospikeStats?.connected ? loadAerospike : false}
                                        onCheckedChange={setLoadAerospike}
                                        disabled={!aerospikeStats?.connected}
                                    />
                                </div>
                                <p className="text-xs text-muted-foreground ml-6">
                                    {aerospikeStats?.connected ? (
                                        'Loads users for tracking'
                                    ) : (
                                        <span className="text-amber-600">
                                            Not connected - disabled
                                        </span>
                                    )}
                                </p>
                            </div>
                        </div>
                    </div>

                    {/* Status Display */}
                    {(bulkLoadStatus.message || bulkLoadStatus.status || bulkLoadStatus.graph || bulkLoadStatus.aerospike) && (
                        <div className={`p-4 rounded-lg flex items-start gap-3 ${
                            bulkLoadStatus.complete || bulkLoadStatus.success
                                ? 'bg-green-50 dark:bg-green-950/20 border border-green-200 dark:border-green-800' 
                                : bulkLoadStatus.error 
                                    ? 'bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800'
                                    : 'bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800'
                        }`}>
                            {bulkLoadStatus.loading ? (
                                <Loader2 className="h-5 w-5 animate-spin text-blue-600 flex-shrink-0" />
                            ) : bulkLoadStatus.complete || bulkLoadStatus.success ? (
                                <CheckCircle className="h-5 w-5 text-green-600 flex-shrink-0" />
                            ) : bulkLoadStatus.error ? (
                                <XCircle className="h-5 w-5 text-red-600 flex-shrink-0" />
                            ) : (
                                <AlertTriangle className="h-5 w-5 text-blue-600 flex-shrink-0" />
                            )}
                            <div className="flex-1 min-w-0">
                                <p className={`text-sm font-medium ${
                                    bulkLoadStatus.complete || bulkLoadStatus.success ? 'text-green-800 dark:text-green-300' :
                                    bulkLoadStatus.error ? 'text-red-800 dark:text-red-300' :
                                    'text-blue-800 dark:text-blue-300'
                                }`}>
                                    {bulkLoadStatus.error || bulkLoadStatus.message}
                                </p>
                                
                                {/* Individual system results */}
                                <div className="mt-2 space-y-1">
                                    {bulkLoadStatus.graph && (
                                        <p className="text-xs flex items-center gap-1">
                                            {bulkLoadStatus.graph.success ? (
                                                <CheckCircle className="h-3 w-3 text-green-500" />
                                            ) : (
                                                <XCircle className="h-3 w-3 text-red-500" />
                                            )}
                                            <span className="text-muted-foreground">
                                                Graph: {bulkLoadStatus.graph.success 
                                                    ? `${bulkLoadStatus.graph.statistics?.users || 0} users loaded`
                                                    : 'Failed'}
                                            </span>
                                        </p>
                                    )}
                                    {bulkLoadStatus.aerospike && (
                                        <p className="text-xs flex items-center gap-1">
                                            {bulkLoadStatus.aerospike.success ? (
                                                <CheckCircle className="h-3 w-3 text-green-500" />
                                            ) : (
                                                <XCircle className="h-3 w-3 text-amber-500" />
                                            )}
                                            <span className="text-muted-foreground">
                                                Aerospike KV: {bulkLoadStatus.aerospike.success 
                                                    ? `${bulkLoadStatus.aerospike.loaded || 0} users loaded`
                                                    : bulkLoadStatus.aerospike.message || 'Skipped'}
                                            </span>
                                        </p>
                                    )}
                                </div>
                                
                                {bulkLoadStatus.step && !bulkLoadStatus.complete && (
                                    <p className="text-xs text-muted-foreground mt-1">
                                        Current step: <span className="font-medium">{bulkLoadStatus.step}</span>
                                    </p>
                                )}
                                {bulkLoadStatus.elements_written && (
                                    <p className="text-xs text-muted-foreground mt-1">
                                        Elements written: <span className="font-medium">{bulkLoadStatus.elements_written.toLocaleString()}</span>
                                    </p>
                                )}
                                {bulkLoadStatus.progress_percentage !== undefined && (
                                    <div className="mt-2">
                                        <div className="flex justify-between text-xs text-muted-foreground mb-1">
                                            <span>Progress</span>
                                            <span>{bulkLoadStatus.progress_percentage}%</span>
                                        </div>
                                        <div className="h-2 bg-muted rounded-full overflow-hidden">
                                            <div 
                                                className="h-full bg-blue-500 transition-all duration-300"
                                                style={{ width: `${bulkLoadStatus.progress_percentage}%` }}
                                            />
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {/* Warning */}
                    <div className="p-3 bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-lg">
                        <p className="text-sm text-amber-800 dark:text-amber-300 flex items-center gap-2">
                            <AlertTriangle className="h-4 w-4" />
                            This will load/reload all sample data. Existing data may be affected.
                        </p>
                    </div>

                    {/* Actions */}
                    <div className="flex gap-3">
                        <Button 
                            onClick={handleBulkLoad} 
                            disabled={isLoading || (!loadGraph && !(aerospikeStats?.connected && loadAerospike)) || (!useDefaultData && !uploadedFile)}
                            className="flex-1"
                        >
                            {isLoading ? (
                                <>
                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                    Loading...
                                </>
                            ) : (
                                <>
                                    {useDefaultData ? (
                                        <Play className="h-4 w-4 mr-2" />
                                    ) : (
                                        <Upload className="h-4 w-4 mr-2" />
                                    )}
                                    {useDefaultData ? 'Start Bulk Load' : 'Upload & Load'}
                                </>
                            )}
                        </Button>
                        <Button 
                            variant="outline" 
                            onClick={handleCheckStatus}
                            disabled={loadingStats}
                        >
                            {loadingStats ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                                <RefreshCw className="h-4 w-4" />
                            )}
                        </Button>
                    </div>
                </CardContent>
            </Card>

            {/* Demo Activity Injection */}
            <Card className="md:col-span-2 border-amber-200 dark:border-amber-800 bg-amber-50/50 dark:bg-amber-950/20">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <AlertTriangle className="h-5 w-5 text-amber-500" />
                        Inject Demo Activity
                        <span className="ml-auto text-xs font-normal text-amber-600 bg-amber-100 dark:bg-amber-900/30 px-2 py-1 rounded">
                            Demo Only
                        </span>
                    </CardTitle>
                    <CardDescription>
                        Create realistic fraud patterns with spread timestamps, high-value transactions, and seed flagged accounts.
                        This helps bootstrap the fraud detection system with meaningful data.
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div className="space-y-2">
                            <Label htmlFor="txn-count" className="text-sm font-medium">Transactions</Label>
                            <input
                                id="txn-count"
                                type="number"
                                min={10}
                                max={10000}
                                value={demoTxnCount}
                                onChange={(e) => setDemoTxnCount(Number(e.target.value))}
                                className="w-full px-3 py-2 border rounded-md text-sm bg-white dark:bg-gray-900"
                            />
                            <p className="text-xs text-muted-foreground">10 - 10,000</p>
                        </div>
                        <div className="space-y-2">
                            <Label htmlFor="spread-days" className="text-sm font-medium">Spread (Days)</Label>
                            <input
                                id="spread-days"
                                type="number"
                                min={1}
                                max={365}
                                value={demoSpreadDays}
                                onChange={(e) => setDemoSpreadDays(Number(e.target.value))}
                                className="w-full px-3 py-2 border rounded-md text-sm bg-white dark:bg-gray-900"
                            />
                            <p className="text-xs text-muted-foreground">1 - 365 days</p>
                        </div>
                        <div className="space-y-2">
                            <Label htmlFor="high-value-pct" className="text-sm font-medium">High Value %</Label>
                            <input
                                id="high-value-pct"
                                type="number"
                                min={0}
                                max={100}
                                value={demoHighValuePct}
                                onChange={(e) => setDemoHighValuePct(Number(e.target.value))}
                                className="w-full px-3 py-2 border rounded-md text-sm bg-white dark:bg-gray-900"
                            />
                            <p className="text-xs text-muted-foreground">$15K-$100K txns</p>
                        </div>
                        <div className="space-y-2">
                            <Label htmlFor="seed-flagged" className="text-sm font-medium">Seed Flagged</Label>
                            <input
                                id="seed-flagged"
                                type="number"
                                min={0}
                                max={20}
                                value={demoSeedFlagged}
                                onChange={(e) => setDemoSeedFlagged(Number(e.target.value))}
                                className="w-full px-3 py-2 border rounded-md text-sm bg-white dark:bg-gray-900"
                            />
                            <p className="text-xs text-muted-foreground">0 - 20 accounts</p>
                        </div>
                    </div>
                    
                    <div className="p-3 bg-amber-100 dark:bg-amber-900/30 rounded-lg text-sm">
                        <p className="font-medium text-amber-800 dark:text-amber-200 mb-1">What this does:</p>
                        <ul className="text-amber-700 dark:text-amber-300 space-y-1 list-disc list-inside text-xs">
                            <li><strong>Normal transactions</strong> ($100-$5K) spread over {demoSpreadDays} days</li>
                            <li><strong>High-value transactions</strong> ($15K-$100K) in last 3 days → triggers velocity alerts</li>
                            <li><strong>Seed flagged accounts</strong> → enables "flagged_connections" factor (30 pts)</li>
                        </ul>
                    </div>
                    
                    <Button
                        onClick={handleInjectDemoActivity}
                        disabled={demoLoading}
                        className="w-full bg-amber-600 hover:bg-amber-700 text-white"
                    >
                        {demoLoading ? (
                            <>
                                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                Injecting Demo Activity...
                            </>
                        ) : (
                            <>
                                <Play className="h-4 w-4 mr-2" />
                                Inject {demoTxnCount} Demo Transactions
                            </>
                        )}
                    </Button>
                </CardContent>
            </Card>

            {/* Graph Database Statistics */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <HardDrive className="h-5 w-5 text-blue-500" />
                        Aerospike Graph
                    </CardTitle>
                    <CardDescription>
                        Graph database for relationships and traversals
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                        <div className="p-4 bg-muted/50 rounded-lg text-center">
                            <Users className="h-6 w-6 mx-auto mb-2 text-purple-500" />
                            <p className="text-2xl font-bold">
                                {loadingStats ? (
                                    <Loader2 className="h-6 w-6 animate-spin mx-auto" />
                                ) : (
                                    graphStats?.users?.toLocaleString() || '0'
                                )}
                            </p>
                            <p className="text-sm text-muted-foreground">Users</p>
                        </div>
                        <div className="p-4 bg-muted/50 rounded-lg text-center">
                            <GitBranch className="h-6 w-6 mx-auto mb-2 text-green-500" />
                            <p className="text-2xl font-bold">
                                {loadingStats ? (
                                    <Loader2 className="h-6 w-6 animate-spin mx-auto" />
                                ) : (
                                    graphStats?.transactions?.toLocaleString() || '0'
                                )}
                            </p>
                            <p className="text-sm text-muted-foreground">Transactions</p>
                        </div>
                    </div>

                    <div className="pt-4 border-t">
                        <Button 
                            variant="outline" 
                            className="w-full"
                            onClick={fetchGraphStats}
                            disabled={loadingStats}
                        >
                            {loadingStats ? (
                                <>
                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                    Refreshing...
                                </>
                            ) : (
                                <>
                                    <RefreshCw className="h-4 w-4 mr-2" />
                                    Refresh Statistics
                                </>
                            )}
                        </Button>
                    </div>
                </CardContent>
            </Card>

            {/* Aerospike KV Statistics */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Server className="h-5 w-5 text-green-500" />
                        Aerospike KV Store
                        {aerospikeStats?.connected ? (
                            <span className="ml-auto text-xs font-normal text-green-600 flex items-center gap-1">
                                <CheckCircle className="h-3 w-3" />
                                Connected
                            </span>
                        ) : (
                            <span className="ml-auto text-xs font-normal text-amber-600 flex items-center gap-1">
                                <XCircle className="h-3 w-3" />
                                Not Available
                            </span>
                        )}
                    </CardTitle>
                    <CardDescription>
                        Key-value storage for risk evaluation and workflow
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                    {aerospikeStats?.connected ? (
                        <>
                            <div className="grid grid-cols-2 gap-4">
                                <div className="p-4 bg-muted/50 rounded-lg text-center">
                                    <Users className="h-6 w-6 mx-auto mb-2 text-purple-500" />
                                    <p className="text-2xl font-bold">
                                        {aerospikeStats.users_count?.toLocaleString() || '0'}
                                    </p>
                                    <p className="text-sm text-muted-foreground">Users Loaded</p>
                                </div>
                                <div className="p-4 bg-muted/50 rounded-lg text-center">
                                    <AlertTriangle className="h-6 w-6 mx-auto mb-2 text-amber-500" />
                                    <p className="text-2xl font-bold">
                                        {aerospikeStats.flagged_accounts_count?.toLocaleString() || '0'}
                                    </p>
                                    <p className="text-sm text-muted-foreground">Flagged</p>
                                </div>
                            </div>
                            
                            <div className="grid grid-cols-3 gap-2 text-center">
                                <div className="p-2 bg-amber-50 dark:bg-amber-950/20 rounded">
                                    <p className="text-lg font-bold text-amber-600">{aerospikeStats.pending_review || 0}</p>
                                    <p className="text-xs text-muted-foreground">Pending</p>
                                </div>
                                <div className="p-2 bg-red-50 dark:bg-red-950/20 rounded">
                                    <p className="text-lg font-bold text-red-600">{aerospikeStats.confirmed_fraud || 0}</p>
                                    <p className="text-xs text-muted-foreground">Fraud</p>
                                </div>
                                <div className="p-2 bg-green-50 dark:bg-green-950/20 rounded">
                                    <p className="text-lg font-bold text-green-600">{aerospikeStats.cleared || 0}</p>
                                    <p className="text-xs text-muted-foreground">Cleared</p>
                                </div>
                            </div>
                        </>
                    ) : (
                        <div className="p-4 bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800 rounded-lg">
                            <p className="text-sm text-red-800 dark:text-red-300 flex items-center gap-2">
                                <XCircle className="h-4 w-4" />
                                Aerospike KV service is not available. Risk evaluation features are disabled.
                            </p>
                        </div>
                    )}

                    <div className="pt-4 border-t">
                        <Button 
                            variant="outline" 
                            className="w-full"
                            onClick={fetchAerospikeStats}
                            disabled={loadingStats}
                        >
                            {loadingStats ? (
                                <>
                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                    Refreshing...
                                </>
                            ) : (
                                <>
                                    <RefreshCw className="h-4 w-4 mr-2" />
                                    Refresh Statistics
                                </>
                            )}
                        </Button>
                    </div>
                </CardContent>
            </Card>
        </div>
    )
}

export default DataManagement
