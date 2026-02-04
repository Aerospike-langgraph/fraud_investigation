'use client'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Activity, Database, RefreshCw, Trash2 } from 'lucide-react'
import { useState, type Dispatch, type SetStateAction } from 'react'
import Confirm from '@/components/Confirm'

export interface GenerationStats {
    running: boolean
    total: number
    startTime?: string
    currentRate: number
    maxRate: number
    errors: number
    duration: string
}

interface Props {
    isGenerating: boolean
    stats: GenerationStats
    setStats: Dispatch<SetStateAction<GenerationStats>>
}

const Statistics = ({
    isGenerating,
    stats,
    setStats
}: Props) => {
    const [loading, setLoading] = useState(false)

    const clearTxns = async () => {
        setLoading(true)
        try {
            const response  = await fetch("/api/transactions", { method: "DELETE"})
            if(response.ok) setStats(prev => ({ ...prev, totalGenerated: 0 }))
            else alert("An error occured")
        }
        catch(e) {
            alert(`An error occured: ${e}`)
        }
        finally {
            setLoading(false)
        }
    }

    return (
        <>
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center space-x-2">
                    <Activity className="w-5 h-5" />
                    <span>Statistics</span>
                </CardTitle>
                <CardDescription>
                    Real-time generation metrics
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                    <div className="text-center">
                        <div className="text-2xl font-bold text-primary">
                            {stats.total.toLocaleString()}
                        </div>
                        <div className="text-xs text-muted-foreground">Total Generated</div>
                    </div>
                    <div className="text-center">
                        <div className="text-2xl font-bold text-green-600">
                            {stats.currentRate}/s
                        </div>
                        <div className="text-xs text-muted-foreground">Current Rate</div>
                    </div>
                </div>
                <div className="space-y-2">
                    <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">Duration</span>
                        <span className="font-mono">{stats.duration}</span>
                    </div>
                    <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">Status</span>
                        <Badge variant={stats.running ? "default" : "secondary"}>
                            {stats.running ? "Running" : "Stopped"}
                        </Badge>
                    </div>
                </div>
                <div className="pt-4 border-t">
                    <h4 className="text-sm font-medium mb-3 flex items-center space-x-2">
                        <Database className="w-4 h-4" />
                        <span>Quick Actions</span>
                    </h4>
                    <div className="text-xs text-muted-foreground mb-2">
                        This will remove all generated transactions from the system.
                    </div>
                    <div className="space-y-3">
                        <Confirm
                            title='Are you absolutely sure?'
                            message='This action cannot be undone. This will permanently delete all transactions.'
                            action={clearTxns}
                        >
                            <Button variant="outline" disabled={isGenerating} className="w-full" size="sm">
                                <Trash2 className="w-4 h-4 mr-2" />
                                Clear All Transactions
                            </Button>
                        </Confirm>
                    </div>
                </div>
            </CardContent>
        </Card>
        {loading &&
        <div className='fixed inset-0 z-50 bg-black/80 flex items-center justify-center'>
            <div className="flex flex-col gap-4 items-center justify-center text-white">
                <p className='text-2xl'>Deleting transactions...</p>
                <RefreshCw className='w-16 h-16 animate-spin' />
            </div>
        </div>}
        </>
    )
}

export default Statistics