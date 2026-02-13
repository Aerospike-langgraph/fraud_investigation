'use client'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Activity, Trash2, Loader2 } from 'lucide-react'
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
        <Card className="overflow-hidden border-0 shadow-sm h-full flex flex-col">
            <CardHeader className="p-4 pb-2">
                <CardTitle className="flex items-center gap-2 text-sm">
                    <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-green-500/10">
                        <Activity className="h-3.5 w-3.5 text-green-600" />
                    </div>
                    Statistics
                    <Badge variant={stats.running ? "default" : "secondary"} className="ml-auto text-[10px] font-normal px-1.5 py-0">
                        {stats.running ? "Running" : "Stopped"}
                    </Badge>
                </CardTitle>
                <CardDescription className="text-[11px]">
                    Real-time generation metrics
                </CardDescription>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col space-y-2.5 p-4 pt-1">
                {/* Metrics row */}
                <div className="grid grid-cols-2 gap-2">
                    <div className="rounded-lg bg-muted/50 p-2 text-center">
                        <p className="text-lg font-bold text-primary leading-tight">{stats.total.toLocaleString()}</p>
                        <p className="text-[10px] text-muted-foreground">Total Generated</p>
                    </div>
                    <div className="rounded-lg bg-muted/50 p-2 text-center">
                        <p className="text-lg font-bold text-green-600 leading-tight">{stats.currentRate}/s</p>
                        <p className="text-[10px] text-muted-foreground">Current Rate</p>
                    </div>
                </div>

                {/* Duration & errors compact */}
                <div className="rounded-lg border border-border/80 bg-muted/20 px-2.5 py-1.5 flex items-center justify-between text-[11px]">
                    <span className="text-muted-foreground">Duration <span className="font-mono font-medium text-foreground">{stats.duration}</span></span>
                    <span className="text-muted-foreground">Errors <span className={`font-medium ${stats.errors > 0 ? 'text-red-600' : 'text-foreground'}`}>{stats.errors}</span></span>
                </div>

                {/* Clear action pushed to bottom */}
                <div className="mt-auto">
                    <Confirm
                        title='Are you absolutely sure?'
                        message='This action cannot be undone. This will permanently delete all transactions.'
                        action={clearTxns}
                    >
                        <Button variant="outline" disabled={isGenerating} className="w-full h-7 text-xs" size="sm">
                            <Trash2 className="h-3 w-3 mr-1" />
                            Clear All Transactions
                        </Button>
                    </Confirm>
                </div>
            </CardContent>
        </Card>
        {loading &&
        <div className='fixed inset-0 z-50 bg-black/80 flex items-center justify-center'>
            <div className="flex flex-col gap-4 items-center justify-center text-white">
                <p className='text-2xl'>Deleting transactions...</p>
                <Loader2 className='w-16 h-16 animate-spin' />
            </div>
        </div>}
        </>
    )
}

export default Statistics
