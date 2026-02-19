'use client'

import { useEffect, useState } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Play, Square, Zap, Trash2, Loader2 } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import type { GenerationStats } from './Statistics'
import type { Dispatch, SetStateAction } from 'react'
import Confirm from '@/components/Confirm'

interface Props {
    isGenerating: boolean
    currentRate: number
    stats: GenerationStats
    setStats: Dispatch<SetStateAction<GenerationStats>>
    startGeneration: (rate: number) => Promise<void>
    stopGeneration: () => Promise<void>
}

const Controls = ({
    isGenerating,
    currentRate,
    stats,
    setStats,
    startGeneration,
    stopGeneration
}: Props) => {
    const [generationRate, setGenerationRate] = useState(currentRate)
    const [maxGenerationRate, setMaxGenerationRate] = useState(0)
    const [clearLoading, setClearLoading] = useState(false)

    const getMaxRate = async () => {
        try {
            const response = await fetch("/api/transaction-generation/max-rate")
            const data = await response.json()
            setMaxGenerationRate(data.max_rate)
        }
        catch(e) {
            if(e instanceof Error) {
                console.error(`Could not get max generation rate: ${e.message}`)
            }
        }
    }

    const clearTxns = async () => {
        setClearLoading(true)
        try {
            const response = await fetch("/api/transactions", { method: "DELETE" })
            if (response.ok) setStats(prev => ({ ...prev, total: 0 }))
            else alert("An error occurred")
        }
        catch(e) {
            alert(`An error occurred: ${e}`)
        }
        finally {
            setClearLoading(false)
        }
    }

    useEffect(() => {
        getMaxRate()
    }, [])

    return (
        <>
        <Card className="overflow-hidden border-0 shadow-sm h-full flex flex-col">
            <CardHeader className="p-4 pb-2">
                <CardTitle className="flex items-center gap-2 text-sm">
                    <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-primary/10">
                        <Zap className="h-3.5 w-3.5 text-primary" />
                    </div>
                    Bulk Generation
                    <Badge variant={stats.running ? "default" : "secondary"} className="ml-auto text-[10px] font-normal px-1.5 py-0">
                        {stats.running ? "Running" : "Stopped"}
                    </Badge>
                </CardTitle>
                <CardDescription className="text-[11px]">
                    Configure rate, start/stop, and monitor generation
                </CardDescription>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col space-y-2.5 p-4 pt-1">
                {/* Rate config + start/stop */}
                <div className="flex items-end gap-2">
                    <div className="space-y-1">
                        <label className="text-xs font-medium">Rate (txns/sec)</label>
                        <Input
                            name='generation-rate'
                            type="number"
                            min="1"
                            max={maxGenerationRate}
                            value={generationRate}
                            onChange={(e) => setGenerationRate(parseInt(e.target.value) || 1)}
                            disabled={isGenerating}
                            className="h-7 text-xs w-20"
                        />
                    </div>
                    <Button onClick={() => startGeneration(generationRate)} disabled={isGenerating} size="sm" className="h-7 text-xs px-3">
                        <Play className="h-3 w-3 mr-1" />
                        Start
                    </Button>
                    <Button variant="destructive" onClick={stopGeneration} disabled={!isGenerating} size="sm" className="h-7 text-xs px-3">
                        <Square className="h-3 w-3 mr-1" />
                        Stop
                    </Button>
                    <span className='text-[10px] text-muted-foreground ml-auto'>max {maxGenerationRate}</span>
                </div>

                {/* Live stats row */}
                <div className="grid grid-cols-4 gap-1.5">
                    <div className="rounded-md bg-muted/50 px-2 py-1.5 text-center">
                        <p className="text-sm font-bold text-primary leading-tight">{stats.total.toLocaleString()}</p>
                        <p className="text-[9px] text-muted-foreground">Generated</p>
                    </div>
                    <div className="rounded-md bg-muted/50 px-2 py-1.5 text-center">
                        <p className="text-sm font-bold text-green-600 leading-tight">{stats.currentRate}/s</p>
                        <p className="text-[9px] text-muted-foreground">Rate</p>
                    </div>
                    <div className="rounded-md bg-muted/50 px-2 py-1.5 text-center">
                        <p className="text-sm font-bold font-mono leading-tight">{stats.duration}</p>
                        <p className="text-[9px] text-muted-foreground">Duration</p>
                    </div>
                    <div className="rounded-md bg-muted/50 px-2 py-1.5 text-center">
                        <p className={`text-sm font-bold leading-tight ${stats.errors > 0 ? 'text-red-600' : ''}`}>{stats.errors}</p>
                        <p className="text-[9px] text-muted-foreground">Errors</p>
                    </div>
                </div>

                {/* Clear action at bottom */}
                <div className="mt-auto">
                    <Confirm
                        title='Are you absolutely sure?'
                        message='This action cannot be undone. This will permanently delete all generated transactions.'
                        action={clearTxns}
                    >
                        <Button variant="ghost" disabled={isGenerating} className="w-full h-6 text-[11px] text-muted-foreground hover:text-red-600" size="sm">
                            <Trash2 className="h-3 w-3 mr-1" />
                            Clear all transactions
                        </Button>
                    </Confirm>
                </div>
            </CardContent>
        </Card>
        {clearLoading &&
        <div className='fixed inset-0 z-50 bg-black/80 flex items-center justify-center'>
            <div className="flex flex-col gap-4 items-center justify-center text-white">
                <p className='text-2xl'>Deleting transactions...</p>
                <Loader2 className='w-16 h-16 animate-spin' />
            </div>
        </div>}
        </>
    )
}

export default Controls
