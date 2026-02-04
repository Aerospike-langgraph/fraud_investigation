'use client'

import { Suspense, useEffect, useState } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Play, RefreshCw, Settings, Square } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'

interface Props {
    isGenerating: boolean
    currentRate: number
    startGeneration: (rate: number) => Promise<void>
    stopGeneration: () => Promise<void>
}

const Controls = ({
    isGenerating,
    currentRate,
    startGeneration,
    stopGeneration
}: Props) => {
    const [generationRate, setGenerationRate] = useState(currentRate)
    const [maxGenerationRate, setMaxGenerationRate] = useState(0)

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

    useEffect(() => {
        getMaxRate()
    }, [])

    return (
        <Card className='flex flex-col'>
            <CardHeader>
                <CardTitle className="flex items-center space-x-2">
                    <Settings className="w-5 h-5" />
                    <span>Generation Controls</span>
                </CardTitle>
                <CardDescription>
                    Start, stop, and configure transaction generation
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 grow flex flex-col">
                <div className="space-y-2">
                    <label className="text-sm font-medium">Generation Rate (transactions/sec)</label>
                    <Input
                        name='generation-rate'
                        type="number"
                        min="1"
                        max={maxGenerationRate}
                        value={generationRate}
                        onChange={(e) => setGenerationRate(parseInt(e.target.value) || 1)}
                        disabled={isGenerating}
                    />
                    <Suspense>
                        <span className='text-xs text-muted-foreground'>Maximum generation rate: {maxGenerationRate}</span>
                    </Suspense>
                </div>
                <div className="flex space-x-2 grow items-end">
                    <Button onClick={() => startGeneration(generationRate)} disabled={isGenerating} className="flex-1">
                        <Play className="w-4 h-4 mr-2" />
                        Start
                    </Button>
                    <Button variant="destructive" onClick={stopGeneration} disabled={!isGenerating} className="flex-1">
                        <Square className="w-4 h-4 mr-2" />
                        Stop
                    </Button>
                </div>
            </CardContent>
        </Card>
    )
}

export default Controls