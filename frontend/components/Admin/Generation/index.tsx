'use client'

import {type Dispatch, type SetStateAction, useEffect, useRef, useState } from 'react'
import Statistics, { type GenerationStats } from './Statistics'
import Controls from './Controls'
import Manual from './Manual'
import { getDuration } from '@/lib/utils'
import { toast } from "sonner"


interface Props {
	isGenerating: boolean
	setIsGenerating: Dispatch<SetStateAction<boolean>>
}

const Generation = ({ isGenerating, setIsGenerating }: Props) => {
	const [stats, setStats] = useState<GenerationStats>({
		running: false,
		total: 0,
		errors: 0,
		maxRate: 200,
		currentRate: 1,
		duration: '00:00:00',
		startTime: new Date().toISOString()
	})
	const pollingRef = useRef<NodeJS.Timeout | undefined>(undefined)
	
	const getGenerationStats = async (): Promise<GenerationStats> => {
		const response = await fetch('/generate/status')
		const status = await response.json() as GenerationStats
		return status
	}

	const getPollingInterval = () => {
		return setInterval(async () => {
			getGenerationStats()
			.then(({running, startTime, ...rest}) => setStats({
				running,
				startTime,
				...rest,
				duration: (running && startTime) ? getDuration(startTime) : "00:00:00",
			}))
			.catch((error) => console.error('Error polling status:', error))
		}, 1000)
	}

	const startGeneration = async (rate: number) => {
		if(isGenerating) return
		try {
			const start = new Date().toISOString()
			const response = await fetch('/generate/start', {
				headers: {
					"Content-Type": "application/json"
				},
				method: 'POST',
				body: JSON.stringify({ rate, start })
			})
			const { error } = await response.json()
			if(!error) {
				setIsGenerating(true)
				pollingRef.current = getPollingInterval()
				toast.success("Transaction generation started")
				setStats(prev => ({ ...prev, isRunning: true, startTime: start }))
			}
			else {
				throw new Error(error);
			}
		}
		catch(e) {
			if(e instanceof Error) {
				console.error("Error starting generator", e.message)
				toast.error(e.message)
			}
		}
	}

	const stopGeneration = async () => {
		try {
			clearInterval(pollingRef.current)
			const response = await fetch('/generate/stop', { method: 'POST' });
			const { error } = await response.json()
			if(!error) {
				setIsGenerating(false)
				toast.success("Transaction generation stopped")
				setStats(prev => ({ ...prev, isRunning: false }))
			} 
			else {
				throw new Error(error);
			}
		}
		catch(e) {
			if(e instanceof Error) {
				console.error("Error stopping generator", e.message)
				toast.error(e.message)
			}
		}
	}

	useEffect(() => {
		getGenerationStats()
		.then(({ running, startTime, ...rest }) => {
			if(running) {
				setIsGenerating(true)
				pollingRef.current = getPollingInterval()
			}
			setStats({
				running,
				...rest,
				duration: (running && startTime) ? getDuration(startTime) : "00:00:00"
			})
		})
		return () => {
			if(pollingRef.current) {
				clearInterval(pollingRef.current)
			}
		}
	}, []);

    return (
        <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            <Controls
				key={stats.currentRate.toString()}
				isGenerating={isGenerating}
				currentRate={stats.currentRate}
				startGeneration={startGeneration}
				stopGeneration={stopGeneration} />
			<Statistics
				isGenerating={isGenerating}
				stats={stats}
				setStats={setStats} />
            <Manual />
        </div>
    )
}

export default Generation