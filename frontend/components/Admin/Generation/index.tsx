'use client'

import {type Dispatch, type SetStateAction, useEffect, useRef, useState } from 'react'
import { type GenerationStats } from './Statistics'
import Controls from './Controls'
import Manual from './Manual'
import { getDuration } from '@/lib/utils'
import { toast } from "sonner"
import { CheckCircle, Zap, CreditCard } from 'lucide-react'


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

	// Workflow steps
	const steps = [
		{ label: 'Bulk generate', done: stats.total > 0, icon: Zap },
		{ label: 'Manual transaction', done: false, icon: CreditCard },
	]

    return (
        <div className="space-y-4 pb-4">
			{/* Workflow stepper */}
			<div className="flex items-center justify-center gap-0">
				{steps.map((step, i) => (
					<div key={i} className="flex items-center">
						<div className="flex items-center gap-1.5">
							<div className={`flex h-5 w-5 items-center justify-center rounded-full text-[10px] font-medium ${
								step.done ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
							}`}>
								{step.done ? <CheckCircle className="h-3 w-3" /> : i + 1}
							</div>
							<span className={`text-xs whitespace-nowrap ${step.done ? 'text-foreground font-medium' : 'text-muted-foreground'}`}>
								{step.label}
							</span>
						</div>
						{i < steps.length - 1 && (
							<div className={`mx-2.5 h-px w-8 ${step.done ? 'bg-primary' : 'bg-border'}`} />
						)}
					</div>
				))}
			</div>

			{/* Both cards side by side */}
			<div className="grid gap-4 lg:grid-cols-2">
				<Controls
					key={stats.currentRate.toString()}
					isGenerating={isGenerating}
					currentRate={stats.currentRate}
					stats={stats}
					setStats={setStats}
					startGeneration={startGeneration}
					stopGeneration={stopGeneration} />
				<Manual />
			</div>
        </div>
    )
}

export default Generation
