'use client'

import { CheckCircle, Circle, Clock } from 'lucide-react'
import { cn } from '@/lib/utils'

export type StepStatus = 'completed' | 'current' | 'upcoming'

interface Props {
    step: number
    title: string
    description: string
    status: StepStatus
    isLast?: boolean
}

const WorkflowStep = ({ step, title, description, status, isLast = false }: Props) => {
    return (
        <div className="flex gap-4">
            {/* Step indicator */}
            <div className="flex flex-col items-center">
                <div className={cn(
                    "w-10 h-10 rounded-full flex items-center justify-center border-2 transition-colors",
                    status === 'completed' && "bg-green-500 border-green-500 text-white",
                    status === 'current' && "bg-primary border-primary text-primary-foreground",
                    status === 'upcoming' && "bg-muted border-muted-foreground/30 text-muted-foreground"
                )}>
                    {status === 'completed' ? (
                        <CheckCircle className="h-5 w-5" />
                    ) : status === 'current' ? (
                        <Clock className="h-5 w-5" />
                    ) : (
                        <Circle className="h-5 w-5" />
                    )}
                </div>
                {!isLast && (
                    <div className={cn(
                        "w-0.5 flex-1 min-h-[40px]",
                        status === 'completed' ? "bg-green-500" : "bg-muted-foreground/30"
                    )} />
                )}
            </div>

            {/* Step content */}
            <div className="pb-8">
                <div className="flex items-center gap-2">
                    <span className={cn(
                        "text-xs font-medium px-2 py-0.5 rounded",
                        status === 'completed' && "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400",
                        status === 'current' && "bg-primary/10 text-primary",
                        status === 'upcoming' && "bg-muted text-muted-foreground"
                    )}>
                        Step {step}
                    </span>
                </div>
                <h4 className={cn(
                    "font-semibold mt-1",
                    status === 'upcoming' && "text-muted-foreground"
                )}>
                    {title}
                </h4>
                <p className="text-sm text-muted-foreground mt-1">{description}</p>
            </div>
        </div>
    )
}

export default WorkflowStep
