'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { 
    CheckCircle, 
    XCircle, 
    AlertTriangle, 
    MessageSquare,
    FileText,
    Shield,
    Ban,
    ThumbsUp
} from 'lucide-react'
import WorkflowStep, { type StepStatus } from './WorkflowStep'

interface Props {
    currentStep: number
    onStepChange: (step: number) => void
}

const workflowSteps = [
    {
        title: 'Initial Review',
        description: 'Review account details, transaction history, and flag reasons'
    },
    {
        title: 'Transaction Analysis',
        description: 'Analyze suspicious transactions and identify patterns'
    },
    {
        title: 'Risk Assessment',
        description: 'Evaluate overall risk level and potential impact'
    },
    {
        title: 'Decision & Documentation',
        description: 'Make final determination and document findings'
    }
]

const ReviewWorkflow = ({ currentStep, onStepChange }: Props) => {
    const [notes, setNotes] = useState('')
    const [decision, setDecision] = useState<'fraud' | 'safe' | null>(null)

    const getStepStatus = (stepIndex: number): StepStatus => {
        if (stepIndex < currentStep) return 'completed'
        if (stepIndex === currentStep) return 'current'
        return 'upcoming'
    }

    return (
        <div className="space-y-6">
            {/* Workflow Progress */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <FileText className="h-5 w-5" />
                        Review Workflow
                    </CardTitle>
                    <CardDescription>
                        Complete each step to process this flagged account
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="space-y-0">
                        {workflowSteps.map((step, index) => (
                            <WorkflowStep
                                key={index}
                                step={index + 1}
                                title={step.title}
                                description={step.description}
                                status={getStepStatus(index)}
                                isLast={index === workflowSteps.length - 1}
                            />
                        ))}
                    </div>

                    {/* Step Navigation */}
                    <div className="flex justify-between mt-6 pt-4 border-t">
                        <Button
                            variant="outline"
                            onClick={() => onStepChange(Math.max(0, currentStep - 1))}
                            disabled={currentStep === 0}
                        >
                            Previous Step
                        </Button>
                        {currentStep < workflowSteps.length - 1 ? (
                            <Button onClick={() => onStepChange(currentStep + 1)}>
                                Continue to Next Step
                            </Button>
                        ) : (
                            <Button disabled={!decision}>
                                Submit Decision
                            </Button>
                        )}
                    </div>
                </CardContent>
            </Card>

            {/* Current Step Content */}
            {currentStep === 3 && (
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Shield className="h-5 w-5" />
                            Final Decision
                        </CardTitle>
                        <CardDescription>
                            Based on your analysis, make a determination for this account
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                        {/* Decision Buttons */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <button
                                onClick={() => setDecision('fraud')}
                                className={`p-6 rounded-lg border-2 transition-all text-left ${
                                    decision === 'fraud'
                                        ? 'border-red-500 bg-red-50 dark:bg-red-950/20'
                                        : 'border-muted hover:border-red-300 hover:bg-red-50/50 dark:hover:bg-red-950/10'
                                }`}
                            >
                                <div className="flex items-center gap-3">
                                    <div className={`p-3 rounded-full ${
                                        decision === 'fraud' 
                                            ? 'bg-red-500 text-white' 
                                            : 'bg-red-100 text-red-600 dark:bg-red-900/30 dark:text-red-400'
                                    }`}>
                                        <Ban className="h-6 w-6" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-lg">Mark as Fraud</h4>
                                        <p className="text-sm text-muted-foreground">
                                            Confirm fraudulent activity detected
                                        </p>
                                    </div>
                                </div>
                                {decision === 'fraud' && (
                                    <div className="mt-4 p-3 bg-red-100 dark:bg-red-900/30 rounded-lg">
                                        <p className="text-sm text-red-800 dark:text-red-300">
                                            <strong>Actions to be taken:</strong>
                                        </p>
                                        <ul className="text-sm text-red-700 dark:text-red-400 mt-1 space-y-1">
                                            <li>• Account will be frozen immediately</li>
                                            <li>• Notify account holder via email</li>
                                            <li>• Generate fraud investigation report</li>
                                            <li>• Flag linked accounts for review</li>
                                        </ul>
                                    </div>
                                )}
                            </button>

                            <button
                                onClick={() => setDecision('safe')}
                                className={`p-6 rounded-lg border-2 transition-all text-left ${
                                    decision === 'safe'
                                        ? 'border-green-500 bg-green-50 dark:bg-green-950/20'
                                        : 'border-muted hover:border-green-300 hover:bg-green-50/50 dark:hover:bg-green-950/10'
                                }`}
                            >
                                <div className="flex items-center gap-3">
                                    <div className={`p-3 rounded-full ${
                                        decision === 'safe' 
                                            ? 'bg-green-500 text-white' 
                                            : 'bg-green-100 text-green-600 dark:bg-green-900/30 dark:text-green-400'
                                    }`}>
                                        <ThumbsUp className="h-6 w-6" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-lg">Mark as Safe</h4>
                                        <p className="text-sm text-muted-foreground">
                                            Clear account - no fraud detected
                                        </p>
                                    </div>
                                </div>
                                {decision === 'safe' && (
                                    <div className="mt-4 p-3 bg-green-100 dark:bg-green-900/30 rounded-lg">
                                        <p className="text-sm text-green-800 dark:text-green-300">
                                            <strong>Actions to be taken:</strong>
                                        </p>
                                        <ul className="text-sm text-green-700 dark:text-green-400 mt-1 space-y-1">
                                            <li>• Remove fraud flag from account</li>
                                            <li>• Update risk score accordingly</li>
                                            <li>• Document false positive for ML training</li>
                                            <li>• Notify account holder of clearance</li>
                                        </ul>
                                    </div>
                                )}
                            </button>
                        </div>

                        {/* Notes Section */}
                        <div className="space-y-2">
                            <label className="text-sm font-medium flex items-center gap-2">
                                <MessageSquare className="h-4 w-4" />
                                Investigation Notes
                            </label>
                            <textarea
                                className="w-full min-h-[120px] p-3 border rounded-lg bg-background resize-none focus:outline-none focus:ring-2 focus:ring-primary"
                                placeholder="Document your findings and reasoning for this decision..."
                                value={notes}
                                onChange={(e) => setNotes(e.target.value)}
                            />
                        </div>

                        {/* Warning for incomplete */}
                        {!decision && (
                            <div className="flex items-center gap-2 p-3 bg-amber-50 dark:bg-amber-950/20 rounded-lg border border-amber-200 dark:border-amber-800">
                                <AlertTriangle className="h-5 w-5 text-amber-600" />
                                <p className="text-sm text-amber-800 dark:text-amber-300">
                                    Please select a decision above before submitting
                                </p>
                            </div>
                        )}
                    </CardContent>
                </Card>
            )}
        </div>
    )
}

export default ReviewWorkflow
