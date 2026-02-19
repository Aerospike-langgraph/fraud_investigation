'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'
import { Badge } from '@/components/ui/badge'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Zap, Eye } from 'lucide-react'
import { getRiskLevelColor } from '@/lib/utils'

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

const FraudDetection = () => {
    const [scenarios, setScenarios] = useState<FraudScenario[]>(fraudScenarios)

    const toggleScenario = (scenarioId: string) => {
        setScenarios(prev => prev.map(scenario => 
            scenario.id === scenarioId 
                ? { ...scenario, enabled: !scenario.enabled }
                : scenario
        ))
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
        </div>
    )
}

export default FraudDetection
