import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Eye, Shield } from 'lucide-react'
import { useState } from 'react'
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
    disabled?: boolean // For completely disabling scenarios from UI
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
        detailedDescription: 'Real-time detection system that flags transactions sent to accounts that have been previously identified as fraudulent. Uses 1-hop graph lookups for immediate threat assessment.'
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
        detailedDescription: 'Real-time detection system that flags transactions sent to accounts that have transacted with accounts identified as fraudulaent. Uses 2-hop graph lookups for neighborhood analysis.'
    },
    {
        id: 'RT3',
        name: 'Transactions with Users Associated with Flagged Devices',
        description: 'Detect threats through flagged device usage',
        riskLevel: 'High',
        enabled: true,
        priority: 'Phase 1',
        keyIndicators: [
            'Transactions directed to users associated with fradulent devices',
            'Multi-hop neighborhood analysis',
            'Transaction history analysis'
        ],
        commonUseCase: 'Immediate threat detection, known fraudster connections',
        detailedDescription: ''
    }
]

const Scenarios = () => {
    const [scenarios, setScenarios] = useState<FraudScenario[]>(fraudScenarios)

    const toggleScenario = (scenarioId: string) => {
        setScenarios(prev => prev.map(scenario => 
            scenario.id === scenarioId 
                ? { ...scenario, enabled: !scenario.enabled }
                : scenario
        ))
    }

    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center justify-between">
                    <span className="flex items-center space-x-2">
                        <Shield className="w-5 h-5" />
                        <span>Real Time Fraud Scenarios</span>
                    </span>
                    <div className="flex items-center space-x-2">
                        <Badge variant="secondary">
                            {scenarios.filter(s => s.enabled).length} of {scenarios.length} enabled
                        </Badge>
                    </div>
                </CardTitle>
                <CardDescription>
                    Configure which fraud detection patterns to monitor in real-time
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
                <div className="space-y-3">
                {scenarios.map((scenario) => (
                    <Collapsible key={scenario.id} defaultOpen={!scenario.disabled}>
                        <div className={`border rounded-lg ${scenario.disabled ? 'opacity-50 bg-gray-50 dark:bg-gray-900' : ''}`}>
                            <CollapsibleTrigger asChild>
                                <div className="flex items-center justify-between w-full p-3 hover:bg-gray-50">
                                    <div className="flex items-center space-x-3">
                                        <div onClick={(e) => e.stopPropagation()}>
                                            <Switch
                                                checked={scenario.enabled}
                                                onCheckedChange={() => !scenario.disabled && toggleScenario(scenario.id)}
                                                disabled={scenario.disabled}/>
                                        </div>
                                        <div className="text-left">
                                            <div className={`font-medium ${scenario.disabled ? 'text-gray-400' : ''}`}>
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
                                        <div
                                            className={`inline-flex items-center justify-center h-9 rounded-md px-3 text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 hover:bg-accent hover:text-accent-foreground ${scenario.disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}`}
                                            tabIndex={scenario.disabled ? -1 : 0}
                                            role="button"
                                            aria-label="View scenario details"
                                        >
                                            <Eye className="w-4 h-4" />
                                        </div>
                                    </div>
                                </div>
                            </CollapsibleTrigger>
                            <CollapsibleContent>
                                <div className="space-y-3 text-sm p-3">
                                    <strong>Key Indicators:</strong>
                                    <ul className="list-disc list-inside mt-1 space-y-1">
                                    {scenario.keyIndicators.map((indicator, index) => (
                                        <li key={index} className="text-muted-foreground">{indicator}</li>
                                    ))}
                                    </ul>
                                    <strong>Common Use Case:</strong>
                                    <p className="text-muted-foreground mt-1">{scenario.commonUseCase}</p>
                                </div>
                            </CollapsibleContent>
                        </div>
                    </Collapsible>
                ))}
                </div>
            </CardContent>
        </Card>
    )
}

export default Scenarios