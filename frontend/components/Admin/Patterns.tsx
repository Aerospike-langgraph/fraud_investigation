import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '../ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Activity, AlertTriangle, Globe, Shield, Target, TrendingUp } from 'lucide-react'
import { useState } from 'react'
import { getPatternRiskColor } from '@/lib/utils'

interface FraudPattern {
    id: string
    name: string
    description: string
    risk_level: string
}

interface FraudResult {
    pattern_id: string
    pattern_name: string
    detected_entities: any[]
    risk_score: number
    timestamp: string
    details: any
}

interface ExtendedFraudPattern extends FraudPattern {
	priority: 'Phase 1' | 'Phase 2' | 'Phase 3'
	enabled?: boolean
	disabled?: boolean
	keyIndicators?: string[]
	commonUseCase?: string
	detailedDescription?: string
}

const availablePatterns: ExtendedFraudPattern[] = [
  // Phase 1 - High Priority Patterns
	{
		id: 'A',
		name: 'Multiple Small Credits → Large Debit',
		description: 'Money laundering technique with structured deposits',
		risk_level: 'high',
		priority: 'Phase 1',
		enabled: true,
		keyIndicators: [
			'Multiple credits within 24 hours before debit',
			'Total credits ≈ debit amount (90-100%)',
			'At least 2 credit transactions'
		],
		commonUseCase: 'Money laundering, structuring transactions',
		detailedDescription: 'A sophisticated money laundering technique where an account receives multiple small credit transactions followed by a single large debit transaction.'
	},
	{
		id: 'B',
		name: 'Large Credit → Structured Equal Debits',
		description: 'Organized money distribution pattern',
		risk_level: 'high',
		priority: 'Phase 1',
		enabled: false,
		disabled: true,
		keyIndicators: [
			'Large credit (₹8,00,000-₹40,00,000)',
			'Exactly 4 equal debits within 4 hours',
			'Each debit ≈ 1/4 of credit amount'
		],
		commonUseCase: 'Money mule operations, organized fraud rings',
		detailedDescription: 'A pattern where a large credit is immediately followed by exactly 4 equal-sized debit transactions, indicating organized distribution of funds.'
	},
	{
		id: 'D',
		name: 'High-Frequency Mule Account Transfers',
		description: 'Rapid-fire money movement',
		risk_level: 'high',
		priority: 'Phase 1',
		enabled: true,
		keyIndicators: [
			'10+ transactions in short time',
			'Amounts ₹40,000-₹4,00,000 each',
			'Mix of credits/debits within 1 hour'
		],
		commonUseCase: 'Money mule networks, account takeover',
		detailedDescription: 'Rapid-fire transactions between multiple accounts, indicating money mule activity or account takeover.'
	},
	{
		id: 'circular_flow',
		name: 'Circular Transaction Flow',
		description: 'Detect circular money flows between users',
		risk_level: 'high',
		priority: 'Phase 1',
		enabled: false,
		disabled: true
	},
	{
		id: 'high_amount',
		name: 'High Amount Transactions',
		description: 'Detect unusually high transaction amounts',
		risk_level: 'high',
		priority: 'Phase 1',
		enabled: false,
		disabled: true
	},
	{
		id: 'new_user_high_activity',
		name: 'New User High Activity',
		description: 'Detect new users with high transaction activity',
		risk_level: 'high',
		priority: 'Phase 1',
		enabled: true
	},
  	// Phase 2 - Medium Priority Patterns
	{
		id: 'C',
		name: 'Multiple Large ATM Withdrawals',
		description: 'Systematic cash extraction pattern',
		risk_level: 'medium',
		priority: 'Phase 2',
		enabled: false,
		disabled: true,
		keyIndicators: [
			'3+ ATM withdrawal transactions',
			'Each withdrawal ₹4,00,000-₹8,00,000',
			'Self-directed transactions'
		],
		commonUseCase: 'Cash extraction for money laundering',
		detailedDescription: 'A pattern of multiple large ATM withdrawals that could indicate cash extraction for illicit purposes.'
	},
	{
		id: 'E',
		name: 'Salary-Like Deposits → Suspicious Transfers',
		description: 'Account takeover mimicry',
		risk_level: 'medium',
		priority: 'Phase 2',
		enabled: false,
		disabled: true,
		keyIndicators: [
			'Initial credit ₹4,00,000-₹8,00,000',
			'3+ outgoing transfers',
			'Transfer amounts ₹4,00,000-₹5,60,000'
		],
		commonUseCase: 'Account takeover, identity theft',
		detailedDescription: 'A pattern mimicking legitimate salary deposits but followed by suspicious outgoing transfers.'
	},
	{
		id: 'F',
		name: 'Dormant Account Sudden Activity',
		description: 'Account compromise indicator',
		risk_level: 'high',
		priority: 'Phase 2',
		enabled: false,
		disabled: true,
		keyIndicators: [
			'30+ days dormancy',
			'Sudden large credit ₹8,00,000-₹40,00,000',
			'4 equal debits following'
		],
		commonUseCase: 'Account takeover, dormant account exploitation',
		detailedDescription: 'A previously inactive account suddenly receives a large deposit followed by structured withdrawals.'
	},
	{
		id: 'shared_device',
		name: 'Shared Device Transactions',
		description: 'Detect transactions from the same device by different users',
		risk_level: 'medium',
		priority: 'Phase 2',
		enabled: false,
		disabled: true
	},
	{
		id: 'cross_location',
		name: 'Cross-Location Transactions',
		description: 'Detect transactions between users in different locations',
		risk_level: 'medium',
		priority: 'Phase 2',
		enabled: false,
		disabled: true
	},
  	// Phase 3 - Lower Priority Patterns
	{
		id: 'G',
		name: 'International High-Risk Transfers',
		description: 'Cross-border money laundering',
		risk_level: 'high',
		priority: 'Phase 3',
		enabled: false,
		disabled: true,
		keyIndicators: [
			'5+ international transfers',
			'Amounts ₹40,000-₹4,00,000 each',
			'High-risk jurisdictions'
		],
		commonUseCase: 'International money laundering, terrorist financing',
		detailedDescription: 'Multiple transfers to specific international locations known for financial crime or money laundering.'
	},
	{
		id: 'H',
		name: 'Region-Specific Fraud (Indian)',
		description: 'Localized fraud patterns',
		risk_level: 'high',
		priority: 'Phase 3',
		enabled: false,
		disabled: true,
		keyIndicators: [
			'3+ large transfers ₹8,00,000-₹40,00,000',
			'High-risk locations',
			'Jamtara, Bharatpur, Alwar'
		],
		commonUseCase: 'Regional fraud networks, location-based scams',
		detailedDescription: 'Fraud patterns specific to the Indian financial landscape, targeting known fraud-prone regions.'
	}
]

const Patterns = () => {
	const [selectedPatterns, setSelectedPatterns] = useState<string[]>(
    	availablePatterns.filter(p => p.enabled && !p.disabled).map(p => p.id)
  	)
	const [patternResults, setPatternResults] = useState<FraudResult[]>([])
	const [patternLoading, setPatternLoading] = useState(false)
	const [patterns, setPatterns] = useState<ExtendedFraudPattern[]>(availablePatterns)

	const togglePattern = (patternId: string) => {
		setSelectedPatterns(prev => 
		prev.includes(patternId) 
			? prev.filter(id => id !== patternId)
			: [...prev, patternId]
		)
	}

	const togglePatternEnabled = (patternId: string) => {
		setPatterns(prev => prev.map(pattern => 
		pattern.id === patternId 
			? { ...pattern, enabled: !pattern.enabled }
			: pattern
		))
		setSelectedPatterns(prev => {
			const pattern = patterns.find(p => p.id === patternId)
			if(pattern?.enabled) {
				return prev.filter(id => id !== patternId)
			}
			else {
				return prev.includes(patternId) ? prev : [...prev, patternId]
			}
		})
	}

	const runPatterns = async () => {
		if (selectedPatterns.length === 0) return	
		setPatternLoading(true)
		try {
			const response = await fetch('/api/fraud-patterns/run', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(selectedPatterns)
			})
			if(response.ok) {
				const data = await response.json()
				setPatternResults(data.results || [])
			} 
			else {
				console.error('Failed to run fraud patterns')
			}
		} 
		catch (error) {
			console.error('Failed to run fraud patterns:', error)
		} 
		finally {
			setPatternLoading(false)
		}
	}

  	const runAllPatterns = async () => {
		setPatternLoading(true)
		try {
			const response = await fetch('/api/detect/fraudulent-transactions')
			if(response.ok) {
				const data = await response.json()
				setPatternResults(data.results || [])
			} 
			else {
				console.error('Failed to run all fraud patterns')
			}
		}
		catch (error) {
			console.error('Failed to run all fraud patterns:', error)
		} 
		finally {
			setPatternLoading(false)
		}
	}

    return (
        <>
        <div>
            <h2 className="text-2xl font-bold tracking-tight mb-2">Fraud Pattern Detection</h2>
            <p className="text-muted-foreground">
              	Run fraud detection algorithms and analyze suspicious patterns
            </p>
        </div>
        <Card className="md:col-span-2">
            <CardHeader>
              	<CardTitle className="flex items-center justify-between">
					<span className="flex items-center gap-2">
						<AlertTriangle className="h-5 w-5" />
						Batch Fraud Patterns
					</span>
					<div className="flex gap-2">
						<Button 
							onClick={runPatterns} 
							disabled={patternLoading || selectedPatterns.length === 0}
							size="sm"
						>
							<Shield className="h-4 w-4 mr-2" />
							Run Selected ({selectedPatterns.length})
						</Button>
						<Button 
							onClick={runAllPatterns} 
							disabled={patternLoading}
							variant="outline"
							size="sm"
						>
							<Activity className="h-4 w-4 mr-2" />
							Run All
						</Button>
					</div>
              	</CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
                {/* Phase 1 Patterns */}
               	<div className="space-y-3">
                 	<h3 className="text-lg font-semibold flex items-center space-x-2">
                   		<Target className="w-4 h-4 text-blue-600" />
                   		<span>Phase 1 - High Priority</span>
                 	</h3>
                 	<div className="grid gap-3 md:grid-cols-2">
                    {patterns.filter(p => p.priority === 'Phase 1').map((pattern) => (
						<div
							key={pattern.id}
							className={`p-4 border rounded-lg transition-colors ${
								selectedPatterns.includes(pattern.id)
								? 'border-primary bg-primary/5'
								: 'border-border'
							} ${pattern.disabled ? 'opacity-50 bg-gray-50 dark:bg-gray-900' : ''}`}
						>
                      		<div className="flex items-start justify-between mb-2">
                        		<div className="flex items-center space-x-2">
									{pattern.enabled !== undefined &&
									<Switch
										checked={pattern.enabled}
										onCheckedChange={() => !pattern.disabled && togglePatternEnabled(pattern.id)}
										disabled={pattern.disabled} />}
									<div 
										className={`flex-1 ${pattern.disabled ? 'cursor-not-allowed' : 'cursor-pointer'}`}
										onClick={() => !pattern.disabled && togglePattern(pattern.id)}
									>
										<h4 className={`font-medium text-sm ${pattern.disabled ? 'text-gray-400' : ''}`}>
											{pattern.name}
											{pattern.disabled && <span className="ml-2 text-xs">(Coming Soon)</span>}
										</h4>
                          			</div>
                        		</div>
								<Badge variant={getPatternRiskColor(pattern.risk_level) as any} className={`text-xs ${pattern.disabled ? 'opacity-50' : ''}`}>
									{pattern.risk_level}
								</Badge>
                      		</div>
                      		<p className={`text-xs mb-2 ${pattern.disabled ? 'text-gray-400' : 'text-muted-foreground'}`}>{pattern.description}</p>
                      		{pattern.keyIndicators &&
							<div className="text-xs">
								<strong className={pattern.disabled ? 'text-gray-400' : ''}>Key Indicators:</strong>
								<ul className="list-disc list-inside mt-1 space-y-0.5">
									{pattern.keyIndicators.slice(0, 2).map((indicator, index) => (
										<li key={index} className={pattern.disabled ? 'text-gray-400' : 'text-muted-foreground'}>{indicator}</li>
									))}
								</ul>
							</div>}
                    	</div>
                  	))}
                	</div>
            	</div>
				{/* Phase 2 Patterns */}
               	<div className="space-y-3">
					<h3 className="text-lg font-semibold flex items-center space-x-2">
						<TrendingUp className="w-4 h-4 text-purple-600" />
						<span>Phase 2 - Medium Priority</span>
					</h3>
                 	<div className="grid gap-3 md:grid-cols-2">
                    {patterns.filter(p => p.priority === 'Phase 2').map((pattern) => (
						<div
							key={pattern.id}
							className={`p-4 border rounded-lg transition-colors ${
								selectedPatterns.includes(pattern.id)
								? 'border-primary bg-primary/5'
								: 'border-border'
							} ${pattern.disabled ? 'opacity-50 bg-gray-50 dark:bg-gray-900' : ''}`}
						>
                      		<div className="flex items-start justify-between mb-2">
                        		<div className="flex items-center space-x-2">
									{pattern.enabled !== undefined &&
									<Switch
										checked={pattern.enabled}
										onCheckedChange={() => !pattern.disabled && togglePatternEnabled(pattern.id)}
										disabled={pattern.disabled} />}
									<div 
										className={`flex-1 ${pattern.disabled ? 'cursor-not-allowed' : 'cursor-pointer'}`}
										onClick={() => !pattern.disabled && togglePattern(pattern.id)}
									>
										<h4 className={`font-medium text-sm ${pattern.disabled ? 'text-gray-400' : ''}`}>
											{pattern.name}
											{pattern.disabled && <span className="ml-2 text-xs">(Coming Soon)</span>}
										</h4>
									</div>
								</div>
								<Badge variant={getPatternRiskColor(pattern.risk_level) as any} className={`text-xs ${pattern.disabled ? 'opacity-50' : ''}`}>
									{pattern.risk_level}
								</Badge>
							</div>
                      		<p className={`text-xs mb-2 ${pattern.disabled ? 'text-gray-400' : 'text-muted-foreground'}`}>{pattern.description}</p>
							{pattern.keyIndicators &&
							<div className="text-xs">
								<strong className={pattern.disabled ? 'text-gray-400' : ''}>Key Indicators:</strong>
								<ul className="list-disc list-inside mt-1 space-y-0.5">
									{pattern.keyIndicators.slice(0, 2).map((indicator, index) => (
										<li key={index} className={pattern.disabled ? 'text-gray-400' : 'text-muted-foreground'}>{indicator}</li>
									))}
								</ul>
							</div>}
                    	</div>
                	))}
                	</div>
            	</div>
		        {/* Phase 3 Patterns */}
               	<div className="space-y-3">
					<h3 className="text-lg font-semibold flex items-center space-x-2">
						<Globe className="w-4 h-4 text-gray-600" />
						<span>Phase 3 - Lower Priority</span>
					</h3>
                 	<div className="grid gap-3 md:grid-cols-2">
                    {patterns.filter(p => p.priority === 'Phase 3').map((pattern) => (
						<div
						key={pattern.id}
						className={`p-4 border rounded-lg transition-colors ${
							selectedPatterns.includes(pattern.id)
							? 'border-primary bg-primary/5'
							: 'border-border'
						} ${pattern.disabled ? 'opacity-50 bg-gray-50 dark:bg-gray-900' : ''}`}
						>
							<div className="flex items-start justify-between mb-2">
								<div className="flex items-center space-x-2">
									{pattern.enabled !== undefined &&
									<Switch
										checked={pattern.enabled}
										onCheckedChange={() => !pattern.disabled && togglePatternEnabled(pattern.id)}
										disabled={pattern.disabled} />}
									<div
										className={`flex-1 ${pattern.disabled ? 'cursor-not-allowed' : 'cursor-pointer'}`}
										onClick={() => !pattern.disabled && togglePattern(pattern.id)}
									>
										<h4 className={`font-medium text-sm ${pattern.disabled ? 'text-gray-400' : ''}`}>
											{pattern.name}
											{pattern.disabled && <span className="ml-2 text-xs">(Coming Soon)</span>}
										</h4>
									</div>
								</div>
								<Badge variant={getPatternRiskColor(pattern.risk_level) as any} className={`text-xs ${pattern.disabled ? 'opacity-50' : ''}`}>
									{pattern.risk_level}
								</Badge>
							</div>
                      		<p className={`text-xs mb-2 ${pattern.disabled ? 'text-gray-400' : 'text-muted-foreground'}`}>{pattern.description}</p>
                      		{pattern.keyIndicators &&
                        	<div className="text-xs">
								<strong className={pattern.disabled ? 'text-gray-400' : ''}>Key Indicators:</strong>
								<ul className="list-disc list-inside mt-1 space-y-0.5">
									{pattern.keyIndicators.slice(0, 2).map((indicator, index) => (
									<li key={index} className={pattern.disabled ? 'text-gray-400' : 'text-muted-foreground'}>{indicator}</li>
									))}
								</ul>
							</div>}
                    	</div>
                  	))}
                 	</div>
               	</div>
            </CardContent>
        </Card>
		<Card>
            <CardHeader>
              	<CardTitle className="flex items-center gap-2">
                	<TrendingUp className="h-5 w-5" />
                	Detection Results
              	</CardTitle>
            </CardHeader>
            <CardContent>
              	{patternLoading ? (
					<div className="flex items-center justify-center py-8">
						<div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
					</div>
              	) : (
					patternResults.length > 0 ? (
						<div className="space-y-4">
						{patternResults.map((result, index) => (
							<div key={index} className="p-4 border rounded-lg">
								<div className="flex justify-between items-start mb-2">
									<h3 className="font-medium">{result.pattern_name}</h3>
									<Badge variant="destructive">
										Score: {result.risk_score.toFixed(1)}
									</Badge>
								</div>
								<p className="text-sm text-muted-foreground mb-2">
									Detected {result.detected_entities.length} entities
								</p>
								<p className="text-xs text-muted-foreground">
									{new Date(result.timestamp).toLocaleString()}
								</p>
							</div>
						))}
						</div>
					) : (
						<div className="flex items-center justify-center py-8">
							<p className="text-muted-foreground">
								No results yet. Run a pattern to see results.
							</p>
						</div>
					)
				)}
            </CardContent>
        </Card>
		{patternResults.length > 0 &&
        <Card>
            <CardHeader>
                <CardTitle>Summary</CardTitle>
            </CardHeader>
            <CardContent>
                <div className="grid gap-4 md:grid-cols-4">
                  	<div className="text-center">
                    	<div className="text-2xl font-bold text-destructive">
                      		{patternResults.length}
                    	</div>
                    	<p className="text-sm text-muted-foreground">Patterns Detected</p>
                  	</div>
                  	<div className="text-center">
                    	<div className="text-2xl font-bold">
                      		{patternResults.reduce((sum, r) => sum + r.detected_entities.length, 0)}
                    	</div>
                    	<p className="text-sm text-muted-foreground">Total Entities</p>
                  	</div>
                  	<div className="text-center">
						<div className="text-2xl font-bold text-yellow-600">
							{(patternResults.reduce((sum, r) => sum + r.risk_score, 0) / patternResults.length).toFixed(1)}
						</div>
                    	<p className="text-sm text-muted-foreground">Avg Risk Score</p>
                  	</div>
                  	<div className="text-center">
                    	<div className="text-2xl font-bold text-green-600">
                      		{patternResults.filter(r => r.risk_score > 70).length}
                    	</div>
                    	<p className="text-sm text-muted-foreground">High Risk</p>
                  	</div>
                </div>
            </CardContent>
        </Card>}
        </>
    )
}

export default Patterns