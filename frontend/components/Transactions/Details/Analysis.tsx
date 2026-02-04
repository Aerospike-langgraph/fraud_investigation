'use client'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { AlertTriangle, Clock, Flag, Shield, Target } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { formatDateTime, getRiskLevel } from "@/lib/utils";
import { type Transaction } from '@/components/Users/Details/Transactions';

export interface FraudResult {
    fraud_score: number
    status: string
    rule: string
    timestamp: string
    reason: string
    details: string
    is_fraud: boolean
}

interface Detail {
    fraud_score: number
    reason: string
    rule: string
    detection_time: string
}

interface Props {
    txn: Transaction
}

const Analysis = ({ txn }: Props) => {
    const { is_fraud, details = [] } = txn
    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <Shield className="h-5 w-5" />
                    Fraud Detection Results ({details.length})
                </CardTitle>
                <CardDescription>
                    Detailed analysis of fraud detection rules and their results
                </CardDescription>
            </CardHeader>
            <CardContent>
                {is_fraud ? (
                    <div className="space-y-4">
                        {details.map((detail, idx) => {
                            const { reason, fraud_score, detection_time = txn.timestamp, rule, ...rest }: Detail = JSON.parse(detail ?? "{}")
                            return (
                            <Card key={idx} className="p-4 border-red-200 bg-red-50">
                            <div className="flex items-start justify-between">
                                <div className="flex-1 space-y-2">
                                    <div className="flex items-center gap-2">
                                        <h4 className="font-semibold">{rule}</h4>
                                        <Badge variant="destructive" className="text-xs">
                                            <AlertTriangle className="h-3 w-3 mr-1" />
                                            Triggered
                                        </Badge>
                                    </div>
                                    <p className="text-sm text-muted-foreground">{reason}</p>
                                    <div className="flex items-center gap-4 text-sm">
                                        <div className="flex items-center gap-1">
                                            <Target className="h-4 w-4 text-muted-foreground" />
                                            <span className="text-muted-foreground">Risk Score:</span>
                                            <span className="font-medium">{fraud_score}/100</span>
                                        </div>
                                        <div className="flex items-center gap-1">
                                            <Clock className="h-4 w-4 text-muted-foreground" />
                                            <span className="text-muted-foreground">Detected:</span>
                                            <span className="font-medium">{formatDateTime(detection_time)}</span>
                                        </div>
                                        <div className="flex items-center gap-1">
                                            <Flag className="h-4 w-4 text-muted-foreground" />
                                            <span className="text-muted-foreground">Status:</span>
                                            <span className="font-medium capitalize">{txn.status}</span>
                                        </div>
                                    </div>
                                    {rest && (
                                    <div className="mt-3 p-3 bg-gray-100 rounded-lg">
                                        <div className="text-sm text-gray-600">
                                            <pre className="whitespace-pre-wrap">{JSON.stringify(rest, null, 2)}</pre>
                                        </div>
                                    </div>)}
                                </div>
                                <div className="ml-4">
                                    <Badge 
                                        variant={fraud_score >= 75 ? 'destructive' : 'secondary'}
                                        className="text-xs"
                                    >
                                        {getRiskLevel(fraud_score).level} Risk
                                    </Badge>
                                </div>
                            </div>
                        </Card>
                        )})}
                    </div>
                ) : (
                    <div className="text-center py-8">
                        <Shield className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                        <h3 className="text-lg font-medium text-muted-foreground mb-2">
                            No fraud detection results available.
                        </h3>
                        <p className="text-sm text-muted-foreground">
                            Fraud detection analysis has not been performed on this transaction.
                        </p>
                    </div>
                )}
            </CardContent>
        </Card>
    )
}

export default Analysis;