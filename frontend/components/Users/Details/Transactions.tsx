'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import Link from 'next/link'
import { Activity, AlertTriangle, Clock, TrendingDown, TrendingUp, CheckCircle, MapPin } from 'lucide-react'
import { formatCurrency, formatDateTime } from '@/lib/utils'
import { Account } from './Accounts'
import { User } from './'

export interface Transaction {
	"1"?: string
	id?: string
	txn_id: string
	IN?: { "1" : string }
	OUT?: { "1" : string }
	amount: number
	currency?: string
	timestamp: string
	status: string
	fraud_score: number
	fraud_status?: string
	reason?: string
	type?: string
	method?: string
	location?: string
	is_fraud?: boolean
	fraud_type?: string
	sender_id?: string
	receiver_id?: string
    details?: string[]
}

export interface TransactionDetail {
    txn: Transaction
    other_party: User & { id?: string }
}

interface Props {
    txns: TransactionDetail[]
    accounts: Account[]
    name: string
}

// Helper to get ID from various formats (Graph DB uses "1", KV uses "id")
const getId = (obj: any): string => obj?.id || obj?.['1'] || ''

const Transactions = ({ txns, accounts, name }: Props) => {
    const getTxnDirection = (txn: any) => {
        // Handle both old format (IN/OUT from Graph) and new format (from KV)
        const senderAccountId = txn?.IN?.['1'] || txn?.sender_id || ''
        const isSender = accounts.some(acc => getId(acc) === senderAccountId)
    
        // If we can't determine from IN, assume outgoing (backend filters for direction='out')
        if (!senderAccountId) return 'sent'
        
        if (isSender) return 'sent'
        else return 'received'
    }
    
    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <Activity className="h-5 w-5" />
                    Recent Transactions ({txns?.length})
                </CardTitle>
            </CardHeader>
            <CardContent>
                {txns?.length > 0 ? (
                <div className="space-y-4">
                    {txns.map(({txn, other_party}) => {
                        const direction = getTxnDirection(txn)
                        const displayAmount = direction === 'sent' ? -Math.abs(txn.amount) : Math.abs(txn.amount)
                        const otherPartyName = other_party?.name || 'Unknown'
                        const otherPartyId = getId(other_party)
                        const sender = direction === 'sent' ? name : otherPartyName
                        const receiver = direction === 'sent' ? otherPartyName : name
                        const isFraud = (txn.fraud_score || 0) > 0
                        const txnId = txn.txn_id || getId(txn)
                        
                        return (
                            <Card key={txnId} className={`p-4 ${isFraud ? 'border-red-200 bg-red-50' : ''}`}>
                                <div className="space-y-3">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <div className="flex items-center gap-2">
                                                <p className="font-semibold">
                                                    {txn.type ? 
                                                    `${txn.type.charAt(0).toUpperCase() + txn.type.slice(1)} Transaction` : 
                                                    'Transfer Transaction'}
                                                </p>
                                                <Badge variant="secondary" className="text-xs font-mono">
                                                    {txnId.substring(0, 8)}
                                                </Badge>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <p className={`text-lg font-bold ${displayAmount < 0 ? 'text-red-600' : 'text-green-600'}`}>
                                                {displayAmount < 0 ? '-' : '+'}{formatCurrency(Math.abs(displayAmount))}
                                            </p>
                                            {displayAmount < 0 ? (
                                                <TrendingDown className="h-4 w-4 text-red-600" />
                                            ) : (
                                                <TrendingUp className="h-4 w-4 text-green-600" />
                                            )}
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg">
                                        <div className="flex-1">
                                            <p className="text-sm font-medium text-gray-600">From</p>
                                            {direction === 'sent' ? (
                                                <p className="font-semibold">{sender}</p>
                                            ): (
                                                otherPartyId ? (
                                                    <Link href={`/users/${otherPartyId}`} className="font-semibold text-blue-600 hover:underline">{sender}</Link>
                                                ) : (
                                                    <p className="font-semibold">{sender}</p>
                                                )
                                            )}
                                        </div>
                                        <div className="flex items-center">
                                            <div className="w-8 h-8 rounded-full bg-blue-100 flex items-center justify-center">
                                                <span className="text-blue-600">→</span>
                                            </div>
                                        </div>
                                        <div className="flex-1 text-right">
                                            <p className="text-sm font-medium text-gray-600">To</p>
                                            {direction === 'sent' ? (
                                                otherPartyId ? (
                                                    <Link href={`/users/${otherPartyId}`} className="font-semibold text-blue-600 hover:underline">{receiver}</Link>
                                                ) : (
                                                    <p className="font-semibold">{receiver}</p>
                                                )
                                            ): (
                                                <p className="font-semibold">{receiver}</p>
                                            )}
                                        </div>
                                    </div>
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <Badge 
                                                variant={txn.status === 'completed' || txn.status === 'clean' ? 'default' : 'secondary'}
                                                className="text-xs"
                                            >
                                                {txn.status === 'completed' || txn.status === 'clean' ? (
                                                    <CheckCircle className="h-3 w-3 mr-1" />
                                                ) : (
                                                    <Clock className="h-3 w-3 mr-1" />
                                                )}
                                                {txn.status}
                                            </Badge>
                                            {txn.location && (
                                                <div className="flex items-center gap-1">
                                                    <MapPin className="h-3 w-3 text-gray-400" />
                                                    <span className="text-xs text-gray-500">{txn.location}</span>
                                                </div>
                                            )}
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <p className="text-xs text-muted-foreground">
                                                {formatDateTime(txn.timestamp)}
                                            </p>
                                        </div>
                                    </div>
                                    {isFraud && (
                                        <div className="border-t pt-3 mt-3">
                                            <div className="flex items-center justify-between">
                                                <div className="flex items-center gap-2">
                                                    <Badge variant="destructive" className="text-xs">
                                                        <AlertTriangle className="h-3 w-3 mr-1" />
                                                        FRAUD DETECTED
                                                    </Badge>
                                                </div>
                                                <Badge variant="secondary" className="text-xs">
                                                    Risk Score: {(txn.fraud_score || 0).toFixed(1)}
                                                </Badge>
                                            </div>
                                        </div>
                                    )}
                                    <div className="border-t pt-2">
                                        <p className="text-xs text-muted-foreground font-mono" title={txnId}>
                                            Full ID: {txnId}
                                        </p>
                                    </div>
                                </div>
                            </Card>
                        )}
                    )}
                </div>
            ) : (
                <div className="text-center py-8">
                    <Activity className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <p className="text-muted-foreground">No transactions found for this user.</p>
                </div>
            )}
            </CardContent>
        </Card>
    )
}

export default Transactions