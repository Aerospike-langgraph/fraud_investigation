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
	IN: { "1" : string }
	OUT: { "1" : string }
	amount: number
	currency: string
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
	sender_id: string
	receiver_id: string
    details?: string[]
}

export interface TransactionDetail {
    txn: Transaction
    other_party: User
}

interface Props {
    txns: TransactionDetail[]
    accounts: Account[]
    name: string
}

const Transactions = ({ txns, accounts, name }: Props) => {
    const getTxnDirection = (txn: any) => {
        const senderAccountId = txn["IN"]["1"]
        const isSender = accounts.some(acc => acc["1"] === senderAccountId)
    
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
                        const sender = direction === 'sent' ? name : other_party.name
                        const receiver = direction === 'sent' ? other_party.name : name
                        const isFraud = txn.fraud_score > 0
                        
                        return (
                            <Card key={txn[1]} className={`p-4 ${isFraud ? 'border-red-200 bg-red-50' : ''}`}>
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
                                                    {txn.txn_id.substring(0, 8)}
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
                                                <Link href={`/users/${other_party[1]}`} className="font-semibold">{sender}</Link>
                                            )}
                                            <p className="text-xs text-gray-500 font-mono">{txn.IN[1]}</p>
                                        </div>
                                        <div className="flex items-center">
                                            <div className="w-8 h-8 rounded-full bg-blue-100 flex items-center justify-center">
                                                <span className="text-blue-600">→</span>
                                            </div>
                                        </div>
                                        <div className="flex-1 text-right">
                                            <p className="text-sm font-medium text-gray-600">To</p>
                                            {direction === 'sent' ?(
                                                <Link href={`/users/${other_party[1]}`} className="font-semibold">{receiver}</Link>
                                            ): (
                                                <p className="font-semibold">{receiver}</p>
                                            )}
                                            <p className="text-xs text-gray-500 font-mono">{txn.OUT[1]}</p>
                                        </div>
                                    </div>
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <Badge 
                                                variant={txn.status === 'completed' ? 'default' : 'secondary'}
                                                className="text-xs"
                                            >
                                                {txn.status === 'completed' ? (
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
                                        <p className="text-xs text-muted-foreground font-mono" title={txn[1]}>
                                            Full ID: {txn.txn_id}
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