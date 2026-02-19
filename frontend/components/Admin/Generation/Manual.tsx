'use client'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import Search, { type Account } from './Search'
import { CreditCard, Loader2, Send } from 'lucide-react'
import { type FormEvent, useEffect, useState } from 'react'
import { toast } from "sonner"

interface ManualTxn {
    fromAcct: string
    toAcct: string
    amount: string
    txnType: 'transfer' | 'payment' | 'deposit' | 'withdrawl'
}

const Manual = () => {
    const [loading, setLoading] = useState(false)
    const [fromAcct, setFromAcct] = useState<ManualTxn['fromAcct']>("")
    const [toAcct, setToAcct] = useState<ManualTxn['toAcct']>("")
    const [amount, setAmount] = useState<ManualTxn['amount']>("")
    const [txnType, setTxnType] = useState<ManualTxn['txnType']>('transfer')
    const [accounts, setAccounts] = useState<Account[]>([])

    const getAccounts = async () => {
        const response = await fetch('/api/accounts')
        const { accounts } = await response.json()
        setAccounts(accounts)
    }  	
    
    const handleTxn = async (e: FormEvent) => {
        e.preventDefault()
        setLoading(true)
            
        const form = e.target as HTMLFormElement
        const formData = new FormData(form)
        const from = formData.get('from-acct');
        const to = formData.get('to-acct')
        const amnt = formData.get('amount')
        const type = formData.get('type')

        if(from === to) {
            alert("From and To accounts must be different")
            setLoading(false);
            return;
        }

        try {
            const response = await fetch(`/api/transaction-generation/manual?from_account_id=${from}&to_account_id=${to}&amount=${amnt}&transaction_type=${type}`, {
                method: 'POST'
            })
            if(response.ok) {
                toast.success("Transaction created successfully!")
                setFromAcct("")
                setToAcct("")
                setAmount("")
                setTxnType('transfer')
            }
            else throw new Error(`Server returned: ${response.status}`)
        } 
        catch(error) {
            console.error('Failed to create manual transaction:', error)
            toast.error("Failed to create manual transaction")
        } 
        finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        getAccounts()
    }, [])

    return (
        <Card className="overflow-hidden border-0 shadow-sm h-full flex flex-col">
            <CardHeader className="p-4 pb-2">
                <CardTitle className="flex items-center gap-2 text-sm">
                    <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-violet-500/10">
                        <CreditCard className="h-3.5 w-3.5 text-violet-600" />
                    </div>
                    Manual Transaction
                </CardTitle>
                <CardDescription className="text-[11px]">
                    Create a single transaction between two accounts
                </CardDescription>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col p-4 pt-1">
                <form onSubmit={handleTxn} name="manual-txn" className="flex-1 flex flex-col space-y-2.5">
                    <div className="grid grid-cols-2 gap-2">
                        <div className="space-y-1">
                            <label className="text-xs font-medium">From Account</label>
                            <Search 
                                name="from-acct"
                                accounts={accounts}
                                loading={loading}
                                value={fromAcct}
                                comp={toAcct}
                                setValue={setFromAcct} />
                        </div>
                        <div className="space-y-1">
                            <label className="text-xs font-medium">To Account</label>
                            <Search 
                                name="to-acct"
                                accounts={accounts}
                                loading={loading}
                                value={toAcct}
                                comp={fromAcct}
                                setValue={setToAcct} />
                        </div>
                    </div>
                    <div className="grid grid-cols-2 gap-2">
                        <div className="space-y-1">
                            <label className="text-xs font-medium">Amount (USD)</label>
                            <Input
                                required
                                name="amount"
                                type="number"
                                value={amount}
                                onChange={(e) => setAmount(e.target.value)}
                                placeholder="0.00"
                                min="0.01"
                                step="0.01"
                                disabled={loading}
                                className="h-7 text-xs"
                            />
                        </div>
                        <div className="space-y-1">
                            <label className="text-xs font-medium">Type</label>
                            <select
                                required
                                name="type"
                                value={txnType}
                                onChange={(e) => setTxnType(e.target.value as ManualTxn['txnType'])}
                                className="w-full h-7 px-2 text-xs border rounded-md bg-background"
                                disabled={loading}
                            >
                                <option value="transfer">Transfer</option>
                                <option value="payment">Payment</option>
                                <option value="deposit">Deposit</option>
                                <option value="withdrawal">Withdrawal</option>
                            </select>
                        </div>
                    </div>
                    <div className="mt-auto">
                        <Button
                            type='submit'
                            disabled={loading || !fromAcct || !toAcct || !amount}
                            size="sm"
                            className="w-full h-7 text-xs"
                        >
                            {loading ? (
                                <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                            ) : (
                                <Send className="h-3 w-3 mr-1" />
                            )}
                            Create Transaction
                        </Button>
                    </div>
                </form>
            </CardContent>
        </Card>
    )
}

export default Manual
