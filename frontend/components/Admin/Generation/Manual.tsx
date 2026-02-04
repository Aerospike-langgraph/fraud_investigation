'use client'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import Search, { type Account } from './Search'
import { CreditCard, RefreshCw } from 'lucide-react'
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
        <Card className='flex flex-col'>
            <CardHeader>
                <CardTitle className="flex items-center space-x-2">
                    <CreditCard className="w-5 h-5" />
                    <span>Manual Transaction</span>
                </CardTitle>
                <CardDescription>
                    Create a transaction between specific accounts
                </CardDescription>
            </CardHeader>
            <CardContent className="grow flex flex-col">
                <form onSubmit={handleTxn} name="manual-txn" className="space-y-4 grow flex flex-col gap-4">
                    <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">From Account</label>
                            <Search 
                                name="from-acct"
                                accounts={accounts}
                                loading={loading}
                                value={fromAcct}
                                comp={toAcct}
                                setValue={setFromAcct} />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">To Account</label>
                            <Search 
                                name="to-acct"
                                accounts={accounts}
                                loading={loading}
                                value={toAcct}
                                comp={fromAcct}
                                setValue={setToAcct} />
                        </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Amount (USD)</label>
                            <Input
                                required
                                name="amount"
                                type="number"
                                value={amount}
                                onChange={(e) => setAmount(e.target.value)}
                                placeholder="Enter amount"
                                min="0.01"
                                step="0.01"
                                disabled={loading}
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Transaction Type</label>
                            <select
                                required
                                name="type"
                                value={txnType}
                                onChange={(e) => setTxnType(e.target.value as ManualTxn['txnType'])}
                                className="w-full p-2 border rounded-md bg-background"
                                disabled={loading}
                            >
                                <option value="transfer">Transfer</option>
                                <option value="payment">Payment</option>
                                <option value="deposit">Deposit</option>
                                <option value="withdrawal">Withdrawal</option>
                            </select>
                        </div>
                    </div>
                    <Button
                        type='submit'
                        disabled={loading || !fromAcct || !toAcct || !amount}
                        className="w-full"
                        style={{ marginTop: 'auto' }}
                    >
                        {loading ? (
                            <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                        ) : (
                            <CreditCard className="w-4 h-4 mr-2" />
                        )}
                        Create Transaction
                    </Button>
                </form>
              </CardContent>
        </Card>
    )
}

export default Manual