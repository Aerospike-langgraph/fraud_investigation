'use client'

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useState } from 'react'
import Analysis from './Analysis'
import AccountCard from './AccountCard'
import type { Transaction } from "@/components/Users/Details/Transactions"
import type { Account } from '@/components/Users/Details/Accounts'
import type { User } from '@/components/Users/Details/'

export interface TxnDetail {
    txn: Transaction
    src: {
        account: Account
        user: User
    }
    dest: {
        account: Account
        user: User
    }
}

const TxnDetails = ({ txn, src, dest }: TxnDetail) => {
    const [active, setActive] = useState('accounts')
    
    return (
        <Tabs value={active} onValueChange={setActive} className="space-y-4">
            <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="accounts">Accounts</TabsTrigger>
                <TabsTrigger value="fraud">Fraud Analysis</TabsTrigger>
            </TabsList>
            <TabsContent value="accounts" className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                    <AccountCard {...src} variant='source' />
                    <AccountCard {...dest} variant='destination' />
                </div>
            </TabsContent>
            <TabsContent value="fraud" className="space-y-4">
                <Analysis txn={txn} />
            </TabsContent>
        </Tabs>
    )
}

export default TxnDetails;