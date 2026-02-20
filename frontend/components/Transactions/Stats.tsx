'use client'

import Stat from "@/components/Stat"
import useSWR from 'swr'

interface TransactionStats {
	total_txns: number
	total_blocked: number
	total_review: number
	total_clean: number
}

export default function TxnStats(){
    const { data, isLoading } = useSWR<TransactionStats>('/api/transactions/stats')

    return (
        <>
        <Stat
            title='Total Transactions'
            subtitle='Total transactions processed'
            {...!isLoading && data ? { stat: data.total_txns } : { loading: true } }
            icon='credit-card' />
        <Stat
            color='destructive'
            title='Blocked'
            subtitle='Total blocked transactions'
            {...!isLoading && data ? { stat: data.total_blocked } : { loading: true } }
            icon='shield' />
        <Stat
            title='Review'
            subtitle='Total transactions needing review'
            {...!isLoading && data ? { stat: data.total_review } : { loading: true } }
            icon='shield' />
        <Stat
            color='green-600'
            title='Clean'
            subtitle='Total transactions without fraud'
            {...!isLoading && data ? { stat: data.total_clean } : { loading: true } }
            icon='shield' />
        </>
    )
}
