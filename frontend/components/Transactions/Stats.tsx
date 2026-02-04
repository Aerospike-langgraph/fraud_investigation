import Stat from "@/components/Stat"

interface TransactionStats {
	total_txns: number
	total_blocked: number
	total_review: number
	total_clean: number
}

interface Props {
    loading?: boolean
}

const API_BASE_URL = process.env.BASE_URL || "http://localhost:8080/api"

export default async function TxnStats({ loading }: Props){
    const loadStats = async () => {
        const response = await fetch(`${API_BASE_URL}/transactions/stats`, { cache: 'no-store' })
        const data: TransactionStats = await response.json()
        return data;
    }
    
    const data: TransactionStats | null = loading ? null : await loadStats()

    return (
        <>
        <Stat
            title='Total Transactions'
            subtitle='Total transactions processed'
            {...data ? { stat: data.total_txns } : { loading: true } }
            icon='credit-card' />
        <Stat
            color='destructive'
            title='Blocked'
            subtitle='Total blocked transactions'
            {...data ? { stat: data.total_blocked } : { loading: true } }
            icon='shield' />
        <Stat
            title='Review'
            subtitle='Total transactions needing review'
            {...data ? { stat: data.total_review } : { loading: true } }
            icon='shield' />
        <Stat
            color='green-600'
            title='Clean'
            subtitle='Total transactions without fraud'
            {...data ? { stat: data.total_clean } : { loading: true } }
            icon='shield' />
        </>
    )
}