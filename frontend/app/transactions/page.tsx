'use server'

import Results, { type Option } from '@/components/ResultTable'
import { Suspense } from 'react'
import TransactionStats from '@/components/Transactions/Stats'

export interface TransactionStats {
	total_txns: number
	total_blocked: number
	total_review: number
	total_clean: number
}

const options: Option[] = [
	{
		name: 'Transaction ID',
		item: 'id',
		width: '250px',
		label: {
			size: 'sm',
			text: 'txn_id',
			icon: 'credit-card'
		}
	},
	{
		name: 'Sender',
		item: 'sender',
		width: '100px',
		label: {
			size: 'sm',
			text: 'sender',
			className: 'font-mono'
		}
	},
	{
		name: 'Receiver',
		item: 'receiver',
		width: '100px',
		label: {
			size: 'sm',
			text: 'receiver',
			className: 'font-mono'
		}
	},
	{
		name: 'Amount',
		item: 'amount',
		width: '125px',
		type: 'currency',
		sortable: true,
		label: {
			text: 'amount'
		}
	},
	{
		name: 'Risk Score',
		item: 'fraud_score',
		width: '125px',
		type: 'risk',
		sortable: true,
		label: {
			badge: {
				text: 'fraud_score'
			}
		}
	},
	{
		name: 'Date',
		item: 'timestamp',
		type: 'datetime',
		width: '225px',
		label: {
			size: 'sm',
			text: 'timestamp',
			icon: 'calendar',
		},
		sortable: true,
		defaultSort: true,
		defaultOrder: 'desc'
	},
	{
		name: 'Location',
		item: 'location',
		width: '225px',
		label: {
			size: 'sm',
			text: 'location',
			icon: 'map-pin',
		},
	},
	{
		name: 'Status',
		item: 'fraud_status',
		width: '125px',
		type: 'fraud',
		label: {
			badge: {
				text: 'fraud_status'
			}
		}
	}
]

const API_BASE_URL = process.env.BASE_URL || "http://localhost:8080/api"

export default async function TransactionsPage() { 
	async function handleSearch(
		page: number = 1,
		size: number = 10,
		orderBy: string = "date",
		order: 'asc' | 'desc' = 'desc', 
		query?: string
	) {
		"use server"
		
		const response = await fetch(`${API_BASE_URL}/transactions?page=${page}&page_size=${size}&order_by=${orderBy}&order=${order}${query ? `&query=${query}` : ''}`, { cache: 'no-store' });
		const search = await response.json()
		return search
	}

	return (
    	<div className="space-y-6 flex flex-col grow">
      		<div className="flex items-center justify-between">
        		<div>
          			<h1 className="text-3xl font-bold tracking-tight">Transaction Explorer</h1>
          			<p className="text-muted-foreground">Search and explore transaction details and patterns</p>
        		</div>
      		</div>
			<div className="grid gap-4 md:grid-cols-4">
				<Suspense fallback={<TransactionStats loading />}>
					<TransactionStats />
				</Suspense>
			</div>
			<Results 
				handleSearch={handleSearch}
				title="Transactions"
				options={options} />
		</div>
  	)
} 