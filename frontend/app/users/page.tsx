'use server'

import Results, { type Option } from '@/components/ResultTable'
import UserStats from '@/components/Users/Stats'
import { Suspense } from 'react'

const options: Option[] = [
	{
		name: "Name",
		item: 'name',
		width: "275px",
		sortable: true,
		defaultSort: true,
		label: {
			size: 'md',
			text: 'name',
			icon: 'user'
		}
	},
	{
		name: "ID",
		item: 'id',
		width: "75px",
		label: {
			subtitle: 'id'
		}
	},
	{
		name: "Email",
		item: 'email',
		width: "300px",
		label: {
			size: 'sm',
			text: 'email',
			icon: 'mail',
			className: 'lowercase'
		}
	},
	{
		name: "Location",
		item: 'location',
		width: "175px",
		label: {
			size: 'sm',
			text: 'location',
			icon: 'map-pin'
		}
	},
	{
		name: "Age",
		item: 'age',
		width: "100px",
		label: {
			size: 'sm',
			text: 'age'
		}
	},
	{
		name: "Risk Score",
		item: 'risk_score',
		width: "150px",
		type: 'risk',
		sortable: true,
		label: {
			badge: {
				text: 'risk_score',
			}
		}
	},
	{
		name: "Signup Date",
		item: 'signup_date',
		type: 'date',
		width: "200px",
		sortable: true,
		label: {
			size: 'sm',
			text: 'signup_date',
			icon: 'calendar'
		}
	}
]

const API_BASE_URL = process.env.BASE_URL || "http://localhost:8080/api"

export default async function UsersPage() {
	async function handleSearch(
		page: number = 1,
		size: number = 10,
		orderBy: string = "date",
		order: 'asc' | 'desc' = 'desc', 
		query?: string
	) {
		"use server"
		
		const response = await fetch(`${API_BASE_URL}/users?page=${page}&page_size=${size}&order_by=${orderBy}&order=${order}${query ? `&query=${query}` : ''}`, { cache: 'no-store' });
		const search = await response.json()
		return search
	}
	
  	return (
    	<div className="space-y-6 flex flex-col grow">
			<div>
				<h1 className="text-3xl font-bold tracking-tight">User Explorer</h1>
				<p className="text-muted-foreground">Browse and search user profiles with detailed information</p>
			</div>
			<div className="grid gap-4 md:grid-cols-4">
				<Suspense fallback={<UserStats loading />}>
					<UserStats />
				</Suspense>
			</div>
			<Results 
				handleSearch={handleSearch}
				title='Users'
				options={options} />
    	</div>
  	)
}