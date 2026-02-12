'use server'

import { formatDate } from '@/lib/utils';
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import Details, { type UserSummary } from '@/components/Users/Details';
import Label from '@/components/Label'

const API_BASE_URL = process.env.BASE_URL || "http://localhost:8080/api"

export default async function UserDetailPage({ params }: { params: Promise<{ id: string }>}) {
  	const { id: userId } = await params;
  	const response = await fetch(`${API_BASE_URL}/users/${userId}`, { cache: 'no-store' })
    const { user, risk_level, ...userDetails }: UserSummary = await response.json();
	
	const riskScore = user?.risk_score ?? 0;

  	return (
    	<div className="space-y-6">
			<div className="flex items-center justify-between">
				<div className="flex items-center gap-4">
					<div>
						<h1 className="text-3xl font-bold tracking-tight">{user?.name ?? 'Unknown User'}</h1>
						<p className="text-muted-foreground">User ID: {user?.id ?? userId}</p>
					</div>
				</div>
				<Badge variant={risk_level === 'LOW' ? 'default' : 'destructive'} className="text-lg px-4 py-2">
					{risk_level ?? 'LOW'} Risk ({riskScore.toFixed(1)})
				</Badge>
			</div>
			<div className="grid gap-4 md:grid-cols-4">

			</div>
            <div className="grid gap-4 md:grid-cols-2">
				<Card>
					<CardHeader>
						<Label
							size='2xl'
							className='font-semibold'
							icon="user"
							text='Personal Information' />
					</CardHeader>
					<CardContent className="grid grid-cols-2 gap-4">
						<Label
							size='lg'
							title='Full Name'
							text={user?.name ?? 'N/A'} />
						<Label
							size='lg'
							title='Age'
							text={`${user?.age ?? 0} years`} />
						<Label
							size='sm'
							title='Email'
							text={user?.email ?? 'N/A'}
							icon='mail' />
						<Label
							size='sm'
							title='Phone'
							text={user?.phone ?? 'N/A'}
							icon='phone' />
						<Label
							size='lg'
							title='Location'
							text={user?.location ?? 'N/A'}
							icon='map-pin' />
						<Label
							size='lg'
							title='Occupation'
							text={user?.occupation ?? 'N/A'}
							icon='building' />
					</CardContent>
				</Card>
				<Card>
					<CardHeader>
						<Label
							size='2xl'
							className='font-semibold'
							icon="shield"
							text='Risk Assessment' />
					</CardHeader>
					<CardContent className="grid grid-cols-1 gap-4">
						<Label
							size='xl'
							title='Risk Score'
							className='font-semibold'
							text={riskScore.toFixed(1)}
							badge={{
								variant: risk_level === 'LOW' ? 'default' : 'destructive',
								text: risk_level ?? 'LOW'
							}} />
						<Label
							size='sm'
							title='Signup Date'
							icon='calendar'
							text={formatDate(user?.signup_date ?? '')} />
						<Label
							title='Account Status'
							icon={user?.is_flagged ? 'alert-triangle' : 'check-circle'}
							color={user?.is_flagged ? 'destructive' : 'green-600'}
							badge={{
								variant: user?.is_flagged ? 'destructive' : 'default',
								text: user?.is_flagged ? 'Flagged' : 'Active'
							}} />
					</CardContent>
				</Card>
			</div>
			<Details userDetails={{user, risk_level, ...userDetails}} />
		</div>
  	)
} 