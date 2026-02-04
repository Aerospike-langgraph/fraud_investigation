'use server'

import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import Label from '@/components/Label'
import Stat from '@/components/Stat'

export default async function TxnLoadingPage() {
	return (
    	<div className="space-y-6">
      		<div className="flex items-center justify-between">
				<div>
					<h1 className="text-3xl font-bold tracking-tight">Transaction Details</h1>
					<Skeleton className='w-[350px] h-[16px] mt-1 mb-1 rounded' />
				</div>
				<div className="flex items-center gap-2">
					<Skeleton className='w-[120px] h-[30px] rounded-full' />
				</div>
      		</div>
      		<div className="grid gap-4 md:grid-cols-4">
				<Stat title='Amount' loading />
				<Stat title='Status' label loading />
        		<Stat title='Type' label loading/>
        		<Stat title='Fraud Rules' icon='shield' loading/>
      		</div>
			<div className="grid gap-4 md:grid-cols-2">
            	<Card>
              		<CardHeader>
						<Label
							size='2xl'
							text='Transaction Information'
							className='font-semibold'
							icon='activity' />
              		</CardHeader>
              		<CardContent className="grid grid-cols-2 gap-4">
						<Label title='Transaction ID' loading />
						<Label size='lg' title='Amount' loading />
						<Label size='lg' title='Method' loading />
						<Label size='lg' title="Type" loading hasIcon />
						<Label size='lg' title='Status' loading hasIcon />
						<Label title='Date & Time' icon='calendar' loading />
              		</CardContent>
            	</Card>
            	<Card>
              		<CardHeader>
						<Label
							size='2xl'
							text='Risk Assessment'
							className='font-semibold'
							icon='shield' />
              		</CardHeader>
              		<CardContent className='flex flex-col gap-4'>
						<Label size='xl' title='Fraud Score' loading />
						<Label title='Fraud Status' loading hasIcon hasBadge />
              		</CardContent>
            	</Card>
          	</div>
    	</div>
  	)
} 
