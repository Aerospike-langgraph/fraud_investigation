'use server'

import Main from "@/components/Main"
import { Suspense } from "react"

export default async function Dashboard() {
  	return (
    	<div className="space-y-6">
      		<div className="flex justify-between items-center">
        		<div>
          			<h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          			<p className="text-muted-foreground">Real-time fraud detection overview</p>
        		</div>
      		</div>
			<Suspense fallback={<Main loading />}>
				<Main />
			</Suspense>
    	</div>
  	)
} 