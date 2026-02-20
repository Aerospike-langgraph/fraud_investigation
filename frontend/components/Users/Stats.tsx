'use client'

import Stat from "@/components/Stat"
import useSWR from 'swr'

interface UserStats {
    total_users: number
    total_low_risk: number
    total_med_risk: number
    total_high_risk: number
}

export default function UserStats(){
    const { data, isLoading } = useSWR<UserStats>('/api/users/stats')

    return (
        <>
        <Stat
            title='Total Users'
            subtitle='Total users in system'
            {...!isLoading && data ? { stat: data.total_users } : { loading: true }}
            icon='users' />
        <Stat
            color='destructive'
            title='High Risk'
            subtitle='Total users with a risk score > 70'
            {...!isLoading && data ? { stat: data.total_high_risk } : { loading: true }}
            icon='shield' />
        <Stat
            title='Medium Risk'
            subtitle='Total users with a risk score > 25 & < 70'
            {...!isLoading && data ? { stat: data.total_med_risk } : { loading: true }}
            icon='shield' />
        <Stat
            color='green-600'
            title='Low Risk'
            subtitle='Total users with a risk score > 25'
            {...!isLoading && data ? { stat: data.total_low_risk } : { loading: true }}
            icon="shield" />
        </>
    )
}
