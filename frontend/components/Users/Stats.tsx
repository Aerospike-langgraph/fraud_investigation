import Stat from "@/components/Stat"

interface UserStats {
    total_users: number
    total_low_risk: number
    total_med_risk: number
    total_high_risk: number
}

interface Props {
    loading?: boolean
}

const API_BASE_URL = process.env.BASE_URL || "http://localhost:8080/api"

export default async function UserStats({ loading }: Props){
    const loadStats = async () => {
        const response = await fetch(`${API_BASE_URL}/users/stats`, { cache: 'no-store' })
        const data: UserStats = await response.json()
        return data
    }
    
    const data: UserStats | null = loading ? null : await loadStats()

    return (
        <>
        <Stat
            title='Total Users'
            subtitle='Total users in system'
            {...data ? { stat: data.total_users } : { loading: true }}
            icon='users' />
        <Stat
            color='destructive'
            title='High Risk'
            subtitle='Total users with a risk score > 70'
            {...data ? { stat: data.total_high_risk } : { loading: true }}
            icon='shield' />
        <Stat
            title='Medium Risk'
            subtitle='Total users with a risk score > 25 & < 70'
            {...data ? { stat: data.total_med_risk } : { loading: true }}
            icon='shield' />
        <Stat
            color='green-600'
            title='Low Risk'
            subtitle='Total users with a risk score > 25'
            {...data ? { stat: data.total_low_risk } : { loading: true }}
            icon="shield" />
        </>
    )
}