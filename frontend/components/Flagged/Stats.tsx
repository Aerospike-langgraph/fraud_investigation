'use client'

import Stat from '@/components/Stat'

interface Props {
    loading?: boolean
    stats?: {
        pending_review: number
        under_investigation: number
        confirmed_fraud: number
        cleared: number
    }
}

const FlaggedStats = ({ loading = false, stats }: Props) => {
    // Mock data for UI design
    const mockStats = stats || {
        pending_review: 847,
        under_investigation: 234,
        confirmed_fraud: 156,
        cleared: 1728
    }

    return (
        <>
            <Stat
                title="Pending Review"
                stat={mockStats.pending_review}
                subtitle="Accounts awaiting analyst review"
                color="destructive"
                icon="alert-triangle"
                loading={loading}
            />
            <Stat
                title="Under Investigation"
                stat={mockStats.under_investigation}
                subtitle="Currently being investigated"
                color="warning"
                icon="shield"
                loading={loading}
            />
            <Stat
                title="Confirmed Fraud"
                stat={mockStats.confirmed_fraud}
                subtitle="Marked as fraudulent"
                color="destructive"
                icon="x-circle"
                loading={loading}
            />
            <Stat
                title="Cleared"
                stat={mockStats.cleared}
                subtitle="Verified as legitimate"
                color="green-600"
                icon="check-circle"
                loading={loading}
            />
        </>
    )
}

export default FlaggedStats
