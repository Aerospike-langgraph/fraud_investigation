'use client'

import { SWRConfig } from 'swr'
import { ReactNode } from 'react'

/**
 * Global fetcher for SWR. Handles JSON responses from the API.
 */
export const fetcher = async (url: string) => {
    const res = await fetch(url)
    if (!res.ok) {
        const error = new Error('An error occurred while fetching the data.')
        throw error
    }
    return res.json()
}

/**
 * SWR configuration defaults:
 * - dedupingInterval: 10s — won't refetch the same key within 10s
 * - revalidateOnFocus: false — don't refetch when user switches tabs
 * - revalidateOnReconnect: true — refetch when network reconnects
 * - errorRetryCount: 2 — retry failed requests up to 2 times
 */
export function SWRProvider({ children }: { children: ReactNode }) {
    return (
        <SWRConfig
            value={{
                fetcher,
                dedupingInterval: 10000,
                revalidateOnFocus: false,
                revalidateOnReconnect: true,
                errorRetryCount: 2,
            }}
        >
            {children}
        </SWRConfig>
    )
}
