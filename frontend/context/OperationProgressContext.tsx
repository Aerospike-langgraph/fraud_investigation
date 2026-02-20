'use client'

import React, { createContext, useContext, useState, useRef, useCallback, useEffect } from 'react'

// ── Types ──────────────────────────────────────────────────────────────
export interface OperationProgress {
    current: number
    total: number
    percentage: number
    message: string
    estimated_remaining_seconds: number | null
}

export interface OperationState {
    /** Whether this operation is currently running */
    running: boolean
    /** Live progress data (null when idle or cleared) */
    progress: OperationProgress | null
    /** Result of the last completed run */
    result: any
    /** Error message if failed */
    error: string | null
}

const EMPTY_STATE: OperationState = {
    running: false,
    progress: null,
    result: null,
    error: null,
}

/** All operation keys used across the admin panel */
export type OperationKey =
    | 'bulk_load'
    | 'inject_transactions'
    | 'compute_features'
    | 'ml_detection'

const ALL_KEYS: OperationKey[] = ['bulk_load', 'inject_transactions', 'compute_features', 'ml_detection']

const STORAGE_KEY = 'fraud_ops_state'

interface OperationProgressContextValue {
    /** Get the state for a specific operation */
    get: (key: OperationKey) => OperationState

    /** Mark an operation as started (running=true, clear previous result/error) */
    start: (key: OperationKey, initialProgress?: OperationProgress) => void

    /** Update the live progress of a running operation */
    setProgress: (key: OperationKey, progress: OperationProgress | null) => void

    /** Mark an operation as successfully completed */
    complete: (key: OperationKey, result?: any) => void

    /** Mark an operation as failed */
    fail: (key: OperationKey, error: string) => void

    /** Start polling /api/operation-progress/{operationId} and pipe into progress */
    startPolling: (key: OperationKey, operationId: string, intervalMs?: number) => void

    /** Stop polling for an operation */
    stopPolling: (key: OperationKey) => void
}

const OperationProgressContext = createContext<OperationProgressContextValue | null>(null)

// ── Helpers ─────────────────────────────────────────────────────────────

function defaultOps(): Record<OperationKey, OperationState> {
    return {
        bulk_load: { ...EMPTY_STATE },
        inject_transactions: { ...EMPTY_STATE },
        compute_features: { ...EMPTY_STATE },
        ml_detection: { ...EMPTY_STATE },
    }
}

/** Read persisted state from sessionStorage (browser only) */
function loadFromSession(): Record<OperationKey, OperationState> {
    if (typeof window === 'undefined') return defaultOps()
    try {
        const raw = sessionStorage.getItem(STORAGE_KEY)
        if (!raw) return defaultOps()
        const parsed = JSON.parse(raw) as Record<OperationKey, OperationState>

        // If an operation was "running" when the page was refreshed,
        // mark it as failed since the polling was lost
        const restored = defaultOps()
        for (const key of ALL_KEYS) {
            if (parsed[key]) {
                restored[key] = {
                    ...parsed[key],
                    // A reload kills the in-flight request — mark stale "running" as failed
                    running: false,
                    ...(parsed[key].running
                        ? { error: 'Page was reloaded while this operation was running. Check the backend for status.' }
                        : {}),
                }
            }
        }
        return restored
    } catch {
        return defaultOps()
    }
}

/** Write state to sessionStorage */
function saveToSession(ops: Record<OperationKey, OperationState>) {
    if (typeof window === 'undefined') return
    try {
        sessionStorage.setItem(STORAGE_KEY, JSON.stringify(ops))
    } catch {
        // Storage full or unavailable — ignore silently
    }
}

// ── Provider ───────────────────────────────────────────────────────────
export function OperationProgressProvider({ children }: { children: React.ReactNode }) {
    const [ops, setOps] = useState<Record<OperationKey, OperationState>>(defaultOps)

    // On first client-side mount, restore from sessionStorage
    const hydrated = useRef(false)
    useEffect(() => {
        if (!hydrated.current) {
            hydrated.current = true
            const restored = loadFromSession()
            setOps(restored)
        }
    }, [])

    // Persist to sessionStorage on every state change (after hydration)
    useEffect(() => {
        if (hydrated.current) {
            saveToSession(ops)
        }
    }, [ops])

    // Keep polling interval refs stable across renders
    const pollingRefs = useRef<Record<string, NodeJS.Timeout | null>>({})

    const get = useCallback((key: OperationKey): OperationState => {
        return ops[key] ?? { ...EMPTY_STATE }
    }, [ops])

    const start = useCallback((key: OperationKey, initialProgress?: OperationProgress) => {
        setOps(prev => ({
            ...prev,
            [key]: {
                running: true,
                progress: initialProgress ?? null,
                result: null,
                error: null,
            },
        }))
    }, [])

    const setProgress = useCallback((key: OperationKey, progress: OperationProgress | null) => {
        setOps(prev => ({
            ...prev,
            [key]: { ...prev[key], progress },
        }))
    }, [])

    const complete = useCallback((key: OperationKey, result?: any) => {
        // Stop any active polling
        if (pollingRefs.current[key]) {
            clearInterval(pollingRefs.current[key]!)
            pollingRefs.current[key] = null
        }
        setOps(prev => ({
            ...prev,
            [key]: {
                running: false,
                progress: prev[key]?.progress
                    ? { ...prev[key].progress!, percentage: 100, message: 'Complete!' }
                    : null,
                result: result ?? prev[key]?.result,
                error: null,
            },
        }))
    }, [])

    const fail = useCallback((key: OperationKey, error: string) => {
        if (pollingRefs.current[key]) {
            clearInterval(pollingRefs.current[key]!)
            pollingRefs.current[key] = null
        }
        setOps(prev => ({
            ...prev,
            [key]: {
                running: false,
                progress: null,
                result: null,
                error,
            },
        }))
    }, [])

    const startPolling = useCallback((key: OperationKey, operationId: string, intervalMs = 500) => {
        // Clear any existing polling for this key
        if (pollingRefs.current[key]) {
            clearInterval(pollingRefs.current[key]!)
        }

        const id = setInterval(async () => {
            try {
                const response = await fetch(`/api/operation-progress/${operationId}`)
                if (response.ok) {
                    const data = await response.json()
                    if (data.found) {
                        setOps(prev => ({
                            ...prev,
                            [key]: {
                                ...prev[key],
                                progress: {
                                    current: data.current,
                                    total: data.total,
                                    percentage: data.percentage,
                                    message: data.message,
                                    estimated_remaining_seconds: data.estimated_remaining_seconds,
                                },
                            },
                        }))
                    }
                }
            } catch (err) {
                console.error(`Failed to poll progress for ${key}:`, err)
            }
        }, intervalMs)

        pollingRefs.current[key] = id
    }, [])

    const stopPolling = useCallback((key: OperationKey) => {
        if (pollingRefs.current[key]) {
            clearInterval(pollingRefs.current[key]!)
            pollingRefs.current[key] = null
        }
    }, [])

    return (
        <OperationProgressContext.Provider
            value={{ get, start, setProgress, complete, fail, startPolling, stopPolling }}
        >
            {children}
        </OperationProgressContext.Provider>
    )
}

// ── Hook ───────────────────────────────────────────────────────────────
export function useOperationProgress() {
    const ctx = useContext(OperationProgressContext)
    if (!ctx) throw new Error('useOperationProgress must be used within OperationProgressProvider')
    return ctx
}
