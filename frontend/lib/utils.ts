import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import type { Color } from "@/components/Stat"
import type { IconName } from "@/components/Icon"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export const getRiskLevel = (score: number) => {
	if (score < 25) return { level: 'Low', color: 'success' }
	if (score < 50) return { level: 'Medium', color: 'warning' }
	if (score < 75) return { level: 'High', color: 'destructive' }
	return { level: 'Critical', color: 'destructive' }
}

export const formatDate = (dateString: string) => {
	return new Date(dateString).toLocaleDateString('en-US', {
		year: 'numeric',
		month: 'long',
		day: 'numeric'
	})
}

export const formatDateTime = (dateString: string) => {
	return new Date(dateString).toLocaleString('en-US', {
		year: 'numeric',
		month: 'short',
		day: 'numeric',
		hour: '2-digit',
		minute: '2-digit'
	})
}

// Locale identifiers used in Data Management (aligned with backend REGIONAL_CURRENCY)
export const LOCALE_CURRENCY: Record<string, string> = {
	american: 'USD',
	indian: 'INR',
	en_GB: 'GBP',
	en_AU: 'AUD',
	zh_CN: 'CNY',
}

const DISPLAY_LOCALE_KEY = 'fraud_app_display_locale'

/** Current display locale for currency (persisted in localStorage). */
export function getDisplayLocale(): string {
	if (typeof window === 'undefined') return 'american'
	const stored = localStorage.getItem(DISPLAY_LOCALE_KEY)
	return stored && stored in LOCALE_CURRENCY ? stored : 'american'
}

/** Set display locale for currency app-wide (e.g. when user selects locale in Data Management). */
export function setDisplayLocale(locale: string): void {
	if (typeof window === 'undefined') return
	if (locale in LOCALE_CURRENCY) localStorage.setItem(DISPLAY_LOCALE_KEY, locale)
}

/** Format amount with currency symbol for a given locale (american → $, indian → ₹, etc.). */
export function formatCurrencyWithLocale(amount: number, locale?: string): string {
	const loc = locale ?? getDisplayLocale()
	const currency = LOCALE_CURRENCY[loc] ?? 'USD'
	return new Intl.NumberFormat(undefined, { style: 'currency', currency }).format(amount)
}

export const formatCurrency = (amount: number, locale?: string) => {
	return formatCurrencyWithLocale(amount, locale)
}

export const getRiskLevelColor = (riskLevel: string) => {
    switch (riskLevel) {
      	case 'High': return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
      	case 'Medium-High': return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
      	case 'Medium': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
      	case 'Low': return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
      	default: return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200'
    }
}

export const getFraudScoreColor = (score: number) => {
	if (score >= 80) return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
	if (score >= 50) return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
	return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
}


export const getPriorityColor = (priority: string) => {
	switch (priority) {
	  case 'Phase 1': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
	  case 'Phase 2': return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200'
	  case 'Phase 3': return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200'
	  default: return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200'
	}
}

export const getPatternRiskColor = (level: string) => {
	switch (level) {
		case 'high': return 'destructive'
		case 'medium': return 'secondary'
		case 'low': return 'outline'
		default: return 'default'
	}
}

export const getDuration = (startTime: string) => {
	const start = new Date(startTime!)
	const now = new Date()
	const diff = now.getTime() - start.getTime()
	const hours = Math.floor(diff / (1000 * 60 * 60))
	const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))
	const seconds = Math.floor((diff % (1000 * 60)) / 1000)

	return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`
}

export const getTransactionTypeIcon = (type: string | undefined): { name: IconName, color: Color } => {
	switch (type?.toLowerCase()) {
		case 'deposit':
			return { name: 'trending-up', color: 'green-600' }
		case 'withdrawal':
			return { name: 'trending-down', color: 'destructive' }
		case 'transfer':
			return { name: 'activity', color: 'blue-600' }
		default:
			return { name: 'activity', color: 'foreground' }
	}
}

export const getStatusIcon = (status: string | undefined): { name: IconName, color: Color } => {
	switch (status?.toLowerCase()) {
		case 'completed':
			return { name: 'check-circle', color: 'green-600' }
		case 'pending':
			return { name: 'clock', color: 'yellow-600' }
		case 'failed':
			return { name: 'x-circle', color: 'destructive' }
		default:
			return { name: 'clock', color: 'foreground' }
	}
}