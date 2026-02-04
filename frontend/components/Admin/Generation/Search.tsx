import { Input } from '@/components/ui/input'
import { type ChangeEvent, useEffect, useState, type Dispatch, type SetStateAction, useRef } from 'react'

export interface Account {
    account_id: string
    account_type: string
}

interface Props {
    name: string
    accounts: Account[]
    loading: boolean
    value: string
    comp: string
    setValue: Dispatch<SetStateAction<string>>
}

const Search = ({
    name,
    accounts = [],
    loading,
    value,
    comp,
    setValue
}: Props) => {
    const [filteredAccts, setFilteredAccts] = useState<Account[]>(accounts)
    const [search, setSearch] = useState("")
    const [show, setShow] = useState(false)
    const [error, setError] = useState(false)
    const wrapper = useRef<HTMLDivElement | null>(null)

    const handleChange = (e: ChangeEvent) => {
        const val = (e.currentTarget as HTMLInputElement).value ?? ""
        setFilteredAccts(accounts.filter(item => item.account_id.startsWith(val.toUpperCase())))
        setSearch(val)
        setShow(true)
    }

    const handleSelect = (account: { id: string, type: string }) => {
        setError(false);
        if(comp !== "" && comp === account.id) {
            setError(true)
        }
        else {
            setValue(account.id)
            setSearch(`${account.id} (${account.type})`)
        }
        setShow(false)
        setFilteredAccts(accounts)
    }

    const handleClear = () => {
        setSearch("")
        setValue("")
        setError(false)
        setShow(false)
        setFilteredAccts(accounts)
    }

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as Element
            if (wrapper.current && !wrapper.current.contains(target)) {
                setShow(false)
            }   
        }
        document.addEventListener('mousedown', handleClickOutside)
        return () => document.removeEventListener('mousedown', handleClickOutside)
    }, [])

    useEffect(() => {
        if(value === "") setSearch("")
        if(comp === "") setError(false)
    }, [value, comp])

    useEffect(() => {
        setFilteredAccts(accounts)
    }, [accounts])
    
    return (
        <div className="relative account-dropdown-container" ref={wrapper}>
            <input name={name} type="hidden" value={value} />
            <Input
                required
                type="text"
                placeholder="Search by account ID"
                value={search}
                onChange={handleChange}
                onFocus={() => setShow(true)}
                className="w-full"
                disabled={loading}
            />
            {search && 
            <button
                type="button"
                onClick={handleClear}
                className="absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600"
            >
                ✕
            </button>}
            {show &&
            <div className="absolute z-50 w-full mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-60 overflow-y-auto">
                {filteredAccts.length > 0 ? (
                    filteredAccts.slice(0, 10).map(({ account_id, account_type }) => (
                        <div
                            key={account_id}
                            className="px-3 py-2 hover:bg-gray-100 cursor-pointer text-sm"
                            onClick={() => handleSelect({ id: account_id, type: account_type })}
                        >
                            <div className="font-medium">{account_id}</div>
                            <div className="text-gray-500 text-xs">{account_type}</div>
                        </div>
                    ))
                ) : (
                    <div className="px-3 py-2 text-gray-500 text-sm">No accounts found</div>
                )}
                {filteredAccts.length > 10 &&
                <div className="px-3 py-2 text-gray-500 text-xs border-t">
                    Showing first 10 results. Type more to narrow down.
                </div>}
            </div>}
            {value && 
            <div className="text-xs text-green-600 bg-green-50 p-2 rounded absolute w-full">
                ✓ {search}
            </div>}
            {error && 
            <div className="text-xs text-red-600 bg-red-50 p-2 rounded absolute w-full">
               ✕ Accounts must be different
            </div>}
        </div>
    )
}

export default Search