'use client'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { SearchIcon } from 'lucide-react'
import { useRef, useState } from 'react'

interface Props {
    fetchData: (q: string) => void
    placeholder: string
    setCurrentPage: () => void
}

export const SearchNoProps = ({ placeholder }: { placeholder: string }) => <Search fetchData={() => {}} placeholder={placeholder} setCurrentPage={() => {}} />

const Search = ({
    fetchData,
    placeholder = "Search",
    setCurrentPage
}: Props ) => {
    const [query, setQuery] = useState("");
    const debounce = useRef<NodeJS.Timeout | null>(null)
    
    const handleChange = (q: string) => {
        setQuery(q);
        setCurrentPage();
        if(debounce.current) clearTimeout(debounce.current);
        debounce.current = setTimeout(() => fetchData(q), 300)
    }

    return (
        <div className="flex gap-2 items-center">
            <Input
                onChange={(e) => handleChange(e.currentTarget.value)}
                value={query}
                autoComplete="off"
                data-1p-ignore 
                data-bwignore
                data-lpignore="true" 
                data-form-type="other"
                name='search'
                type='search'
                placeholder={placeholder}
                className="flex-1" />
            <Button onClick={() => fetchData(query)}>
                <SearchIcon className='w-4 h-4' />
            </Button>
        </div>
    )
}

export default Search;