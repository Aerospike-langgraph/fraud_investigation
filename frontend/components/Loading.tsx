import { Skeleton } from "./ui/skeleton"
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { SearchNoProps } from "./ResultTable/Search"

interface LoadingTableProps {
    pageSize: number
}

export const LoadingTable = ({ pageSize }: LoadingTableProps) => (
    <table className="w-full">
        <thead>
            <tr className="border-b">
                <th className="h-[48px] p-3  flex items-center">
                    <Skeleton className="h-[20px] w-full rounded-full" />                    
                </th>
            </tr>
        </thead>
        <tbody>
            {Array.from({ length: pageSize }).map((_, idx) => (
                <tr key={idx} className="border-b hover:bg-muted/50">
                    <td className="p-3 h-[61px]">
                        <Skeleton className="h-[20px] w-full rounded-full" />
                    </td> 
                </tr>
            ))}
        </tbody>
    </table>
)

interface LoadingStatProps { 
    icon?: boolean, 
    subtitle?: boolean 
}

export const LoadingStat = ({icon, subtitle}: LoadingStatProps ) => {
    return (
        <Card>
            <CardHeader className='p-4 pb-1'>
                <CardTitle>
                    <Skeleton className="h-[16px] w-[100px] rounded mb-1" />
                </CardTitle>
            </CardHeader>
            <CardContent className='p-4 pt-1'>
                <div className={`flex items-center justify-between`}>
                    <div>
                        <Skeleton className="h-[28px] w-[120px] rounded mb-2" />
                        {subtitle && <Skeleton className="h-[12px] w-[220px] rounded" />}
                    </div>
                    {icon && <Skeleton className="h-10 w-10 rounded-full" />}
                </div>
            </CardContent>
        </Card>
    )
}

export const LoadingStats = ({ icon, subtitle}: LoadingStatProps) => {
    return (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, idx) => (
            <LoadingStat icon={icon} subtitle={subtitle} key={idx} />
        ))}
        </div>
    )
}

interface LoadingPageProps {
    title: string, 
    text: string, 
    search: string
}

export const LoadingPage = ({ title, text, search }: LoadingPageProps) => {
    return (
        <div className="space-y-6 flex flex-col grow">
      		<div className="flex items-center justify-between">
        		<div>
          			<h1 className="text-3xl font-bold tracking-tight">{title}</h1>
          			<p className="text-muted-foreground">{text}</p>
        		</div>
      		</div>
			<LoadingStats />
            <Card className='grow flex flex-col'>
                <CardHeader className='gap-4'>
                    <CardTitle>{search}</CardTitle>
                    <SearchNoProps placeholder={`Search ${search}`} />
                </CardHeader>
                <CardContent className='grow'>
                    <div className="overflow-x-auto">
                        <LoadingTable pageSize={10} />
                    </div>
                </CardContent>
                <CardFooter>
                    
                </CardFooter>
            </Card>
		</div>
    )
}