import { Card, CardContent, CardFooter, CardHeader } from '@/components/ui/card'
import Link from 'next/link'
import { Button } from '@/components/ui/button'
import { ExternalLink } from 'lucide-react'
import { formatCurrency, formatDate } from '@/lib/utils'
import Label from '@/components/Label'
import { type User } from '@/components/Users/Details'
import { type Account } from '@/components/Users/Details/Accounts'

interface Props {
    variant: 'source' | 'destination'
    account: Account
    user: User
}

const AccountCard = ({ 
    variant,
    account,
	user
 }: Props) => {
    const isSource = variant === 'source';
    const { balance, created_date, type } = account
    const { email, name, location } = user

    return (
        <Card>
            <CardHeader>
                <Label
                    size='2xl'
                    className='font-semibold'
                    icon={isSource ? 'trending-down' : 'trending-up'}
                    color={isSource ? 'destructive' : 'green-600'}
                    text={`${isSource ? 'Source' : 'Destination'} Account`} />
            </CardHeader>
            <CardContent className="grid grid-cols-2 gap-4">
                <Label
                    size='lg'
                    title='Account ID'
                    text={account[1]} />
                <Label
                    size='lg'
                    title='Account Type'
                    text={type} />
                <Label
                    size='lg'
                    title='Balance'
                    className={`font-semibold ${balance < 0 ? 'text-destructive' : 'text-green-600'}`}
                    text={formatCurrency(balance)} />
                <Label
                    size='lg'
                    title='Owner'
                    subtitle={email}
                    icon='user'
                    text={name} />
                <Label
                    size='sm'
                    title='Created'
                    icon='calendar'
                    text={formatDate(created_date)} />
                <Label
                    size='sm'
                    title='Location'
                    icon='map-pin'
                    text={location} />
            </CardContent>
            <CardFooter>
                <Link href={`/users/${user[1]}`} className="w-full">
                    <Button variant="outline" className="w-full">
                        <ExternalLink className="h-4 w-4 mr-2" />
                        View User Profile
                    </Button>
                </Link>
            </CardFooter>
        </Card>
    )
}

export default AccountCard;