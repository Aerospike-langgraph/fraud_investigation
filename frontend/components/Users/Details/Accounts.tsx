import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { AlertTriangle, Calendar, CreditCard } from 'lucide-react'
import { formatCurrency, formatDate } from '@/lib/utils'

export interface Account {
	"1"?: string
    id?: string
	type: string
	balance: number
    bank_name: string
	created_date: string
	fraud_flag?: boolean
    status: string
}

const Accounts = ({ accounts }: { accounts: Account[] }) => {
    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <CreditCard className="h-5 w-5" />
                    Accounts ({accounts.length})
                </CardTitle>
            </CardHeader>
            <CardContent>
                {accounts.length > 0 ? (
                    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                    {accounts.map((account) => (
                        <Card key={account["1"]} className="p-4">
                        <div className="space-y-3">
                            <div className="flex items-center justify-between">
                            <div>
                                <div className="flex items-center gap-2">
                                <p className="font-semibold capitalize">{account.type} Account</p>
                                {account.fraud_flag && (
                                    <Badge variant="destructive" className="text-xs flex items-center gap-1">
                                    <AlertTriangle className="h-3 w-3" />
                                        FRAUD
                                    </Badge>
                                )}
                                </div>
                                <p className="text-sm text-muted-foreground">ID: {account["1"]}</p>
                            </div>
                            <CreditCard className="h-8 w-8 text-muted-foreground" />
                            </div>
                            <div>
                                <p className="text-sm font-medium text-muted-foreground">Balance</p>
                                <p className={`text-2xl font-bold ${account.balance < 0 ? 'text-destructive' : 'text-green-600'}`}>
                                    {formatCurrency(account.balance)}
                                </p>
                            </div>
                            <div>
                                <p className="text-sm font-medium text-muted-foreground">Created</p>
                                <div className="flex items-center gap-2">
                                    <Calendar className="h-4 w-4 text-muted-foreground" />
                                    <p className="text-sm">{formatDate(account.created_date)}</p>
                                </div>
                            </div>
                        </div>
                        </Card>
                    ))}
                </div>
              ) : (
                <div className="text-center py-8">
                    <CreditCard className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <p className="text-muted-foreground">No accounts found for this user.</p>
                </div>
              )}
            </CardContent>
        </Card>
    )
}

export default Accounts;