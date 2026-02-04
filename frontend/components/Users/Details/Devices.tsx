import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { AlertTriangle, Monitor, Smartphone, Tablet } from 'lucide-react'
import { Badge } from '@/components/ui/badge'

export interface Device {
    "1": string
    type: string
    os: string
    browser: string
    fraud_flag?: boolean
}

export const getDeviceIcon = (deviceType: string) => {
    switch (deviceType.toLowerCase()) {
        case 'mobile':
            return <Smartphone className="h-4 w-4" />
        case 'desktop':
            return <Monitor className="h-4 w-4" />
        case 'tablet':
            return <Tablet className="h-4 w-4" />
        default:
            return <Monitor className="h-4 w-4" />
    }
}

const Devices = ({ devices }: { devices?: Device[]}) => (
    <Card>
        <CardHeader>
            <CardTitle className="flex items-center gap-2">
                <Smartphone className="h-5 w-5" />
                User Devices ({devices?.length || 0})
            </CardTitle>
            <CardDescription>
                Devices associated with this user account
            </CardDescription>
        </CardHeader>
        <CardContent>
            {devices && devices.length > 0 ? (
                <div className="grid gap-4 md:grid-cols-1 lg:grid-cols-2">
                    {devices.map((device) => (
                        <Card key={device["1"]} className="p-4 hover:shadow-md transition-shadow">
                            <div className="space-y-3">
                                <div className="flex items-start justify-between">
                                    <div className="flex items-center gap-3">
                                        {getDeviceIcon(device.type)}
                                        <div>
                                            <div className="flex items-center gap-2">
                                                <p className="font-semibold capitalize">{device.type}</p>
                                                {device.fraud_flag &&
                                                <Badge variant="destructive" className="text-xs flex items-center gap-1">
                                                    <AlertTriangle className="h-3 w-3" />
                                                    FRAUD
                                                </Badge>}
                                            </div>
                                            <p className="text-sm text-muted-foreground">{device.os}</p>
                                            <p className="text-xs text-muted-foreground">{device.browser}</p>
                                        </div>
                                    </div>
                                    <Badge variant="secondary" className="text-xs">
                                        {device["1"]}
                                    </Badge>
                                </div>
                            </div>
                        </Card>
                    ))}
                </div>
            ) : (
                <div className="text-center py-8">
                    <Smartphone className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <p className="text-muted-foreground">No devices found for this user.</p>
                    <p className="text-sm text-muted-foreground mt-2">
                        This user doesn't have any registered devices.
                    </p>
                </div>
            )}
        </CardContent>
    </Card>
)

export default Devices;