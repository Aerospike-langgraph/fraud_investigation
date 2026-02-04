'use client'

import { useRouter } from "next/navigation";
import { getDeviceIcon, type Device } from "./Devices";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { ExternalLink, Smartphone, User, Users } from 'lucide-react'
import { Badge } from '@/components/ui/badge'

interface ConnectedDeviceUser {
    "1": string
    name: string
    email: string
    risk_score: number
    shared_devices: Device[]
    shared_device_count: number
}

export interface Connection {
    device_id: string
    users: ConnectedDeviceUser[]
}
interface Props {
    devices?: Device[],
    connections?: Connection[]
}

interface ConnectedUsers {
    [id: string]: {
        user: {
            name: string
            email: string
            risk_score: number
        }
        devices: Device[]
    }
}

const Connections = ({ devices, connections }: Props) => {
    const connectedUsers: ConnectedUsers = {}
    if(connections && devices) {
        for(let { device_id, users } of connections) {
            const device = devices.find(device => device["1"] === device_id)
            if(!device) continue
            for(let user of users) {
                if(!connectedUsers[user["1"]]) connectedUsers[user["1"]] = { user, devices: [ device ] }
                else connectedUsers[user["1"]].devices.push(device)
            }
        }
    }
    
    const router = useRouter();
    const userIds = Object.keys(connectedUsers)
    return (
        <div className="space-y-6">
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Smartphone className="h-5 w-5" />
                        Device Connections
                    </CardTitle>
                    <CardDescription>
                        Users who share devices with this user
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    {userIds.length > 0 ? (
                        <div className="space-y-4">
                            {userIds.map((userId) => (
                                <Card 
                                    key={userId} 
                                    className="p-4 hover:shadow-md transition-all duration-200 cursor-pointer group" 
                                    onClick={() => router.push(`/users/${userId}`)}
                                >
                                    <div className="space-y-3">
                                        <div className="flex items-start justify-between">
                                            <div className="flex items-center gap-3">
                                                <div className="relative">
                                                    <User className="h-10 w-10 text-muted-foreground group-hover:text-primary transition-colors" />
                                                    <div className="absolute -bottom-1 -right-1 w-4 h-4 bg-primary rounded-full flex items-center justify-center">
                                                        <Smartphone className="h-2.5 w-2.5 text-white" />
                                                    </div>
                                                </div>
                                                <div className="flex-1">
                                                    <div className="flex items-center gap-2">
                                                        <p className="font-semibold text-lg group-hover:text-primary transition-colors">
                                                            {connectedUsers[userId].user.name}
                                                        </p>
                                                        <Badge 
                                                            variant={connectedUsers[userId].user.risk_score > 50 ? 'destructive' : connectedUsers[userId].user.risk_score > 25 ? 'default' : 'secondary'}
                                                            className="text-xs"
                                                        >
                                                            Risk: {connectedUsers[userId].user.risk_score.toFixed(1)}
                                                        </Badge>
                                                    </div>
                                                    <p className="text-sm text-muted-foreground">{connectedUsers[userId].user.email}</p>
                                                    <p className="text-xs text-muted-foreground">ID: {userId}</p>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                                                <ExternalLink className="h-4 w-4 text-muted-foreground" />
                                                <span className="text-sm text-muted-foreground">View Profile</span>
                                            </div>
                                        </div>
                                        <div className="border-t pt-3">
                                            <div className="flex items-center gap-2 mb-3">
                                                <Smartphone className="h-4 w-4 text-muted-foreground" />
                                                <p className="text-sm font-medium text-muted-foreground">
                                                    Shared Devices ({connectedUsers[userId].devices.length})
                                                </p>
                                            </div>
                                            <div className="space-y-2">
                                            {connectedUsers[userId].devices.map((device) => (
                                                <div key={device['1']} className="flex items-center gap-2 p-2 bg-muted/50 rounded-md border">
                                                    {getDeviceIcon(device.type)}
                                                    <div className="flex-1 min-w-0">
                                                        <p className="text-sm font-medium truncate">{device.type} - {device.os}</p>
                                                        <p className="text-xs text-muted-foreground truncate">{device.browser}</p>
                                                    </div>
                                                    <Badge variant="secondary" className="text-xs shrink-0">
                                                    {device['1']}
                                                    </Badge>
                                                </div>
                                            ))}
                                            </div>
                                        </div>
                                    </div>
                                </Card>
                            ))}
                        </div>
                    ) : (
                        <div className="text-center py-8">
                            <Smartphone className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                            <p className="text-muted-foreground">No device connections found.</p>
                            <p className="text-sm text-muted-foreground mt-2">
                                This user doesn't share any devices with other users.
                            </p>
                        </div>
                    )}
                </CardContent>
            </Card>
            <Card className="opacity-60">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Users className="h-5 w-5" />
                        Other Connections
                    </CardTitle>
                    <CardDescription>
                        Additional connection types will be available soon
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="text-center py-6">
                        <Users className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                        <p className="text-muted-foreground">Coming Soon</p>
                        <p className="text-sm text-muted-foreground mt-2">
                            Transaction connections, location-based connections, and more will be available here.
                        </p>
                    </div>
                </CardContent>
            </Card>
        </div>
    )
}

export default Connections;