'use client'

import type { LucideProps } from 'lucide-react'
import { 
    Activity,
    AlertTriangle,
    Building,
    Calendar,
    Clock,
    CreditCard,
    CheckCircle,
    Mail,
    MapPin,
    Flag,
    Phone,
    Shield,
    TrendingDown,
    TrendingUp,
    User,
    Users,
    XCircle
} from 'lucide-react'

export type IconName = keyof typeof Icons

interface Props extends LucideProps {
    icon: IconName
}

const Icons = {
    activity: Activity,
    'alert-triangle': AlertTriangle,
    building: Building,
    calendar: Calendar,
    clock: Clock,
    'credit-card': CreditCard,
    'check-circle': CheckCircle,
    flag: Flag,
    mail: Mail,
    'map-pin': MapPin,
    phone: Phone,
    shield: Shield,
    'trending-down': TrendingDown,
    'trending-up': TrendingUp,
    user: User,
    users: Users,
    'x-circle': XCircle
}

const Icon = ({icon, ...props}: Props) => {
    const Component = Icons[icon]
    return (
        <Component {...props} />
    )    
}

export default Icon