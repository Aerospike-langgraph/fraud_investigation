import clsx from 'clsx'
import Label from '../Label'
import { formatCurrency, formatDate, formatDateTime, getRiskLevel } from '@/lib/utils'
import { Badge } from '../ui/badge'
import {   
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger
} from '../ui/tooltip'
import type { Option } from './index'

interface Props extends Option {
    result: Record<string, any>
}

export default function TableData({ item, type, className, label, result }: Props) {
    let value = label?.text ? result[label.text]
        : label?.subtitle ? result[label.subtitle]
            : label?.badge ? result[label.badge.text] : ""
    if(item === 'sender') {
        value = result.OUT[1]
    }
    if(item === 'receiver') {
        value = result.IN[1]
    }
    
    let risk = { level: "low", color: "success" }

    if(type === 'risk') risk = getRiskLevel(result?.risk_score ?? result?.fraud_score ?? 0)
    else if(type === 'date') value = formatDate(value);
    else if(type === 'datetime') value = formatDateTime(value);
    else if(type === 'currency') value = formatCurrency(value);

    return (
        <td key={item} className={clsx('p-3', className)}>
            {type !== 'fraud' ? (
                <Label
                    {...label}
                    className={`${label?.className ?? ""} truncate`}
                    {...label?.text && { text: value }}
                    {...label?.subtitle && { subtitle: value }}
                    {...label?.badge && type === 'risk' && { 
                        badge: { 
                            ...label.badge, 
                            text: `${risk.level} ${(value as number ?? 0).toFixed(1)}`,
                            variant: risk.color as any
                        }
                    }} />
            ) : (
                !value ? (
                    <Badge variant="default" className="text-xs">CLEAN</Badge>
                ) : (
                    <TooltipProvider>
                        <Tooltip>
                            <TooltipTrigger className='hover:cursor-default'>
                                <Badge 
                                    variant={value === 'blocked' ? 'destructive' : 'secondary'}
                                    className="text-xs"
                                >
                                    {(value as string)?.toUpperCase() ?? ""}
                                </Badge>
                            </TooltipTrigger>
                            <TooltipContent>
                                {value === 'review' ? (
                                    <span className="text-xs text-muted-foreground">Connected to 1 flagged account(s)</span>
                                ) : (
                                    result?.details?.length > 1 ? (
                                        <span className="text-xs text-muted-foreground">Multiple fraud triggers - check analysis</span>
                                    ) : (
                                        <span className="text-xs text-muted-foreground">{JSON.parse(result?.details ?? "{}")?.reason ?? "Undefined reason"}</span>
                                    )
                                )}
                            </TooltipContent>
                        </Tooltip>
                    </TooltipProvider>
                )
            )}
        </td>
    )
}