import { cn } from "@/lib/utils"

export interface BadgeProps {
  children: React.ReactNode
  variant?: 'default' | 'destructive' | 'secondary' | 'outline'
  className?: string
}

export function Badge({ children, variant = 'default', className }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium",
        {
          "bg-primary text-primary-foreground": variant === 'default',
          "bg-destructive text-destructive-foreground": variant === 'destructive',
          "bg-secondary text-secondary-foreground": variant === 'secondary',
          "border border-input bg-background text-foreground": variant === 'outline',
        },
        className
      )}
    >
      {children}
    </span>
  )
} 