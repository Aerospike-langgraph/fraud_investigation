import * as React from "react"
import { cn } from "@/lib/utils"

interface SwitchProps {
  id?: string
  checked?: boolean
  onCheckedChange?: (checked: boolean) => void
  disabled?: boolean
  className?: string
}

const Switch = React.forwardRef<HTMLDivElement, SwitchProps>(
  ({ checked = false, onCheckedChange, disabled = false, className, ...props }, ref) => {
    return (
      <div
        ref={ref}
        role="switch"
        aria-checked={checked}
        aria-disabled={disabled}
        className={cn(
          "peer inline-flex h-6 w-11 shrink-0 cursor-pointer items-center rounded-full border-2 border-transparent transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-primary data-[state=unchecked]:bg-input",
          className
        )}
        data-state={checked ? "checked" : "unchecked"}
        onClick={(e) => {
          e.stopPropagation()
          if (!disabled) {
            onCheckedChange?.(!checked)
          }
        }}
        onKeyDown={(e) => {
          if (e.key === ' ' || e.key === 'Enter') {
            e.preventDefault()
            e.stopPropagation()
            if (!disabled) {
          onCheckedChange?.(!checked)
            }
          }
        }}
        tabIndex={disabled ? -1 : 0}
        {...props}
      >
        <span
          className={cn(
            "pointer-events-none block h-5 w-5 rounded-full bg-background shadow-lg ring-0 transition-transform data-[state=checked]:translate-x-5 data-[state=unchecked]:translate-x-0"
          )}
          data-state={checked ? "checked" : "unchecked"}
        />
      </div>
    )
  }
)
Switch.displayName = "Switch"

export { Switch } 