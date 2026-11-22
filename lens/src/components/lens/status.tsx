import { CheckIcon, CircleDashedIcon, LoaderIcon, TriangleAlertIcon, XIcon } from 'lucide-react'
import type { State } from '@/lib/api'
import { stateInfo, type Tone } from '@/lib/format'
import { cn } from '@/lib/utils'

const TONE_TEXT: Record<Tone, string> = {
  ok: 'text-ok', ko: 'text-ko', run: 'text-run', wait: 'text-wait', warn: 'text-warn',
}
const TONE_BG: Record<Tone, string> = {
  ok: 'bg-ok', ko: 'bg-ko', run: 'bg-run', wait: 'bg-wait', warn: 'bg-warn',
}

export const toneText = (t: Tone) => TONE_TEXT[t]
export const toneBg = (t: Tone) => TONE_BG[t]

/** Icône d'état : la forme porte l'information autant que la couleur. */
export function StateIcon({ state, className }: { state: State | undefined; className?: string }) {
  const { tone } = stateInfo(state)
  const cls = cn('size-3.5 shrink-0', TONE_TEXT[tone], className)
  if (tone === 'ok') return <CheckIcon aria-hidden className={cls} strokeWidth={2.5} />
  if (tone === 'ko') return <XIcon aria-hidden className={cls} strokeWidth={2.5} />
  if (tone === 'warn') return <TriangleAlertIcon aria-hidden className={cls} />
  if (tone === 'run') return <LoaderIcon aria-hidden className={cn(cls, 'motion-safe:animate-spin')} />
  return <CircleDashedIcon aria-hidden className={cls} />
}

export function StatePill({ state, className }: { state: State | undefined; className?: string }) {
  const { label, tone } = stateInfo(state)
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium',
        TONE_TEXT[tone],
        className,
      )}
      style={{ borderColor: `color-mix(in oklab, var(--${tone}) 35%, transparent)`, background: `color-mix(in oklab, var(--${tone}) 9%, transparent)` }}
    >
      <StateIcon state={state} className="size-3" />
      {label}
    </span>
  )
}
