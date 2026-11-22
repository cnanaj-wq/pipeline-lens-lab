import { motion, useReducedMotion } from 'motion/react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import type { DagRun } from '@/lib/api'
import { fmtDateTime, fmtDuration, fmtDurationPrecise, stateInfo } from '@/lib/format'
import { cn } from '@/lib/utils'
import { toneBg } from './status'

const RUN_TYPE: Record<string, string> = { scheduled: 'Planifiée', manual: 'Manuelle', backfill: 'Rattrapage', asset_triggered: 'Déclenchée par un asset' }
export const runTypeLabel = (t: string) => RUN_TYPE[t] ?? t

/** Durée anormale : plus de deux fois la médiane des exécutions réussies (au moins 5 pour juger). */
export function anomalyThreshold(runs: DagRun[]): number | null {
  const d = runs.filter((r) => r.state === 'success' && r.duration != null).map((r) => r.duration as number).sort((a, b) => a - b)
  if (d.length < 5) return null
  const mid = Math.floor(d.length / 2)
  const median = d.length % 2 ? d[mid] : (d[mid - 1] + d[mid]) / 2
  return median * 2
}

/** Bande des 25 dernières exécutions : hauteur = durée, couleur et motif = état, point = durée anormale. */
export function RunHistory({
  runs, selectedRunId, onSelect,
}: { runs: DagRun[]; selectedRunId: string | null; onSelect: (id: string) => void }) {
  const reduce = useReducedMotion()
  const ordered = [...runs].reverse() // la plus récente à droite
  const maxDur = Math.max(1, ...ordered.map((r) => r.duration ?? 0))
  const seuil = anomalyThreshold(runs)

  if (!runs.length)
    return <p className="text-sm text-muted-foreground">Aucune exécution pour l’instant. La première aura lieu à l’heure planifiée.</p>

  return (
    <TooltipProvider delay={120}>
      <div role="listbox" aria-label="Dernières exécutions" className="flex h-16 items-end gap-1 overflow-x-auto pt-2 pb-1">
        {ordered.map((r, i) => {
          const info = stateInfo(r.state)
          const h = r.duration ? 22 + (r.duration / maxDur) * 78 : 22
          const sel = r.dag_run_id === selectedRunId
          const lent = seuil != null && r.duration != null && r.duration > seuil
          const quand = fmtDateTime(r.start_date ?? r.run_after)
          return (
            <Tooltip key={r.dag_run_id}>
              <TooltipTrigger
                role="option"
                aria-selected={sel}
                type="button"
                onClick={() => onSelect(r.dag_run_id)}
                aria-label={`Exécution du ${quand}, ${info.label}, ${fmtDurationPrecise(r.duration)}${lent ? ', durée anormale' : ''}`}
                style={{ height: `${h}%` }}
                className={cn(
                  'relative w-3.5 shrink-0 rounded-[3px] outline-offset-2 sm:w-4',
                  sel && 'ring-2 ring-foreground ring-offset-2 ring-offset-card',
                )}
              >
                <motion.span
                  aria-hidden
                  initial={reduce ? false : { scaleY: 0 }}
                  animate={{ scaleY: 1 }}
                  transition={{ duration: 0.35, delay: reduce ? 0 : i * 0.015 }}
                  style={{ originY: 1 }}
                  className={cn(
                    'absolute inset-0 rounded-[3px] opacity-80 transition-opacity hover:opacity-100',
                    sel && 'opacity-100',
                    toneBg(info.tone),
                    info.tone === 'ko' && 'bg-[repeating-linear-gradient(135deg,transparent_0_3px,rgba(255,255,255,.35)_3px_5px)]',
                  )}
                />
                {lent && <span aria-hidden className="absolute -top-2 left-1/2 size-1.5 -translate-x-1/2 rounded-full bg-warn" />}
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-sm flex-col items-start gap-0.5 py-2">
                <span className="font-medium">{info.label} · {fmtDurationPrecise(r.duration)}</span>
                <span>{quand} · {runTypeLabel(r.run_type)}</span>
                {lent && <span>Durée anormale : plus de 2 × la médiane ({fmtDuration(seuil)})</span>}
                <span className="font-mono text-[0.65rem] opacity-80">{r.dag_run_id}</span>
              </TooltipContent>
            </Tooltip>
          )
        })}
      </div>
      <p className="mt-1.5 text-xs text-muted-foreground">
        {runs.length} dernière{runs.length > 1 ? 's' : ''} exécution{runs.length > 1 ? 's' : ''}, la plus récente à droite. Hauteur = durée
        {seuil != null ? ', point orange = plus de 2 × la durée médiane' : ''}. Survolez une barre pour son détail.
      </p>
    </TooltipProvider>
  )
}
