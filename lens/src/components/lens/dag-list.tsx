import { PauseIcon } from 'lucide-react'
import type { DagSummary } from '@/hooks/use-lens'
import { cronToFrench, fmtAgo } from '@/lib/format'
import { cn } from '@/lib/utils'
import { StateIcon } from './status'

export function dagTitle(d: { dag_id: string; description: string | null }) {
  const t = d.description?.split(/\s:\s|\s—\s|\(/)[0]?.trim()
  return t || d.dag_id
}

/** Liste des DAG : colonne sur grand écran, bande défilante sur mobile. */
export function DagList({
  dags, selected, onSelect, now,
}: { dags: DagSummary[]; selected: string | null; onSelect: (id: string) => void; now: number }) {
  return (
    <nav aria-label="DAG planifiés">
      <ul className="flex gap-2 overflow-x-auto pb-1 lg:flex-col lg:overflow-visible lg:pb-0">
        {dags.map((d) => {
          const sel = d.dag_id === selected
          return (
            <li key={d.dag_id} className="shrink-0 lg:shrink">
              <button
                type="button"
                onClick={() => onSelect(d.dag_id)}
                aria-current={sel ? 'page' : undefined}
                className={cn(
                  'flex w-60 flex-col gap-1 rounded-lg border px-3 py-2.5 text-left transition-colors lg:w-full',
                  sel ? 'border-primary bg-card ring-1 ring-primary' : 'border-transparent bg-sidebar-accent/50 hover:bg-card',
                )}
              >
                <span className="flex items-center gap-2">
                  {d.is_paused ? <PauseIcon aria-label="En pause" className="size-3.5 text-wait" /> : <StateIcon state={d.lastRun?.state} />}
                  <span className="truncate text-sm font-medium">{dagTitle(d)}</span>
                </span>
                <span className="truncate font-mono text-[0.7rem] text-muted-foreground">{d.dag_id}</span>
                <span className="truncate text-xs text-muted-foreground">
                  {d.is_paused ? 'En pause' : cronToFrench(d.timetable_summary) ?? d.timetable_description ?? 'Manuel'}
                  {d.lastRun && ` · ${fmtAgo(d.lastRun.end_date ?? d.lastRun.start_date, now)}`}
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </nav>
  )
}
