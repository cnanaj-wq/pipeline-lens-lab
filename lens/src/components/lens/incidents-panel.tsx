import { useMemo } from 'react'
import { AnimatePresence, motion, useReducedMotion } from 'motion/react'
import { ArrowUpRightIcon, SirenIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useIncidents } from '@/hooks/use-history'
import { fmtDateTime, fmtDuration, humanize } from '@/lib/format'
import { buildIncidents, incidentStats, type Incident } from '@/lib/incidents'
import { cn } from '@/lib/utils'
import { AnimatedNumber } from './animated-number'

const STATUT: Record<Incident['statut'], { label: string; tone: 'ok' | 'run' | 'ko' }> = {
  retabli: { label: 'Rétabli', tone: 'ok' },
  en_cours: { label: 'Nouvel essai en cours', tone: 'run' },
  echec: { label: 'Échec, à reprendre', tone: 'ko' },
}

/**
 * Incidents des 30 derniers jours, reconstitués à partir des événements que les DAG écrivent
 * eux-mêmes à l'instant de l'échec, du nouvel essai et de la reprise.
 */
export function IncidentsPanel({ now, refreshMs, onOpenRun }: { now: number; refreshMs: number; onOpenRun: (dagId: string, runId: string) => void }) {
  const reduce = useReducedMotion()
  const q = useIncidents(refreshMs)
  const list = useMemo(() => {
    const depuis = new Date(now - 30 * 24 * 3600_000).toISOString()
    return buildIncidents(q.data ?? [], now).filter((i) => i.debut >= depuis)
  }, [q.data, now])
  const s = incidentStats(list)

  return (
    <section aria-label="Incidents" className="rounded-lg border bg-card p-4">
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="flex items-center gap-2 text-sm font-semibold"><SirenIcon aria-hidden className="size-4 text-muted-foreground" />Incidents, 30 derniers jours</h2>
        <p className="text-xs text-muted-foreground">Enregistrés par les DAG au moment de l’échec, du nouvel essai et de la reprise.</p>
      </div>

      {q.status === 'error' && !q.data ? (
        <p role="alert" className="text-sm text-ko">Journal des incidents illisible : le serveur Pipeline Lens n’a pas répondu.</p>
      ) : !q.data ? (
        <div className="h-24 animate-pulse rounded bg-muted" />
      ) : (
        <>
          <dl className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Stat label="Ouverts" value={s.ouverts} tone={s.ouverts ? 'ko' : 'ok'} />
            <Stat label="Rétablis" value={s.retablis} />
            <Stat label="Repris sans intervention" value={s.automatiques} sub={s.retablis ? `sur ${s.retablis} rétabli${s.retablis > 1 ? 's' : ''}` : undefined} />
            <div>
              <dt className="text-xs text-muted-foreground">Durée médiane de rétablissement</dt>
              <dd className="tabular text-xl font-semibold">{s.mttrS == null ? '—' : fmtDuration(s.mttrS)}</dd>
            </div>
          </dl>

          {!list.length ? (
            <p className="text-sm text-muted-foreground">Aucun incident sur 30 jours. Le prochain échec de tâche apparaîtra ici dès qu’il se produit.</p>
          ) : (
            <ul className="divide-y text-sm">
              <AnimatePresence initial={false}>
                {list.slice(0, 10).map((i) => {
                  const st = STATUT[i.statut]
                  return (
                    <motion.li
                      key={i.key}
                      layout={reduce ? false : 'position'}
                      initial={reduce ? false : { opacity: 0, y: -6 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.3 }}
                      className="grid grid-cols-[1fr_auto] items-start gap-x-3 gap-y-1 py-2.5 lg:grid-cols-[150px_minmax(0,1.2fr)_minmax(0,2fr)_120px_170px_auto]"
                    >
                      <span className="tabular text-muted-foreground">{fmtDateTime(i.debut)}</span>
                      <span className="min-w-0 lg:order-none">
                        <span className="block truncate font-medium">{i.dag_id}</span>
                        <span className="block truncate text-xs text-muted-foreground">{humanize(i.task_id)}</span>
                      </span>
                      <span className="col-span-2 min-w-0 break-words text-xs lg:col-span-1">
                        {i.raison}
                        {i.echecs > 1 && <span className="text-muted-foreground"> · {i.echecs} échecs</span>}
                      </span>
                      <span className="tabular">
                        <span className="text-xs text-muted-foreground lg:hidden">Durée </span>
                        {fmtDuration(i.dureeS)}
                        {i.statut !== 'retabli' && <span className="text-xs text-muted-foreground"> (en cours)</span>}
                      </span>
                      <span>
                        <span
                          className={cn('inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium', `text-${st.tone}`)}
                          style={{ borderColor: `color-mix(in oklab, var(--${st.tone}) 35%, transparent)`, background: `color-mix(in oklab, var(--${st.tone}) 9%, transparent)` }}
                        >
                          {i.statut === 'en_cours' && <span aria-hidden className="size-1.5 rounded-full bg-run motion-safe:animate-pulse" />}
                          {st.label}
                        </span>
                        {i.reprise && <span className="mt-0.5 block text-xs text-muted-foreground">{i.reprise === 'automatique' ? 'par nouvel essai automatique' : 'après rejeu'}</span>}
                      </span>
                      <Button size="sm" variant="ghost" className="justify-self-end" onClick={() => onOpenRun(i.dag_id, i.run_id)}
                        aria-label={`Ouvrir l’exécution ${i.run_id} du DAG ${i.dag_id}`}>
                        Ouvrir <ArrowUpRightIcon aria-hidden />
                      </Button>
                    </motion.li>
                  )
                })}
              </AnimatePresence>
            </ul>
          )}
        </>
      )}
    </section>
  )
}

function Stat({ label, value, sub, tone }: { label: string; value: number; sub?: string; tone?: 'ok' | 'ko' }) {
  return (
    <div>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className={cn('tabular text-xl font-semibold', tone && `text-${tone}`)}>
        <AnimatedNumber value={value} />
        {sub && <span className="ml-1 text-xs font-normal text-muted-foreground">{sub}</span>}
      </dd>
    </div>
  )
}
