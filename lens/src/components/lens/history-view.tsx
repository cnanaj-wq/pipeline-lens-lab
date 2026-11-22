import { useEffect, useMemo, useState } from 'react'
import { AnimatePresence, motion, useReducedMotion } from 'motion/react'
import { ActivityIcon, ArrowUpRightIcon, ChevronLeftIcon, ChevronRightIcon, HistoryIcon, RotateCcwIcon, SearchIcon, ShieldCheckIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useAudit, useFailedTasks, useHistory, useLoadedRows, useWeekRuns } from '@/hooks/use-history'
import type { DagSummary } from '@/hooks/use-lens'
import type { GlobalRun, LensConfig, Task } from '@/lib/api'
import { fmtAgo, fmtDateTime, fmtDuration, fmtDurationPrecise, fmtInt, humanize } from '@/lib/format'
import { buckets, eta, kpis, matchRun, median, replayGate, type Range } from '@/lib/history'
import { cn } from '@/lib/utils'
import { ActivityChart } from './activity-chart'
import { AnimatedNumber } from './animated-number'
import { ErrorPanel } from './feedback'
import { IncidentsPanel } from './incidents-panel'
import { runTypeLabel } from './run-history'
import { StatePill } from './status'
import type { ReplayTarget } from './replay-dialog'

const PAGE = 20
const PERIODES = [
  { id: '24h', label: '24 heures', ms: 24 * 3600_000 },
  { id: '7j', label: '7 jours', ms: 7 * 24 * 3600_000 },
  { id: '30j', label: '30 jours', ms: 30 * 24 * 3600_000 },
  { id: 'tout', label: 'Tout', ms: 0 },
] as const
const ETATS = [
  { id: '', label: 'Tous les états' }, { id: 'success', label: 'Réussies' }, { id: 'failed', label: 'En échec' },
  { id: 'running', label: 'En cours' }, { id: 'queued', label: 'En file' },
]
const TYPES = [{ id: '', label: 'Tous les types' }, { id: 'scheduled', label: 'Planifiées' }, { id: 'manual', label: 'Manuelles' }, { id: 'backfill', label: 'Rattrapages' }]

interface Props {
  dags: DagSummary[]
  now: number
  refreshMs: number
  config: LensConfig | null
  onOpenRun: (dagId: string, runId: string) => void
  onReplay: (t: ReplayTarget) => void
  /** Incrémenté après chaque rejeu : relit la liste et le journal sans attendre l'actualisation. */
  version?: number
}

/** Volet Historique : toutes les exécutions de tous les DAG, filtrables, avec rejeu contrôlé. */
export function HistoryView({ dags, now, refreshMs, config, onOpenRun, onReplay, version = 0 }: Props) {
  const reduce = useReducedMotion()
  const [dagId, setDagId] = useState('')
  const [state, setState] = useState('')
  const [runType, setRunType] = useState('')
  const [periode, setPeriode] = useState<(typeof PERIODES)[number]['id']>('7j')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const [range, setRange] = useState<Range>('24h')

  // Arrondi à la minute : la requête ne change pas à chaque seconde.
  const sinceMs = PERIODES.find((p) => p.id === periode)!.ms
  const since = sinceMs ? new Date(Math.floor((now - sinceMs) / 60_000) * 60_000).toISOString() : null
  const hist = useHistory({ dagId: dagId || null, state: state || null, runType: runType || null, since, limit: PAGE, offset: page * PAGE }, refreshMs, version)
  const week = useWeekRuns(refreshMs, version)
  const failed = useFailedTasks(refreshMs, version)
  const dagIds = useMemo(() => dags.map((d) => d.dag_id), [dags])
  const rows = useLoadedRows(dagIds, refreshMs)
  const audit = useAudit(refreshMs, version)

  const k = useMemo(() => kpis(dags, week.data ?? [], now), [dags, week.data, now])
  const chart = useMemo(() => buckets(week.data ?? [], now, range), [week.data, now, range])
  const byDag = useMemo(() => {
    const m: Record<string, GlobalRun[]> = {}
    for (const r of week.data ?? []) (m[r.dag_id] ??= []).push(r)
    return m
  }, [week.data])
  const seuils = useMemo(() => {
    const out: Record<string, number | null> = {}
    for (const [id, runs] of Object.entries(byDag)) {
      const d = runs.filter((r) => r.state === 'success' && r.duration != null).map((r) => r.duration as number)
      out[id] = d.length >= 5 ? (median(d) as number) * 2 : null
    }
    return out
  }, [byDag])

  const list = (hist.data?.runs ?? []).filter((r) => matchRun(r, search))
  const total = hist.data?.total ?? 0
  const pages = Math.max(1, Math.ceil(total / PAGE))
  // Lignes déjà affichées : les nouvelles apparaissent avec un bref surlignage.
  const [seen, setSeen] = useState<Set<string> | null>(null)
  const isNew = (key: string) => seen != null && !seen.has(key)
  // Mémorise les lignes déjà vues après chaque rendu, pour signaler les nouvelles.
  const keys = list.map((r) => `${r.dag_id}|${r.dag_run_id}`)
  const keySig = keys.join(',')
  useEffect(() => {
    if (hist.data) setSeen((prev) => new Set([...(prev ?? []), ...keySig.split(',')]))
  }, [hist.data, keySig])

  const resetPage = <T,>(set: (v: T) => void) => (v: T) => { set(v); setPage(0) }
  const pct = (r: { rate: number | null }) => (r.rate == null ? null : r.rate * 100)

  return (
    <div className="space-y-6">
      <section className="flex flex-col gap-1">
        <h1 className="flex items-center gap-2 text-2xl font-semibold tracking-tight"><HistoryIcon aria-hidden className="size-5 text-muted-foreground" />Historique des traitements</h1>
        <p className="max-w-[72ch] text-sm text-muted-foreground">
          Toutes les exécutions de tous les DAG, lues en direct dans Airflow. Ouvrez une exécution pour son détail, ou rejouez-la lorsque son DAG est déclaré rejouable.
        </p>
      </section>

      {/* Indicateurs du cockpit */}
      <section aria-label="Indicateurs" className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-6">
        <Kpi label="DAG surveillés" value={k.surveilles} sub={`${k.actifs} actif${k.actifs > 1 ? 's' : ''}`} />
        <Kpi label="En cours" value={k.enCours} tone={k.enCours ? 'run' : undefined} pulse={k.enCours > 0} />
        <Kpi label="DAG en échec" value={k.enEchec} tone={k.enEchec ? 'ko' : 'ok'} sub="dernière exécution" />
        <Kpi label="Succès 24 h" value={pct(k.succes24)} suffix=" %" sub={`${k.succes24.ok} / ${k.succes24.done} terminées`} tone={rateTone(k.succes24.rate)} />
        <Kpi label="Succès 7 jours" value={pct(k.succes7)} suffix=" %" sub={`${k.succes7.ok} / ${k.succes7.done} terminées`} tone={rateTone(k.succes7.rate)} />
        <Kpi label="Durée médiane" text={k.dureeMediane == null ? '—' : fmtDuration(k.dureeMediane)} sub="exécutions réussies, 7 j" />
      </section>

      {/* Activité */}
      <section aria-label="Activité" className="rounded-lg border bg-card p-4">
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
          <h2 className="flex items-center gap-2 text-sm font-semibold"><ActivityIcon aria-hidden className="size-4 text-muted-foreground" />Activité</h2>
          <div role="radiogroup" aria-label="Période du graphique" className="inline-flex rounded-md border p-0.5 text-xs">
            {(['24h', '7j'] as Range[]).map((r) => (
              <button key={r} role="radio" aria-checked={range === r} onClick={() => setRange(r)}
                className={cn('relative rounded px-2.5 py-1', range === r ? 'text-foreground' : 'text-muted-foreground hover:text-foreground')}>
                {range === r && <motion.span layoutId="range-pill" className="absolute inset-0 rounded bg-muted" transition={reduce ? { duration: 0 } : { type: 'spring', stiffness: 400, damping: 32 }} />}
                <span className="relative">{r === '24h' ? '24 heures' : '7 jours'}</span>
              </button>
            ))}
          </div>
        </div>
        {week.status === 'error' && !week.data ? <p className="text-sm text-ko">Activité indisponible : Airflow n’a pas répondu.</p>
          : !week.data ? <div className="h-32 animate-pulse rounded bg-muted" />
          : <ActivityChart data={chart} range={range} />}
      </section>

      {/* Incidents : historisés par les DAG eux-mêmes, à l'événement */}
      <IncidentsPanel now={now} refreshMs={refreshMs} onOpenRun={onOpenRun} />

      {/* Filtres */}
      <section aria-label="Filtres" className="flex flex-wrap items-end gap-3">
        <Select label="DAG" value={dagId} onChange={resetPage(setDagId)} options={[{ id: '', label: 'Tous les DAG' }, ...dags.map((d) => ({ id: d.dag_id, label: d.dag_id }))]} />
        <Select label="État" value={state} onChange={resetPage(setState)} options={ETATS} />
        <Select label="Type" value={runType} onChange={resetPage(setRunType)} options={TYPES} />
        <Select label="Période" value={periode} onChange={resetPage((v: string) => setPeriode(v as typeof periode))} options={PERIODES.map((p) => ({ id: p.id, label: p.label }))} />
        <label className="flex min-w-48 flex-1 flex-col gap-1 text-xs text-muted-foreground sm:max-w-72">
          Rechercher
          <span className="relative">
            <SearchIcon aria-hidden className="absolute top-1/2 left-2 size-3.5 -translate-y-1/2" />
            <input type="search" value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Identifiant d’exécution ou DAG"
              className="h-8 w-full rounded-md border bg-background pr-2 pl-7 text-sm text-foreground placeholder:text-muted-foreground" />
          </span>
        </label>
      </section>

      {/* Liste des exécutions */}
      <section aria-label="Exécutions" aria-busy={hist.status === 'loading'}>
        {hist.status === 'error' && !hist.data ? (
          <ErrorPanel error={hist.error} onRetry={hist.refresh} />
        ) : !hist.data ? (
          <div className="space-y-2">{[0, 1, 2, 3].map((i) => <div key={i} className="h-12 animate-pulse rounded-md bg-muted" />)}</div>
        ) : !list.length ? (
          <div className="rounded-lg border border-dashed p-8 text-center text-sm">
            <p className="font-medium">{total ? 'Aucune exécution ne correspond à la recherche.' : 'Aucune exécution sur cette période.'}</p>
            <p className="mt-1 text-muted-foreground">{total ? 'Modifiez le texte recherché.' : 'Élargissez la période ou retirez un filtre. Les DAG sans exécution apparaîtront ici dès leur premier passage.'}</p>
          </div>
        ) : (
          <TooltipProvider delay={150}>
            <div className="hidden grid-cols-[minmax(150px,1.1fr)_minmax(140px,1fr)_120px_110px_100px_minmax(120px,1fr)_96px_150px] gap-3 border-b px-3 pb-2 text-xs text-muted-foreground lg:grid">
              <span>Date</span><span>DAG</span><span>État</span><span>Durée</span><span>Type</span><span>Tâches en échec</span><span className="text-right">Lignes chargées</span><span className="text-right">Actions</span>
            </div>
            <ul className="divide-y">
              <AnimatePresence initial={false}>
                {list.map((r) => {
                  const key = `${r.dag_id}|${r.dag_run_id}`
                  const dag = dags.find((d) => d.dag_id === r.dag_id)
                  const gate = replayGate(dag, r, config)
                  const ko = failed.data?.[key] ?? []
                  const lignes = rows.data?.loaded[key]
                  const seuil = seuils[r.dag_id]
                  const lent = seuil != null && r.duration != null && r.duration > seuil
                  const e = eta(r, byDag[r.dag_id] ?? [], now)
                  const tasks: Task[] = rows.data?.tasks[r.dag_id] ?? []
                  return (
                    <motion.li
                      key={key}
                      layout={reduce ? false : 'position'}
                      initial={reduce || !isNew(key) ? false : { opacity: 0, y: -8, backgroundColor: 'color-mix(in oklab, var(--run) 14%, transparent)' }}
                      animate={{ opacity: 1, y: 0, backgroundColor: 'rgba(0,0,0,0)' }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.35, backgroundColor: { duration: 2.4 } }}
                      className="grid grid-cols-[1fr_auto] items-center gap-x-3 gap-y-1.5 px-3 py-2.5 text-sm lg:grid-cols-[minmax(150px,1.1fr)_minmax(140px,1fr)_120px_110px_100px_minmax(120px,1fr)_96px_150px]"
                    >
                      <span className="min-w-0">
                        <span className="block">{fmtDateTime(r.start_date ?? r.run_after)}</span>
                        <span className="block text-xs text-muted-foreground">{fmtAgo(r.start_date ?? r.run_after, now)}</span>
                      </span>
                      <span className="min-w-0 truncate font-medium lg:font-normal" title={r.dag_run_id}>{r.dag_id}</span>
                      <span className="col-span-2 lg:col-span-1">
                        <StatePill state={r.state} />
                        {e && (
                          <span className="mt-1 block w-28" title={`Médiane : ${fmtDuration(e.median)}`}>
                            <span className="block h-1 overflow-hidden rounded-full bg-muted">
                              <motion.span className={cn('block h-full rounded-full', e.overrun ? 'bg-warn' : 'bg-run')} initial={false} animate={{ width: `${Math.round(e.progress * 100)}%` }} transition={{ duration: 0.8 }} />
                            </span>
                            <span className="mt-0.5 block text-[0.7rem] text-muted-foreground">{e.overrun ? 'plus long que d’habitude' : `≈ ${fmtDuration(e.remaining)} restantes`}</span>
                          </span>
                        )}
                      </span>
                      <span className="tabular flex items-center gap-1.5">
                        <span className="text-xs text-muted-foreground lg:hidden">Durée</span>
                        {fmtDurationPrecise(r.duration)}
                        {lent && (
                          <Tooltip>
                            <TooltipTrigger render={<span tabIndex={0} aria-label="Durée anormale" className="inline-block size-2 rounded-full bg-warn" />} />
                            <TooltipContent>Durée anormale : plus de 2 × la médiane ({fmtDuration(seuil)})</TooltipContent>
                          </Tooltip>
                        )}
                      </span>
                      <span className="text-muted-foreground">{runTypeLabel(r.run_type)}</span>
                      <span className="col-span-2 min-w-0 text-xs lg:col-span-1">
                        {ko.length ? <span className="text-ko">{ko.map(humanize).join(', ')}</span> : <span className="text-muted-foreground lg:opacity-60">—</span>}
                      </span>
                      <span className="tabular text-left lg:text-right">
                        <span className="text-xs text-muted-foreground lg:hidden">Lignes chargées </span>
                        {lignes != null ? fmtInt(lignes) : <span className="text-xs text-muted-foreground">non mesuré</span>}
                      </span>
                      <span className="flex justify-end gap-1.5">
                        <Button size="sm" variant="ghost" onClick={() => onOpenRun(r.dag_id, r.dag_run_id)} aria-label={`Ouvrir l’exécution ${r.dag_run_id} de ${r.dag_id}`}>
                          Ouvrir<ArrowUpRightIcon aria-hidden />
                        </Button>
                        <Tooltip>
                          <TooltipTrigger render={
                            <Button size="sm" variant="outline" aria-disabled={!gate.ok}
                              className={cn(!gate.ok && 'cursor-not-allowed opacity-45')}
                              onClick={() => gate.ok && onReplay({ run: r, tasks })}
                              aria-label={gate.ok ? `Rejouer l’exécution ${r.dag_run_id} de ${r.dag_id}` : `Rejeu indisponible : ${gate.reason}`}>
                              <RotateCcwIcon aria-hidden />Rejouer
                            </Button>
                          } />
                          <TooltipContent className="max-w-xs">{gate.ok ? 'Simuler puis confirmer le rejeu' : gate.reason}</TooltipContent>
                        </Tooltip>
                      </span>
                    </motion.li>
                  )
                })}
              </AnimatePresence>
            </ul>
            <nav aria-label="Pagination" className="mt-3 flex items-center justify-between gap-3 text-sm text-muted-foreground">
              <span>{fmtInt(total)} exécution{total > 1 ? 's' : ''}{search && ` · ${list.length} affichée${list.length > 1 ? 's' : ''}`}</span>
              <span className="flex items-center gap-1">
                <Button size="icon-sm" variant="ghost" disabled={page === 0} onClick={() => setPage((p) => p - 1)} aria-label="Page précédente"><ChevronLeftIcon aria-hidden /></Button>
                <span className="tabular px-1">Page {page + 1} / {pages}</span>
                <Button size="icon-sm" variant="ghost" disabled={page + 1 >= pages} onClick={() => setPage((p) => p + 1)} aria-label="Page suivante"><ChevronRightIcon aria-hidden /></Button>
              </span>
            </nav>
          </TooltipProvider>
        )}
      </section>

      {/* Journal des rejeux */}
      <section aria-label="Derniers rejeux" className="rounded-lg border bg-card p-4">
        <h2 className="mb-1 flex items-center gap-2 text-sm font-semibold"><ShieldCheckIcon aria-hidden className="size-4 text-muted-foreground" />Derniers rejeux</h2>
        <p className="mb-3 text-xs text-muted-foreground">
          {config?.actions ? 'Chaque simulation et chaque rejeu sont journalisés par le serveur Pipeline Lens.' : 'Rejeu désactivé : Pipeline Lens fonctionne en lecture seule.'}
        </p>
        {!audit.data?.length ? (
          <p className="text-sm text-muted-foreground">Aucun rejeu pour l’instant.</p>
        ) : (
          <ul className="space-y-1.5 text-sm">
            {audit.data.filter((a) => a.action === 'rejeu').slice(0, 8).map((a, i) => (
              <li key={`${a.ts}-${i}`} className="flex flex-wrap items-baseline gap-x-2">
                <span className={cn('size-1.5 shrink-0 self-center rounded-full', a.resultat === 'lance' ? 'bg-ok' : 'bg-ko')} aria-hidden />
                <span className="tabular text-muted-foreground">{fmtDateTime(a.ts)}</span>
                <span className="font-medium">{a.dag_id}</span>
                <span>{a.mode === 'failed' ? 'tâches en échec' : a.mode === 'task' ? `tâche ${humanize(a.task_id ?? '')}` : 'exécution complète'}</span>
                <span className="text-muted-foreground">{a.resultat === 'lance' ? `${a.taches} tâche${(a.taches ?? 0) > 1 ? 's' : ''} relancée${(a.taches ?? 0) > 1 ? 's' : ''}` : `refusé (${a.motif})`}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  )
}

const rateTone = (r: number | null) => (r == null ? undefined : r >= 0.95 ? 'ok' : r >= 0.8 ? 'warn' : 'ko')

function Kpi({ label, value, text, suffix = '', sub, tone, pulse }: {
  label: string; value?: number | null; text?: string; suffix?: string; sub?: string; tone?: 'ok' | 'ko' | 'run' | 'warn'; pulse?: boolean
}) {
  const fmt = (n: number) => `${new Intl.NumberFormat('fr-FR').format(Math.round(n))}${suffix}`
  return (
    <div className="rounded-lg border bg-card p-3">
      <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
        {pulse && <span aria-hidden className="relative flex size-2"><span className="absolute inline-flex size-full rounded-full bg-run opacity-60 motion-safe:animate-ping" /><span className="relative inline-flex size-2 rounded-full bg-run" /></span>}
        {label}
      </p>
      <p className={cn('tabular mt-1 text-2xl font-semibold tracking-tight', tone && `text-${tone}`)}>
        {text ?? (value == null ? '—' : <AnimatedNumber value={value} format={fmt} />)}
      </p>
      {sub && <p className="mt-0.5 text-xs text-muted-foreground">{sub}</p>}
    </div>
  )
}

function Select({ label, value, onChange, options }: { label: string; value: string; onChange: (v: string) => void; options: readonly { id: string; label: string }[] }) {
  return (
    <label className="flex flex-col gap-1 text-xs text-muted-foreground">
      {label}
      <select value={value} onChange={(e) => onChange(e.target.value)} className="h-8 rounded-md border bg-background px-2 text-sm text-foreground">
        {options.map((o) => <option key={o.id} value={o.id}>{o.label}</option>)}
      </select>
    </label>
  )
}
