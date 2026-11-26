import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { AnimatePresence, motion } from 'motion/react'
import { CalendarClockIcon, HistoryIcon, MoonIcon, RefreshCwIcon, RotateCcwIcon, SunIcon, TimerIcon, WorkflowIcon } from 'lucide-react'
import { toast } from 'sonner'
import { Toaster } from '@/components/ui/sonner'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { HistoryView } from '@/components/lens/history-view'
import { ReplayDialog, type ReplayTarget } from '@/components/lens/replay-dialog'
import { useLensConfig } from '@/hooks/use-history'
import { replayGate, snapshotStates, transitions } from '@/lib/history'
import { Button } from '@/components/ui/button'
import { DagList, dagTitle } from '@/components/lens/dag-list'
import { GapSheet, gapsSansPerte } from '@/components/lens/gap-sheet'
import { IncidentBanner } from '@/components/lens/incident'
import { QualityPanel } from '@/components/lens/quality'
import { EmptyDags, ErrorPanel, PipelineSkeleton } from '@/components/lens/feedback'
import { Pipeline } from '@/components/lens/pipeline'
import { RunHistory, runTypeLabel } from '@/components/lens/run-history'
import { StatePill } from '@/components/lens/status'
import { TaskSheet } from '@/components/lens/task-sheet'
import { REFRESH_CHOICES, useDagBundle, useDags, useNow, useRefreshInterval, useRunDetails } from '@/hooks/use-lens'
import { DEMO, snapshotInfo, type DagRun, type TaskInstance } from '@/lib/api'
import { cronToFrench, fmtCountdown, fmtDateTime, fmtDuration, fmtInt, stateInfo } from '@/lib/format'
import { classify, stageVolume } from '@/lib/stages'

function readParam(k: string) {
  return new URLSearchParams(location.search).get(k)
}

export default function App() {
  const now = useNow(15_000)
  const [refreshMs, setRefreshMs] = useRefreshInterval()
  const dags = useDags(refreshMs)
  const [vue, setVue] = useState<'pipeline' | 'historique'>(readParam('vue') === 'historique' ? 'historique' : 'pipeline')
  const [dagId, setDagId] = useState<string | null>(readParam('dag'))
  const [runId, setRunId] = useState<string | null>(readParam('run'))
  /** Exécution à afficher après un changement de DAG (ouverture depuis l'historique). */
  const pendingRun = useRef<string | null>(readParam('run'))
  const [replayTarget, setReplayTarget] = useState<ReplayTarget | null>(null)
  const [replayVersion, setReplayVersion] = useState(0)
  const [taskId, setTaskId] = useState<string | null>(null)
  const [sheetOpen, setSheetOpen] = useState(false)
  const [gap, setGap] = useState<1 | 2 | null>(null)
  const dark = useTheme()
  const config = useLensConfig()
  const airflowUrl = config?.airflowUrl ?? null

  // Notifications automatiques : nouvelle exécution, fin en succès ou en échec, nouvel essai.
  const lastStates = useRef<ReturnType<typeof snapshotStates> | null>(null)
  useEffect(() => {
    if (!dags.data) return
    for (const t of transitions(lastStates.current, dags.data)) notifyTransition(t.dag_id, t.run, t.from === 'nouvelle')
    lastStates.current = snapshotStates(dags.data)
  }, [dags.data])

  useEffect(() => {
    if (dags.data?.length && (!dagId || !dags.data.some((d) => d.dag_id === dagId))) setDagId(dags.data[0].dag_id)
  }, [dags.data, dagId])

  useEffect(() => {
    setRunId(pendingRun.current)
    pendingRun.current = null
    setTaskId(null)
    setSheetOpen(false)
    setGap(null)
  }, [dagId])

  // En vue Historique, le détail du DAG n'est plus actualisé (moins d'appels à Airflow).
  const bundle = useDagBundle(dagId, vue === 'pipeline' ? refreshMs : 0)
  const dag = dags.data?.find((d) => d.dag_id === dagId) ?? null
  const runs = bundle.data?.runs ?? []
  const run = runs.find((r) => r.dag_run_id === runId) ?? runs[0] ?? null
  const stages = useMemo(() => classify(bundle.data?.tasks ?? []), [bundle.data?.tasks])
  const instances = useMemo(() => {
    const m: Record<string, TaskInstance> = {}
    for (const ti of (run && bundle.data?.instances[run.dag_run_id]) || []) m[ti.task_id] = ti
    return m
  }, [run, bundle.data?.instances])
  const runVolumes = run ? bundle.data?.volumes[run.dag_run_id] : undefined
  const details = useRunDetails(dagId, run?.dag_run_id ?? null, vue === 'pipeline' ? refreshMs : 0)

  const openTask = (id: string) => {
    setGap(null)
    setTaskId(id)
    setSheetOpen(true)
  }
  const openTaskOfRun = (rid: string, id: string) => {
    setRunId(rid)
    openTask(id)
  }

  // Adresse partageable : vue, DAG et exécution affichés.
  useEffect(() => {
    const p = new URLSearchParams()
    if (vue === 'historique') p.set('vue', 'historique')
    else {
      if (dagId) p.set('dag', dagId)
      if (runId) p.set('run', runId)
    }
    history.replaceState(null, '', `?${p}`)
  }, [vue, dagId, runId])

  const openRun = useCallback((d: string, r: string) => {
    setVue('pipeline')
    if (d === dagId) setRunId(r)
    else { pendingRun.current = r; setDagId(d) }
    window.scrollTo({ top: 0 })
  }, [dagId])

  const refreshAll = () => { dags.refresh(); bundle.refresh(); details.refresh() }
  const dagGate = (r: DagRun) => replayGate(dag ?? undefined, r, config)
  const replayRun = (r: DagRun, task?: string) => {
    if (!dag || !bundle.data) return
    setSheetOpen(false)
    setReplayTarget({ run: { ...r, dag_id: dag.dag_id }, tasks: bundle.data.tasks, taskId: task })
  }

  return (
    <div className="min-h-dvh bg-background text-foreground">
      <a href="#contenu" className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-50 focus:rounded focus:bg-card focus:px-3 focus:py-2">
        Aller au contenu
      </a>
      <header className="sticky top-0 z-30 border-b bg-background/90 backdrop-blur">
        <div className="mx-auto flex h-14 max-w-[1440px] items-center gap-3 px-4 lg:px-6">
          <LensMark />
          <p className="text-[0.95rem] font-semibold tracking-tight whitespace-nowrap">Pipeline Lens</p>
          {DEMO ? (
            <span className="hidden rounded-full border px-2 py-0.5 text-xs text-muted-foreground sm:inline-flex">Instantané figé</span>
          ) : (
            <ConnectionStatus status={dags.status} updatedAt={dags.updatedAt} />
          )}
          <div className="ml-auto flex items-center gap-1">
            {!DEMO && <RefreshSelect value={refreshMs} onChange={setRefreshMs} />}
            {!DEMO && <Button variant="ghost" size="icon" aria-label="Actualiser maintenant" onClick={refreshAll}>
              <RefreshCwIcon aria-hidden className={bundle.status === 'loading' ? 'motion-safe:animate-spin' : undefined} />
            </Button>}
            <Button variant="ghost" size="icon" aria-label={dark.value ? 'Passer en thème clair' : 'Passer en thème sombre'} onClick={dark.toggle}>
              {dark.value ? <SunIcon aria-hidden /> : <MoonIcon aria-hidden />}
            </Button>
          </div>
        </div>
        <div className="mx-auto flex max-w-[1440px] items-center px-4 pb-2 lg:px-6">
          <Tabs value={vue} onValueChange={(v) => setVue(v as typeof vue)}>
            <TabsList aria-label="Vue">
              <TabsTrigger value="pipeline" className="px-3"><WorkflowIcon aria-hidden />Pipeline</TabsTrigger>
              <TabsTrigger value="historique" className="px-3"><HistoryIcon aria-hidden />Historique</TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
      </header>
      {DEMO && <DemoBanner />}

      {vue === 'historique' ? (
        <main id="contenu" className="mx-auto max-w-[1440px] px-4 py-5 lg:px-6 lg:py-7">
          {dags.status === 'error' && !dags.data ? (
            <ErrorPanel error={dags.error} onRetry={dags.refresh} />
          ) : !dags.data ? (
            <PipelineSkeleton />
          ) : (
            <motion.div initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.2 }}>
              <HistoryView dags={dags.data} now={now} refreshMs={refreshMs} config={config} onOpenRun={openRun} onReplay={setReplayTarget} version={replayVersion} />
            </motion.div>
          )}
        </main>
      ) : (
      <div className="mx-auto grid max-w-[1440px] gap-5 px-4 py-5 lg:grid-cols-[260px_minmax(0,1fr)] lg:gap-8 lg:px-6 lg:py-7">
        <aside className="min-w-0 lg:sticky lg:top-20 lg:self-start">
          <h2 className="mb-2 text-xs font-medium text-muted-foreground">DAG planifiés</h2>
          {dags.status === 'loading' && !dags.data ? (
            <div className="flex gap-2 lg:flex-col">{[0, 1].map((i) => <div key={i} className="h-[72px] w-60 animate-pulse rounded-lg bg-muted lg:w-full" />)}</div>
          ) : dags.data?.length ? (
            <DagList dags={dags.data} selected={dagId} onSelect={setDagId} now={now} />
          ) : null}
        </aside>

        <main id="contenu" className="min-w-0">
          {dags.status === 'error' && !dags.data ? (
            <ErrorPanel error={dags.error} onRetry={dags.refresh} />
          ) : dags.data && dags.data.length === 0 ? (
            <EmptyDags />
          ) : !dag ? (
            <PipelineSkeleton />
          ) : (
            <AnimatePresence mode="wait">
              <motion.div
                key={dag.dag_id}
                initial={{ opacity: 0, y: 6 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.2 }}
                className="space-y-6"
              >
                <section className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2.5">
                      <h1 className="text-2xl font-semibold tracking-tight text-balance">{dagTitle(dag)}</h1>
                      {dag.is_paused ? <span className="rounded-full border px-2.5 py-0.5 text-xs text-wait">En pause</span> : <StatePill state={dag.lastRun?.state} />}
                    </div>
                    <p className="mt-1 font-mono text-xs text-muted-foreground">{dag.dag_id}</p>
                    {dag.description && <p className="mt-2 max-w-[68ch] text-sm text-muted-foreground">{dag.description}</p>}
                  </div>
                  <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:flex sm:gap-8">
                    <Stat icon={<CalendarClockIcon aria-hidden className="size-4" />} label="Planification"
                      value={cronToFrench(dag.timetable_summary) ?? dag.timetable_description ?? 'Manuelle'} />
                    <Stat icon={<TimerIcon aria-hidden className="size-4" />} label="Prochaine exécution"
                      value={dag.is_paused ? 'en pause' : fmtCountdown(dag.next_dagrun_run_after, now)}
                      sub={dag.is_paused ? undefined : fmtDateTime(dag.next_dagrun_run_after)} />
                  </dl>
                </section>

                {bundle.data && (
                  <IncidentBanner
                    runs={runs}
                    instances={bundle.data.instances}
                    selectedRunId={run?.dag_run_id ?? null}
                    now={now}
                    onSelectRun={setRunId}
                    onOpenTask={openTaskOfRun}
                  />
                )}

                {bundle.status === 'error' && !bundle.data ? (
                  <ErrorPanel error={bundle.error} onRetry={bundle.refresh} />
                ) : !bundle.data ? (
                  <PipelineSkeleton />
                ) : (
                  <>
                    <section className="rounded-lg border bg-card p-4" aria-label="Historique des exécutions">
                      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
                        <h2 className="text-sm font-semibold">Historique</h2>
                        {bundle.status === 'error' && <p className="text-xs text-ko">Actualisation impossible : données affichées non à jour.</p>}
                      </div>
                      <RunHistory runs={runs} selectedRunId={run?.dag_run_id ?? null} onSelect={setRunId} />
                    </section>

                    {run ? (
                      <section aria-label="Exécution sélectionnée" className="space-y-3">
                        <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5">
                          <h2 className="text-base font-semibold">Exécution du {fmtDateTime(run.start_date ?? run.run_after)}</h2>
                          <StatePill state={run.state} />
                          <span className="text-sm text-muted-foreground">
                            {runTypeLabel(run.run_type)}
                            {' · '}{fmtDuration(run.duration)}
                          </span>
                          <FunnelSummary stages={stages} volumes={runVolumes} />
                          {!DEMO && <ReplayButton gate={dagGate(run)} onClick={() => replayRun(run)} />}
                        </div>
                        <Pipeline
                          stages={stages}
                          instances={instances}
                          volumes={runVolumes}
                          selectedTask={sheetOpen ? taskId : null}
                          onSelectTask={openTask}
                          runKey={run.dag_run_id}
                          onOpenGap={(g) => { setSheetOpen(false); setGap(g) }}
                          sansPerte={gapsSansPerte(stages, details.data)}
                        />
                        <p className="text-xs text-muted-foreground">
                          Cliquez sur une tâche pour sa durée, ses essais, son volume et son journal, ou sur un écart pour son explication. « Non mesuré » : la tâche ne publie pas son nombre de lignes.
                        </p>
                        <QualityPanel run={run} runs={runs} stages={stages} volumes={bundle.data.volumes} details={details} now={now} />
                      </section>
                    ) : (
                      <section className="space-y-3">
                        <p className="text-sm text-muted-foreground">Structure du DAG, en attente de sa première exécution :</p>
                        <Pipeline stages={stages} instances={{}} volumes={undefined} selectedTask={null} onSelectTask={openTask} runKey="none" />
                      </section>
                    )}
                  </>
                )}
              </motion.div>
            </AnimatePresence>
          )}
        </main>
      </div>
      )}

      {DEMO && (
        <footer className="mx-auto max-w-[1440px] border-t px-4 py-6 text-xs text-muted-foreground lg:px-6">
          <p>Pipeline Lens, une vue de suivi pour Apache Airflow : application indépendante, non affiliée à l’Apache Software Foundation.</p>
          <p className="mt-1">Cédric NANA — Expert BI & Data | Analytics & AI Automation.</p>
        </footer>
      )}

      {dag && bundle.data && (
        <TaskSheet
          open={sheetOpen}
          onOpenChange={setSheetOpen}
          dagId={dag.dag_id}
          taskId={taskId}
          run={run}
          ti={taskId ? instances[taskId] : undefined}
          runs={runs}
          volumes={bundle.data.volumes}
          airflowUrl={airflowUrl}
          replay={DEMO || !run ? undefined : (() => {
            const g = dagGate(run)
            return { onReplay: () => replayRun(run, taskId ?? undefined), reason: g.ok ? null : g.reason }
          })()}
        />
      )}
      <ReplayDialog target={replayTarget} onOpenChange={(o) => { if (!o) setReplayTarget(null) }}
        onLaunched={() => setReplayVersion((v) => v + 1)} onDone={() => { refreshAll(); setReplayVersion((v) => v + 1) }} />
      <Toaster theme={dark.value ? 'dark' : 'light'} position="bottom-right" closeButton />
      {dag && bundle.data && run && gap && (
        <GapSheet
          open={gap != null}
          onOpenChange={(o) => { if (!o) setGap(null) }}
          gap={gap}
          stages={stages}
          volumes={runVolumes}
          details={details}
          run={run}
          onOpenTask={openTask}
        />
      )}
    </div>
  )
}

/** Notification d'un changement d'état détecté à l'actualisation (aria-live géré par Sonner). */
function notifyTransition(dagId: string, run: DagRun, nouvelle: boolean) {
  const label = stateInfo(run.state).label
  const description = `${fmtDateTime(run.start_date ?? run.run_after)}${run.duration != null ? ` · ${fmtDuration(run.duration)}` : ''}`
  if (run.state === 'failed') toast.error(`${dagId} : exécution en échec`, { description, duration: 10_000 })
  else if (run.state === 'success') toast.success(`${dagId} : exécution réussie`, { description })
  else if (run.state === 'running') toast.info(`${dagId} : exécution en cours`, { description })
  else if (nouvelle) toast.info(`${dagId} : nouvelle exécution (${label.toLowerCase()})`, { description })
  else toast(`${dagId} : ${label.toLowerCase()}`, { description })
}

function ReplayButton({ gate, onClick }: { gate: ReturnType<typeof replayGate>; onClick: () => void }) {
  return (
    <TooltipProvider delay={150}>
      <Tooltip>
        <TooltipTrigger render={
          <Button size="sm" variant="outline" className={gate.ok ? 'ml-auto' : 'ml-auto cursor-not-allowed opacity-45'} aria-disabled={!gate.ok}
            onClick={() => { if (gate.ok) onClick() }}
            aria-label={gate.ok ? 'Rejouer cette exécution' : `Rejeu indisponible : ${gate.reason}`}>
            <RotateCcwIcon aria-hidden />Rejouer
          </Button>
        } />
        <TooltipContent className="max-w-xs">{gate.ok ? 'Simuler puis confirmer le rejeu' : gate.reason}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

function DemoBanner() {
  const s = snapshotInfo()
  if (!s) return null
  return (
    <div className="border-b bg-accent/60">
      <p className="mx-auto max-w-[1440px] px-4 py-2 text-xs text-accent-foreground lg:px-6">
        Démonstration : instantané figé d’un Airflow {s.airflowVersion} local, pris le {fmtDateTime(s.generatedAt)}.
        Données synthétiques, générées par les DAG du projet ; les volumes sont ceux publiés par les tâches.
      </p>
    </div>
  )
}

function FunnelSummary({ stages, volumes }: { stages: ReturnType<typeof classify>; volumes: Record<string, number> | undefined }) {
  const e = stageVolume(stages.extract, volumes)
  const l = stageVolume(stages.load, volumes)
  if (e == null || l == null) return null
  const kept = e ? (l / e) * 100 : 0
  return (
    <span className="tabular text-sm text-muted-foreground">
      {fmtInt(e)} lues, {fmtInt(l)} chargées ({kept.toFixed(1).replace('.', ',')} %)
    </span>
  )
}

function Stat({ icon, label, value, sub }: { icon: ReactNode; label: string; value: string; sub?: string }) {
  return (
    <div>
      <dt className="flex items-center gap-1.5 text-xs text-muted-foreground">{icon}{label}</dt>
      <dd className="font-medium">{value}</dd>
      {sub && <dd className="text-xs text-muted-foreground">{sub}</dd>}
    </div>
  )
}

/** État de la connexion et fraîcheur : heure de la dernière synchronisation réussie. */
function ConnectionStatus({ status, updatedAt }: { status: 'loading' | 'success' | 'error'; updatedAt: number | null }) {
  const [, tick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => tick((t) => t + 1), 5_000)
    return () => clearInterval(id)
  }, [])
  const age = updatedAt ? Math.max(0, Math.round((Date.now() - updatedAt) / 1000)) : null
  const ago = age == null ? '' : age < 5 ? 'à l’instant' : age < 60 ? `il y a ${age} s` : `il y a ${Math.round(age / 60)} min`
  const map = {
    loading: { c: 'bg-wait', t: updatedAt ? `Synchronisation… (dernière ${ago})` : 'Connexion à Airflow…', court: '…' },
    success: { c: 'bg-ok', t: `Synchronisé ${ago}`, court: age == null ? '' : age < 60 ? `${age} s` : `${Math.round(age / 60)} min` },
    error: { c: 'bg-ko', t: updatedAt ? `Airflow injoignable · données d’${ago}` : 'Airflow injoignable', court: 'Injoignable' },
  }[status]
  return (
    <span
      className="inline-flex max-w-[52vw] shrink-0 items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs text-muted-foreground"
      role="status"
      title={updatedAt ? `Dernière synchronisation réussie : ${new Date(updatedAt).toLocaleTimeString('fr-FR')}` : undefined}
    >
      <span className={`size-1.5 shrink-0 rounded-full ${map.c}`} aria-hidden />
      <span className="truncate sm:hidden">{map.court}</span>
      <span className="hidden truncate sm:inline">{map.t}</span>
    </span>
  )
}

function RefreshSelect({ value, onChange }: { value: number; onChange: (v: number) => void }) {
  return (
    <label className="hidden items-center gap-1.5 text-xs text-muted-foreground sm:flex">
      <span className="hidden md:inline">Actualisation</span>
      <select
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        aria-label="Fréquence d’actualisation"
        className="h-8 rounded-md border bg-background px-2 text-xs text-foreground focus-visible:outline-2 focus-visible:outline-ring"
      >
        {REFRESH_CHOICES.map((c) => <option key={c.ms} value={c.ms}>{c.label}</option>)}
      </select>
    </label>
  )
}

/** Marque : une lentille traversée par les trois étapes. */
function LensMark() {
  return (
    <svg viewBox="0 0 28 28" className="size-7 shrink-0" aria-hidden>
      <circle cx="14" cy="14" r="11" fill="none" stroke="var(--flow)" strokeWidth="2" />
      <path d="M5 14h4.5M11.75 14h4.5M18.5 14H23" stroke="var(--foreground)" strokeWidth="2.4" strokeLinecap="round" />
    </svg>
  )
}

function useTheme() {
  const [value, setValue] = useState<boolean>(() => {
    try {
      const s = localStorage.getItem('lens-theme')
      if (s) return s === 'dark'
    } catch { /* stockage indisponible */ }
    return window.matchMedia('(prefers-color-scheme: dark)').matches
  })
  useEffect(() => {
    document.documentElement.classList.toggle('dark', value)
    try { localStorage.setItem('lens-theme', value ? 'dark' : 'light') } catch { /* ignoré */ }
  }, [value])
  return { value, toggle: () => setValue((v) => !v) }
}
