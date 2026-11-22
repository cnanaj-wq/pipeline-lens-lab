import { useCallback, useEffect, useRef, useState } from 'react'
import { DEMO, lens, snapshotTime, type Dag, type DagRun, type Task, type RunDetails, type TaskInstance, type Volumes } from '@/lib/api'

export type Async<T> =
  | { status: 'loading'; data?: T; error?: undefined }
  | { status: 'success'; data: T; error?: undefined }
  | { status: 'error'; data?: T; error: Error }

/** Charge une ressource, puis la rafraîchit à intervalle régulier (sans écran blanc). */
export function usePolled<T>(load: (s: AbortSignal) => Promise<T>, deps: unknown[], intervalMs = 15_000) {
  const [state, setState] = useState<Async<T>>({ status: 'loading' })
  /** Heure de la dernière synchronisation réussie avec Airflow (ms). */
  const [updatedAt, setUpdatedAt] = useState<number | null>(null)
  const [tick, setTick] = useState(0)
  const loadRef = useRef(load)
  loadRef.current = load

  useEffect(() => {
    setState({ status: 'loading' })
  }, deps) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const ctrl = new AbortController()
    loadRef.current(ctrl.signal)
      .then((data) => {
        setState({ status: 'success', data })
        setUpdatedAt(Date.now())
      })
      .catch((error: Error) => {
        if (error.name === 'AbortError') return
        setState((prev) => ({ status: 'error', data: prev.data, error }))
      })
    return () => ctrl.abort()
  }, [...deps, tick]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!intervalMs) return
    const id = setInterval(() => {
      if (document.visibilityState === 'visible') setTick((t) => t + 1)
    }, intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])

  const refresh = useCallback(() => setTick((t) => t + 1), [])
  return { ...state, refresh, updatedAt }
}

/** Intervalle d'actualisation choisi par l'utilisateur (0 = désactivée), mémorisé dans le navigateur. */
export const REFRESH_CHOICES = [
  { ms: 10_000, label: '10 s' },
  { ms: 30_000, label: '30 s' },
  { ms: 60_000, label: '1 min' },
  { ms: 0, label: 'Désactivée' },
] as const

export function useRefreshInterval() {
  const [ms, setMs] = useState<number>(() => {
    if (DEMO) return 0
    try {
      const v = Number(localStorage.getItem('lens-refresh'))
      if (REFRESH_CHOICES.some((c) => c.ms === v) && localStorage.getItem('lens-refresh') !== null) return v
    } catch { /* stockage indisponible */ }
    return 10_000
  })
  const choose = useCallback((v: number) => {
    setMs(v)
    try { localStorage.setItem('lens-refresh', String(v)) } catch { /* ignoré */ }
  }, [])
  return [ms, choose] as const
}

export interface DagBundle {
  runs: DagRun[]
  tasks: Task[]
  volumes: Volumes
  /** instances par run_id, pour la bande d'historique */
  instances: Record<string, TaskInstance[]>
}

export function useDagBundle(dagId: string | null, intervalMs = 10_000) {
  return usePolled<DagBundle | null>(
    async (s) => {
      if (!dagId) return null
      const [runs, tasks, volumes, all] = await Promise.all([
        lens.runs(dagId, s),
        lens.tasks(dagId, s),
        lens.volumes(dagId, s),
        lens.taskInstances(dagId, '~', s), // toutes les exécutions en un appel
      ])
      const instances: Record<string, TaskInstance[]> = {}
      for (const ti of all) (instances[ti.dag_run_id] ??= []).push(ti)
      return { runs, tasks, volumes, instances }
    },
    [dagId],
    intervalMs,
  )
}

export interface DagSummary extends Dag {
  lastRun: DagRun | null
}

export function useDags(intervalMs = 15_000) {
  return usePolled<DagSummary[]>(
    async (s) => {
      const [dags, recent] = await Promise.all([lens.dags(s), lens.recentRuns(s)])
      return dags.map((d) => ({ ...d, lastRun: recent.find((r) => r.dag_id === d.dag_id) ?? null }))
    },
    [],
    intervalMs,
  )
}

export function useNow(ms = 30_000) {
  // En démonstration, le temps est figé à l'heure de l'instantané (comptes à rebours cohérents).
  const fixed = snapshotTime()
  const [now, setNow] = useState(() => fixed ?? Date.now())
  useEffect(() => {
    if (fixed != null) return
    const id = setInterval(() => setNow(Date.now()), ms)
    return () => clearInterval(id)
  }, [ms, fixed])
  return now
}

/** Écarts documentés et contrôles qualité d'une exécution (XCom publiés par les tâches). */
export function useRunDetails(dagId: string | null, runId: string | null, intervalMs = 10_000) {
  return usePolled<RunDetails | null>(
    async (s) => (dagId && runId ? lens.runDetails(dagId, runId, s) : null),
    [dagId, runId],
    intervalMs,
  )
}
