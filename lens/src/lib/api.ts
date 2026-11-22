import type { IncidentEvent } from './incidents'
// Accès aux données via le proxy local (/lens/*). Lecture, plus une seule action : le rejeu contrôlé.

export type State =
  | 'success' | 'failed' | 'running' | 'queued' | 'scheduled' | 'upstream_failed'
  | 'skipped' | 'up_for_retry' | 'up_for_reschedule' | 'deferred' | 'removed' | 'restarting' | null

export interface Dag {
  dag_id: string
  dag_display_name: string
  description: string | null
  is_paused: boolean
  timetable_summary: string | null
  timetable_description: string | null
  next_dagrun_run_after: string | null
  tags: { name: string }[]
  has_import_errors: boolean
}

export interface DagRun {
  dag_run_id: string
  state: State
  run_type: string
  start_date: string | null
  end_date: string | null
  run_after: string
  duration: number | null
  conf: Record<string, unknown> | null
  triggered_by?: string
}

export interface Task {
  task_id: string
  task_display_name: string
  downstream_task_ids: string[]
  operator_name: string | null
  doc_md?: string | null
}

export interface TaskInstance {
  task_id: string
  dag_run_id: string
  state: State
  start_date: string | null
  end_date: string | null
  duration: number | null
  try_number: number
  max_tries: number
  operator_name: string | null
}

/** run_id → task_id → lignes publiées */
export type Volumes = Record<string, Record<string, number>>

/** Nature d'un écart de lignes, telle que déclarée par la tâche (jamais déduite par l'interface). */
export type GapNature = 'rejetee' | 'filtree' | 'dedupliquee' | 'non_chargee' | 'jointure'

export interface Breakdown {
  entree: number
  sortie: number
  ecarts: { motif: string; libelle: string; nature: GapNature; lignes: number }[]
  fichier_rejets: string | null
}

export interface QualityCheck {
  controle: string
  valeur: number
  seuil: number
  statut: 'ok' | 'ko'
}

/** Détail métier d'une exécution : par tâche, XCom « row_breakdown » et « quality_checks ». */
export interface RunDetails {
  breakdown: Record<string, Breakdown>
  checks: Record<string, QualityCheck[]>
}

/** Exécution vue depuis l'historique global (tous DAG). */
export type GlobalRun = DagRun & { dag_id: string }

export interface HistoryQuery {
  dagId?: string | null
  state?: string | null
  runType?: string | null
  /** ISO : exécutions dont run_after est postérieur */
  since?: string | null
  limit: number
  offset: number
}

export type ReplayMode = 'run' | 'failed' | 'task'
export interface ReplayBody {
  dag_id: string
  dag_run_id: string
  mode: ReplayMode
  task_id?: string
  include_downstream?: boolean
  dry_run: boolean
  confirm_token?: string
}
export interface ReplayResult {
  dry_run: boolean
  tasks: { task_id: string; state: State }[]
  confirm_token?: string
  expires_in?: number
}
export interface AuditEntry {
  ts: string
  action: 'simulation' | 'rejeu'
  dag_id: string
  dag_run_id: string
  mode: ReplayMode
  task_id: string | null
  taches?: number
  resultat?: 'lance' | 'refuse'
  motif?: string
}
export interface LensConfig {
  airflowUrl: string | null
  /** Rejeu autorisé côté serveur (LENS_ACTIONS=on et compte de rejeu présent). */
  actions: boolean
  replayableTag: string
}

export class LensError extends Error {
  code: string
  status: number
  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

async function get<T>(url: string, signal?: AbortSignal, init?: RequestInit): Promise<T> {
  let res: Response
  try {
    res = await fetch(url, { ...init, signal })
  } catch (e) {
    if ((e as Error).name === 'AbortError') throw e
    throw new LensError(0, 'proxy_down', 'Le proxy Pipeline Lens ne répond pas. Relancez « npm run dev » dans C:\\airflow\\lens.')
  }
  const body = await res.json().catch(() => ({}))
  if (!res.ok) throw new LensError(res.status, body.error ?? 'api', body.message ?? `Erreur HTTP ${res.status}`)
  return body as T
}

const enc = encodeURIComponent

/**
 * Mode démonstration (build « npm run build:demo ») : l'application lit un instantané
 * figé (data.js → window.LENS_DATA), exporté depuis un vrai Airflow par
 * scripts/exporter_instantane.py. Aucun appel réseau.
 */
export interface Snapshot {
  generatedAt: string
  airflowVersion: string
  dags: Dag[]
  recentRuns: (DagRun & { dag_id: string })[]
  perDag: Record<string, { runs: DagRun[]; tasks: Task[]; instances: TaskInstance[]; volumes: Volumes }>
  logs: Record<string, string[]>
  /** dag_id → run_id → détail métier (absent des anciens instantanés) */
  details?: Record<string, Record<string, RunDetails>>
}

export const DEMO = import.meta.env.VITE_DEMO === '1'
const snap = (): Snapshot => (window as unknown as { LENS_DATA: Snapshot }).LENS_DATA
export const snapshotTime = () => (DEMO ? new Date(snap().generatedAt).getTime() : null)
export const snapshotInfo = () => (DEMO ? snap() : null)

const demo = {
  dags: async () => snap().dags,
  runs: async (dagId: string) => snap().perDag[dagId]?.runs ?? [],
  recentRuns: async () => snap().recentRuns,
  tasks: async (dagId: string) => snap().perDag[dagId]?.tasks ?? [],
  taskInstances: async (dagId: string) => snap().perDag[dagId]?.instances ?? [],
  volumes: async (dagId: string) => snap().perDag[dagId]?.volumes ?? {},
  log: async (dagId: string, runId: string, taskId: string, tryNumber: number) =>
    snap().logs[`${dagId}|${runId}|${taskId}|${tryNumber}`] ?? [],
  runDetails: async (dagId: string, runId: string): Promise<RunDetails> =>
    snap().details?.[dagId]?.[runId] ?? { breakdown: {}, checks: {} },
  config: async (): Promise<LensConfig> => ({ airflowUrl: null, actions: false, replayableTag: 'rejouable' }),
  history: async (q: HistoryQuery) => {
    const all = snap().recentRuns.filter((r) =>
      (!q.dagId || r.dag_id === q.dagId) && (!q.state || r.state === q.state) &&
      (!q.runType || r.run_type === q.runType) && (!q.since || r.run_after >= q.since))
    return { runs: all.slice(q.offset, q.offset + q.limit), total: all.length }
  },
  failedTasks: async () => Object.entries(snap().perDag).flatMap(([dag_id, p]) =>
    p.instances.filter((t) => t.state === 'failed').map((t) => ({ ...t, dag_id }))),
  allTasks: async (dagId: string) => snap().perDag[dagId]?.tasks ?? [],
  replay: async (): Promise<ReplayResult> => {
    throw new LensError(403, 'actions_off', 'Démonstration statique : le rejeu n’est disponible que sur une installation reliée à Airflow.')
  },
  audit: async (): Promise<AuditEntry[]> => [],
  incidents: async (): Promise<IncidentEvent[]> => [],
  run: async (dagId: string, runId: string) => snap().perDag[dagId]?.runs.find((r) => r.dag_run_id === runId) ?? null,
}

const live = {
  dags: (s?: AbortSignal) =>
    get<{ dags: Dag[] }>('/lens/api/dags?limit=100&order_by=dag_id', s).then((r) => r.dags),
  runs: (dagId: string, s?: AbortSignal) =>
    get<{ dag_runs: DagRun[] }>(`/lens/api/dags/${enc(dagId)}/dagRuns?limit=25&order_by=-run_after`, s).then((r) => r.dag_runs),
  /** Dernières exécutions, tous DAG confondus (pour l'état de chaque DAG dans la liste). */
  recentRuns: (s?: AbortSignal) =>
    get<{ dag_runs: (DagRun & { dag_id: string })[] }>('/lens/api/dags/~/dagRuns?limit=200&order_by=-run_after', s).then((r) => r.dag_runs),
  tasks: (dagId: string, s?: AbortSignal) =>
    get<{ tasks: Task[] }>(`/lens/api/dags/${enc(dagId)}/tasks`, s).then((r) => r.tasks),
  taskInstances: (dagId: string, runId: string, s?: AbortSignal) =>
    get<{ task_instances: TaskInstance[] }>(
      `/lens/api/dags/${enc(dagId)}/dagRuns/${enc(runId)}/taskInstances?limit=1000&order_by=-run_after`, s,
    ).then((r) => r.task_instances),
  volumes: (dagId: string, s?: AbortSignal) => get<Volumes>(`/lens/volumes?dag_id=${enc(dagId)}`, s),
  log: (dagId: string, runId: string, taskId: string, tryNumber: number, s?: AbortSignal) =>
    get<{ lines: string[] }>(
      `/lens/log?dag_id=${enc(dagId)}&run_id=${enc(runId)}&task_id=${enc(taskId)}&try=${tryNumber}`, s,
    ).then((r) => r.lines),
  runDetails: (dagId: string, runId: string, s?: AbortSignal) =>
    get<RunDetails>(`/lens/run-details?dag_id=${enc(dagId)}&run_id=${enc(runId)}`, s),
  config: (s?: AbortSignal) => get<LensConfig>('/lens/config', s),
  /** Historique de toutes les exécutions, filtré côté Airflow et paginé. */
  history: (q: HistoryQuery, s?: AbortSignal) => {
    const p = new URLSearchParams({ limit: String(q.limit), offset: String(q.offset), order_by: '-run_after' })
    if (q.state) p.append('state', q.state)
    if (q.runType) p.append('run_type', q.runType)
    if (q.since) p.set('run_after_gte', q.since)
    return get<{ dag_runs: GlobalRun[]; total_entries: number }>(`/lens/api/dags/${q.dagId ? enc(q.dagId) : '~'}/dagRuns?${p}`, s)
      .then((r) => ({ runs: r.dag_runs, total: r.total_entries }))
  },
  /** Tâches en échec, tous DAG confondus (pour l'historique). */
  failedTasks: (s?: AbortSignal) =>
    get<{ task_instances: (TaskInstance & { dag_id: string })[] }>('/lens/api/dags/~/dagRuns/~/taskInstances?state=failed&limit=1000', s)
      .then((r) => r.task_instances),
  allTasks: (dagId: string, s?: AbortSignal) => get<{ tasks: Task[] }>(`/lens/api/dags/${enc(dagId)}/tasks`, s).then((r) => r.tasks),
  run: (dagId: string, runId: string, s?: AbortSignal) => get<GlobalRun>(`/lens/api/dags/${enc(dagId)}/dagRuns/${enc(runId)}`, s),
  /** Rejeu contrôlé : simulation (dry_run) puis confirmation avec le jeton reçu. */
  replay: (body: ReplayBody) =>
    get<ReplayResult>('/lens/actions/replay', undefined, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Lens-Action': '1' },
      body: JSON.stringify(body),
    }),
  audit: (s?: AbortSignal) => get<{ entries: AuditEntry[] }>('/lens/actions/audit?limit=30', s).then((r) => r.entries),
  incidents: (s?: AbortSignal) => get<{ events: IncidentEvent[] }>('/lens/incidents', s).then((r) => r.events),
}

export const lens: typeof live = DEMO ? (demo as unknown as typeof live) : live
