/**
 * Serveur Pipeline Lens : relais vers l'API REST v2 d'Airflow 3, indépendant de Vite (donc testable).
 *
 * Lecture : compte « lens_lecture » (rôle viewer). Le navigateur n'appelle que /lens/* :
 * pas de CORS, aucun identifiant ni jeton envoyé au navigateur.
 *
 * Écriture : une seule opération, le rejeu (« clear ») d'une exécution ou d'une tâche, avec le
 * compte « lens_ops » (rôle op) et des garde-fous cumulés :
 *   1. LENS_ACTIONS=on dans .env (sinon 403) ;
 *   2. en-tête « X-Lens-Action: 1 » (une page tierce ne peut pas l'envoyer sans pré-vol CORS) ;
 *   3. le DAG porte le tag « rejouable » : son auteur garantit que ses tâches sont idempotentes ;
 *   4. une simulation (dry_run) précède toujours le rejeu ; elle renvoie la liste des tâches touchées
 *      et un jeton de confirmation valable 2 minutes, lié exactement à cette demande ;
 *   5. pas de rejeu d'une exécution encore en cours ; une action par exécution toutes les 5 s ;
 *   6. chaque simulation et chaque rejeu est inscrit dans un journal d'audit (JSON Lines).
 */
import { createHash, randomUUID } from 'node:crypto'
import { appendFile, mkdir, readFile } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { IncomingMessage, ServerResponse } from 'node:http'

export type Env = Record<string, string | undefined>
export type Handler = (req: IncomingMessage, res: ServerResponse, next: () => void) => void

export const REPLAYABLE_TAG = 'rejouable'

export interface ServerOptions {
  /** Délai maximal d'une requête vers Airflow (ms). */
  timeoutMs?: number
  /** Fichier du journal d'audit des actions. */
  auditFile?: string
  /** Journal des incidents écrit par les DAG (logs/incidents/incidents.jsonl), lu seulement. */
  incidentsFile?: string
  now?: () => number
}

export class HttpError extends Error {
  status: number
  code: string
  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

type Mode = 'run' | 'failed' | 'task'
interface ReplayRequest {
  dag_id: string
  dag_run_id: string
  mode: Mode
  task_id?: string
  include_downstream?: boolean
  dry_run: boolean
  confirm_token?: string
}

export function createLensHandler(env: Env, opts: ServerOptions = {}): Handler {
  const base = (env.AIRFLOW_URL || 'http://localhost:8085').replace(/\/$/, '')
  const timeoutMs = opts.timeoutMs ?? Number(env.LENS_TIMEOUT_MS || 8000)
  const now = opts.now ?? Date.now
  const auditFile = opts.auditFile ?? env.LENS_AUDIT_FILE
  const incidentsFile = opts.incidentsFile ?? env.LENS_INCIDENTS_FILE
  const actionsOn = (env.LENS_ACTIONS ?? 'off').toLowerCase() === 'on'
  const accounts = {
    read: { user: env.AIRFLOW_LENS_USER, password: env.AIRFLOW_LENS_PASSWORD },
    ops: { user: env.AIRFLOW_LENS_OPS_USER, password: env.AIRFLOW_LENS_OPS_PASSWORD },
  }
  const tokens: Record<'read' | 'ops', { value: string; at: number } | null> = { read: null, ops: null }
  /** Demande de jeton en cours : les requêtes simultanées partagent la même authentification. */
  const inflight: Record<'read' | 'ops', Promise<string> | null> = { read: null, ops: null }
  /**
   * Valeurs XCom déjà lues. La clé inclut l'horodatage de l'XCom : après un rejeu, la tâche
   * republie sa valeur et l'ancienne n'est plus servie. On garde la promesse : des requêtes
   * simultanées partagent le même appel à Airflow.
   */
  const xcomCache = new Map<string, Promise<unknown>>()
  function xcomValue(cacheKey: string, path: string): Promise<unknown> {
    let p = xcomCache.get(cacheKey)
    if (!p) {
      p = api<{ value: unknown }>(path).then((r) => r.value)
      p.catch(() => xcomCache.delete(cacheKey))
      xcomCache.set(cacheKey, p)
      if (xcomCache.size > 20_000) xcomCache.delete(xcomCache.keys().next().value!)
    }
    return p
  }
  const pending = new Map<string, { key: string; expires: number; tasks: number }>()
  const lastAction = new Map<string, number>()
  const enc = encodeURIComponent

  async function call(url: string, init: RequestInit = {}): Promise<Response> {
    try {
      return await fetch(url, { ...init, signal: AbortSignal.timeout(timeoutMs) })
    } catch (e) {
      const name = (e as Error).name
      if (name === 'TimeoutError' || name === 'AbortError')
        throw new HttpError(504, 'timeout', `Airflow n'a pas répondu en ${Math.round(timeoutMs / 1000)} s.`)
      throw new HttpError(502, 'unreachable', `Airflow ne répond pas sur ${base}.`)
    }
  }

  async function getToken(who: 'read' | 'ops', force = false): Promise<string> {
    const { user, password } = accounts[who]
    if (!user || !password)
      throw new HttpError(500, 'config', who === 'read'
        ? 'Identifiants de lecture absents de C:\\airflow\\.env (AIRFLOW_LENS_USER / AIRFLOW_LENS_PASSWORD).'
        : 'Compte de rejeu absent de C:\\airflow\\.env (AIRFLOW_LENS_OPS_USER / AIRFLOW_LENS_OPS_PASSWORD).')
    const t = tokens[who]
    if (t && !force && now() - t.at < 30 * 60_000) return t.value
    if (inflight[who]) return inflight[who]!
    const p = authenticate(who, user, password)
    inflight[who] = p
    try { return await p } finally { inflight[who] = null }
  }

  async function authenticate(who: 'read' | 'ops', user: string, password: string): Promise<string> {
    const res = await call(`${base}/auth/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: user, password }),
    })
    if (res.status === 401 || res.status === 403) throw new HttpError(401, 'auth', 'Identifiants refusés par Airflow.')
    if (!res.ok) throw new HttpError(502, 'auth', `Échec d'authentification (HTTP ${res.status}).`)
    const value = ((await res.json()) as { access_token: string }).access_token
    tokens[who] = { value, at: now() }
    return value
  }

  async function api<T = unknown>(path: string, init: { method?: string; body?: unknown; who?: 'read' | 'ops' } = {}, retry = true): Promise<T> {
    const who = init.who ?? 'read'
    const t = await getToken(who)
    const res = await call(`${base}/api/v2${path}`, {
      method: init.method ?? 'GET',
      headers: { Authorization: `Bearer ${t}`, ...(init.body ? { 'Content-Type': 'application/json' } : {}) },
      body: init.body ? JSON.stringify(init.body) : undefined,
    })
    if (res.status === 401 && retry) {
      await getToken(who, true)
      return api<T>(path, init, false)
    }
    if (res.status === 403) throw new HttpError(403, 'forbidden', 'Droits insuffisants dans Airflow pour cette opération.')
    if (res.status === 404) throw new HttpError(404, 'not_found', 'Ressource introuvable dans Airflow.')
    if (!res.ok) {
      const detail = await res.json().then((b) => {
        const d = (b as { detail?: unknown }).detail
        return typeof d === 'string' ? ` : ${d}` : ''
      }).catch(() => '')
      throw new HttpError(res.status >= 500 ? 502 : res.status, 'api', `Airflow a répondu HTTP ${res.status}${detail}.`)
    }
    return (await res.json()) as T
  }

  /** Volumes publiés (XCom « row_count ») pour les dernières exécutions d'un DAG. */
  async function volumes(dagId: string) {
    const list = await api<{ xcom_entries: { task_id: string; run_id: string; map_index: number; timestamp?: string }[] }>(
      `/dags/${enc(dagId)}/dagRuns/~/taskInstances/~/xcomEntries?xcom_key=row_count&limit=1000`,
    )
    const out: Record<string, Record<string, number>> = {}
    await Promise.all(list.xcom_entries.map(async (e) => {
      let value: unknown
      try {
        value = await xcomValue(`row_count|${dagId}|${e.run_id}|${e.task_id}|${e.map_index}|${e.timestamp ?? ''}`,
          `/dags/${enc(dagId)}/dagRuns/${enc(e.run_id)}/taskInstances/${enc(e.task_id)}/xcomEntries/row_count?map_index=${e.map_index}`)
      } catch { return }
      const n = typeof value === 'number' ? value : Number(value)
      if (!Number.isFinite(n)) return
      out[e.run_id] ??= {}
      out[e.run_id][e.task_id] = (out[e.run_id][e.task_id] ?? 0) + n
    }))
    return out
  }

  /** Écarts documentés (« row_breakdown ») et contrôles qualité (« quality_checks ») d'une exécution. */
  async function runDetails(dagId: string, runId: string) {
    const lire = async (key: string) => {
      const list = await api<{ xcom_entries: { task_id: string; map_index: number; timestamp?: string }[] }>(
        `/dags/${enc(dagId)}/dagRuns/${enc(runId)}/taskInstances/~/xcomEntries?xcom_key=${key}&limit=200`,
      )
      const out: Record<string, unknown> = {}
      await Promise.all(list.xcom_entries.map(async (e) => {
        let value: unknown
        try {
          value = await xcomValue(`${key}|${dagId}|${runId}|${e.task_id}|${e.map_index}|${e.timestamp ?? ''}`,
            `/dags/${enc(dagId)}/dagRuns/${enc(runId)}/taskInstances/${enc(e.task_id)}/xcomEntries/${key}?map_index=${e.map_index}&deserialize=true`)
        } catch { return }
        if (value && typeof value === 'object') out[e.task_id] = value
      }))
      return out
    }
    const [breakdown, checks] = await Promise.all([lire('row_breakdown'), lire('quality_checks')])
    return { breakdown, checks }
  }

  /** Dernières lignes du journal d'une tentative de tâche, en texte brut. */
  async function logTail(dagId: string, runId: string, taskId: string, tryNumber: string) {
    const r = await api<{ content: unknown }>(
      `/dags/${enc(dagId)}/dagRuns/${enc(runId)}/taskInstances/${enc(taskId)}/logs/${enc(tryNumber)}?full_content=true`,
    )
    const lines: string[] = []
    const walk = (c: unknown) => {
      if (typeof c === 'string') lines.push(...c.split('\n'))
      else if (Array.isArray(c)) c.forEach(walk)
      else if (c && typeof c === 'object') {
        const o = c as Record<string, unknown>
        if (typeof o.event === 'string') {
          const lvl = typeof o.level === 'string' ? o.level.toUpperCase() : ''
          lines.push([lvl, o.event].filter(Boolean).join(' - '))
        } else if ('content' in o) walk(o.content)
      }
    }
    walk(r.content)
    return lines.filter((l) => l.trim() && !/::(end)?group::/.test(l) && !l.startsWith('/opt/airflow/logs')).slice(-40)
  }

  // ------------------------------------------------------------------ rejeu contrôlé
  async function audit(entry: Record<string, unknown>) {
    if (!auditFile) return
    await mkdir(dirname(auditFile), { recursive: true })
    await appendFile(auditFile, JSON.stringify({ ts: new Date(now()).toISOString(), ...entry }) + '\n', 'utf-8')
  }

  async function readAudit(limit = 50) {
    if (!auditFile) return []
    try {
      const txt = await readFile(auditFile, 'utf-8')
      return txt.trim().split('\n').filter(Boolean).slice(-limit).reverse().map((l) => JSON.parse(l))
    } catch { return [] }
  }

  /** Événements d'incident (échec d'essai, échec, reprise), les plus récents ; lignes illisibles ignorées. */
  async function readIncidents(limit = 2000) {
    if (!incidentsFile) return []
    let txt = ''
    try { txt = await readFile(incidentsFile, 'utf-8') } catch { return [] }
    const out: unknown[] = []
    for (const l of txt.split(/\r?\n/).filter(Boolean).slice(-limit)) {
      try { out.push(JSON.parse(l)) } catch { /* ligne tronquée : ignorée */ }
    }
    return out
  }

  function validate(b: Partial<ReplayRequest>): ReplayRequest {
    if (!b || typeof b.dag_id !== 'string' || !b.dag_id || typeof b.dag_run_id !== 'string' || !b.dag_run_id)
      throw new HttpError(400, 'invalid', 'dag_id et dag_run_id sont obligatoires.')
    if (!['run', 'failed', 'task'].includes(b.mode as string)) throw new HttpError(400, 'invalid', 'Mode de rejeu inconnu.')
    if (b.mode === 'task' && (typeof b.task_id !== 'string' || !b.task_id)) throw new HttpError(400, 'invalid', 'task_id est obligatoire pour rejouer une tâche.')
    if (typeof b.dry_run !== 'boolean') throw new HttpError(400, 'invalid', 'dry_run doit être précisé.')
    return b as ReplayRequest
  }

  const requestKey = (r: ReplayRequest) =>
    createHash('sha256').update(JSON.stringify([r.dag_id, r.dag_run_id, r.mode, r.task_id ?? null, !!r.include_downstream])).digest('hex')

  async function replay(r: ReplayRequest) {
    const dag = await api<{ tags: { name: string }[]; is_paused: boolean }>(`/dags/${enc(r.dag_id)}`)
    if (!dag.tags.some((t) => t.name === REPLAYABLE_TAG))
      throw new HttpError(403, 'not_replayable', `Ce DAG n'est pas déclaré rejouable (tag « ${REPLAYABLE_TAG} ») : son idempotence n'est pas garantie.`)
    const run = await api<{ state: string }>(`/dags/${enc(r.dag_id)}/dagRuns/${enc(r.dag_run_id)}`)
    if (['running', 'queued'].includes(run.state))
      throw new HttpError(409, 'run_active', 'Cette exécution est encore en cours : attendez sa fin avant de la rejouer.')
    const key = requestKey(r)
    let simulated = 0

    if (!r.dry_run) {
      const p = r.confirm_token ? pending.get(r.confirm_token) : undefined
      if (!p || p.key !== key || p.expires < now())
        throw new HttpError(409, 'confirm_required', 'Simulation requise : relancez l’aperçu puis confirmez dans les 2 minutes.')
      const last = lastAction.get(`${r.dag_id}|${r.dag_run_id}`)
      if (last && now() - last < 5000) throw new HttpError(429, 'too_fast', 'Un rejeu vient d’être lancé sur cette exécution.')
      pending.delete(r.confirm_token!)
      simulated = p.tasks
    }

    const note = `Rejeu demandé depuis Pipeline Lens (${r.mode})`
    const res = r.mode === 'task'
      ? await api<{ task_instances: { task_id: string; state: string | null }[] }>(`/dags/${enc(r.dag_id)}/clearTaskInstances`, {
          method: 'POST', who: 'ops',
          body: { dry_run: r.dry_run, dag_run_id: r.dag_run_id, task_ids: [r.task_id], include_downstream: !!r.include_downstream, include_upstream: false, only_failed: false, reset_dag_runs: true, ...(r.dry_run ? {} : { note }) },
        })
      : await api<{ task_instances?: { task_id: string; state: string | null }[] }>(`/dags/${enc(r.dag_id)}/dagRuns/${enc(r.dag_run_id)}/clear`, {
          method: 'POST', who: 'ops',
          body: { dry_run: r.dry_run, only_failed: r.mode === 'failed', ...(r.dry_run ? {} : { note }) },
        })
    // Hors simulation, Airflow renvoie l'exécution et non la liste : on reprend celle de la simulation.
    const tasks = (res.task_instances ?? []).map((t) => ({ task_id: t.task_id, state: t.state }))

    if (r.dry_run) {
      const token = randomUUID()
      pending.set(token, { key, expires: now() + 120_000, tasks: tasks.length })
      await audit({ action: 'simulation', dag_id: r.dag_id, dag_run_id: r.dag_run_id, mode: r.mode, task_id: r.task_id ?? null, taches: tasks.length })
      return { dry_run: true, tasks, confirm_token: token, expires_in: 120 }
    }
    lastAction.set(`${r.dag_id}|${r.dag_run_id}`, now())
    await audit({ action: 'rejeu', dag_id: r.dag_id, dag_run_id: r.dag_run_id, mode: r.mode, task_id: r.task_id ?? null, taches: tasks.length || simulated, resultat: 'lance', compte: accounts.ops.user })
    return { dry_run: false, tasks, relaunched: tasks.length || simulated }
  }

  async function readBody(req: IncomingMessage): Promise<unknown> {
    let size = 0
    const chunks: Buffer[] = []
    for await (const c of req) {
      size += (c as Buffer).length
      if (size > 16_384) throw new HttpError(413, 'too_large', 'Requête trop volumineuse.')
      chunks.push(c as Buffer)
    }
    try { return JSON.parse(Buffer.concat(chunks).toString('utf-8') || '{}') } catch { throw new HttpError(400, 'invalid', 'Corps JSON invalide.') }
  }

  return async (req, res, next) => {
    if (!req.url?.startsWith('/lens/')) return next()
    const send = (status: number, body: unknown) => {
      res.statusCode = status
      res.setHeader('Content-Type', 'application/json; charset=utf-8')
      res.setHeader('Cache-Control', 'no-store')
      res.setHeader('X-Content-Type-Options', 'nosniff')
      res.end(JSON.stringify(body))
    }
    const url = new URL(req.url, 'http://lens.local')
    const q = url.searchParams
    try {
      if (req.method === 'POST' && url.pathname === '/lens/actions/replay') {
        if (!actionsOn) throw new HttpError(403, 'actions_off', 'Le rejeu est désactivé (LENS_ACTIONS=off) : Pipeline Lens est en lecture seule.')
        if (req.headers['x-lens-action'] !== '1') throw new HttpError(403, 'csrf', 'En-tête X-Lens-Action manquant.')
        const r = validate((await readBody(req)) as Partial<ReplayRequest>)
        try {
          return send(200, await replay(r))
        } catch (e) {
          if (!r.dry_run && e instanceof HttpError) await audit({ action: 'rejeu', dag_id: r.dag_id, dag_run_id: r.dag_run_id, mode: r.mode, task_id: r.task_id ?? null, resultat: 'refuse', motif: e.code })
          throw e
        }
      }
      if (req.method !== 'GET') return send(405, { error: 'read_only', message: 'Seule l’action de rejeu est autorisée ; tout le reste est en lecture seule.' })
      if (url.pathname === '/lens/volumes') return send(200, await volumes(q.get('dag_id') ?? ''))
      if (url.pathname === '/lens/run-details') return send(200, await runDetails(q.get('dag_id') ?? '', q.get('run_id') ?? ''))
      if (url.pathname === '/lens/log')
        return send(200, { lines: await logTail(q.get('dag_id') ?? '', q.get('run_id') ?? '', q.get('task_id') ?? '', q.get('try') ?? '1') })
      if (url.pathname === '/lens/config')
        return send(200, { airflowUrl: base, actions: actionsOn && !!accounts.ops.user, replayableTag: REPLAYABLE_TAG })
      if (url.pathname === '/lens/incidents') return send(200, { events: await readIncidents() })
      if (url.pathname === '/lens/actions/audit') return send(200, { entries: await readAudit(Number(q.get('limit') || 50)) })
      if (url.pathname.startsWith('/lens/api/')) {
        // Uniquement l'API v2 : aucun « .. », même encodé, ne peut sortir de ce périmètre.
        if (/(^|\/)\.\.(\/|$)|%2e|%2f|%5c|\\/i.test(url.pathname)) throw new HttpError(400, 'invalid', 'Chemin refusé.')
        const path = url.pathname.slice('/lens/api'.length) + url.search
        return send(200, await api(path))
      }
      return send(404, { error: 'not_found', message: 'Route inconnue.' })
    } catch (e) {
      const err = e instanceof HttpError ? e : new HttpError(500, 'internal', 'Erreur interne du proxy.')
      return send(err.status, { error: err.code, message: err.message })
    }
  }
}
