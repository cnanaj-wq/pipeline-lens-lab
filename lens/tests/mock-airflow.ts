/**
 * Faux Airflow 3 (API REST v2) pour les tests : mêmes routes et mêmes formats que la vraie API,
 * état modifiable par les tests (scénarios, panne, lenteur, expiration des jetons).
 *
 * Utilisé par les tests d'intégration du proxy (vitest) et par les tests E2E (Playwright),
 * qui pilotent l'état via POST /__mock/scenario. Aucune donnée réelle : tout est synthétique.
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'

type St = string | null
export interface MockRun { dag_id: string; dag_run_id: string; state: St; run_type: string; run_after: string; start_date: string | null; end_date: string | null; duration: number | null; conf: Record<string, unknown> | null; logical_date: null }
export interface MockTI { task_id: string; dag_id: string; dag_run_id: string; state: St; start_date: string | null; end_date: string | null; duration: number | null; try_number: number; max_tries: number; operator_name: string; map_index: number }
export interface MockDag { dag_id: string; dag_display_name: string; description: string | null; is_paused: boolean; timetable_summary: string | null; timetable_description: string | null; next_dagrun_run_after: string | null; tags: { name: string }[]; has_import_errors: boolean }

export interface MockState {
  dags: MockDag[]
  tasks: Record<string, { task_id: string; task_display_name: string; downstream_task_ids: string[]; operator_name: string }[]>
  runs: MockRun[]
  tis: MockTI[]
  /** clé : dag|run|task|xcom_key */
  xcom: Record<string, unknown>
  logs: Record<string, string[]>
}

const ETL = (_dag: string) => [
  { task_id: 'extract.lire_source', task_display_name: 'extract.lire_source', downstream_task_ids: ['transform.nettoyer'], operator_name: '@task' },
  { task_id: 'transform.nettoyer', task_display_name: 'transform.nettoyer', downstream_task_ids: ['load.charger'], operator_name: '@task' },
  { task_id: 'load.charger', task_display_name: 'load.charger', downstream_task_ids: [], operator_name: '@task' },
]

const iso = (ms: number) => new Date(ms).toISOString()

/** Scénarios des 8 tests obligatoires, plus les cas de rejeu. Heure de référence : maintenant. */
export function scenario(name = 'complet', now = Date.now()): MockState {
  const dag = (dag_id: string, tags: string[], extra: Partial<MockDag> = {}): MockDag => ({
    dag_id, dag_display_name: dag_id, description: `DAG de test ${dag_id}`, is_paused: false,
    timetable_summary: '0 9 * * *', timetable_description: 'Chaque jour à 09:00', next_dagrun_run_after: iso(now + 3600_000),
    tags: tags.map((name) => ({ name })), has_import_errors: false, ...extra,
  })
  const s: MockState = { dags: [], tasks: {}, runs: [], tis: [], xcom: {}, logs: {} }
  const addRun = (dag_id: string, i: number, state: St, durations: [number, number, number], tiStates?: St[], tries?: number[]) => {
    const start = now - (i + 1) * 3600_000
    const run_id = `scheduled__${iso(start)}`
    const total = durations.reduce((a, b) => a + b, 0)
    const done = state !== 'running' && state !== 'queued'
    s.runs.push({ dag_id, dag_run_id: run_id, state, run_type: i % 3 === 2 ? 'manual' : 'scheduled', run_after: iso(start), start_date: iso(start), end_date: done ? iso(start + total * 1000) : null, duration: done ? total : null, conf: {}, logical_date: null })
    const states = tiStates ?? [state, state, state]
    let t = start
    s.tasks[dag_id].forEach((task, k) => {
      const st = states[k]
      const ended = st === 'success' || st === 'failed' || st === 'upstream_failed' || st === 'up_for_retry'
      s.tis.push({ task_id: task.task_id, dag_id, dag_run_id: run_id, state: st, start_date: st && st !== 'upstream_failed' && st !== 'scheduled' ? iso(t) : null, end_date: ended ? iso(t + durations[k] * 1000) : null, duration: ended ? durations[k] : null, try_number: tries?.[k] ?? 1, max_tries: 1, operator_name: '@task', map_index: -1 })
      s.logs[`${dag_id}|${run_id}|${task.task_id}|${tries?.[k] ?? 1}`] = st === 'failed'
        ? ['INFO - Lecture de la source', 'ERROR - Task failed with exception', 'ValueError: colonne « prix » absente du fichier source']
        : ['INFO - Démarrage', `INFO - ${task.task_id} terminé`]
      t += durations[k] * 1000
    })
    return run_id
  }
  const volumes = (dag_id: string, run_id: string, e: number, tr: number, l: number) => {
    s.xcom[`${dag_id}|${run_id}|extract.lire_source|row_count`] = e
    s.xcom[`${dag_id}|${run_id}|transform.nettoyer|row_count`] = tr
    s.xcom[`${dag_id}|${run_id}|load.charger|row_count`] = l
  }

  // 1. DAG en succès, instrumenté (volumes, écarts, contrôles qualité), rejouable.
  s.dags.push(dag('produits_etl', ['etl', 'rejouable']))
  s.tasks.produits_etl = ETL('produits_etl')
  for (let i = 7; i >= 0; i--) {
    const r = addRun('produits_etl', i, 'success', [1.2, i === 3 ? 30 : 2.5, 0.9])
    volumes('produits_etl', r, 11500 + i, 11210 + i, 11210 + i)
    if (i === 0) {
      s.xcom[`produits_etl|${r}|transform.nettoyer|row_breakdown`] = { entree: 11500, sortie: 11210, ecarts: [
        { motif: 'doublon_id', libelle: 'Identifiant en double', nature: 'dedupliquee', lignes: 167 },
        { motif: 'prix_manquant', libelle: 'Prix manquant', nature: 'rejetee', lignes: 123 },
      ], fichier_rejets: 'rejets_produits.csv' }
      s.xcom[`produits_etl|${r}|load.charger|quality_checks`] = [
        { controle: 'Identifiant vide', valeur: 0, seuil: 0, statut: 'ok' },
        { controle: 'Prix manquant', valeur: 0, seuil: 0, statut: 'ok' },
      ]
    }
  }
  if (name === 'vide') return { dags: [dag('nouveau_dag', ['etl'])], tasks: { nouveau_dag: ETL('nouveau_dag') }, runs: [], tis: [], xcom: {}, logs: {} }

  // 2. DAG dont une tâche a échoué (non rejouable : écritures non idempotentes).
  s.dags.push(dag('ventes_jour', ['etl']))
  s.tasks.ventes_jour = ETL('ventes_jour')
  for (let i = 4; i >= 1; i--) volumes('ventes_jour', addRun('ventes_jour', i, 'success', [1, 2, 1]), 3000, 3000, 3000)
  addRun('ventes_jour', 0, 'failed', [1, 0.4, 0], ['success', 'failed', 'upstream_failed'], [1, 2, 0])

  // 3. DAG en cours d'exécution.
  s.dags.push(dag('stocks_horaire', ['etl', 'rejouable']))
  s.tasks.stocks_horaire = ETL('stocks_horaire')
  for (let i = 5; i >= 1; i--) addRun('stocks_horaire', i, 'success', [2, 3, 1])
  addRun('stocks_horaire', 0, 'running', [2, 3, 1], ['success', 'running', null])

  // 4. DAG avec une tâche en attente de nouvel essai.
  s.dags.push(dag('clients_sync', ['etl']))
  s.tasks.clients_sync = ETL('clients_sync')
  addRun('clients_sync', 1, 'success', [1, 1, 1])
  addRun('clients_sync', 0, 'running', [1, 1, 1], ['success', 'up_for_retry', null], [1, 1, 0])

  // 6. DAG sans aucune exécution ; 7. aucune donnée qualité (ventes_jour, stocks_horaire).
  s.dags.push(dag('nouveau_dag', ['etl'], { next_dagrun_run_after: null }))
  s.tasks.nouveau_dag = ETL('nouveau_dag')
  return s
}

export interface MockAirflow {
  url: string
  server: Server
  state: MockState
  /** Requêtes reçues, pour vérifier ce que le proxy envoie. */
  calls: { method: string; path: string; body?: unknown; user?: string }[]
  /** Simule une panne : connexion refusée côté test via close(), ou réponses 503. */
  down: boolean
  delayMs: number
  /** Nombre de jetons délivrés (POST /auth/token réussis). */
  tokensIssued: number
  /** Horodatage de chaque XCom (clé dag|run|task|xcom_key) ; un rejeu le change. */
  xcomTimes: Record<string, string>
  /** Invalide tous les jetons émis (le proxy doit se ré-authentifier). */
  revokeTokens(): void
  setScenario(name: string): void
  close(): Promise<void>
}

export const USERS: Record<string, { password: string; role: 'viewer' | 'op' }> = {
  lens_lecture: { password: 'lecture-test', role: 'viewer' },
  lens_ops: { password: 'ops-test', role: 'op' },
}

export async function startMockAirflow(port = 0, scenarioName = 'complet'): Promise<MockAirflow> {
  let tokens = new Map<string, string>()
  let n = 0
  const m = { state: scenario(scenarioName), calls: [], down: false, delayMs: 0, tokensIssued: 0, xcomTimes: {} } as unknown as MockAirflow

  const json = (res: ServerResponse, status: number, body: unknown) => {
    res.statusCode = status
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(body))
  }
  const readBody = async (req: IncomingMessage) => {
    const chunks: Buffer[] = []
    for await (const c of req) chunks.push(c as Buffer)
    const t = Buffer.concat(chunks).toString('utf-8')
    return t ? JSON.parse(t) : undefined
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://mock')
    const p = url.pathname
    const q = url.searchParams
    const body = req.method === 'POST' ? await readBody(req).catch(() => undefined) : undefined

    // Pilotage du faux Airflow par les tests E2E (hors API Airflow).
    if (p === '/__mock/scenario' && req.method === 'POST') {
      const b = body as { name?: string; down?: boolean; delayMs?: number; patch?: { dag_id: string; state: string } }
      if (b?.name) { m.setScenario(b.name); m.calls.length = 0 }
      if (typeof b?.down === 'boolean') m.down = b.down
      if (typeof b?.delayMs === 'number') m.delayMs = b.delayMs
      if (b?.patch) {
        const r = m.state.runs.filter((x) => x.dag_id === b.patch!.dag_id).sort((a, c) => c.run_after.localeCompare(a.run_after))[0]
        if (r) {
          r.state = b.patch.state
          r.end_date = new Date().toISOString(); r.duration = 6
          m.state.tis.filter((t) => t.dag_run_id === r.dag_run_id && t.dag_id === r.dag_id).forEach((t) => { t.state = b.patch!.state; t.duration = 2; t.end_date = r.end_date; t.start_date ??= r.start_date })
        }
      }
      return json(res, 200, { ok: true })
    }
    if (p === '/__mock/calls') return json(res, 200, m.calls)

    if (m.delayMs) await new Promise((r) => setTimeout(r, m.delayMs))
    if (m.down) return json(res, 503, { detail: 'Service Unavailable' })

    if (p === '/auth/token' && req.method === 'POST') {
      const b = body as { username?: string; password?: string }
      const u = b?.username ? USERS[b.username] : undefined
      if (!u || u.password !== b?.password) return json(res, 401, { detail: 'Invalid credentials' })
      m.tokensIssued++
      const t = `jeton-${b!.username}-${++n}`
      tokens.set(t, b!.username!)
      return json(res, 201, { access_token: t })
    }
    if (!p.startsWith('/api/v2/')) return json(res, 404, { detail: 'Not Found' })
    const user = tokens.get((req.headers.authorization ?? '').replace('Bearer ', ''))
    m.calls.push({ method: req.method ?? 'GET', path: p + url.search, body, user })
    if (!user) return json(res, 401, { detail: 'Not authenticated' })
    const role = USERS[user].role

    const seg = p.slice('/api/v2/'.length).split('/').map(decodeURIComponent)
    const S = m.state
    const limit = Number(q.get('limit') ?? 50)
    const offset = Number(q.get('offset') ?? 0)
    const runsOf = (dag: string) => S.runs.filter((r) => dag === '~' || r.dag_id === dag)

    // POST : rejeu (clear), réservé au rôle op.
    if (req.method === 'POST') {
      if (role !== 'op') return json(res, 403, { detail: 'Forbidden' })
      const b = (body ?? {}) as Record<string, unknown>
      if (seg[0] === 'dags' && seg[2] === 'dagRuns' && seg[4] === 'clear') {
        const run = S.runs.find((r) => r.dag_id === seg[1] && r.dag_run_id === seg[3])
        if (!run) return json(res, 404, { detail: 'DagRun not found' })
        const tis = S.tis.filter((t) => t.dag_id === run.dag_id && t.dag_run_id === run.dag_run_id && (!b.only_failed || t.state === 'failed' || t.state === 'upstream_failed'))
        // Comme Airflow 3 : la simulation renvoie les tâches, le rejeu réel renvoie l'exécution.
        if (!b.dry_run) { tis.forEach((t) => { t.state = null; t.try_number += 1 }); run.state = 'queued'; return json(res, 200, run) }
        return json(res, 200, { task_instances: tis.map((t) => ({ task_id: t.task_id, state: t.state })), total_entries: tis.length })
      }
      if (seg[0] === 'dags' && seg[2] === 'clearTaskInstances') {
        const run = S.runs.find((r) => r.dag_id === seg[1] && r.dag_run_id === b.dag_run_id)
        if (!run) return json(res, 404, { detail: 'DagRun not found' })
        const ids = S.tasks[run.dag_id].map((t) => t.task_id)
        const start = ids.indexOf((b.task_ids as string[])[0])
        const sel = b.include_downstream ? ids.slice(start) : [ids[start]]
        const tis = S.tis.filter((t) => t.dag_id === run.dag_id && t.dag_run_id === run.dag_run_id && sel.includes(t.task_id))
        if (!b.dry_run) { tis.forEach((t) => { t.state = null; t.try_number += 1 }); run.state = 'queued' }
        return json(res, 200, { task_instances: tis.map((t) => ({ task_id: t.task_id, state: t.state })), total_entries: tis.length })
      }
      return json(res, 404, { detail: 'Not Found' })
    }

    if (seg[0] === 'dags' && seg.length === 1) return json(res, 200, { dags: S.dags.slice(offset, offset + limit), total_entries: S.dags.length })
    if (seg[0] !== 'dags') return json(res, 404, { detail: 'Not Found' })
    const dagId = seg[1]
    if (dagId !== '~' && !S.dags.some((d) => d.dag_id === dagId)) return json(res, 404, { detail: 'DAG not found' })
    if (seg.length === 2) return json(res, 200, S.dags.find((d) => d.dag_id === dagId))
    if (seg[2] === 'tasks') return json(res, 200, { tasks: S.tasks[dagId], total_entries: S.tasks[dagId].length })
    if (seg[2] !== 'dagRuns') return json(res, 404, { detail: 'Not Found' })

    if (seg.length === 3) {
      let runs = runsOf(dagId)
      const states = q.getAll('state'), types = q.getAll('run_type')
      if (states.length) runs = runs.filter((r) => states.includes(r.state ?? ''))
      if (types.length) runs = runs.filter((r) => types.includes(r.run_type))
      if (q.get('run_after_gte')) runs = runs.filter((r) => r.run_after >= q.get('run_after_gte')!)
      runs = [...runs].sort((a, b) => b.run_after.localeCompare(a.run_after))
      return json(res, 200, { dag_runs: runs.slice(offset, offset + limit), total_entries: runs.length })
    }
    const runId = seg[3]
    if (seg.length === 4) {
      const r = S.runs.find((x) => x.dag_id === dagId && x.dag_run_id === runId)
      return r ? json(res, 200, r) : json(res, 404, { detail: 'DagRun not found' })
    }
    if (seg[4] !== 'taskInstances') return json(res, 404, { detail: 'Not Found' })
    const tis = S.tis.filter((t) => (dagId === '~' || t.dag_id === dagId) && (runId === '~' || t.dag_run_id === runId))
    if (seg.length === 5) {
      const states = q.getAll('state')
      const out = states.length ? tis.filter((t) => states.includes(t.state ?? '')) : tis
      return json(res, 200, { task_instances: out.slice(offset, offset + limit), total_entries: out.length })
    }
    const taskId = seg[5]
    if (seg[6] === 'xcomEntries') {
      const key = seg[7] ?? q.get('xcom_key')
      if (seg.length === 7) {
        const entries = Object.keys(S.xcom).map((k) => k.split('|')).filter(([d, r, t, x]) =>
          (dagId === '~' || d === dagId) && (runId === '~' || r === runId) && (taskId === '~' || t === taskId) && (!key || x === key))
          .map(([d, r, t, x]) => ({ dag_id: d, run_id: r, task_id: t, key: x, map_index: -1, timestamp: m.xcomTimes[`${d}|${r}|${t}|${x}`] ?? '2026-11-10T00:00:00Z' }))
        return json(res, 200, { xcom_entries: entries, total_entries: entries.length })
      }
      const v = S.xcom[`${dagId}|${runId}|${taskId}|${key}`]
      if (v === undefined) return json(res, 404, { detail: 'XCom not found' })
      // Comme Airflow : sans deserialize, un objet est renvoyé sous forme de chaîne.
      return json(res, 200, { key, value: typeof v === 'object' && q.get('deserialize') !== 'true' ? String(v) : v })
    }
    if (seg[6] === 'logs') {
      const lines = S.logs[`${dagId}|${runId}|${taskId}|${seg[7]}`]
      if (!lines) return json(res, 404, { detail: 'Log not found' })
      return json(res, 200, { content: lines.map((l) => { const [level, ...rest] = l.split(' - '); return rest.length ? { level: level.toLowerCase(), event: rest.join(' - ') } : { event: l } }) })
    }
    return json(res, 404, { detail: 'Not Found' })
  })

  await new Promise<void>((r) => server.listen(port, '127.0.0.1', r))
  const addr = server.address() as { port: number }
  return Object.assign(m, {
    url: `http://127.0.0.1:${addr.port}`,
    server,
    revokeTokens: () => { tokens = new Map() },
    setScenario: (name: string) => { m.state = scenario(name); m.xcomTimes = {} },
    close: () => new Promise<void>((r) => { server.closeAllConnections(); server.close(() => r()) }),
  })
}
