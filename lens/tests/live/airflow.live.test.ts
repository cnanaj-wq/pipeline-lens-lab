// @vitest-environment node
/**
 * Tests sur le vrai Airflow local (npm run test:live). Lit C:\airflow\.env sans rien afficher.
 * Ignorés si Airflow ne répond pas. Le rejeu réel n'est lancé que si LENS_LIVE_REJEU=1.
 */
import { mkdtempSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { loadEnv } from 'vite'
import { checkContract } from '../../proxy/airflow-contract.ts'
import { createLensHandler } from '../../proxy/lens-server.ts'

/* eslint-disable @typescript-eslint/no-explicit-any */
const env = loadEnv('development', fileURLToPath(new URL('../../..', import.meta.url)), '')
const AIRFLOW = (env.AIRFLOW_URL || 'http://localhost:8085').replace(/\/$/, '')
const up = await fetch(`${AIRFLOW}/api/v2/monitor/health`, { signal: AbortSignal.timeout(3000) }).then((r) => r.ok).catch(() => false)

let server: Server
let base = ''
const get = async (p: string) => { const r = await fetch(base + p); return { status: r.status, body: (await r.json()) as any } }
const post = async (b: unknown) => {
  const r = await fetch(base + '/lens/actions/replay', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Lens-Action': '1' }, body: JSON.stringify(b) })
  return { status: r.status, body: (await r.json()) as any }
}

describe.skipIf(!up)(`Airflow réel (${AIRFLOW})`, () => {
  beforeAll(async () => {
    const h = createLensHandler(env, { auditFile: join(mkdtempSync(join(tmpdir(), 'lens-live-')), 'audit.jsonl') })
    server = createServer((req, res) => h(req, res, () => { res.statusCode = 404; res.end() }))
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    base = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  })
  afterAll(() => server?.close())

  it('la spécification OpenAPI de la version installée respecte le contrat de Pipeline Lens', async () => {
    const spec = (await fetch(`${AIRFLOW}/openapi.json`).then((r) => r.json())) as Parameters<typeof checkContract>[0]
    expect(checkContract(spec)).toEqual([])
  })

  it('lit les DAG avec le compte de lecture', async () => {
    const r = await get('/lens/api/dags?limit=100')
    expect(r.status).toBe(200)
    expect(r.body.dags.length).toBeGreaterThan(0)
  })

  it('le compte de lecture ne peut pas rejouer (rôle viewer)', async () => {
    const t = await fetch(`${AIRFLOW}/auth/token`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: env.AIRFLOW_LENS_USER, password: env.AIRFLOW_LENS_PASSWORD }),
    }).then((r) => r.json())
    const r = await fetch(`${AIRFLOW}/api/v2/dags/product_etl_dag/dagRuns/inexistant/clear`, {
      method: 'POST', headers: { Authorization: `Bearer ${t.access_token}`, 'Content-Type': 'application/json' }, body: '{"dry_run":true}',
    })
    expect(r.status).toBe(403)
  })

  it('les écarts publiés réconcilient exactement les volumes (exécution instrumentée)', async () => {
    const runs = (await get('/lens/api/dags/product_etl_dag/dagRuns?limit=25&order_by=-run_after&state=success')).body.dag_runs
    const vols = (await get('/lens/volumes?dag_id=product_etl_dag')).body
    let verifie = 0
    for (const run of runs) {
      const d = (await get(`/lens/run-details?dag_id=product_etl_dag&run_id=${encodeURIComponent(run.dag_run_id)}`)).body
      for (const [task, b] of Object.entries<any>(d.breakdown)) {
        const ecarts = b.ecarts.reduce((s: number, e: { lignes: number }) => s + e.lignes, 0)
        expect(b.entree - b.sortie, `${run.dag_run_id} ${task}`).toBe(ecarts)
        expect(vols[run.dag_run_id]?.[task], `${task} : row_count = sortie`).toBe(b.sortie)
        verifie++
      }
      if (verifie) break
    }
    expect(verifie).toBeGreaterThan(0)
  })

  it('refuse le rejeu d’un DAG non déclaré rejouable', async () => {
    const run = (await get('/lens/api/dags/sales_daily_dag/dagRuns?limit=1&order_by=-run_after')).body.dag_runs[0]
    const r = await post({ dag_id: 'sales_daily_dag', dag_run_id: run.dag_run_id, mode: 'run', dry_run: true })
    expect(r.body.error).toBe('not_replayable')
  })

  it('simule le rejeu d’une exécution rejouable sans rien modifier', async () => {
    const run = (await get('/lens/api/dags/product_etl_dag/dagRuns?limit=1&order_by=-run_after&state=success')).body.dag_runs[0]
    const r = await post({ dag_id: 'product_etl_dag', dag_run_id: run.dag_run_id, mode: 'run', dry_run: true })
    expect(r.status).toBe(200)
    expect(r.body.tasks.length).toBe(6)
    const apres = (await get(`/lens/api/dags/product_etl_dag/dagRuns/${encodeURIComponent(run.dag_run_id)}`)).body
    expect(apres.state).toBe('success')
  })

  it.skipIf(process.env.LENS_LIVE_REJEU !== '1')('rejoue réellement une tâche (et ses dépendantes) jusqu’au succès', async () => {
    const run = (await get('/lens/api/dags/product_etl_dag/dagRuns?limit=1&order_by=-run_after&state=success')).body.dag_runs[0]
    const corps = { dag_id: 'product_etl_dag', dag_run_id: run.dag_run_id, mode: 'task', task_id: 'load.build_product_model', include_downstream: true }
    const sim = await post({ ...corps, dry_run: true })
    expect(sim.body.tasks.map((t: { task_id: string }) => t.task_id)).toEqual(['load.build_product_model'])
    const ok = await post({ ...corps, dry_run: false, confirm_token: sim.body.confirm_token })
    expect(ok.status).toBe(200)
    let state = ''
    for (let i = 0; i < 60 && state !== 'success' && state !== 'failed'; i++) {
      await new Promise((r) => setTimeout(r, 2000))
      state = (await get(`/lens/api/dags/product_etl_dag/dagRuns/${encodeURIComponent(run.dag_run_id)}`)).body.state
    }
    expect(state).toBe('success')
  }, 150_000)
})
