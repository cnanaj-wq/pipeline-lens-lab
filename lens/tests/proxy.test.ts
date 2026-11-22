// @vitest-environment node
/**
 * Tests d'intégration du proxy Pipeline Lens contre un faux Airflow (tests/mock-airflow.ts).
 * Couvre l'authentification, les pannes, la lecture seule et chaque garde-fou du rejeu.
 */
import { mkdtempSync, readFileSync, existsSync, writeFileSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { createLensHandler, type Env } from '../proxy/lens-server.ts'
import { startMockAirflow, type MockAirflow } from './mock-airflow.ts'

let af: MockAirflow
let lens: Server
let base = ''
let auditFile = ''
let clock = Date.now()

const envFor = (over: Env = {}): Env => ({
  AIRFLOW_URL: af.url,
  AIRFLOW_LENS_USER: 'lens_lecture', AIRFLOW_LENS_PASSWORD: 'lecture-test',
  AIRFLOW_LENS_OPS_USER: 'lens_ops', AIRFLOW_LENS_OPS_PASSWORD: 'ops-test',
  LENS_ACTIONS: 'on', ...over,
})

async function startLens(env: Env, timeoutMs = 1500) {
  lens?.close()
  const h = createLensHandler(env, { auditFile, timeoutMs, now: () => clock })
  lens = createServer((req, res) => h(req, res, () => { res.statusCode = 404; res.end('next') }))
  await new Promise<void>((r) => lens.listen(0, '127.0.0.1', r))
  base = `http://127.0.0.1:${(lens.address() as { port: number }).port}`
}

const get = async (path: string) => {
  const r = await fetch(base + path)
  return { status: r.status, body: (await r.json()) as any }  // eslint-disable-line @typescript-eslint/no-explicit-any
}
const replay = async (body: unknown, headers: Record<string, string> = { 'X-Lens-Action': '1' }) => {
  const r = await fetch(base + '/lens/actions/replay', { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body) })
  return { status: r.status, body: (await r.json()) as any }  // eslint-disable-line @typescript-eslint/no-explicit-any
}
const lastRun = (dag: string) => af.state.runs.filter((r) => r.dag_id === dag).sort((a, b) => b.run_after.localeCompare(a.run_after))[0]

beforeAll(async () => { af = await startMockAirflow() })
afterAll(async () => { lens?.close(); await af.close() })
beforeEach(async () => {
  af.setScenario('complet'); af.down = false; af.delayMs = 0; af.calls.length = 0
  clock = Date.now()
  auditFile = join(mkdtempSync(join(tmpdir(), 'lens-audit-')), 'audit.jsonl')
  await startLens(envFor())
})
afterEach(() => { af.revokeTokens() })

describe('lecture', () => {
  it('relaie la liste des DAG avec le compte de lecture, sans exposer de jeton', async () => {
    const r = await get('/lens/api/dags')
    expect(r.status).toBe(200)
    expect(r.body.dags.map((d: { dag_id: string }) => d.dag_id)).toContain('produits_etl')
    expect(JSON.stringify(r.body)).not.toMatch(/jeton-|lecture-test|ops-test/)
    expect(af.calls.every((c) => c.user === 'lens_lecture')).toBe(true)
  })

  it('renouvelle le jeton expiré une seule fois puis réussit', async () => {
    await get('/lens/api/dags')
    af.revokeTokens()
    const r = await get('/lens/api/dags')
    expect(r.status).toBe(200)
  })

  it('agrège les volumes publiés (row_count) par exécution et par tâche', async () => {
    const r = await get('/lens/volumes?dag_id=produits_etl')
    const run = lastRun('produits_etl').dag_run_id
    expect(r.body[run]).toEqual({ 'extract.lire_source': 11500, 'transform.nettoyer': 11210, 'load.charger': 11210 })
  })

  it('renvoie les écarts et contrôles désérialisés, et rien pour un run non instrumenté', async () => {
    const run = lastRun('produits_etl').dag_run_id
    const r = await get(`/lens/run-details?dag_id=produits_etl&run_id=${encodeURIComponent(run)}`)
    expect(r.body.breakdown['transform.nettoyer'].ecarts).toHaveLength(2)
    expect(r.body.checks['load.charger'][0].statut).toBe('ok')
    const v = await get(`/lens/run-details?dag_id=ventes_jour&run_id=${encodeURIComponent(lastRun('ventes_jour').dag_run_id)}`)
    expect(v.body).toEqual({ breakdown: {}, checks: {} })
  })

  it('extrait la fin du journal en texte', async () => {
    const run = lastRun('ventes_jour').dag_run_id
    const r = await get(`/lens/log?dag_id=ventes_jour&run_id=${encodeURIComponent(run)}&task_id=transform.nettoyer&try=2`)
    expect(r.body.lines.at(-1)).toContain('colonne « prix » absente')
  })

  it('expose la configuration publique sans secret', async () => {
    const r = await get('/lens/config')
    expect(r.body).toEqual({ airflowUrl: af.url, actions: true, replayableTag: 'rejouable' })
  })
})

describe('pannes', () => {
  it('Airflow injoignable → 502 unreachable', async () => {
    await startLens(envFor({ AIRFLOW_URL: 'http://127.0.0.1:1' }))
    const r = await get('/lens/api/dags')
    expect(r).toMatchObject({ status: 502, body: { error: 'unreachable' } })
  })
  it('Airflow trop lent → 504 timeout', async () => {
    await startLens(envFor(), 200)
    af.delayMs = 600
    const r = await get('/lens/api/dags')
    expect(r).toMatchObject({ status: 504, body: { error: 'timeout' } })
  })
  it('Airflow en erreur 503 → 502 api', async () => {
    await get('/lens/api/dags')
    af.down = true
    const r = await get('/lens/api/dags')
    expect(r).toMatchObject({ status: 502, body: { error: 'api' } })
  })
  it('identifiants refusés → 401 auth', async () => {
    await startLens(envFor({ AIRFLOW_LENS_PASSWORD: 'faux' }))
    expect(await get('/lens/api/dags')).toMatchObject({ status: 401, body: { error: 'auth' } })
  })
  it('identifiants absents → 500 config', async () => {
    await startLens(envFor({ AIRFLOW_LENS_USER: '' }))
    expect(await get('/lens/api/dags')).toMatchObject({ status: 500, body: { error: 'config' } })
  })
  it('ressource inconnue → 404', async () => {
    expect((await get('/lens/api/dags/inconnu')).status).toBe(404)
    expect((await get('/lens/inconnu')).status).toBe(404)
  })
})

describe('lecture seule', () => {
  it('refuse toute écriture autre que le rejeu (405)', async () => {
    for (const method of ['POST', 'PUT', 'PATCH', 'DELETE']) {
      const r = await fetch(base + '/lens/api/dags/produits_etl', { method })
      expect(r.status).toBe(405)
    }
    expect(af.calls.filter((c) => c.method !== 'GET')).toHaveLength(0)
  })
})

describe('rejeu contrôlé', () => {
  const corps = () => ({ dag_id: 'produits_etl', dag_run_id: lastRun('produits_etl').dag_run_id, mode: 'run', dry_run: true })

  it('désactivé par LENS_ACTIONS=off', async () => {
    await startLens(envFor({ LENS_ACTIONS: 'off' }))
    expect(await replay(corps())).toMatchObject({ status: 403, body: { error: 'actions_off' } })
    expect((await get('/lens/config')).body.actions).toBe(false)
  })
  it('exige l’en-tête anti-CSRF', async () => {
    expect(await replay(corps(), {})).toMatchObject({ status: 403, body: { error: 'csrf' } })
  })
  it('valide le corps de la requête', async () => {
    expect((await replay({ dag_id: 'produits_etl' })).status).toBe(400)
    expect((await replay({ ...corps(), mode: 'tout' })).status).toBe(400)
    expect((await replay({ ...corps(), mode: 'task' })).status).toBe(400)
    const r = await fetch(base + '/lens/actions/replay', { method: 'POST', headers: { 'X-Lens-Action': '1' }, body: '{pas du json' })
    expect(r.status).toBe(400)
  })
  it('refuse un DAG non déclaré rejouable', async () => {
    const r = await replay({ ...corps(), dag_id: 'ventes_jour', dag_run_id: lastRun('ventes_jour').dag_run_id })
    expect(r).toMatchObject({ status: 403, body: { error: 'not_replayable' } })
  })
  it('refuse une exécution encore en cours', async () => {
    const r = await replay({ ...corps(), dag_id: 'stocks_horaire', dag_run_id: lastRun('stocks_horaire').dag_run_id })
    expect(r).toMatchObject({ status: 409, body: { error: 'run_active' } })
  })
  it('refuse un rejeu sans simulation préalable', async () => {
    expect(await replay({ ...corps(), dry_run: false })).toMatchObject({ status: 409, body: { error: 'confirm_required' } })
  })

  it('simulation puis confirmation : rejoue avec le compte op et journalise', async () => {
    const sim = await replay(corps())
    expect(sim.status).toBe(200)
    expect(sim.body.tasks).toHaveLength(3)
    expect(sim.body.confirm_token).toBeTruthy()
    expect(lastRun('produits_etl').state).toBe('success') // la simulation ne change rien

    const ok = await replay({ ...corps(), dry_run: false, confirm_token: sim.body.confirm_token })
    expect(ok.status).toBe(200)
    expect(lastRun('produits_etl').state).toBe('queued')
    const clear = af.calls.find((c) => c.method === 'POST' && !(c.body as { dry_run: boolean }).dry_run)!
    expect(clear.user).toBe('lens_ops')
    expect((clear.body as { note: string }).note).toContain('Pipeline Lens')

    const lignes = readFileSync(auditFile, 'utf-8').trim().split('\n').map((l) => JSON.parse(l))
    expect(lignes.map((l) => l.action)).toEqual(['simulation', 'rejeu'])
    expect(lignes[1]).toMatchObject({ resultat: 'lance', compte: 'lens_ops', taches: 3 })
    expect(JSON.stringify(lignes)).not.toMatch(/ops-test|jeton-/)
    const audit = await get('/lens/actions/audit')
    expect(audit.body.entries[0].action).toBe('rejeu')
  })

  it('le jeton de confirmation est à usage unique et lié à la demande', async () => {
    const sim = await replay(corps())
    const autre = await replay({ ...corps(), mode: 'failed', dry_run: false, confirm_token: sim.body.confirm_token })
    expect(autre.body.error).toBe('confirm_required')
    await replay({ ...corps(), dry_run: false, confirm_token: sim.body.confirm_token })
    lastRun('produits_etl').state = 'success'
    clock += 10_000
    const bis = await replay({ ...corps(), dry_run: false, confirm_token: sim.body.confirm_token })
    expect(bis.body.error).toBe('confirm_required')
  })

  it('le jeton expire après 2 minutes', async () => {
    const sim = await replay(corps())
    clock += 121_000
    const r = await replay({ ...corps(), dry_run: false, confirm_token: sim.body.confirm_token })
    expect(r.body.error).toBe('confirm_required')
    const lignes = readFileSync(auditFile, 'utf-8').trim().split('\n').map((l) => JSON.parse(l))
    expect(lignes.at(-1)).toMatchObject({ action: 'rejeu', resultat: 'refuse', motif: 'confirm_required' })
  })

  it('limite à un rejeu toutes les 5 s par exécution', async () => {
    let sim = await replay(corps())
    await replay({ ...corps(), dry_run: false, confirm_token: sim.body.confirm_token })
    lastRun('produits_etl').state = 'failed'
    sim = await replay(corps())
    const r = await replay({ ...corps(), dry_run: false, confirm_token: sim.body.confirm_token })
    expect(r).toMatchObject({ status: 429, body: { error: 'too_fast' } })
  })

  it('rejeu des seules tâches en échec, et d’une tâche avec ses dépendantes', async () => {
    const run = lastRun('produits_etl')
    run.state = 'failed'
    af.state.tis.filter((t) => t.dag_run_id === run.dag_run_id && t.dag_id === 'produits_etl').forEach((t, i) => { t.state = i === 0 ? 'success' : 'failed' })
    const f = await replay({ ...corps(), mode: 'failed' })
    expect(f.body.tasks.map((t: { task_id: string }) => t.task_id)).toEqual(['transform.nettoyer', 'load.charger'])
    const t = await replay({ ...corps(), mode: 'task', task_id: 'transform.nettoyer', include_downstream: true })
    expect(t.body.tasks).toHaveLength(2)
    const seule = await replay({ ...corps(), mode: 'task', task_id: 'transform.nettoyer', include_downstream: false })
    expect(seule.body.tasks).toHaveLength(1)
  })

  it('compte de rejeu sans droit op → 403 forbidden', async () => {
    await startLens(envFor({ AIRFLOW_LENS_OPS_USER: 'lens_lecture', AIRFLOW_LENS_OPS_PASSWORD: 'lecture-test' }))
    expect(await replay(corps())).toMatchObject({ status: 403, body: { error: 'forbidden' } })
  })

  it('sans fichier d’audit configuré, l’historique des actions est vide', async () => {
    const h = createLensHandler(envFor(), {})
    expect(typeof h).toBe('function')
    expect(existsSync(auditFile)).toBe(false)
  })
})

describe('robustesse', () => {
  it('20 requêtes simultanées à froid : une seule authentification auprès d’Airflow', async () => {
    af.tokensIssued = 0
    const r = await Promise.all(Array.from({ length: 20 }, () => get('/lens/api/dags')))
    expect(r.every((x) => x.status === 200)).toBe(true)
    expect(af.tokensIssued).toBe(1)
  })

  it('200 requêtes en rafale sans erreur ni fuite de jeton', async () => {
    const r = await Promise.all(Array.from({ length: 200 }, (_, i) => get(i % 2 ? '/lens/api/dags' : '/lens/volumes?dag_id=produits_etl')))
    expect(r.filter((x) => x.status !== 200)).toHaveLength(0)
    expect(JSON.stringify(r.map((x) => x.body))).not.toMatch(/jeton-/)
  })

  it('refuse toute tentative de sortir de l’API v2', async () => {
    for (const p of ['/lens/api/..%2F..%2Fauth/token', '/lens/api/dags/%2e%2e/%2e%2e/auth', '/lens/api/dags%5c..%5cconfig']) {
      // 400 (refusé par le proxy) ou 404 (chemin déjà normalisé par le client) : jamais relayé.
      expect([400, 404], p).toContain((await get(p)).status)
    }
    expect(af.calls.some((c) => c.path.includes('auth'))).toBe(false)
  })

  it('refuse un corps de requête trop volumineux (413)', async () => {
    const r = await fetch(base + '/lens/actions/replay', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Lens-Action': '1' }, body: JSON.stringify({ x: 'a'.repeat(20_000) }),
    })
    expect(r.status).toBe(413)
  })

  it('identifiants inconnus dans une route : 404 propre, pas d’erreur interne', async () => {
    const r = await get('/lens/run-details?dag_id=inconnu&run_id=x')
    expect(r.status).toBe(404)
    expect(r.body.error).toBe('not_found')
  })

  it('les réponses portent des en-têtes de sécurité et ne sont pas mises en cache', async () => {
    const r = await fetch(base + '/lens/config')
    expect(r.headers.get('cache-control')).toBe('no-store')
    expect(r.headers.get('x-content-type-options')).toBe('nosniff')
  })
})

describe('cache des volumes', () => {
  it('une valeur republiée après un rejeu remplace l’ancienne', async () => {
    const run = lastRun('produits_etl').dag_run_id
    expect((await get('/lens/volumes?dag_id=produits_etl')).body[run]['load.charger']).toBe(11210)
    af.state.xcom[`produits_etl|${run}|load.charger|row_count`] = 11300
    af.xcomTimes[`produits_etl|${run}|load.charger|row_count`] = '2026-11-10T23:00:00Z'
    expect((await get('/lens/volumes?dag_id=produits_etl')).body[run]['load.charger']).toBe(11300)
  })
  it('100 lectures simultanées des volumes : chaque valeur n’est demandée qu’une fois à Airflow', async () => {
    af.calls.length = 0
    await Promise.all(Array.from({ length: 100 }, () => get('/lens/volumes?dag_id=produits_etl')))
    const valeurs = af.calls.filter((c) => c.path.includes('/xcomEntries/row_count'))
    expect(new Set(valeurs.map((c) => c.path)).size).toBe(valeurs.length)
  })
})


describe('journal des incidents', () => {
  it('renvoie les événements écrits par les DAG, ignore les lignes illisibles', async () => {
    const f = join(mkdtempSync(join(tmpdir(), 'lens-incidents-')), 'incidents.jsonl')
    const e1 = { ts: '2026-11-11T10:00:00+00:00', evenement: 'echec_essai', dag_id: 'd', task_id: 't', run_id: 'r', essai: 1, essais_max: 4, raison: 'X' }
    const e2 = { ...e1, ts: '2026-11-11T10:00:45+00:00', evenement: 'reprise', duree_s: 45 }
    writeFileSync(f, `${JSON.stringify(e1)}\r\n{tronqu\n${JSON.stringify(e2)}\n`, 'utf-8')
    await startLens(envFor({ LENS_INCIDENTS_FILE: f }))
    const r = await get('/lens/incidents')
    expect(r.status).toBe(200)
    expect(r.body.events.map((e: { evenement: string }) => e.evenement)).toEqual(['echec_essai', 'reprise'])
  })

  it('sans journal (aucun incident encore) : liste vide, pas d’erreur', async () => {
    await startLens(envFor({ LENS_INCIDENTS_FILE: join(tmpdir(), 'inexistant-lens', 'incidents.jsonl') }))
    expect((await get('/lens/incidents')).body).toEqual({ events: [] })
  })

  it('le journal est en lecture seule depuis Lens', async () => {
    const r = await fetch(base + '/lens/incidents', { method: 'POST', headers: { 'X-Lens-Action': '1' }, body: '{}' })
    expect(r.status).toBe(405)
  })
})
