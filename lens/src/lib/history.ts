// Calculs du volet Historique : fonctions pures, testées dans history.test.ts.
import type { Dag, DagRun, GlobalRun, LensConfig, State } from './api'

export const ACTIVE: State[] = ['running', 'queued']
export const TERMINAL: State[] = ['success', 'failed']

export function median(values: number[]): number | null {
  const d = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b)
  if (!d.length) return null
  const m = Math.floor(d.length / 2)
  return d.length % 2 ? d[m] : (d[m - 1] + d[m]) / 2
}

const ms = (iso: string | null | undefined) => (iso ? new Date(iso).getTime() : NaN)

/** Taux de succès des exécutions terminées depuis `since` ; null si aucune n'est terminée. */
export function successRate(runs: DagRun[], since: number): { rate: number | null; done: number; ok: number } {
  const done = runs.filter((r) => TERMINAL.includes(r.state) && ms(r.run_after) >= since)
  const ok = done.filter((r) => r.state === 'success').length
  return { rate: done.length ? ok / done.length : null, done: done.length, ok }
}

export interface Kpis {
  surveilles: number
  actifs: number
  enCours: number
  enEchec: number
  succes24: ReturnType<typeof successRate>
  succes7: ReturnType<typeof successRate>
  dureeMediane: number | null
}

/** Indicateurs du cockpit, à partir des DAG et des exécutions des 7 derniers jours. */
export function kpis(dags: (Dag & { lastRun: DagRun | null })[], runs: DagRun[], now: number): Kpis {
  return {
    surveilles: dags.length,
    actifs: dags.filter((d) => !d.is_paused).length,
    enCours: runs.filter((r) => ACTIVE.includes(r.state)).length,
    enEchec: dags.filter((d) => d.lastRun?.state === 'failed').length,
    succes24: successRate(runs, now - 24 * 3600_000),
    succes7: successRate(runs, now - 7 * 24 * 3600_000),
    dureeMediane: median(runs.filter((r) => r.state === 'success' && r.duration != null).map((r) => r.duration as number)),
  }
}

export type Range = '24h' | '7j'
export interface Bucket { start: number; end: number; success: number; failed: number; active: number; other: number }

/** Exécutions regroupées par heure (24 h) ou par jour (7 j), de la plus ancienne à la plus récente. */
export function buckets(runs: DagRun[], now: number, range: Range): Bucket[] {
  const step = range === '24h' ? 3600_000 : 24 * 3600_000
  const n = range === '24h' ? 24 : 7
  const end = range === '24h' ? Math.ceil(now / step) * step : startOfDay(now) + step
  const out: Bucket[] = Array.from({ length: n }, (_, i) => ({ start: end - (n - i) * step, end: end - (n - i - 1) * step, success: 0, failed: 0, active: 0, other: 0 }))
  for (const r of runs) {
    const t = ms(r.run_after)
    const b = out.find((x) => t >= x.start && t < x.end)
    if (!b) continue
    if (r.state === 'success') b.success++
    else if (r.state === 'failed') b.failed++
    else if (ACTIVE.includes(r.state)) b.active++
    else b.other++
  }
  return out
}

function startOfDay(t: number) {
  const d = new Date(t)
  d.setHours(0, 0, 0, 0)
  return d.getTime()
}

export interface Eta { progress: number; remaining: number | null; overrun: boolean; median: number }

/**
 * Avancement estimé d'une exécution en cours : temps écoulé rapporté à la durée médiane
 * des exécutions réussies du même DAG (au moins 3). null si on ne peut pas estimer.
 */
export function eta(run: DagRun, sameDag: DagRun[], now: number): Eta | null {
  if (!ACTIVE.includes(run.state) || !run.start_date) return null
  const med = median(sameDag.filter((r) => r.state === 'success' && r.duration != null).map((r) => r.duration as number))
  if (med == null || sameDag.filter((r) => r.state === 'success').length < 3 || med <= 0) return null
  const elapsed = Math.max(0, (now - ms(run.start_date)) / 1000)
  const overrun = elapsed > med
  return { progress: Math.min(0.97, elapsed / med), remaining: overrun ? null : med - elapsed, overrun, median: med }
}

export type Gate = { ok: true } | { ok: false; reason: string }

/** Le rejeu est-il proposé pour cette exécution ? Les mêmes règles sont revérifiées par le serveur. */
export function replayGate(dag: Pick<Dag, 'tags'> | undefined, run: Pick<DagRun, 'state'>, config: LensConfig | null): Gate {
  if (!config?.actions) return { ok: false, reason: 'Rejeu désactivé : Pipeline Lens est en lecture seule (LENS_ACTIONS=off).' }
  if (!dag) return { ok: false, reason: 'DAG inconnu.' }
  if (!dag.tags.some((t) => t.name === config.replayableTag))
    return { ok: false, reason: `DAG non déclaré rejouable (tag « ${config.replayableTag} ») : idempotence non garantie.` }
  if (ACTIVE.includes(run.state)) return { ok: false, reason: 'Exécution en cours : attendez sa fin.' }
  return { ok: true }
}

export interface Transition { dag_id: string; run: DagRun; from: State | 'nouvelle'; to: State }

/**
 * Changements d'état entre deux lectures successives (dernière exécution de chaque DAG) :
 * sert aux notifications automatiques. La première lecture ne produit aucune notification.
 */
export function transitions(prev: Map<string, { runId: string; state: State }> | null, dags: { dag_id: string; lastRun: DagRun | null }[]): Transition[] {
  if (!prev) return []
  const out: Transition[] = []
  for (const d of dags) {
    const r = d.lastRun
    if (!r) continue
    const p = prev.get(d.dag_id)
    if (!p || p.runId !== r.dag_run_id) out.push({ dag_id: d.dag_id, run: r, from: 'nouvelle', to: r.state })
    else if (p.state !== r.state) out.push({ dag_id: d.dag_id, run: r, from: p.state, to: r.state })
  }
  return out
}

export const snapshotStates = (dags: { dag_id: string; lastRun: DagRun | null }[]) =>
  new Map(dags.filter((d) => d.lastRun).map((d) => [d.dag_id, { runId: d.lastRun!.dag_run_id, state: d.lastRun!.state }]))

/** Filtre local par identifiant d'exécution (la recherche porte sur la page chargée). */
export const matchRun = (r: GlobalRun, search: string) =>
  !search.trim() || r.dag_run_id.toLowerCase().includes(search.trim().toLowerCase()) || r.dag_id.toLowerCase().includes(search.trim().toLowerCase())
