import { describe, expect, it } from 'vitest'
import type { DagRun } from './api'
import { buckets, eta, kpis, matchRun, median, replayGate, snapshotStates, successRate, transitions } from './history'

const NOW = Date.parse('2026-11-10T12:00:00Z')
const h = (n: number) => new Date(NOW - n * 3600_000).toISOString()
const run = (id: string, state: DagRun['state'], hoursAgo: number, duration: number | null = 10): DagRun & { dag_id: string } => ({
  dag_id: 'd', dag_run_id: id, state, run_type: 'scheduled', run_after: h(hoursAgo), start_date: h(hoursAgo),
  end_date: state === 'running' ? null : h(hoursAgo), duration: state === 'running' ? null : duration, conf: null,
})
const config = { airflowUrl: null, actions: true, replayableTag: 'rejouable' }

describe('médiane et taux de succès', () => {
  it('médiane paire, impaire, vide', () => {
    expect(median([3, 1, 2])).toBe(2)
    expect(median([4, 1, 2, 3])).toBe(2.5)
    expect(median([])).toBeNull()
  })
  it('ne compte que les exécutions terminées de la période', () => {
    const r = successRate([run('a', 'success', 1), run('b', 'failed', 2), run('c', 'running', 1), run('d', 'success', 30)], NOW - 24 * 3600_000)
    expect(r).toEqual({ rate: 0.5, done: 2, ok: 1 })
  })
  it('sans exécution terminée : taux inconnu, pas 0 %', () => {
    expect(successRate([run('c', 'running', 1)], 0).rate).toBeNull()
  })
})

describe('indicateurs du cockpit', () => {
  it('compte DAG actifs, en cours, en échec', () => {
    const dags = [
      { dag_id: 'a', is_paused: false, lastRun: run('1', 'failed', 1) },
      { dag_id: 'b', is_paused: true, lastRun: run('2', 'success', 1) },
      { dag_id: 'c', is_paused: false, lastRun: null },
    ] as never
    const k = kpis(dags, [run('1', 'failed', 1, 5), run('2', 'success', 1, 20), run('3', 'running', 0), run('4', 'queued', 0)], NOW)
    expect(k).toMatchObject({ surveilles: 3, actifs: 2, enCours: 2, enEchec: 1, dureeMediane: 20 })
  })
})

describe('graphique d’activité', () => {
  it('24 tranches horaires, la dernière contient l’heure courante', () => {
    const b = buckets([run('a', 'success', 0.5), run('b', 'failed', 0.2), run('c', 'running', 0.1), run('x', 'success', 30)], NOW, '24h')
    expect(b).toHaveLength(24)
    expect(b.at(-1)!.end).toBeGreaterThanOrEqual(NOW)
    const total = b.reduce((s, x) => s + x.success + x.failed + x.active + x.other, 0)
    expect(total).toBe(3) // l'exécution d'il y a 30 h est hors fenêtre
  })
  it('7 tranches journalières', () => {
    expect(buckets([], NOW, '7j')).toHaveLength(7)
  })
})

describe('avancement estimé', () => {
  const passe = [run('1', 'success', 5, 100), run('2', 'success', 4, 100), run('3', 'success', 3, 100)]
  it('progression rapportée à la médiane', () => {
    const r = { ...run('x', 'running', 0), start_date: new Date(NOW - 50_000).toISOString() }
    expect(eta(r, passe, NOW)).toMatchObject({ progress: 0.5, remaining: 50, overrun: false })
  })
  it('dépassement : plus d’estimation de fin, signalé', () => {
    const r = { ...run('x', 'running', 0), start_date: new Date(NOW - 150_000).toISOString() }
    expect(eta(r, passe, NOW)).toMatchObject({ remaining: null, overrun: true, progress: 0.97 })
  })
  it('pas d’estimation sans au moins 3 exécutions réussies, ni pour une exécution terminée', () => {
    expect(eta(run('x', 'running', 0), passe.slice(0, 2), NOW)).toBeNull()
    expect(eta(run('x', 'success', 0), passe, NOW)).toBeNull()
  })
})

describe('règles du rejeu', () => {
  const dag = { tags: [{ name: 'rejouable' }] }
  it('autorisé pour un DAG rejouable dont l’exécution est terminée', () => {
    expect(replayGate(dag, { state: 'failed' }, config)).toEqual({ ok: true })
  })
  it('refusé en lecture seule, DAG non rejouable, ou exécution en cours', () => {
    expect(replayGate(dag, { state: 'failed' }, { ...config, actions: false })).toMatchObject({ ok: false, reason: expect.stringContaining('lecture seule') })
    expect(replayGate({ tags: [] }, { state: 'failed' }, config)).toMatchObject({ ok: false, reason: expect.stringContaining('idempotence') })
    expect(replayGate(dag, { state: 'running' }, config)).toMatchObject({ ok: false, reason: expect.stringContaining('en cours') })
    expect(replayGate(dag, { state: 'success' }, null).ok).toBe(false)
  })
})

describe('notifications de changement d’état', () => {
  const avant = [{ dag_id: 'a', lastRun: run('1', 'running', 0) }, { dag_id: 'b', lastRun: run('2', 'success', 1) }]
  it('rien à la première lecture', () => {
    expect(transitions(null, avant)).toEqual([])
  })
  it('détecte une fin d’exécution et une nouvelle exécution', () => {
    const prev = snapshotStates(avant)
    const apres = [{ dag_id: 'a', lastRun: run('1', 'failed', 0) }, { dag_id: 'b', lastRun: run('3', 'running', 0) }]
    expect(transitions(prev, apres).map((t) => [t.dag_id, t.from, t.to])).toEqual([['a', 'running', 'failed'], ['b', 'nouvelle', 'running']])
  })
  it('rien si rien ne change', () => {
    expect(transitions(snapshotStates(avant), avant)).toEqual([])
  })
})

it('recherche par identifiant d’exécution ou de DAG, insensible à la casse', () => {
  const r = run('manual__2026', 'success', 1)
  expect(matchRun(r, 'MANUAL')).toBe(true)
  expect(matchRun(r, ' d ')).toBe(true)
  expect(matchRun(r, 'zzz')).toBe(false)
  expect(matchRun(r, '')).toBe(true)
})
