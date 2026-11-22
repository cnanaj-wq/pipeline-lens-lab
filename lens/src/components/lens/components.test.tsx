import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { DagRun, TaskInstance } from '@/lib/api'
import { classify } from '@/lib/stages'
import { AnimatedNumber } from './animated-number'
import { IncidentBanner } from './incident'
import { QualityPanel } from './quality'

const NOW = Date.parse('2026-11-10T12:00:00Z')
const run = (id: string, state: DagRun['state'], minutesAgo: number): DagRun => ({
  dag_run_id: id, state, run_type: 'scheduled', run_after: new Date(NOW - minutesAgo * 60_000).toISOString(),
  start_date: new Date(NOW - minutesAgo * 60_000).toISOString(), end_date: null, duration: 5, conf: null,
})
const ti = (task_id: string, dag_run_id: string, state: TaskInstance['state'], try_number = 1): TaskInstance => ({
  task_id, dag_run_id, state, start_date: null, end_date: null, duration: 1, try_number, max_tries: 1, operator_name: null,
})

describe('bandeau d’incident', () => {
  it('montre la dernière exécution en échec, la tâche fautive et son essai', () => {
    const runs = [run('r2', 'failed', 5), run('r1', 'success', 60)]
    render(<IncidentBanner runs={runs} instances={{ r2: [ti('transform.nettoyer', 'r2', 'failed', 2), ti('load.charger', 'r2', 'upstream_failed')] }}
      selectedRunId="r1" now={NOW} onSelectRun={vi.fn()} onOpenTask={vi.fn()} />)
    expect(screen.getByText(/Nettoyer/)).toBeInTheDocument()
    expect(screen.getByText(/essai 2 sur 2/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Voir le journal/ })).toBeInTheDocument()
  })
  it('absent quand aucune exécution n’a échoué', () => {
    const { container } = render(<IncidentBanner runs={[run('r1', 'success', 5)]} instances={{}} selectedRunId={null} now={NOW} onSelectRun={vi.fn()} onOpenTask={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
  })
})

describe('panneau qualité', () => {
  const tasks = ['extract.lire', 'transform.nettoyer', 'load.charger'].map((task_id, i, a) => ({ task_id, task_display_name: task_id, downstream_task_ids: a[i + 1] ? [a[i + 1]] : [], operator_name: null }))
  it('affiche « Non instrumenté » sans données qualité, sans rien inventer', () => {
    const r = run('r1', 'success', 5)
    render(<QualityPanel run={r} runs={[r]} stages={classify(tasks)} volumes={{}} details={{ status: 'success', data: { breakdown: {}, checks: {} } }} now={NOW} />)
    expect(screen.getAllByText(/non instrumenté/i).length).toBeGreaterThan(0)
    expect(screen.queryByText(/^0$/)).not.toBeInTheDocument()
  })
})

describe('nombre animé', () => {
  it('affiche directement la valeur finale si les animations sont réduites', () => {
    vi.stubGlobal('matchMedia', (q: string) => ({ matches: q.includes('reduce'), media: q, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }))
    render(<AnimatedNumber value={1234} />)
    expect(screen.getByText(/1\s?234/)).toBeInTheDocument()
    vi.unstubAllGlobals()
  })
})
