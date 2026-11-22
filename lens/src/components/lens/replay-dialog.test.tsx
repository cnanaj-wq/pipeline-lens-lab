import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { LensError, type GlobalRun } from '@/lib/api'

const replay = vi.fn()
const run = vi.fn()
const taskInstances = vi.fn()
vi.mock('@/lib/api', async (orig) => ({ ...(await orig<typeof import('@/lib/api')>()), lens: { replay: (...a: unknown[]) => replay(...a), run: (...a: unknown[]) => run(...a), taskInstances: (...a: unknown[]) => taskInstances(...a) } }))
vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { success: vi.fn(), error: vi.fn(), info: vi.fn() }) }))

const { ReplayDialog } = await import('./replay-dialog')

const r: GlobalRun = { dag_id: 'produits_etl', dag_run_id: 'scheduled__1', state: 'failed', run_type: 'scheduled', run_after: '2026-11-10T09:00:00Z', start_date: '2026-11-10T09:00:00Z', end_date: null, duration: 4, conf: null }
const tasks = ['extract.lire', 'transform.nettoyer', 'load.charger'].map((task_id) => ({ task_id, task_display_name: task_id, downstream_task_ids: [], operator_name: null }))

beforeEach(() => { replay.mockReset(); run.mockReset(); taskInstances.mockReset() })

describe('fenêtre de rejeu', () => {
  it('propose les tâches en échec par défaut, simule puis confirme et suit jusqu’au succès', async () => {
    const u = userEvent.setup()
    replay.mockResolvedValueOnce({ dry_run: true, tasks: [{ task_id: 'transform.nettoyer', state: 'failed' }, { task_id: 'load.charger', state: 'upstream_failed' }], confirm_token: 'jeton', expires_in: 120 })
      .mockResolvedValueOnce({ dry_run: false, tasks: [] })
    run.mockResolvedValue({ ...r, state: 'success' })
    taskInstances.mockResolvedValue([{ task_id: 'transform.nettoyer', state: 'success' }, { task_id: 'load.charger', state: 'success' }])
    const onDone = vi.fn()
    render(<ReplayDialog target={{ run: r, tasks }} onOpenChange={() => {}} onDone={onDone} />)

    expect(screen.getByRole('radio', { name: /tâches en échec/i })).toBeChecked()
    await u.click(screen.getByRole('button', { name: 'Simuler le rejeu' }))
    expect(replay).toHaveBeenLastCalledWith({ dag_id: 'produits_etl', dag_run_id: 'scheduled__1', mode: 'failed', dry_run: true })
    expect(await screen.findByText(/2 tâches seront relancées/)).toBeInTheDocument()
    expect(screen.getByText(/valable 120 s/)).toBeInTheDocument()

    await u.click(screen.getByRole('button', { name: 'Confirmer le rejeu' }))
    expect(replay).toHaveBeenLastCalledWith(expect.objectContaining({ dry_run: false, confirm_token: 'jeton' }))
    expect(await screen.findByText('Exécution rejouée avec succès.')).toBeInTheDocument()
    expect(onDone).toHaveBeenCalled()
  })

  it('désactive « tâches en échec » pour une exécution réussie', () => {
    render(<ReplayDialog target={{ run: { ...r, state: 'success' }, tasks }} onOpenChange={() => {}} />)
    expect(screen.getByRole('radio', { name: /tâches en échec/i })).toBeDisabled()
    expect(screen.getByRole('radio', { name: /toute l’exécution/i })).toBeChecked()
  })

  it('rejeu d’une tâche : envoie la tâche et l’option « dépendantes »', async () => {
    const u = userEvent.setup()
    replay.mockResolvedValueOnce({ dry_run: true, tasks: [{ task_id: 'load.charger', state: 'success' }], confirm_token: 't', expires_in: 120 })
    render(<ReplayDialog target={{ run: { ...r, state: 'success' }, tasks, taskId: 'load.charger' }} onOpenChange={() => {}} />)
    await u.click(screen.getByRole('checkbox', { name: /dépendantes/ }))
    await u.click(screen.getByRole('button', { name: 'Simuler le rejeu' }))
    expect(replay).toHaveBeenLastCalledWith(expect.objectContaining({ mode: 'task', task_id: 'load.charger', include_downstream: false }))
  })

  it('affiche le refus du serveur sans rien lancer', async () => {
    const u = userEvent.setup()
    replay.mockRejectedValueOnce(new LensError(403, 'not_replayable', 'Ce DAG n’est pas déclaré rejouable.'))
    render(<ReplayDialog target={{ run: r, tasks }} onOpenChange={() => {}} />)
    await u.click(screen.getByRole('button', { name: 'Simuler le rejeu' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('pas déclaré rejouable')
    expect(screen.queryByRole('button', { name: 'Confirmer le rejeu' })).not.toBeInTheDocument()
  })

  it('la simulation expire au bout de son délai', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const u = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })
    replay.mockResolvedValueOnce({ dry_run: true, tasks: [], confirm_token: 't', expires_in: 2 })
    render(<ReplayDialog target={{ run: r, tasks }} onOpenChange={() => {}} />)
    await u.click(screen.getByRole('button', { name: 'Simuler le rejeu' }))
    await screen.findByRole('button', { name: 'Confirmer le rejeu' })
    await vi.advanceTimersByTimeAsync(3000)
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('expiré'))
    vi.useRealTimers()
  })
})
