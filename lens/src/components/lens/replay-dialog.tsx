import { useEffect, useRef, useState } from 'react'
import { AnimatePresence, motion, useReducedMotion } from 'motion/react'
import { CheckCircle2Icon, LoaderIcon, RotateCcwIcon, ShieldCheckIcon, TriangleAlertIcon } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { lens, LensError, type GlobalRun, type ReplayMode, type ReplayResult, type State, type Task } from '@/lib/api'
import { fmtDateTime, humanize, stateInfo } from '@/lib/format'
import { TERMINAL } from '@/lib/history'
import { StateIcon, StatePill } from './status'

export interface ReplayTarget {
  run: GlobalRun
  tasks: Task[]
  /** Pré-sélection : rejouer une tâche précise (depuis le détail d'une tâche). */
  taskId?: string
}

type Phase = 'choix' | 'simulation' | 'apercu' | 'envoi' | 'suivi' | 'fini'

/**
 * Rejeu contrôlé en trois temps : choisir quoi rejouer, voir la simulation (tâches touchées,
 * rien n'est modifié), confirmer. Puis suivi en direct jusqu'à la fin de l'exécution.
 */
export function ReplayDialog({ target, onOpenChange, onDone, onLaunched }: {
  target: ReplayTarget | null
  onOpenChange: (open: boolean) => void
  onDone?: () => void
  onLaunched?: () => void
}) {
  const reduce = useReducedMotion()
  const run = target?.run
  const [mode, setMode] = useState<ReplayMode>('run')
  const [taskId, setTaskId] = useState<string>('')
  const [downstream, setDownstream] = useState(true)
  const [phase, setPhase] = useState<Phase>('choix')
  const [preview, setPreview] = useState<ReplayResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [live, setLive] = useState<{ state: State; tasks: Record<string, State> } | null>(null)
  const [expires, setExpires] = useState(0)
  const confirmRef = useRef<HTMLButtonElement>(null)

  // Réinitialise à chaque ouverture.
  useEffect(() => {
    if (!target) return
    setMode(target.taskId ? 'task' : target.run.state === 'failed' ? 'failed' : 'run')
    setTaskId(target.taskId ?? target.tasks[0]?.task_id ?? '')
    setDownstream(true)
    setPhase('choix'); setPreview(null); setError(null); setLive(null)
  }, [target])

  // Compte à rebours de validité de la simulation (2 min côté serveur).
  useEffect(() => {
    if (phase !== 'apercu') return
    const id = setInterval(() => setExpires((e) => e - 1), 1000)
    return () => clearInterval(id)
  }, [phase])
  useEffect(() => {
    if (phase === 'apercu' && expires <= 0) {
      setPhase('choix'); setPreview(null)
      setError('La simulation a expiré : relancez-la avant de confirmer.')
    }
  }, [phase, expires])

  // Suivi en direct après le rejeu, jusqu'à un état terminal.
  useEffect(() => {
    if (phase !== 'suivi' || !run) return
    let stop = false
    const poll = async () => {
      try {
        const [r, tis] = await Promise.all([lens.run(run.dag_id, run.dag_run_id), lens.taskInstances(run.dag_id, run.dag_run_id)])
        if (stop || !r) return
        setLive({ state: r.state, tasks: Object.fromEntries(tis.map((t) => [t.task_id, t.state])) })
        if (TERMINAL.includes(r.state)) {
          setPhase('fini')
          const ok = r.state === 'success'
          toast[ok ? 'success' : 'error'](ok ? 'Rejeu terminé avec succès' : 'Le rejeu a échoué', { description: `${run.dag_id} · ${fmtDateTime(run.start_date ?? run.run_after)}` })
          onDone?.()
          return
        }
      } catch { /* nouvelle tentative au prochain tour */ }
      if (!stop) setTimeout(poll, 2000)
    }
    poll()
    return () => { stop = true }
  }, [phase, run, onDone])

  if (!target || !run) return <Dialog open={false} onOpenChange={onOpenChange} />

  const body = () => ({ dag_id: run.dag_id, dag_run_id: run.dag_run_id, mode, ...(mode === 'task' ? { task_id: taskId, include_downstream: downstream } : {}) })
  const failure = (e: unknown) => setError(e instanceof LensError ? e.message : 'Le serveur Pipeline Lens ne répond pas.')

  const simulate = async () => {
    setPhase('simulation'); setError(null)
    try {
      const r = await lens.replay({ ...body(), dry_run: true })
      setPreview(r); setExpires(r.expires_in ?? 120); setPhase('apercu')
      setTimeout(() => confirmRef.current?.focus(), 50)
    } catch (e) { failure(e); setPhase('choix') }
  }
  const confirm = async () => {
    if (!preview?.confirm_token) return
    setPhase('envoi'); setError(null)
    try {
      await lens.replay({ ...body(), dry_run: false, confirm_token: preview.confirm_token })
      toast.info('Rejeu lancé', { description: `${preview.tasks.length} tâche${preview.tasks.length > 1 ? 's' : ''} remise${preview.tasks.length > 1 ? 's' : ''} en file` })
      setPhase('suivi')
      onLaunched?.()
    } catch (e) { failure(e); setPhase('choix'); setPreview(null) }
  }

  const modes: { id: ReplayMode; label: string; hint: string; disabled?: boolean }[] = [
    { id: 'failed', label: 'Les tâches en échec', hint: 'Relance les tâches en échec et celles qu’elles ont bloquées.', disabled: run.state !== 'failed' },
    { id: 'run', label: 'Toute l’exécution', hint: 'Relance toutes les tâches de cette exécution.' },
    { id: 'task', label: 'Une tâche', hint: 'Relance une tâche, avec ou sans les tâches qui en dépendent.' },
  ]
  const busy = phase === 'simulation' || phase === 'envoi'
  const enSuivi = phase === 'suivi' || phase === 'fini'
  const shown = enSuivi && live ? (preview?.tasks ?? []).map((t) => ({ task_id: t.task_id, state: live.tasks[t.task_id] ?? null })) : preview?.tasks ?? []

  return (
    <Dialog open onOpenChange={(o) => { if (!busy) onOpenChange(o) }}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2"><RotateCcwIcon aria-hidden className="size-4" />Rejouer une exécution</DialogTitle>
          <DialogDescription>
            <span className="font-medium text-foreground">{run.dag_id}</span>, exécution du {fmtDateTime(run.start_date ?? run.run_after)}
            <span className="mt-0.5 block font-mono text-[0.7rem]">{run.dag_run_id}</span>
          </DialogDescription>
        </DialogHeader>

        {!enSuivi && (
          <fieldset disabled={phase !== 'choix'} className="space-y-2">
            <legend className="mb-2 text-sm font-medium">Que rejouer ?</legend>
            {modes.map((m) => (
              <label key={m.id} className={`flex cursor-pointer gap-3 rounded-md border p-2.5 text-sm has-checked:border-foreground/40 has-checked:bg-muted/60 ${m.disabled ? 'cursor-not-allowed opacity-50' : ''}`}>
                <input type="radio" name="mode" value={m.id} checked={mode === m.id} disabled={m.disabled}
                  onChange={() => { setMode(m.id); setPreview(null) }} className="mt-0.5 accent-foreground" />
                <span>
                  <span className="block font-medium">{m.label}</span>
                  <span className="text-xs text-muted-foreground">{m.disabled ? 'Aucune tâche en échec dans cette exécution.' : m.hint}</span>
                </span>
              </label>
            ))}
            {mode === 'task' && (
              <div className="flex flex-wrap items-center gap-3 pl-1 pt-1">
                <label className="flex items-center gap-2 text-sm">
                  Tâche
                  <select value={taskId} onChange={(e) => { setTaskId(e.target.value); setPreview(null) }}
                    className="h-8 max-w-56 rounded-md border bg-background px-2 text-sm">
                    {target.tasks.map((t) => <option key={t.task_id} value={t.task_id}>{humanize(t.task_id)}</option>)}
                  </select>
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={downstream} onChange={(e) => { setDownstream(e.target.checked); setPreview(null) }} className="accent-foreground" />
                  avec les tâches dépendantes
                </label>
              </div>
            )}
          </fieldset>
        )}

        <AnimatePresence initial={false}>
          {(preview || enSuivi) && (
            <motion.section
              key="apercu"
              initial={reduce ? false : { opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="overflow-hidden"
              aria-live="polite"
            >
              <div className="rounded-md border">
                <p className="flex items-center justify-between gap-2 border-b px-3 py-2 text-xs text-muted-foreground">
                  {enSuivi
                    ? <span className="flex items-center gap-2">Suivi en direct {live && <StatePill state={live.state} />}</span>
                    : <span>Simulation : {shown.length} tâche{shown.length > 1 ? 's' : ''} seront relancée{shown.length > 1 ? 's' : ''}. Rien n’a encore été modifié.</span>}
                  {phase === 'apercu' && <span className="tabular shrink-0">valable {expires} s</span>}
                </p>
                <ul className="max-h-48 divide-y overflow-y-auto">
                  {shown.map((t, i) => (
                    <motion.li key={t.task_id} initial={reduce ? false : { opacity: 0, x: -6 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: reduce ? 0 : i * 0.04 }}
                      className="flex items-center justify-between gap-3 px-3 py-1.5 text-sm">
                      <span className="flex min-w-0 items-center gap-2"><StateIcon state={t.state ?? undefined} /><span className="truncate">{humanize(t.task_id)}</span></span>
                      <span className="shrink-0 text-xs text-muted-foreground">{enSuivi ? (t.state ? stateInfo(t.state).short : 'en attente') : `état actuel : ${stateInfo(t.state ?? undefined).short.toLowerCase()}`}</span>
                    </motion.li>
                  ))}
                </ul>
              </div>
            </motion.section>
          )}
        </AnimatePresence>

        {phase === 'apercu' && (
          <p className="flex gap-2 rounded-md bg-muted/60 p-2.5 text-xs text-muted-foreground">
            <ShieldCheckIcon aria-hidden className="mt-px size-3.5 shrink-0" />
            DAG déclaré idempotent par son auteur (tag « rejouable ») : le rejeu réécrit les mêmes fichiers sans doublon. Action journalisée.
          </p>
        )}
        {error && (
          <p role="alert" className="flex gap-2 rounded-md bg-ko/8 p-2.5 text-sm text-ko"><TriangleAlertIcon aria-hidden className="mt-0.5 size-4 shrink-0" />{error}</p>
        )}
        {phase === 'fini' && live && (
          <p className={`flex items-center gap-2 text-sm font-medium ${live.state === 'success' ? 'text-ok' : 'text-ko'}`}>
            {live.state === 'success' ? <CheckCircle2Icon aria-hidden className="size-4" /> : <TriangleAlertIcon aria-hidden className="size-4" />}
            {live.state === 'success' ? 'Exécution rejouée avec succès.' : 'L’exécution rejouée a échoué : consultez le journal de la tâche.'}
          </p>
        )}

        <DialogFooter>
          {enSuivi ? (
            <Button variant={phase === 'fini' ? 'default' : 'outline'} onClick={() => onOpenChange(false)}>
              {phase === 'fini' ? 'Fermer' : 'Continuer en arrière-plan'}
            </Button>
          ) : (
            <>
              <Button variant="outline" disabled={busy} onClick={() => onOpenChange(false)}>Annuler</Button>
              {phase === 'apercu' || phase === 'envoi' ? (
                <Button ref={confirmRef} onClick={confirm} disabled={busy}>
                  {phase === 'envoi' ? <LoaderIcon aria-hidden className="motion-safe:animate-spin" /> : <RotateCcwIcon aria-hidden />}
                  Confirmer le rejeu
                </Button>
              ) : (
                <Button onClick={simulate} disabled={busy || (mode === 'task' && !taskId)}>
                  {phase === 'simulation' && <LoaderIcon aria-hidden className="motion-safe:animate-spin" />}
                  Simuler le rejeu
                </Button>
              )}
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
