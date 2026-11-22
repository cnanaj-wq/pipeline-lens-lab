import { motion, useReducedMotion } from 'motion/react'
import type { Task, TaskInstance } from '@/lib/api'
import { fmtDuration, fmtInt, humanize, stateInfo } from '@/lib/format'
import { STAGES, stageVolume, type StageKey } from '@/lib/stages'
import { cn } from '@/lib/utils'
import { StateIcon, toneText } from './status'

interface Props {
  stages: Record<StageKey, Task[]>
  instances: Record<string, TaskInstance>
  volumes: Record<string, number> | undefined
  selectedTask: string | null
  onSelectTask: (taskId: string) => void
  runKey: string
  /** Ouvre l'explication de l'écart (1 = Extract → Transform, 2 = Transform → Load). */
  onOpenGap?: (gap: 1 | 2) => void
  /** Écarts déclarés par les tâches comme de simples jointures (pas des pertes). */
  sansPerte?: Partial<Record<1 | 2, boolean>>
}

/** Pipeline Extract → Transform → Load avec rubans de débit entre les étapes. */
export function Pipeline({ stages, instances, volumes, selectedTask, onSelectTask, runKey, onOpenGap, sansPerte }: Props) {
  const stageState = (k: StageKey) => aggregateState(stages[k].map((t) => instances[t.task_id]))
  // Une étape non terminée avec succès n'a qu'un volume partiel : on ne calcule pas d'écart dessus.
  const complete = STAGES.map((s) => stages[s.key].length > 0 && stages[s.key].every((t) => instances[t.task_id]?.state === 'success'))
  const vols = STAGES.map((s) => stageVolume(stages[s.key], volumes))
  const max = Math.max(1, ...vols.map((v) => v ?? 0))

  return (
    <div className="grid gap-0 lg:grid-cols-[minmax(0,1fr)_76px_minmax(0,1fr)_76px_minmax(0,1fr)]">
      {STAGES.map((s, i) => (
        <div key={s.key} className="contents">
          {i > 0 && (
            <FlowRibbon
              from={complete[i - 1] ? vols[i - 1] : null}
              to={complete[i] ? vols[i] : null}
              partial={!complete[i - 1] || !complete[i]}
              max={max}
              active={stageState(s.key) === 'running' || stageState(STAGES[i - 1].key) === 'running'}
              runKey={runKey}
              index={i}
              onOpen={onOpenGap ? () => onOpenGap(i as 1 | 2) : undefined}
              jointure={!!sansPerte?.[i as 1 | 2]}
            />
          )}
          <StageColumn
            stage={s}
            tasks={stages[s.key]}
            instances={instances}
            volume={vols[i]}
            partial={!complete[i] && vols[i] != null}
            volumes={volumes}
            selectedTask={selectedTask}
            onSelectTask={onSelectTask}
          />
        </div>
      ))}
    </div>
  )
}

function aggregateState(tis: (TaskInstance | undefined)[]) {
  const st = tis.map((t) => t?.state)
  if (st.includes('failed')) return 'failed'
  if (st.some((s) => s === 'running' || s === 'queued' || s === 'up_for_retry')) return 'running'
  if (st.length && st.every((s) => s === 'success' || s === 'skipped')) return 'success'
  if (st.includes('upstream_failed')) return 'upstream_failed'
  return null
}

function StageColumn({
  stage, tasks, instances, volume, partial, volumes, selectedTask, onSelectTask,
}: {
  stage: (typeof STAGES)[number]
  tasks: Task[]
  instances: Record<string, TaskInstance>
  volume: number | null
  partial: boolean
  volumes: Record<string, number> | undefined
  selectedTask: string | null
  onSelectTask: (id: string) => void
}) {
  const state = aggregateState(tasks.map((t) => instances[t.task_id]))
  return (
    <section aria-label={`Étape ${stage.label}`} className="flex flex-col rounded-lg border bg-card">
      <header className="flex items-end justify-between gap-3 border-b px-4 pt-3.5 pb-3">
        <div>
          <h3 className="flex items-center gap-1.5 text-[0.95rem] font-semibold">
            <StateIcon state={state} />
            {stage.label}
          </h3>
          <p className="text-xs text-muted-foreground">{stage.hint}</p>
        </div>
        <div className="text-right">
          <p className={cn('tabular text-xl leading-none font-semibold', volume == null && 'text-base font-normal text-muted-foreground')}>
            {volume == null ? (state === 'upstream_failed' || state === 'failed' ? 'non exécuté' : 'non mesuré') : fmtInt(volume)}
          </p>
          {volume != null && <p className={cn('text-xs', partial ? 'text-warn' : 'text-muted-foreground')}>{partial ? 'lignes, partiel' : 'lignes'}</p>}
        </div>
      </header>
      <ul className="flex flex-col gap-2 p-3">
        {tasks.length === 0 && <li className="px-1 py-2 text-sm text-muted-foreground">Aucune tâche dans cette étape.</li>}
        {tasks.map((t) => (
          <li key={t.task_id}>
            <TaskChip
              task={t}
              ti={instances[t.task_id]}
              rows={volumes?.[t.task_id]}
              selected={selectedTask === t.task_id}
              onClick={() => onSelectTask(t.task_id)}
            />
          </li>
        ))}
      </ul>
    </section>
  )
}

function TaskChip({
  task, ti, rows, selected, onClick,
}: { task: Task; ti?: TaskInstance; rows?: number; selected: boolean; onClick: () => void }) {
  const info = stateInfo(ti?.state)
  return (
    <motion.button
      type="button"
      onClick={onClick}
      whileTap={{ scale: 0.98 }}
      aria-pressed={selected}
      aria-label={`${humanize(task.task_id)}, ${info.label}${rows != null ? `, ${fmtInt(rows)} lignes` : ', volume non mesuré'}. Ouvrir le détail`}
      className={cn(
        'group flex w-full items-center gap-3 rounded-md border bg-background/60 px-3 py-2.5 text-left transition-colors',
        'hover:border-primary/50 hover:bg-accent/60',
        selected && 'border-primary bg-accent ring-1 ring-primary',
      )}
    >
      <StateIcon state={ti?.state} />
      <span className="min-w-0 flex-1">
        <span className="block truncate text-sm font-medium">{humanize(task.task_id)}</span>
        <span className="block truncate font-mono text-[0.7rem] text-muted-foreground">{task.task_id}</span>
      </span>
      <span className="shrink-0 text-right">
        <span className={cn('tabular block text-sm', rows == null ? 'text-muted-foreground' : 'font-medium')}>
          {rows == null ? 'non mesuré' : fmtInt(rows)}
        </span>
        <span className={cn('tabular block text-[0.7rem]', toneText(info.tone))}>
          {ti?.state === 'success' || ti?.state === 'failed' ? fmtDuration(ti.duration) : info.short}
        </span>
      </span>
    </motion.button>
  )
}

/**
 * Ruban de débit : son épaisseur suit le nombre de lignes qui passe d'une
 * étape à l'autre. Il se dessine à chaque changement d'exécution et s'anime
 * pendant qu'une étape tourne (sauf si l'utilisateur réduit les animations).
 */
function FlowRibbon({
  from, to, partial, max, active, runKey, index, onOpen, jointure,
}: { from: number | null; to: number | null; partial: boolean; max: number; active: boolean; runKey: string; index: number; onOpen?: () => void; jointure?: boolean }) {
  const reduce = useReducedMotion()
  const thick = (v: number | null) => (v == null ? 3 : 4 + (v / max) * 22)
  const a = thick(from)
  const b = thick(to)
  const unknown = from == null || to == null
  const delta = !unknown ? (to as number) - (from as number) : null
  const label = unknown ? (partial ? 'débit incomplet' : 'débit non mesuré') : delta === 0 ? 'sans perte' : `${delta! > 0 ? '+' : '−'}${fmtInt(Math.abs(delta!))}`

  return (
    <div className="relative flex h-16 items-center justify-center lg:h-auto">
      {/* horizontal sur grand écran */}
      <svg aria-hidden viewBox="0 0 76 100" preserveAspectRatio="none" className="absolute inset-0 hidden size-full lg:block">
        <motion.path
          key={`h-${runKey}`}
          d={`M0 ${50 - a / 2} C38 ${50 - a / 2} 38 ${50 - b / 2} 76 ${50 - b / 2} L76 ${50 + b / 2} C38 ${50 + b / 2} 38 ${50 + a / 2} 0 ${50 + a / 2} Z`}
          fill={unknown ? 'var(--muted)' : 'var(--flow-soft)'}
          stroke={unknown ? 'var(--border)' : 'var(--flow)'}
          strokeWidth={0.6}
          vectorEffect="non-scaling-stroke"
          initial={reduce ? false : { clipPath: 'inset(0 100% 0 0)' }}
          animate={{ clipPath: 'inset(0 0% 0 0)' }}
          transition={{ duration: 0.55, delay: 0.12 * index, ease: [0.22, 1, 0.36, 1] }}
        />
        {active && !reduce && (
          <motion.line
            x1="0" y1="50" x2="76" y2="50"
            stroke="var(--flow)" strokeWidth={1.5} strokeDasharray="4 6" vectorEffect="non-scaling-stroke"
            animate={{ strokeDashoffset: [0, -20] }}
            transition={{ duration: 0.8, repeat: Infinity, ease: 'linear' }}
          />
        )}
      </svg>
      {/* vertical sur mobile et tablette */}
      <svg aria-hidden viewBox="0 0 100 64" preserveAspectRatio="none" className="absolute inset-0 size-full lg:hidden">
        <motion.path
          key={`v-${runKey}`}
          d={`M${50 - a / 2} 0 C${50 - a / 2} 32 ${50 - b / 2} 32 ${50 - b / 2} 64 L${50 + b / 2} 64 C${50 + b / 2} 32 ${50 + a / 2} 32 ${50 + a / 2} 0 Z`}
          fill={unknown ? 'var(--muted)' : 'var(--flow-soft)'}
          stroke={unknown ? 'var(--border)' : 'var(--flow)'}
          strokeWidth={0.6}
          vectorEffect="non-scaling-stroke"
          initial={reduce ? false : { clipPath: 'inset(0 0 100% 0)' }}
          animate={{ clipPath: 'inset(0 0 0% 0)' }}
          transition={{ duration: 0.5, delay: 0.1 * index }}
        />
      </svg>
      {onOpen && !unknown ? (
        <button
          type="button"
          onClick={onOpen}
          aria-label={`Écart de ${label} lignes${jointure ? ', lignes jointes, pas une perte' : ''} : expliquer`}
          className={cn(
            'tabular relative rounded-full border bg-card px-2 py-0.5 text-[0.7rem] whitespace-nowrap shadow-xs underline decoration-dotted underline-offset-2',
            'transition-colors hover:border-primary hover:bg-accent',
            delta! < 0 && !jointure ? 'text-warn' : 'text-flow',
          )}
          title={jointure ? 'Lignes de référence jointes au modèle : pas une perte. Cliquer pour le détail' : 'Expliquer cet écart'}
        >
          {jointure ? `${label} jointes` : label}
        </button>
      ) : (
        <span
          className={cn(
            'tabular relative rounded-full border bg-card px-2 py-0.5 text-[0.7rem] whitespace-nowrap shadow-xs',
            unknown ? 'text-muted-foreground' : delta! < 0 ? 'text-warn' : 'text-flow',
          )}
          title={unknown ? (partial ? 'Une des deux étapes ne s’est pas terminée avec succès' : 'Une des deux étapes ne publie pas son nombre de lignes') : 'Écart de lignes entre les deux étapes'}
        >
          {label}
        </span>
      )}
    </div>
  )
}
