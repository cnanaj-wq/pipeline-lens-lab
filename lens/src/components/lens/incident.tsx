import { AlertTriangleIcon, CircleXIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import type { DagRun, TaskInstance } from '@/lib/api'
import { fmtAgo, fmtDateTime, humanize } from '@/lib/format'
import { cn } from '@/lib/utils'

/**
 * Dernier incident du DAG, visible quelle que soit l'exécution sélectionnée.
 * Uniquement des faits lus dans Airflow : exécution, tâche en échec, essai.
 */
export function IncidentBanner({
  runs, instances, selectedRunId, now, onSelectRun, onOpenTask,
}: {
  runs: DagRun[]
  instances: Record<string, TaskInstance[]>
  selectedRunId: string | null
  now: number
  onSelectRun: (runId: string) => void
  onOpenTask: (runId: string, taskId: string) => void
}) {
  const failed = runs.find((r) => r.state === 'failed')
  if (!failed || failed.dag_run_id === selectedRunId) return null
  const enCours = runs[0]?.dag_run_id === failed.dag_run_id
  const depuis = runs.findIndex((r) => r.dag_run_id === failed.dag_run_id)
  const taches = (instances[failed.dag_run_id] ?? []).filter((t) => t.state === 'failed')
  const bloquees = (instances[failed.dag_run_id] ?? []).filter((t) => t.state === 'upstream_failed').length
  const quand = failed.end_date ?? failed.start_date ?? failed.run_after

  return (
    <section
      aria-label="Dernier incident"
      className={cn('flex flex-col gap-3 rounded-lg border p-3.5 sm:flex-row sm:items-center', enCours ? 'border-ko/50 bg-ko/8' : 'border-warn/40 bg-warn/6')}
    >
      {enCours ? <CircleXIcon aria-hidden className="size-5 shrink-0 text-ko" /> : <AlertTriangleIcon aria-hidden className="size-5 shrink-0 text-warn" />}
      <div className="min-w-0 flex-1 text-sm">
        <p className="font-medium">
          {enCours ? 'La dernière exécution a échoué' : `Dernier incident, ${fmtAgo(quand, now)}`}
          <span className="font-normal text-muted-foreground"> · {fmtDateTime(failed.start_date ?? failed.run_after)}</span>
        </p>
        <p className="text-muted-foreground">
          {taches.length
            ? taches.map((t) => `${humanize(t.task_id)} en échec (essai ${t.try_number} sur ${t.max_tries + 1})`).join(', ')
            : 'Aucune tâche en échec enregistrée : l’exécution a été marquée en échec sans tâche fautive.'}
          {bloquees ? `, ${bloquees} tâche${bloquees > 1 ? 's' : ''} bloquée${bloquees > 1 ? 's' : ''} en aval` : ''}
          {!enCours && depuis > 0 ? `. ${depuis} exécution${depuis > 1 ? 's' : ''} depuis.` : '.'}
        </p>
      </div>
      <div className="flex shrink-0 flex-wrap gap-2">
        {taches[0] && (
          <Button size="sm" variant="outline" onClick={() => onOpenTask(failed.dag_run_id, taches[0].task_id)}>
            Voir le journal
          </Button>
        )}
        <Button size="sm" variant={enCours ? 'default' : 'outline'} onClick={() => onSelectRun(failed.dag_run_id)}>
          Afficher cette exécution
        </Button>
      </div>
    </section>
  )
}
