import { useEffect, useState } from 'react'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { lens, type DagRun, type TaskInstance, type Volumes } from '@/lib/api'
import { ExternalLinkIcon, RotateCcwIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { fmtDateTime, fmtDurationPrecise, fmtInt, fmtTime, humanize } from '@/lib/format'
import { useIsMobile } from '@/hooks/use-mobile'
import { StatePill } from './status'

interface Props {
  open: boolean
  onOpenChange: (o: boolean) => void
  dagId: string
  taskId: string | null
  run: DagRun | null
  ti: TaskInstance | undefined
  runs: DagRun[]
  volumes: Volumes
  /** Adresse de l'interface Airflow (null en démonstration : pas de lien). */
  airflowUrl: string | null
  /** Rejeu de cette tâche : action, ou motif d'indisponibilité. */
  replay?: { onReplay: () => void; reason: string | null }
}

export function TaskSheet({ open, onOpenChange, dagId, taskId, run, ti, runs, volumes, airflowUrl, replay }: Props) {
  const mobile = useIsMobile()
  const rows = taskId && run ? volumes[run.dag_run_id]?.[taskId] : undefined
  const trend = taskId
    ? [...runs].reverse().map((r) => ({ id: r.dag_run_id, v: volumes[r.dag_run_id]?.[taskId] }))
    : []
  const measuredTrend = trend.filter((t) => t.v != null)
  const max = Math.max(1, ...measuredTrend.map((t) => t.v as number))

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side={mobile ? 'bottom' : 'right'}
        className={mobile ? 'max-h-[92dvh] overflow-y-auto rounded-t-xl' : 'w-full overflow-y-auto sm:max-w-md'}
      >
        {taskId && (
          <>
            <SheetHeader className="border-b pb-4">
              <SheetTitle className="pr-8 text-lg">{humanize(taskId)}</SheetTitle>
              <SheetDescription className="font-mono text-xs">{taskId}</SheetDescription>
              <div className="pt-1"><StatePill state={ti?.state} /></div>
            </SheetHeader>

            <div className="space-y-6 px-4 pb-6">
              <section aria-label="Lignes produites">
                <p className="text-xs text-muted-foreground">Lignes produites par cette exécution</p>
                {rows != null ? (
                  <p className="tabular text-4xl font-semibold tracking-tight">{fmtInt(rows)}</p>
                ) : ti?.state !== 'success' ? (
                  <div className="mt-1 rounded-md border border-dashed p-3">
                    <p className="font-medium">Aucun volume pour cette exécution</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {ti?.state === 'failed'
                        ? 'La tâche a échoué avant de publier son nombre de lignes. Consultez la fin du journal ci-dessous.'
                        : 'La tâche ne s’est pas terminée : elle n’a donc rien publié.'}
                    </p>
                  </div>
                ) : (
                  <div className="mt-1 rounded-md border border-dashed p-3">
                    <p className="font-medium">Non mesuré</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      Cette tâche ne publie pas son nombre de lignes. Ajoutez à la fin de la tâche :
                    </p>
                    <pre className="mt-2 overflow-x-auto rounded bg-muted px-2.5 py-2 font-mono text-xs">
                      ti.xcom_push(key="row_count", value=len(df))
                    </pre>
                  </div>
                )}
              </section>

              <dl className="grid grid-cols-2 gap-3 text-sm">
                <Fact label="Durée" value={fmtDurationPrecise(ti?.duration)} />
                <Fact label="Essai" value={ti ? `${ti.try_number} sur ${ti.max_tries + 1}` : '—'} />
                <Fact label="Début" value={fmtTime(ti?.start_date ?? null)} />
                <Fact label="Fin" value={fmtTime(ti?.end_date ?? null)} />
                <Fact label="Exécution" value={run ? fmtDateTime(run.start_date ?? run.run_after) : '—'} wide />
                <div className="col-span-2">
                  <dt className="text-xs text-muted-foreground">Identifiant de l’exécution</dt>
                  <dd className="font-mono text-xs break-all">{run?.dag_run_id ?? '—'}</dd>
                </div>
              </dl>
              {airflowUrl && run && (
                <a
                  href={`${airflowUrl}/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(run.dag_run_id)}/tasks/${encodeURIComponent(taskId)}`}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1.5 text-sm font-medium text-primary underline-offset-4 hover:underline"
                >
                  Ouvrir cette tâche dans Airflow <ExternalLinkIcon aria-hidden className="size-3.5" />
                </a>
              )}
              {replay && run && (
                <div className="space-y-1">
                  <Button size="sm" variant="outline" aria-disabled={!!replay.reason}
                    className={replay.reason ? 'cursor-not-allowed opacity-45' : undefined}
                    onClick={() => { if (!replay.reason) replay.onReplay() }}>
                    <RotateCcwIcon aria-hidden />Rejouer cette tâche
                  </Button>
                  {replay.reason && <p className="text-xs text-muted-foreground">{replay.reason}</p>}
                </div>
              )}

              {measuredTrend.length > 1 && (
                <section aria-label="Évolution des volumes">
                  <p className="mb-2 text-xs text-muted-foreground">Volume sur les dernières exécutions</p>
                  <div className="flex h-16 items-end gap-1">
                    {trend.map((t) => (
                      <div
                        key={t.id}
                        title={t.v != null ? `${fmtInt(t.v)} lignes` : 'non mesuré'}
                        className={t.id === run?.dag_run_id ? 'w-2.5 rounded-sm bg-flow' : 'w-2.5 rounded-sm bg-flow/35'}
                        style={{ height: t.v != null ? `${8 + (t.v / max) * 92}%` : '4%' }}
                      />
                    ))}
                  </div>
                </section>
              )}

              {run && ti && ti.try_number > 0 && <LogTail dagId={dagId} runId={run.dag_run_id} taskId={taskId} tryNumber={ti.try_number} />}
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  )
}

function Fact({ label, value, wide }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={wide ? 'col-span-2' : undefined}>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="tabular font-medium">{value}</dd>
    </div>
  )
}

function LogTail({ dagId, runId, taskId, tryNumber }: { dagId: string; runId: string; taskId: string; tryNumber: number }) {
  const [lines, setLines] = useState<string[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  useEffect(() => {
    const ctrl = new AbortController()
    setLines(null)
    setError(null)
    lens.log(dagId, runId, taskId, tryNumber, ctrl.signal).then(setLines).catch((e: Error) => {
      if (e.name !== 'AbortError') setError(e.message)
    })
    return () => ctrl.abort()
  }, [dagId, runId, taskId, tryNumber])

  return (
    <section aria-label="Fin du journal">
      <p className="mb-2 text-xs text-muted-foreground">Fin du journal</p>
      {error ? (
        <p className="text-sm text-ko">Journal indisponible : {error}</p>
      ) : !lines ? (
        <div className="space-y-1.5"><Skeleton className="h-3 w-full" /><Skeleton className="h-3 w-4/5" /><Skeleton className="h-3 w-3/5" /></div>
      ) : lines.length === 0 ? (
        <p className="text-sm text-muted-foreground">Journal vide.</p>
      ) : (
        <pre className="max-h-64 overflow-auto rounded-md bg-muted p-3 font-mono text-[0.7rem] leading-relaxed whitespace-pre-wrap">
          {lines.map((l, i) => (
            <span key={i} className={/ERROR|ALERTE|Traceback|Exception/i.test(l) ? 'text-ko' : undefined}>{l}{'\n'}</span>
          ))}
        </pre>
      )}
    </section>
  )
}
