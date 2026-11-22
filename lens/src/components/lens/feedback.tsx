import { PlugZapIcon, RefreshCwIcon, WorkflowIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { LensError } from '@/lib/api'

export function ErrorPanel({ error, onRetry }: { error: Error; onRetry: () => void }) {
  const code = error instanceof LensError ? error.code : 'unknown'
  const hint: Record<string, string> = {
    unreachable: 'Démarrez Airflow : « docker compose up -d » dans C:\\airflow, puis réessayez.',
    proxy_down: 'Relancez l’application : « npm run dev » dans C:\\airflow\\lens.',
    auth: 'Vérifiez AIRFLOW_LENS_USER et AIRFLOW_LENS_PASSWORD dans C:\\airflow\\.env, puis relancez l’application.',
    config: 'Complétez C:\\airflow\\.env, puis relancez l’application.',
  }
  return (
    <div role="alert" className="mx-auto flex max-w-md flex-col items-start gap-3 rounded-lg border border-ko/30 bg-card p-5">
      <div className="flex items-center gap-2 text-ko">
        <PlugZapIcon aria-hidden className="size-5" />
        <p className="font-medium">{error.message}</p>
      </div>
      {hint[code] && <p className="text-sm text-muted-foreground">{hint[code]}</p>}
      <Button variant="outline" size="sm" onClick={onRetry}>
        <RefreshCwIcon aria-hidden /> Réessayer
      </Button>
    </div>
  )
}

export function EmptyDags() {
  return (
    <div className="mx-auto flex max-w-md flex-col items-start gap-2 rounded-lg border bg-card p-5">
      <WorkflowIcon aria-hidden className="size-5 text-primary" />
      <p className="font-medium">Aucun DAG dans Airflow</p>
      <p className="text-sm text-muted-foreground">
        Déposez un fichier Python dans C:\airflow\dags. Il apparaît ici après environ 30 secondes.
      </p>
    </div>
  )
}

export function PipelineSkeleton() {
  return (
    <div aria-busy="true" aria-label="Chargement du pipeline" className="grid gap-4 lg:grid-cols-3">
      {[0, 1, 2].map((i) => (
        <div key={i} className="space-y-3 rounded-lg border bg-card p-4">
          <Skeleton className="h-4 w-24" />
          <Skeleton className="h-8 w-32" />
          <Skeleton className="h-14 w-full" />
          <Skeleton className="h-14 w-full" />
        </div>
      ))}
    </div>
  )
}
