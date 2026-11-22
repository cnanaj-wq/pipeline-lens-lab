import { CheckCircle2Icon, CircleAlertIcon } from 'lucide-react'
import type { DagRun, RunDetails, Task, Volumes } from '@/lib/api'
import { fmtAgo, fmtDateTime, fmtInt, humanize } from '@/lib/format'
import { stageVolume, type StageKey } from '@/lib/stages'
import { cn } from '@/lib/utils'
import { NATURE } from './gap-sheet'

const NI = 'Non instrumenté'

/**
 * Qualité des données de l'exécution sélectionnée. Chaque valeur est soit lue dans Airflow
 * (volumes, dates), soit publiée par une tâche (écarts, contrôles) ; sinon « Non instrumenté ».
 */
export function QualityPanel({
  run, runs, stages, volumes, details, now,
}: {
  run: DagRun
  runs: DagRun[]
  stages: Record<StageKey, Task[]>
  volumes: Volumes
  details: { data?: RunDetails | null; status: 'loading' | 'success' | 'error' }
  now: number
}) {
  const v = volumes[run.dag_run_id]
  const extraites = stageVolume(stages.extract, v)
  const chargees = run.state === 'success' ? stageVolume(stages.load, v) : null
  const bd = details.data ? Object.values(details.data.breakdown) : []
  const ecartees = bd.length ? bd.flatMap((b) => b.ecarts).filter((e) => NATURE[e.nature]?.perte).reduce((s, e) => s + e.lignes, 0) : null
  const checks = details.data ? Object.entries(details.data.checks).flatMap(([t, cs]) => cs.map((c) => ({ ...c, task: t }))) : []
  const ko = checks.filter((c) => c.statut === 'ko').length

  // Exécution réussie précédente, pour comparer le volume chargé.
  const idx = runs.findIndex((r) => r.dag_run_id === run.dag_run_id)
  const prec = runs.slice(idx + 1).find((r) => r.state === 'success' && stageVolume(stages.load, volumes[r.dag_run_id]) != null)
  const chargeesPrec = prec ? stageVolume(stages.load, volumes[prec.dag_run_id]) : null
  const variation = chargees != null && chargeesPrec ? ((chargees - chargeesPrec) / chargeesPrec) * 100 : null
  const dernierValide = runs.find((r) => r.state === 'success')

  return (
    <section aria-label="Qualité des données" className="rounded-lg border bg-card">
      <header className="flex flex-wrap items-baseline justify-between gap-2 border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Qualité des données de cette exécution</h2>
        <p className="text-xs text-muted-foreground">
          {details.status === 'error' && !details.data ? 'Contrôles indisponibles : Airflow n’a pas répondu.' : 'Valeurs lues dans Airflow ou publiées par les tâches'}
        </p>
      </header>
      <dl className="grid grid-cols-2 gap-px bg-border sm:grid-cols-4">
        <Tile label="Lignes extraites" value={extraites != null ? fmtInt(extraites) : NI} muted={extraites == null} />
        <Tile
          label="Lignes écartées"
          value={ecartees != null ? fmtInt(ecartees) : NI}
          sub={ecartees != null ? 'rejets, doublons, filtres, non chargées' : 'répartition des écarts non publiée'}
          muted={ecartees == null}
        />
        <Tile
          label="Lignes chargées"
          value={chargees != null ? fmtInt(chargees) : run.state === 'success' ? NI : 'aucune'}
          sub={variation != null ? `${variation >= 0 ? '+' : '−'}${Math.abs(variation).toFixed(1).replace('.', ',')} % vs ${fmtDateTime(prec!.start_date ?? prec!.run_after)}` : run.state !== 'success' ? 'exécution non réussie' : 'pas d’exécution précédente comparable'}
          tone={variation != null && Math.abs(variation) >= 10 ? 'warn' : undefined}
          muted={chargees == null}
        />
        <Tile
          label="Dernier chargement valide"
          value={dernierValide ? fmtAgo(dernierValide.end_date, now) : 'aucun'}
          sub={dernierValide ? fmtDateTime(dernierValide.end_date) : 'aucune exécution réussie'}
        />
      </dl>
      <div className="px-4 py-3">
        {checks.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            <span className="font-medium text-foreground">Contrôles qualité : {NI.toLowerCase()}.</span>{' '}
            {run.state === 'success'
              ? 'Aucune tâche ne publie de contrôle (XCom « quality_checks ») pour cette exécution.'
              : 'L’exécution n’est pas allée jusqu’aux contrôles.'}
          </p>
        ) : (
          <>
            <p className={cn('mb-2 text-sm font-medium', ko ? 'text-ko' : 'text-ok')}>
              {ko ? `${ko} contrôle${ko > 1 ? 's' : ''} non conforme${ko > 1 ? 's' : ''} sur ${checks.length}` : `${checks.length} contrôles exécutés, tous conformes`}
            </p>
            <ul className="grid gap-x-6 gap-y-1.5 text-sm sm:grid-cols-2">
              {checks.map((c) => (
                <li key={`${c.task}-${c.controle}`} className="flex items-center justify-between gap-3">
                  <span className="flex min-w-0 items-center gap-2">
                    {c.statut === 'ok' ? <CheckCircle2Icon aria-label="Conforme" className="size-4 shrink-0 text-ok" /> : <CircleAlertIcon aria-label="Non conforme" className="size-4 shrink-0 text-ko" />}
                    <span className="truncate" title={humanize(c.task)}>{c.controle}</span>
                  </span>
                  <span className="tabular shrink-0 text-muted-foreground">{fmtInt(c.valeur)}</span>
                </li>
              ))}
            </ul>
          </>
        )}
      </div>
    </section>
  )
}

function Tile({ label, value, sub, muted, tone }: { label: string; value: string; sub?: string; muted?: boolean; tone?: 'warn' }) {
  return (
    <div className="bg-card px-4 py-3">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className={cn('tabular mt-0.5 font-semibold', muted ? 'text-sm font-normal text-muted-foreground' : 'text-lg')}>{value}</dd>
      {sub && <dd className={cn('mt-0.5 text-xs', tone === 'warn' ? 'text-warn' : 'text-muted-foreground')}>{sub}</dd>}
    </div>
  )
}
