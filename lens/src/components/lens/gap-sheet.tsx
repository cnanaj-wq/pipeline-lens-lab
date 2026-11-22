import { CheckCircle2Icon, CircleAlertIcon, FileWarningIcon, ScrollTextIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import type { DagRun, GapNature, RunDetails, Task } from '@/lib/api'
import { fmtDateTime, fmtInt, humanize } from '@/lib/format'
import { STAGES, stageVolume, type StageKey } from '@/lib/stages'
import { useIsMobile } from '@/hooks/use-mobile'
import { cn } from '@/lib/utils'

export const NATURE: Record<GapNature, { label: string; perte: boolean }> = {
  rejetee: { label: 'Rejetée', perte: true },
  dedupliquee: { label: 'Dédupliquée', perte: true },
  filtree: { label: 'Filtrée volontairement', perte: true },
  non_chargee: { label: 'Non chargée', perte: true },
  jointure: { label: 'Jointe, pas une perte', perte: false },
}

/** Lignes déclarées « jointes » (devenues colonnes) sur toute l'exécution. */
export function jointuresDuRun(d: RunDetails | null | undefined): number {
  if (!d) return 0
  return Object.values(d.breakdown).flatMap((b) => b.ecarts).filter((e) => e.nature === 'jointure').reduce((s, e) => s + e.lignes, 0)
}

/** Pour chaque écart entre étapes : true si les tâches aval déclarent qu'il n'est fait que de jointures. */
export function gapsSansPerte(stages: Record<StageKey, Task[]>, d: RunDetails | null | undefined): Partial<Record<1 | 2, boolean>> {
  const out: Partial<Record<1 | 2, boolean>> = {}
  if (!d) return out
  for (const g of [1, 2] as const) {
    const ecarts = stages[STAGES[g].key].flatMap((t) => d.breakdown[t.task_id]?.ecarts ?? [])
    if (ecarts.length && ecarts.every((e) => e.nature === 'jointure' || e.lignes === 0)) out[g] = true
  }
  return out
}

interface Props {
  open: boolean
  onOpenChange: (o: boolean) => void
  /** 1 = Extract → Transform, 2 = Transform → Load */
  gap: 1 | 2
  stages: Record<StageKey, Task[]>
  volumes: Record<string, number> | undefined
  details: { data?: RunDetails | null; status: 'loading' | 'success' | 'error' }
  run: DagRun
  onOpenTask: (taskId: string) => void
}

/** Explication d'un écart de lignes entre deux étapes, à partir des seuls faits publiés par les tâches. */
export function GapSheet({ open, onOpenChange, gap, stages, volumes, details, run, onOpenTask }: Props) {
  const mobile = useIsMobile()
  const amont = STAGES[gap - 1]
  const aval = STAGES[gap]
  const vols = STAGES.map((s) => stageVolume(stages[s.key], volumes))
  const observe = vols[gap - 1] != null && vols[gap] != null ? (vols[gap - 1] as number) - (vols[gap] as number) : null
  const tachesAval = stages[aval.key]
  const instrumentees = tachesAval.filter((t) => details.data?.breakdown[t.task_id])
  const nonInstrumentees = tachesAval.filter((t) => !details.data?.breakdown[t.task_id])
  const lignes = instrumentees.flatMap((t) => details.data!.breakdown[t.task_id].ecarts.map((e) => ({ ...e, task: t.task_id })))
  const documente = lignes.reduce((s, e) => s + e.lignes, 0)
  const pertes = lignes.filter((e) => NATURE[e.nature]?.perte).reduce((s, e) => s + e.lignes, 0)
  const reste = observe != null && instrumentees.length ? observe - documente : null
  const fichiers = instrumentees.map((t) => details.data!.breakdown[t.task_id].fichier_rejets).filter(Boolean) as string[]
  const controles = tachesAval.flatMap((t) => (details.data?.checks[t.task_id] ?? []).map((c) => ({ ...c, task: t.task_id })))
  const total = vols[0] != null && vols[2] != null ? (vols[0] as number) - (vols[2] as number) : null
  const joints = jointuresDuRun(details.data)

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side={mobile ? 'bottom' : 'right'} className={mobile ? 'max-h-[92dvh] overflow-y-auto rounded-t-xl' : 'w-full overflow-y-auto sm:max-w-lg'}>
        <SheetHeader className="border-b pb-4">
          <SheetTitle className="pr-8 text-lg">Écart {amont.label} → {aval.label}</SheetTitle>
          <SheetDescription>
            Exécution du {fmtDateTime(run.start_date ?? run.run_after)}
            <span className="mt-0.5 block font-mono text-[0.7rem]">{run.dag_run_id}</span>
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-6 px-4 pb-6">
          <section aria-label="Volumes par étape">
            <table className="w-full text-sm">
              <tbody className="tabular">
                {STAGES.map((s, i) => (
                  <tr key={s.key} className={cn('border-b', (i === gap - 1 || i === gap) && 'font-medium')}>
                    <th scope="row" className="py-2 text-left font-normal">{['Source (Extract)', 'Après transformation', 'Chargement final'][i]}</th>
                    <td className="py-2 text-right">{vols[i] != null ? fmtInt(vols[i] as number) : 'non mesuré'}</td>
                  </tr>
                ))}
                <tr>
                  <th scope="row" className="py-2 text-left font-normal">Écart total</th>
                  <td className="py-2 text-right font-semibold">{total != null ? `${fmtInt(total)} lignes` : '—'}</td>
                </tr>
                {total != null && joints > 0 && (
                  <tr>
                    <td colSpan={2} className="pb-1 text-right text-xs text-muted-foreground">
                      dont {fmtInt(joints)} lignes de référence jointes au modèle (pas une perte)
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>

          <section aria-label="Répartition documentée">
            <h3 className="mb-1 text-sm font-semibold">Répartition déclarée par les tâches</h3>
            <p className="mb-3 text-xs text-muted-foreground">
              Faits publiés par les tâches de l’étape {aval.label} (XCom « row_breakdown »). Pipeline Lens ne déduit aucune cause.
            </p>
            {details.status === 'loading' && !details.data ? (
              <div className="space-y-2"><Skeleton className="h-4 w-full" /><Skeleton className="h-4 w-3/4" /></div>
            ) : details.status === 'error' && !details.data ? (
              <p className="text-sm text-ko">Détail indisponible : Airflow n’a pas répondu.</p>
            ) : !instrumentees.length ? (
              <div className="rounded-md border border-dashed p-3 text-sm">
                <p className="font-medium">Non instrumenté</p>
                <p className="mt-1 text-muted-foreground">
                  Aucune tâche de l’étape {aval.label} ne publie la répartition de ses écarts pour cette exécution.
                  L’écart observé ne permet pas, seul, d’en identifier la cause.
                </p>
              </div>
            ) : (
              <ul className="divide-y rounded-md border">
                {lignes.map((e) => (
                  <li key={`${e.task}-${e.motif}`} className="flex items-start justify-between gap-3 px-3 py-2.5 text-sm">
                    <span className="min-w-0">
                      <span className="block font-medium">{e.libelle}</span>
                      <span className="mt-0.5 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
                        <span className={cn('rounded-full border px-1.5 py-px', NATURE[e.nature]?.perte ? 'border-warn/40 text-warn' : 'border-flow/40 text-flow')}>
                          {NATURE[e.nature]?.label ?? e.nature}
                        </span>
                        <span className="font-mono">{humanize(e.task)}</span>
                      </span>
                    </span>
                    <span className="tabular shrink-0 font-semibold">{fmtInt(e.lignes)}</span>
                  </li>
                ))}
              </ul>
            )}
          </section>

          {instrumentees.length > 0 && observe != null && (
            <section aria-label="Rapprochement" className={cn('flex gap-2.5 rounded-md p-3 text-sm', reste === 0 ? 'bg-ok/8' : 'bg-warn/8')}>
              {reste === 0 ? <CheckCircle2Icon aria-hidden className="mt-0.5 size-4 shrink-0 text-ok" /> : <CircleAlertIcon aria-hidden className="mt-0.5 size-4 shrink-0 text-warn" />}
              <div>
                <p className="font-medium">
                  {reste === 0
                    ? `Écart de ${fmtInt(observe)} lignes entièrement expliqué`
                    : `${fmtInt(Math.abs(reste!))} ligne${Math.abs(reste!) > 1 ? 's' : ''} ${reste! > 0 ? 'non expliquée' : 'expliquée en trop'}${Math.abs(reste!) > 1 ? 's' : ''} sur ${fmtInt(observe)}`}
                </p>
                <p className="mt-0.5 text-muted-foreground">
                  Pertes réelles : {fmtInt(pertes)} ligne{pertes > 1 ? 's' : ''}.
                  {documente - pertes > 0 && ` ${fmtInt(documente - pertes)} ligne${documente - pertes > 1 ? 's' : ''} de référence jointe${documente - pertes > 1 ? 's' : ''} au modèle, donc présentes sous forme de colonnes.`}
                </p>
                {reste !== 0 && (
                  <p className="mt-1.5 text-xs text-muted-foreground">
                    Hypothèse à vérifier, non établie : une règle de la tâche n’est pas déclarée dans sa répartition
                    {nonInstrumentees.length ? `, ou l’écart vient d’une tâche non instrumentée (${nonInstrumentees.map((t) => humanize(t.task_id)).join(', ')})` : ''}.
                  </p>
                )}
              </div>
            </section>
          )}

          {controles.length > 0 && (
            <section aria-label="Contrôles qualité">
              <h3 className="mb-2 text-sm font-semibold">Contrôles qualité exécutés</h3>
              <ul className="space-y-1.5 text-sm">
                {controles.map((c) => (
                  <li key={`${c.task}-${c.controle}`} className="flex items-center justify-between gap-3">
                    <span className="flex items-center gap-2">
                      {c.statut === 'ok' ? <CheckCircle2Icon aria-label="Conforme" className="size-4 text-ok" /> : <CircleAlertIcon aria-label="Non conforme" className="size-4 text-ko" />}
                      {c.controle}
                    </span>
                    <span className="tabular text-muted-foreground">{fmtInt(c.valeur)} (seuil {fmtInt(c.seuil)})</span>
                  </li>
                ))}
              </ul>
            </section>
          )}

          {fichiers.length > 0 && (
            <section aria-label="Lignes rejetées" className="flex gap-2.5 rounded-md border p-3 text-sm">
              <FileWarningIcon aria-hidden className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
              <div>
                <p className="font-medium">Lignes rejetées conservées côté serveur</p>
                <p className="mt-0.5 text-muted-foreground">
                  Fichier <span className="font-mono">{fichiers.join(', ')}</span>, avec le motif de chaque rejet.
                  Sa consultation n’est pas ouverte dans Pipeline Lens : elle exige un contrôle des droits sur les données ligne à ligne.
                </p>
              </div>
            </section>
          )}

          <section aria-label="Journaux">
            <h3 className="mb-2 text-sm font-semibold">Journaux des tâches concernées</h3>
            <div className="flex flex-wrap gap-2">
              {tachesAval.map((t) => (
                <Button key={t.task_id} size="sm" variant="outline" onClick={() => onOpenTask(t.task_id)}>
                  <ScrollTextIcon aria-hidden />{humanize(t.task_id)}
                </Button>
              ))}
            </div>
          </section>
        </div>
      </SheetContent>
    </Sheet>
  )
}
