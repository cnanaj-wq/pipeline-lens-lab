import { useEffect, useState } from 'react'
import { lens, type GlobalRun, type HistoryQuery, type LensConfig, type Task } from '@/lib/api'
import { classify, stageVolume } from '@/lib/stages'
import { usePolled } from './use-lens'

/** Configuration publique du serveur (adresse Airflow, rejeu autorisé ou non). */
export function useLensConfig() {
  const [config, setConfig] = useState<LensConfig | null>(null)
  useEffect(() => {
    lens.config().then(setConfig).catch(() => setConfig(null))
  }, [])
  return config
}

/** Une page de l'historique global, filtrée côté Airflow. */
export function useHistory(q: HistoryQuery, intervalMs: number, version = 0) {
  return usePolled(
    (s) => lens.history(q, s),
    [q.dagId, q.state, q.runType, q.since, q.limit, q.offset, version],
    intervalMs,
  )
}

/** Toutes les exécutions des 7 derniers jours : indicateurs et graphique d'activité. */
export function useWeekRuns(intervalMs: number, version = 0) {
  return usePolled<GlobalRun[]>(
    async (s) => {
      const since = new Date(Date.now() - 7 * 24 * 3600_000).toISOString()
      return (await lens.history({ since, limit: 1000, offset: 0 }, s)).runs
    },
    [version],
    intervalMs,
  )
}

/** Tâches en échec par exécution (clé « dag|run »). */
export function useFailedTasks(intervalMs: number, version = 0) {
  return usePolled<Record<string, string[]>>(
    async (s) => {
      const out: Record<string, string[]> = {}
      for (const t of await lens.failedTasks(s)) (out[`${t.dag_id}|${t.dag_run_id}`] ??= []).push(t.task_id)
      return out
    },
    [version],
    intervalMs,
  )
}

/**
 * Lignes chargées par exécution (somme des volumes publiés par les tâches de l'étape Load)
 * et tâches de chaque DAG. Une exécution sans volume publié est absente : « non mesuré ».
 */
export function useLoadedRows(dagIds: string[], intervalMs: number) {
  const key = dagIds.join(',')
  return usePolled<{ loaded: Record<string, number>; tasks: Record<string, Task[]> }>(
    async (s) => {
      const loaded: Record<string, number> = {}
      const tasks: Record<string, Task[]> = {}
      await Promise.all(dagIds.map(async (id) => {
        const [t, v] = await Promise.all([lens.allTasks(id, s), lens.volumes(id, s)])
        tasks[id] = t
        const load = classify(t).load
        for (const [run, vols] of Object.entries(v)) {
          const n = stageVolume(load, vols)
          if (n != null) loaded[`${id}|${run}`] = n
        }
      }))
      return { loaded, tasks }
    },
    [key],
    intervalMs,
  )
}

export function useAudit(intervalMs: number, version = 0) {
  return usePolled((s) => lens.audit(s), [version], intervalMs)
}

/** Événements d'incident écrits par les DAG (échecs, nouveaux essais, reprises). */
export function useIncidents(intervalMs: number) {
  return usePolled((s) => lens.incidents(s), [], intervalMs)
}
