/**
 * Incidents reconstitués à partir des événements écrits par les DAG au moment même où ils se
 * produisent (on_retry_callback, on_failure_callback, on_success_callback) :
 * un incident s'ouvre au premier échec d'une tâche dans une exécution, et se clôt à sa reprise.
 */
export type IncidentEventKind = 'echec_essai' | 'echec' | 'reprise'

export interface IncidentEvent {
  ts: string
  evenement: IncidentEventKind
  dag_id: string
  task_id: string
  run_id: string
  essai: number | null
  essais_max: number
  raison?: string
  duree_s?: number
  debut?: string
}

export type IncidentStatus = 'retabli' | 'en_cours' | 'echec'

export interface Incident {
  key: string
  dag_id: string
  task_id: string
  run_id: string
  debut: string
  fin: string | null
  /** Durée jusqu'à la reprise (s), ou depuis le début si l'incident n'est pas clos. */
  dureeS: number
  statut: IncidentStatus
  /** Échecs successifs dans l'incident */
  echecs: number
  raison: string
  /** Reprise par un nouvel essai automatique, ou après un échec définitif (rejeu) */
  reprise: 'automatique' | 'rejeu' | null
}

/** Incidents, du plus récent au plus ancien. Les événements peuvent arriver dans le désordre. */
export function buildIncidents(events: IncidentEvent[], now: number): Incident[] {
  const sorted = [...events].filter((e) => e && e.ts && e.dag_id && e.task_id && e.run_id).sort((a, b) => a.ts.localeCompare(b.ts))
  const open = new Map<string, Incident & { definitif: boolean }>()
  const done: Incident[] = []
  for (const e of sorted) {
    const k = `${e.dag_id}|${e.task_id}|${e.run_id}`
    const cur = open.get(k)
    if (e.evenement === 'echec_essai' || e.evenement === 'echec') {
      if (!cur) {
        open.set(k, {
          key: `${k}|${e.ts}`, dag_id: e.dag_id, task_id: e.task_id, run_id: e.run_id,
          debut: e.ts, fin: null, dureeS: 0, statut: 'en_cours', echecs: 1,
          raison: e.raison ?? 'non transmise', reprise: null, definitif: e.evenement === 'echec',
        })
      } else {
        cur.echecs++
        cur.raison = e.raison ?? cur.raison
        cur.definitif = e.evenement === 'echec'
      }
    } else if (e.evenement === 'reprise' && cur) {
      const { definitif, ...inc } = cur
      done.push({
        ...inc, fin: e.ts, statut: 'retabli',
        dureeS: e.duree_s ?? (Date.parse(e.ts) - Date.parse(cur.debut)) / 1000,
        reprise: definitif ? 'rejeu' : 'automatique',
      })
      open.delete(k)
    }
  }
  for (const { definitif, ...inc } of open.values())
    done.push({ ...inc, statut: definitif ? 'echec' : 'en_cours', dureeS: Math.max(0, (now - Date.parse(inc.debut)) / 1000) })
  return done.sort((a, b) => b.debut.localeCompare(a.debut))
}

export interface IncidentStats {
  total: number
  ouverts: number
  retablis: number
  /** Durée médiane de rétablissement (s), null sans incident rétabli */
  mttrS: number | null
  automatiques: number
}

export function incidentStats(list: Incident[]): IncidentStats {
  const r = list.filter((i) => i.statut === 'retabli').map((i) => i.dureeS).sort((a, b) => a - b)
  const m = r.length ? (r.length % 2 ? r[(r.length - 1) / 2] : (r[r.length / 2 - 1] + r[r.length / 2]) / 2) : null
  return {
    total: list.length,
    ouverts: list.filter((i) => i.statut !== 'retabli').length,
    retablis: r.length,
    mttrS: m,
    automatiques: list.filter((i) => i.reprise === 'automatique').length,
  }
}
