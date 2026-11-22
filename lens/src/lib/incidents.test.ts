import { describe, expect, it } from 'vitest'
import { buildIncidents, incidentStats, type IncidentEvent } from './incidents'

const ev = (ts: string, evenement: IncidentEvent['evenement'], extra: Partial<IncidentEvent> = {}): IncidentEvent => ({
  ts, evenement, dag_id: 'd', task_id: 't', run_id: 'r1', essai: 1, essais_max: 4, raison: 'ConnectionError : source', ...extra,
})
const NOW = Date.parse('2026-11-11T12:00:00Z')

describe('buildIncidents', () => {
  it('panne passagère : échec d’essai puis reprise automatique, avec sa durée', () => {
    const [i] = buildIncidents([ev('2026-11-11T10:00:00Z', 'echec_essai'), ev('2026-11-11T10:00:45Z', 'reprise', { duree_s: 45 })], NOW)
    expect(i).toMatchObject({ statut: 'retabli', reprise: 'automatique', dureeS: 45, echecs: 1, fin: '2026-11-11T10:00:45Z' })
  })

  it('échec définitif sans reprise : reste ouvert et compte sa durée jusqu’à maintenant', () => {
    const [i] = buildIncidents([ev('2026-11-11T11:00:00Z', 'echec_essai'), ev('2026-11-11T11:01:00Z', 'echec', { raison: 'ValueError : prix' })], NOW)
    expect(i).toMatchObject({ statut: 'echec', echecs: 2, raison: 'ValueError : prix', fin: null, dureeS: 3600 })
  })

  it('échec définitif puis rejeu réussi : rétabli « par rejeu »', () => {
    const [i] = buildIncidents([ev('2026-11-11T09:00:00Z', 'echec'), ev('2026-11-11T09:30:00Z', 'reprise')], NOW)
    expect(i).toMatchObject({ statut: 'retabli', reprise: 'rejeu', dureeS: 1800 })
  })

  it('incident en cours (Airflow réessaie)', () => {
    expect(buildIncidents([ev('2026-11-11T11:59:00Z', 'echec_essai')], NOW)[0]).toMatchObject({ statut: 'en_cours', dureeS: 60 })
  })

  it('sépare les tâches et exécutions, ignore une reprise orpheline et trie du plus récent au plus ancien', () => {
    const list = buildIncidents([
      ev('2026-11-11T08:00:00Z', 'reprise'), // sans échec : ignorée
      ev('2026-11-11T10:00:00Z', 'echec_essai', { task_id: 'a' }),
      ev('2026-11-11T09:00:00Z', 'echec_essai', { run_id: 'r0' }),
      ev('2026-11-11T10:00:30Z', 'reprise', { task_id: 'a' }),
    ], NOW)
    expect(list.map((i) => [i.task_id, i.run_id, i.statut])).toEqual([['a', 'r1', 'retabli'], ['t', 'r0', 'en_cours']])
  })

  it('un nouvel incident peut s’ouvrir sur la même tâche après une reprise', () => {
    const list = buildIncidents([
      ev('2026-11-11T09:00:00Z', 'echec'), ev('2026-11-11T09:10:00Z', 'reprise'), ev('2026-11-11T09:20:00Z', 'echec'),
    ], NOW)
    expect(list.map((i) => i.statut)).toEqual(['echec', 'retabli'])
  })
})

describe('incidentStats', () => {
  it('compte et calcule la durée médiane de rétablissement', () => {
    const list = buildIncidents([
      ev('2026-11-11T09:00:00Z', 'echec_essai', { run_id: 'a' }), ev('2026-11-11T09:01:00Z', 'reprise', { run_id: 'a', duree_s: 60 }),
      ev('2026-11-11T09:00:00Z', 'echec', { run_id: 'b' }), ev('2026-11-11T09:05:00Z', 'reprise', { run_id: 'b', duree_s: 300 }),
      ev('2026-11-11T11:00:00Z', 'echec', { run_id: 'c' }),
    ], NOW)
    expect(incidentStats(list)).toEqual({ total: 3, ouverts: 1, retablis: 2, mttrS: 180, automatiques: 1 })
  })
  it('sans incident', () => expect(incidentStats([]).mttrS).toBeNull())
})
