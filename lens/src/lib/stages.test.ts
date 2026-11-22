import { describe, expect, it } from 'vitest'
import { gapsSansPerte, jointuresDuRun } from '@/components/lens/gap-sheet'
import { anomalyThreshold } from '@/components/lens/run-history'
import type { DagRun, RunDetails, Task } from './api'
import { classify, stageVolume } from './stages'

const t = (task_id: string, down: string[] = []): Task => ({ task_id, task_display_name: task_id, downstream_task_ids: down, operator_name: null })

describe('classement Extract / Transform / Load', () => {
  it('par groupe de tâches', () => {
    const s = classify([t('extract.a', ['transform.b']), t('transform.b', ['load.c']), t('load.c')])
    expect([s.extract, s.transform, s.load].map((x) => x.map((y) => y.task_id))).toEqual([['extract.a'], ['transform.b'], ['load.c']])
  })
  it('par mots-clés puis par position dans le graphe', () => {
    const s = classify([t('fetch_api', ['clean']), t('clean', ['etape_x']), t('etape_x')])
    expect(s.extract[0].task_id).toBe('fetch_api')
    expect(s.transform[0].task_id).toBe('clean')
    expect(s.load[0].task_id).toBe('etape_x')
  })
  it('volume d’étape : somme des tâches mesurées, null si rien n’est publié', () => {
    expect(stageVolume([t('a'), t('b')], { a: 10, b: 5 })).toBe(15)
    expect(stageVolume([t('a')], {})).toBeNull()
    expect(stageVolume([t('a')], undefined)).toBeNull()
  })
})

describe('écarts déclarés', () => {
  const details: RunDetails = {
    breakdown: {
      'transform.b': { entree: 100, sortie: 90, ecarts: [{ motif: 'doublon', libelle: 'Doublon', nature: 'dedupliquee', lignes: 10 }], fichier_rejets: null },
      'load.c': { entree: 90, sortie: 50, ecarts: [{ motif: 'dim', libelle: 'Dimensions', nature: 'jointure', lignes: 40 }], fichier_rejets: null },
    },
    checks: {},
  }
  const stages = classify([t('extract.a', ['transform.b']), t('transform.b', ['load.c']), t('load.c')])
  it('repère les écarts faits uniquement de jointures (pas une perte)', () => {
    expect(gapsSansPerte(stages, details)).toEqual({ 2: true })
    expect(jointuresDuRun(details)).toBe(40)
    expect(gapsSansPerte(stages, null)).toEqual({})
  })
})

describe('durée anormale', () => {
  const r = (d: number, state: DagRun['state'] = 'success') => ({ duration: d, state }) as DagRun
  it('2 × la médiane des réussites, à partir de 5 exécutions', () => {
    expect(anomalyThreshold([r(10), r(12), r(11), r(9), r(100, 'failed')])).toBeNull()
    expect(anomalyThreshold([r(10), r(12), r(11), r(9), r(13)])).toBe(22)
  })
})
