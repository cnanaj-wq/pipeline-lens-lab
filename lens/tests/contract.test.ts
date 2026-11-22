// @vitest-environment node
/** Le contrat de Pipeline Lens est compatible avec la spécification d'Airflow 3.4.0 enregistrée. */
import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { CONTRACT, checkContract } from '../proxy/airflow-contract.ts'

const spec = JSON.parse(readFileSync(new URL('./fixtures/openapi-airflow-3.4.0.json', import.meta.url), 'utf-8'))

describe('contrat d’API', () => {
  it('est entièrement couvert par Airflow 3.4.0', () => {
    expect(checkContract(spec)).toEqual([])
  })
  it('signale une route, un paramètre ou un champ disparus', () => {
    const casse = structuredClone(spec)
    delete casse.paths['/api/v2/dags/{dag_id}/clearTaskInstances']
    casse.paths['/api/v2/dags/{dag_id}/dagRuns'].get.parameters = casse.paths['/api/v2/dags/{dag_id}/dagRuns'].get.parameters
      .filter((p: { name: string }) => p.name !== 'run_after_gte')
    delete casse.components.schemas.DAGRunResponse.properties.duration
    expect(checkContract(casse)).toEqual([
      'Paramètre absent : /api/v2/dags/{dag_id}/dagRuns ?run_after_gte',
      'Champ absent : DAGRunResponse.duration',
      'Route absente : POST /api/v2/dags/{dag_id}/clearTaskInstances',
    ])
  })
  it('ne contient que deux opérations d’écriture : les deux formes de rejeu', () => {
    expect(CONTRACT.filter((c) => c.method === 'post').map((c) => c.path.split('/').pop())).toEqual(['clear', 'clearTaskInstances'])
  })
})
