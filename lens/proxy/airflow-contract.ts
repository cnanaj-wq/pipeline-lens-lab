/**
 * Contrat d'interface : tout ce que Pipeline Lens utilise de l'API REST v2 d'Airflow.
 *
 * Avant une montée de version, `npm run test:contrat` compare ce contrat à la spécification
 * OpenAPI publiée par le nouvel Airflow (/openapi.json) : une route, un paramètre ou un champ
 * disparu est signalé avant que l'interface ne casse. Mettre à jour ce fichier à chaque
 * nouvel usage de l'API.
 */
export interface ContractEntry {
  method: 'get' | 'post'
  path: string
  query?: string[]
  /** Champs du corps JSON envoyé */
  body?: { schema: string; fields: string[] }
  /** Champs lus dans la réponse, par schéma */
  reads?: { schema: string; fields: string[] }[]
}

const P = '/api/v2/dags/{dag_id}'
const RUN = `${P}/dagRuns/{dag_run_id}`
const TI = `${RUN}/taskInstances/{task_id}`

export const CONTRACT: ContractEntry[] = [
  { method: 'get', path: '/api/v2/dags', query: ['limit', 'order_by'],
    reads: [{ schema: 'DAGResponse', fields: ['dag_id', 'dag_display_name', 'description', 'is_paused', 'timetable_summary', 'timetable_description', 'next_dagrun_run_after', 'tags', 'has_import_errors'] }] },
  { method: 'get', path: P, reads: [{ schema: 'DAGResponse', fields: ['tags', 'is_paused'] }] },
  { method: 'get', path: `${P}/dagRuns`, query: ['limit', 'offset', 'order_by', 'state', 'run_type', 'run_after_gte'],
    reads: [{ schema: 'DAGRunResponse', fields: ['dag_id', 'dag_run_id', 'state', 'run_type', 'start_date', 'end_date', 'run_after', 'duration', 'conf'] },
      { schema: 'DAGRunCollectionResponse', fields: ['dag_runs', 'total_entries'] }] },
  { method: 'get', path: RUN, reads: [{ schema: 'DAGRunResponse', fields: ['state'] }] },
  { method: 'get', path: `${P}/tasks`, reads: [{ schema: 'TaskResponse', fields: ['task_id', 'task_display_name', 'downstream_task_ids', 'operator_name', 'doc_md'] }] },
  { method: 'get', path: `${RUN}/taskInstances`, query: ['limit', 'order_by', 'state'],
    reads: [{ schema: 'TaskInstanceResponse', fields: ['task_id', 'dag_id', 'dag_run_id', 'state', 'start_date', 'end_date', 'duration', 'try_number', 'max_tries', 'operator_name'] }] },
  { method: 'get', path: `${TI}/xcomEntries`, query: ['xcom_key', 'limit'],
    reads: [{ schema: 'XComResponse', fields: ['task_id', 'run_id', 'map_index', 'timestamp'] }] },
  { method: 'get', path: `${TI}/xcomEntries/{xcom_key}`, query: ['map_index', 'deserialize'],
    reads: [{ schema: 'XComResponseNative', fields: ['value'] }] },
  { method: 'get', path: `${TI}/logs/{try_number}`, query: ['full_content'],
    reads: [{ schema: 'TaskInstancesLogResponse', fields: ['content'] }] },
  { method: 'post', path: `${RUN}/clear`, body: { schema: 'DAGRunClearBody', fields: ['dry_run', 'only_failed', 'note'] } },
  { method: 'post', path: `${P}/clearTaskInstances`,
    body: { schema: 'ClearTaskInstancesBody', fields: ['dry_run', 'dag_run_id', 'task_ids', 'include_downstream', 'include_upstream', 'only_failed', 'reset_dag_runs', 'note'] } },
]

export type OpenApi = {
  paths: Record<string, Record<string, { parameters?: { name: string; in: string }[] }>>
  components: { schemas: Record<string, { properties?: Record<string, unknown> }> }
}

/** Liste des écarts entre le contrat et une spécification OpenAPI ; vide si tout est compatible. */
export function checkContract(spec: OpenApi, contract: ContractEntry[] = CONTRACT): string[] {
  const out: string[] = []
  for (const c of contract) {
    const op = spec.paths[c.path]?.[c.method]
    if (!op) { out.push(`Route absente : ${c.method.toUpperCase()} ${c.path}`); continue }
    const params = new Set((op.parameters ?? []).filter((p) => p.in === 'query').map((p) => p.name))
    for (const q of c.query ?? []) if (!params.has(q)) out.push(`Paramètre absent : ${c.path} ?${q}`)
    for (const r of [...(c.reads ?? []), ...(c.body ? [c.body] : [])]) {
      const props = spec.components.schemas[r.schema]?.properties
      if (!props) { out.push(`Schéma absent : ${r.schema}`); continue }
      for (const f of r.fields) if (!(f in props)) out.push(`Champ absent : ${r.schema}.${f}`)
    }
  }
  return out
}
