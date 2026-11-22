import type { Task } from './api'

export type StageKey = 'extract' | 'transform' | 'load'

export const STAGES: { key: StageKey; label: string; hint: string }[] = [
  { key: 'extract', label: 'Extract', hint: 'Lecture des sources' },
  { key: 'transform', label: 'Transform', hint: 'Nettoyage et règles' },
  { key: 'load', label: 'Load', hint: 'Écriture de la cible' },
]

const KEYWORDS: Record<StageKey, RegExp> = {
  extract: /(^|[._])(extract|ingest|src|source|get|fetch|read|bronze)/i,
  transform: /(^|[._])(transform|clean|aggregate|agg|silver|compute|enrich|join)/i,
  load: /(^|[._])(load|publish|write|export|gold|model|report|sink)/i,
}

/**
 * Range chaque tâche dans Extract, Transform ou Load.
 * 1) groupe de tâches nommé extract / transform / load (convention recommandée) ;
 * 2) sinon mots-clés du nom ;
 * 3) sinon position dans le graphe (premières = Extract, dernières = Load).
 */
export function classify(tasks: Task[]): Record<StageKey, Task[]> {
  const out: Record<StageKey, Task[]> = { extract: [], transform: [], load: [] }
  const depth = depths(tasks)
  const maxDepth = Math.max(0, ...Object.values(depth))
  for (const t of tasks) {
    const group = t.task_id.includes('.') ? t.task_id.split('.')[0].toLowerCase() : ''
    let stage: StageKey | null = null
    for (const k of ['extract', 'transform', 'load'] as StageKey[]) if (group.includes(k)) stage = k
    if (!stage) {
      const leaf = t.task_id.split('.').pop() ?? ''
      for (const k of ['load', 'transform', 'extract'] as StageKey[]) if (KEYWORDS[k].test(leaf)) stage = k
    }
    if (!stage) {
      const d = depth[t.task_id] ?? 0
      stage = d === 0 ? 'extract' : d === maxDepth ? 'load' : 'transform'
    }
    out[stage].push(t)
  }
  for (const k of Object.keys(out) as StageKey[]) out[k].sort((a, b) => (depth[a.task_id] ?? 0) - (depth[b.task_id] ?? 0))
  return out
}

function depths(tasks: Task[]): Record<string, number> {
  const parents: Record<string, string[]> = {}
  for (const t of tasks) for (const d of t.downstream_task_ids) (parents[d] ??= []).push(t.task_id)
  const memo: Record<string, number> = {}
  const visit = (id: string, seen: Set<string>): number => {
    if (memo[id] != null) return memo[id]
    if (seen.has(id)) return 0
    seen.add(id)
    const ps = parents[id] ?? []
    memo[id] = ps.length ? 1 + Math.max(...ps.map((p) => visit(p, seen))) : 0
    return memo[id]
  }
  for (const t of tasks) visit(t.task_id, new Set())
  return memo
}

/**
 * Volume d'une étape = volume de sa (ses) dernière(s) tâche(s) mesurée(s) :
 * pour Extract et Load, la somme des tâches mesurées ; pour Transform, la
 * somme des tâches mesurées (branches parallèles). null si rien n'est publié.
 */
export function stageVolume(tasks: Task[], volumes: Record<string, number> | undefined): number | null {
  if (!volumes) return null
  const vals = tasks.map((t) => volumes[t.task_id]).filter((v): v is number => typeof v === 'number')
  return vals.length ? vals.reduce((a, b) => a + b, 0) : null
}
