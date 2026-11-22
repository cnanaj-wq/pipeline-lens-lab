/**
 * Enregistre la spécification OpenAPI de l'Airflow en service comme référence des tests de contrat :
 * node tests/enregistrer-contrat.ts  (après une montée de version réussie).
 */
import { writeFileSync } from 'node:fs'

type Op = { parameters?: { name: string; in: string }[] }

const base = (process.env.AIRFLOW_URL || 'http://localhost:8085').replace(/\/$/, '')
type Spec = { info: unknown; paths: Record<string, Record<string, Op>>; components: { schemas: Record<string, { properties?: object }> } }
const o = (await fetch(`${base}/openapi.json`).then((r) => r.json())) as Spec
const version = ((await fetch(`${base}/api/v2/version`).then((r) => r.json())) as { version: string }).version
const paths = Object.fromEntries(Object.entries(o.paths)
  .filter(([p]) => p.startsWith('/api/v2/dags'))
  .map(([p, ops]) => [p, Object.fromEntries(Object.entries(ops).map(([m, d]) => [m, { parameters: (d.parameters ?? []).map(({ name, in: lieu }) => ({ name, in: lieu })) }]))]))
const schemas = Object.fromEntries(Object.entries(o.components.schemas)
  .filter(([k]) => /^(DAG|Task|XCom|Clear)/.test(k))
  .map(([k, v]) => [k, { properties: Object.fromEntries(Object.keys(v.properties ?? {}).map((f) => [f, {}])) }]))
const fichier = new URL(`./fixtures/openapi-airflow-${version}.json`, import.meta.url)
writeFileSync(fichier, JSON.stringify({ info: o.info, paths, components: { schemas } }))
console.log(`Référence enregistrée : tests/fixtures/openapi-airflow-${version}.json (mettez à jour tests/contract.test.ts).`)
