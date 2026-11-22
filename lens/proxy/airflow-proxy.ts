/**
 * Plugin Vite : branche le serveur Pipeline Lens (proxy/lens-server.ts) sur les serveurs
 * de développement et de prévisualisation. Toute la logique est dans lens-server.ts, testée seule.
 */
import { fileURLToPath } from 'node:url'
import type { Plugin } from 'vite'
import { createLensHandler, type Env } from './lens-server.ts'

export function airflowProxy(env: Env): Plugin {
  const auditFile = env.LENS_AUDIT_FILE || fileURLToPath(new URL('../audit/lens-audit.jsonl', import.meta.url))
  const incidentsFile = env.LENS_INCIDENTS_FILE || fileURLToPath(new URL('../../logs/incidents/incidents.jsonl', import.meta.url))
  const handler = createLensHandler(env, { auditFile, incidentsFile })
  return {
    name: 'airflow-lens-proxy',
    configureServer(server) {
      server.middlewares.use(handler)
    },
    configurePreviewServer(server) {
      server.middlewares.use(handler)
    },
  }
}
