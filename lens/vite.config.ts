import path from 'node:path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv, type Plugin } from 'vite'
import { airflowProxy } from './proxy/airflow-proxy.ts'

// Version de démonstration statique (GitHub Pages) : CSP stricte et instantané data.js.
function demoHtml(): Plugin {
  const csp = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' data:; connect-src 'none'; base-uri 'none'; form-action 'none'; object-src 'none'"
  return {
    name: 'demo-html',
    transformIndexHtml(html) {
      return html
        .replace('<meta charset="UTF-8" />', `<meta charset="UTF-8" />
    <meta http-equiv="Content-Security-Policy" content="${csp}" />
    <meta name="referrer" content="strict-origin-when-cross-origin" />
    <meta name="description" content="Pipeline Lens : les DAG Airflow en trois étapes Extract, Transform, Load, avec statut, durée, prochaine exécution et lignes produites à chaque étape." />`)
        .replace('<title>', `<script src="./data.js"></script>
    <title>`)
    },
  }
}

export default defineConfig(({ mode }) => {
  // Lit C:\airflow\.env (hors dépôt) : identifiants utilisés côté serveur uniquement.
  const env = loadEnv(mode, path.resolve(import.meta.dirname, '..'), '')
  return {
    plugins: [react(), tailwindcss(), airflowProxy(env), mode === 'demo' && demoHtml()],
    base: mode === 'demo' ? './' : '/',
    resolve: { alias: { '@': path.resolve(import.meta.dirname, './src') } },
    server: { host: '127.0.0.1', port: Number(process.env.LENS_PORT || 5176), strictPort: true },
    preview: { host: '127.0.0.1', port: 4173, strictPort: true },
  }
})
