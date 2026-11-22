import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { defineConfig, devices } from '@playwright/test'

/**
 * Tests de bout en bout : l'interface réelle (serveur Vite + proxy) contre le faux Airflow
 * (tests/mock-airflow.ts), dans Microsoft Edge déjà installé (aucun navigateur téléchargé).
 * Les identifiants ci-dessous sont ceux du faux Airflow, pas ceux du lab.
 */
const MOCK = 'http://127.0.0.1:18085'

// Journal d'incidents de test, daté par rapport à maintenant : une panne passagère reprise
// automatiquement (45 s) et un échec définitif encore ouvert.
const INCIDENTS = join(mkdtempSync(join(tmpdir(), 'lens-e2e-incidents-')), 'incidents.jsonl')
const il_y_a = (min: number) => new Date(Date.now() - min * 60_000).toISOString()
const evt = (min: number, evenement: string, extra: object) =>
  JSON.stringify({ ts: il_y_a(min), evenement, dag_id: 'produits_etl', run_id: 'scheduled__1', essais_max: 4, ...extra })
writeFileSync(INCIDENTS, [
  evt(90, 'echec_essai', { task_id: 'extract.read_src', essai: 1, raison: 'ConnectionError : source indisponible' }),
  evt(89.25, 'reprise', { task_id: 'extract.read_src', essai: 2, duree_s: 45 }),
  evt(20, 'echec', { dag_id: 'ventes_jour', run_id: 'scheduled__2', task_id: 'load.write_sales', essai: 2, raison: 'ValueError : prix illisibles' }),
].map((l) => `${l}\n`).join(''), 'utf-8')

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 30_000,
  reporter: [['list'], ['html', { open: 'never', outputFolder: 'playwright-report' }]],
  use: {
    baseURL: 'http://127.0.0.1:5174',
    // En local : Edge déjà installé ; en intégration continue : Chromium de Playwright.
    channel: process.env.CI ? undefined : 'msedge',
    locale: 'fr-FR',
    timezoneId: 'Europe/Paris',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'bureau', use: { ...devices['Desktop Edge'], channel: process.env.CI ? undefined : 'msedge', viewport: { width: 1440, height: 900 } } },
  ],
  webServer: [
    { command: 'node tests/serve-mock.ts 18085', url: `${MOCK}/__mock/calls`, reuseExistingServer: false, timeout: 20_000 },
    {
      command: 'npx vite --port 5174 --strictPort',
      url: 'http://127.0.0.1:5174',
      reuseExistingServer: false,
      timeout: 60_000,
      env: {
        LENS_PORT: '5174',
        AIRFLOW_URL: MOCK,
        AIRFLOW_LENS_USER: 'lens_lecture', AIRFLOW_LENS_PASSWORD: 'lecture-test',
        AIRFLOW_LENS_OPS_USER: 'lens_ops', AIRFLOW_LENS_OPS_PASSWORD: 'ops-test',
        LENS_ACTIONS: 'on',
        LENS_TIMEOUT_MS: '3000',
        LENS_INCIDENTS_FILE: INCIDENTS,
        LENS_AUDIT_FILE: join(tmpdir(), `lens-e2e-audit-${Date.now()}.jsonl`),
      },
    },
  ],
})
