/**
 * Parcours réels de Pipeline Lens contre le faux Airflow : les 8 tests obligatoires du cahier
 * des charges, le rejeu contrôlé, les filtres de l'historique, le mobile, le clavier et
 * la réduction des animations.
 */
import { expect, test, type Page } from '@playwright/test'

const MOCK = 'http://127.0.0.1:18085'
const pilote = (page: Page, corps: Record<string, unknown>) => page.request.post(`${MOCK}/__mock/scenario`, { data: corps })

test.beforeEach(async ({ page }) => {
  await pilote(page, { name: 'complet', down: false, delayMs: 0 })
  // Actualisation désactivée par défaut dans les tests : chaque test la déclenche lui-même.
  await page.addInitScript(() => { try { localStorage.setItem('lens-refresh', '0') } catch { /* */ } })
})

test.describe('les 8 tests obligatoires', () => {
  test('1. DAG en succès : statut, volumes et écart entièrement expliqué', async ({ page }) => {
    await page.goto('/?dag=produits_etl')
    await expect(page.getByRole('heading', { level: 1 })).toContainText(/produits/i)
    await expect(page.getByRole('status').filter({ hasText: 'Synchronisé' })).toBeVisible()
    await expect(page.getByText(/11\s500 lues, 11\s210 chargées/)).toBeVisible()
    await page.getByRole('button', { name: /Écart de .*290.* lignes/ }).click()
    const panneau = page.getByRole('dialog')
    await expect(panneau.getByText(/entièrement expliqué/)).toBeVisible()
    await expect(panneau.getByText('Identifiant en double')).toBeVisible()
    await expect(panneau.getByText('Dédupliquée')).toBeVisible()
    await expect(panneau.getByText('rejets_produits.csv')).toBeVisible()
  })

  test('2. tâche en échec : incident, essai et journal', async ({ page }) => {
    await page.goto('/?dag=ventes_jour')
    await expect(page.getByRole('heading', { level: 2, name: /Exécution du/ }).locator('..')).toContainText('Échec')
    await expect(page.getByRole('button', { name: /^Charger, Bloqué par un échec en amont/ })).toBeVisible()
    await page.getByRole('button', { name: /^Nettoyer, Échec/ }).click()
    const fiche = page.getByRole('dialog')
    await expect(fiche).toContainText('2 sur 2')
    await expect(fiche).toContainText('colonne « prix » absente')
    // L'incident reste signalé quand on consulte une exécution plus ancienne.
    await page.keyboard.press('Escape')
    await page.getByRole('listbox', { name: 'Dernières exécutions' }).getByRole('option').first().click()
    const incident = page.getByRole('region', { name: 'Dernier incident' })
    await expect(incident).toContainText('La dernière exécution a échoué')
    await expect(incident).toContainText('Nettoyer en échec (essai 2 sur 2), 1 tâche bloquée en aval')
    await incident.getByRole('button', { name: 'Afficher cette exécution' }).click()
    await expect(incident).toHaveCount(0)
  })

  test('3. DAG en cours d’exécution', async ({ page }) => {
    await page.goto('/?dag=stocks_horaire')
    await expect(page.getByRole('heading', { level: 1 }).locator('..')).toContainText('En cours')
    await expect(page.getByRole('button', { name: /^Nettoyer, En cours/ })).toBeVisible()
  })

  test('4. tâche en attente de nouvel essai', async ({ page }) => {
    await page.goto('/?dag=clients_sync')
    await expect(page.getByRole('button', { name: /^Nettoyer, Nouvel essai prévu/ })).toBeVisible()
  })

  test('5. API indisponible : message clair, puis reprise', async ({ page }) => {
    await pilote(page, { down: true })
    await page.goto('/?dag=produits_etl')
    await expect(page.getByText(/Airflow a répondu HTTP 503/)).toBeVisible()
    await expect(page.getByRole('status').filter({ hasText: 'Airflow injoignable' })).toBeVisible()
    await pilote(page, { down: false })
    await page.getByRole('button', { name: 'Réessayer' }).first().click()
    await expect(page.getByRole('status').filter({ hasText: 'Synchronisé' })).toBeVisible()
  })

  test('6. historique vide', async ({ page }) => {
    await pilote(page, { name: 'vide' })
    await page.goto('/?dag=nouveau_dag')
    await expect(page.getByText(/Aucune exécution pour l’instant/)).toBeVisible()
    await page.getByRole('tab', { name: 'Historique' }).click()
    await expect(page.getByText('Aucune exécution sur cette période.')).toBeVisible()
  })

  test('7. données qualité absentes : « Non instrumenté », aucun chiffre inventé', async ({ page }) => {
    await page.goto('/?dag=ventes_jour')
    await expect(page.getByText(/non instrumenté/i).first()).toBeVisible()
    await page.getByRole('tab', { name: 'Historique' }).click()
    await page.getByRole('combobox', { name: /^DAG/ }).selectOption('stocks_horaire')
    await expect(page.getByText('non mesuré').first()).toBeVisible()
  })

  test('8. actualisation après changement d’état : affichage et notification', async ({ page }) => {
    await page.goto('/?dag=stocks_horaire')
    await expect(page.getByRole('heading', { level: 1 }).locator('..')).toContainText('En cours')
    await pilote(page, { patch: { dag_id: 'stocks_horaire', state: 'success' } })
    await page.getByRole('button', { name: 'Actualiser maintenant' }).click()
    await expect(page.getByRole('heading', { level: 1 }).locator('..')).toContainText('Réussi')
    await expect(page.getByText('stocks_horaire : exécution réussie')).toBeVisible()
  })
})

test.describe('historique et rejeu', () => {
  test('indicateurs, graphique et liste de toutes les exécutions', async ({ page }) => {
    await page.goto('/?vue=historique')
    const kpi = page.getByRole('region', { name: 'Indicateurs' })
    await expect(kpi).toContainText('DAG surveillés')
    await expect(kpi.getByText('5', { exact: true })).toBeVisible()
    await expect(page.getByRole('img', { name: /Activité des dernières 24 heures/ })).toBeVisible()
    await expect(page.getByText(/\d+ exécutions/).last()).toBeVisible()
    await page.getByRole('radio', { name: '7 jours' }).click()
    await expect(page.getByRole('img', { name: /7 derniers jours/ })).toBeVisible()
  })

  test('filtres : état, DAG et recherche', async ({ page }) => {
    await page.goto('/?vue=historique')
    await page.getByLabel('État').selectOption('failed')
    const lignes = page.getByRole('region', { name: 'Exécutions' }).getByRole('listitem')
    await expect(lignes).toHaveCount(1)
    await expect(lignes.first()).toContainText('ventes_jour')
    await expect(lignes.first()).toContainText('Nettoyer')
    await page.getByLabel('État').selectOption('')
    await page.getByRole('combobox', { name: /^DAG/ }).selectOption('produits_etl')
    await expect(lignes).toHaveCount(8)
    await page.getByRole('searchbox').fill('aucun-resultat')
    await expect(page.getByText('Aucune exécution ne correspond à la recherche.')).toBeVisible()
  })

  test('rejeu refusé pour un DAG non rejouable, avec la raison', async ({ page }) => {
    await page.goto('/?vue=historique')
    await page.getByRole('combobox', { name: /^DAG/ }).selectOption('ventes_jour')
    const bouton = page.getByRole('button', { name: /Rejeu indisponible.*idempotence/ }).first()
    await expect(bouton).toHaveAttribute('aria-disabled', 'true')
    await bouton.click({ force: true })
    await expect(page.getByRole('dialog')).toHaveCount(0)
  })

  test('rejeu complet : simulation, confirmation, suivi en direct, journal d’audit', async ({ page }) => {
    await page.goto('/?vue=historique')
    await page.getByRole('combobox', { name: /^DAG/ }).selectOption('produits_etl')
    await page.getByRole('button', { name: /^Rejouer l’exécution/ }).first().click()
    const d = page.getByRole('dialog')
    await expect(d).toContainText('Que rejouer ?')
    await d.getByRole('button', { name: 'Simuler le rejeu' }).click()
    await expect(d).toContainText('3 tâches seront relancées')
    await expect(d).toContainText('Rien n’a encore été modifié')
    await d.getByRole('button', { name: 'Confirmer le rejeu' }).click()
    await expect(d).toContainText('Suivi en direct')
    await expect(page.getByText('Rejeu lancé')).toBeVisible()
    await pilote(page, { patch: { dag_id: 'produits_etl', state: 'success' } })
    await expect(d).toContainText('Exécution rejouée avec succès.', { timeout: 10_000 })
    await d.getByRole('button', { name: 'Fermer' }).last().click()
    await expect(page.getByRole('region', { name: 'Derniers rejeux' })).toContainText(/exécution complète.*3 tâches relancées/, { timeout: 15_000 })
  })

  test('« Ouvrir » mène au détail de l’exécution choisie', async ({ page }) => {
    await page.goto('/?vue=historique')
    await page.getByLabel('État').selectOption('failed')
    await page.getByRole('region', { name: 'Exécutions' }).getByRole('button', { name: /^Ouvrir l’exécution/ }).first().click()
    await expect(page).toHaveURL(/dag=ventes_jour&run=/)
    await expect(page.getByRole('heading', { level: 2, name: /Exécution du/ }).locator('..')).toContainText('Échec')
  })

  test('incidents : historisés à l’événement, avec durée et reprise', async ({ page }) => {
    await page.goto('/?vue=historique')
    const panel = page.getByRole('region', { name: 'Incidents' })
    await expect(panel.getByText('Ouverts').locator('..')).toContainText('1')
    await expect(panel.getByText('Durée médiane de rétablissement').locator('..')).toContainText('45 s')
    const items = panel.getByRole('listitem')
    await expect(items).toHaveCount(2)
    await expect(items.nth(0)).toContainText('ventes_jour')
    await expect(items.nth(0)).toContainText('ValueError : prix illisibles')
    await expect(items.nth(0)).toContainText('Échec, à reprendre')
    await expect(items.nth(1)).toContainText('Rétabli')
    await expect(items.nth(1)).toContainText('par nouvel essai automatique')
    await items.nth(1).getByRole('button', { name: /^Ouvrir l’exécution/ }).click()
    await expect(page).toHaveURL(/dag=produits_etl&run=scheduled__1/)
  })
})

test.describe('accessibilité et affichage', () => {
  test('clavier : lien d’évitement et onglets au clavier', async ({ page }) => {
    await page.goto('/?dag=produits_etl')
    await page.keyboard.press('Tab')
    await expect(page.getByRole('link', { name: 'Aller au contenu' })).toBeFocused()
    await page.getByRole('tab', { name: 'Pipeline' }).focus()
    await page.keyboard.press('ArrowRight')
    await page.keyboard.press('Enter')
    await expect(page.getByRole('heading', { name: 'Historique des traitements' })).toBeVisible()
  })

  test('mobile 375 px : aucun défilement horizontal', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 })
    for (const url of ['/?dag=produits_etl', '/?vue=historique']) {
      await page.goto(url)
      await page.waitForLoadState('networkidle')
      // En cas d'échec, le message liste les éléments qui dépassent de l'écran.
      const fautifs = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth
        ? [...document.querySelectorAll('body *')].filter((e) => e.getBoundingClientRect().right > window.innerWidth + 1)
          .slice(0, 5).map((e) => `${e.tagName}.${String((e as HTMLElement).className).slice(0, 60)}`)
        : [])
      expect(fautifs, url).toEqual([])
    }
  })

  test('animations réduites : l’interface reste complète', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' })
    await page.goto('/?vue=historique')
    await expect(page.getByRole('region', { name: 'Indicateurs' }).getByText('5', { exact: true })).toBeVisible()
  })

  test('lecture seule : aucune écriture n’atteint Airflow sans confirmation', async ({ page }) => {
    await page.goto('/?vue=historique')
    await page.waitForLoadState('networkidle')
    const calls: { method: string; body?: { dry_run?: boolean } }[] = await (await page.request.get(`${MOCK}/__mock/calls`)).json()
    expect(calls.filter((c) => c.method !== 'GET' && c.body?.dry_run === false)).toHaveLength(0)
  })
})
