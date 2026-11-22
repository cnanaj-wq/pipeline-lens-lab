# Lab Airflow 3 + Pipeline Lens

Lab local d'orchestration ETL : **Apache Airflow 3.4** dans Docker, une interface de suivi
lisible (**Pipeline Lens**), des alertes d'échec, une supervision technique (Prometheus + Grafana)
et un contrôle automatique des DAG (GitHub Actions). Données 100 % synthétiques.
Tout reste en local, rien n'est publié.

| Service | Adresse | Rôle |
|---|---|---|
| Pipeline Lens | http://localhost:5176 | Vue métier : étapes E → T → L, statuts, durées, volumes, historique, rejeu contrôlé |
| Airflow (interface d'origine) | http://localhost:8085 | Administration, déclenchements, journaux complets |
| Grafana (option) | http://localhost:3001 | Santé technique : planificateur, échecs, durées |
| Prometheus (option) | http://localhost:9091 | Stockage des métriques |
| Mailpit | http://localhost:8025 | Boîte de test : les mails d'alerte d'échec, sans rien envoyer hors du PC |

Tous les ports écoutent uniquement sur `127.0.0.1` : rien n'est exposé au réseau.

## Reproduire le lab avec Claude Code (plugin)

Ce dépôt est aussi une marketplace de plugins Claude Code. Le plugin `pipeline-lens` installe,
démarre et vérifie le lab pour vous.

**Prérequis** : Git, Docker Desktop démarré (Compose v2), Node.js 22.12+, Python 3.10+,
Claude Code. Ports libres sur 127.0.0.1 : 8085 (Airflow), 5176 (Pipeline Lens), 8025 et 1025 (Mailpit).

Dans Claude Code :

```
/plugin marketplace add cnanaj-wq/pipeline-lens-lab
/plugin install pipeline-lens@pipeline-lens-lab
```

(Claude Code 2.1.275 ou plus récent : `/plugin install pipeline-lens --marketplace cnanaj-wq/pipeline-lens-lab` fait les deux en une fois.)

Puis :

| Commande | Effet |
|---|---|
| `/pipeline-lens:installer` | vérifie le poste, clone le lab dans `~/pipeline-lens-lab`, génère les secrets, télécharge les images, `npm ci` |
| `/pipeline-lens:demarrer` | démarre Airflow, puis Pipeline Lens sur http://localhost:5176, et vérifie |
| `/pipeline-lens:verifier` | Airflow, Lens, proxy authentifié, Mailpit |
| `/pipeline-lens:arreter` | arrête tout, données conservées |
| `/pipeline-lens:greffer-qlik` | ajoute le lab [qlik-airflow-lab](https://github.com/cnanaj-wq/qlik-airflow-lab) (tenant Qlik Cloud et clé API requis) |

Sans Claude Code, le même moteur s'utilise directement :

```bash
python claude-plugin/pipeline-lens/scripts/pipeline_lens.py diagnostic
python claude-plugin/pipeline-lens/scripts/pipeline_lens.py installer --dossier ~/pipeline-lens-lab
python claude-plugin/pipeline-lens/scripts/pipeline_lens.py demarrer
```

## Démarrer (à la main)

```bash
cd pipeline-lens-lab
python scripts/generer-secrets.py      # une seule fois : crée .env, config/passwords.json, monitoring/grafana.env
docker compose -f docker-compose.yaml -f docker-compose.monitoring.yaml up -d
cd lens && npm install && npm run dev  # Pipeline Lens sur http://localhost:5176
```

Sans la supervision : `docker compose up -d` (Airflow seul).
Arrêter : `docker compose -f docker-compose.yaml -f docker-compose.monitoring.yaml down`
(les données Airflow sont conservées dans le volume `airflow-pg`).

## Identifiants

- Le script `generer-secrets.py` tire des mots de passe aléatoires **sans les afficher**.
- Votre mot de passe Airflow (compte `admin`) : ouvrez vous-même `config/passwords.json`.
  Le même fichier contient ceux des deux comptes techniques de Lens (`lens_lecture`, `lens_ops`).
  Pour changer celui d'un compte de Lens : modifiez ce fichier **et** la ligne correspondante de
  `.env` (`AIRFLOW_LENS_PASSWORD` ou `AIRFLOW_LENS_OPS_PASSWORD`), puis
  `docker compose restart airflow-apiserver` et relancez `npm run dev`.
- Grafana est lisible sans compte ; le compte administrateur est dans `monitoring/grafana.env`.
- `.env`, `config/passwords.json` et `monitoring/grafana.env` sont exclus de Git (`.gitignore`).

## Comment Pipeline Lens obtient ses données

```
Navigateur ──/lens/*──▶ proxy local (plugin Vite) ──JWT──▶ API REST Airflow 3 (/api/v2)
```

- Le **proxy** (`lens/proxy/airflow-proxy.ts`) demande un jeton à Airflow (`POST /auth/token`)
  avec les identifiants de `.env`, le renouvelle, et relaie les requêtes. Le navigateur ne voit
  jamais ni mot de passe ni jeton, et il n'y a pas de problème de CORS (même origine).
- **Lecture seule, sauf le rejeu contrôlé** : le proxy refuse toute écriture (`405`), à une
  exception près, `POST /lens/actions/replay`, entourée de garde-fous (voir « Historique et rejeu
  contrôlé »). Pour déclencher ou mettre en pause un DAG, utilisez l'interface d'Airflow.
- Toute la logique serveur est dans `lens/proxy/lens-server.ts`, testée seule contre un faux
  Airflow ; `airflow-proxy.ts` ne fait que la brancher sur Vite.
- Le code d'Airflow n'est pas modifié : l'application ne dépend que de l'API.

## Faire publier le nombre de lignes par vos tâches

Airflow ne mesure pas les volumes. Pipeline Lens lit une **XCom de clé `row_count`** publiée par
chaque tâche. Tant qu'une tâche ne publie rien, Lens affiche « non mesuré » : aucun chiffre n'est inventé.

**TaskFlow (`@task`)**, la forme la plus simple :

```python
from airflow.sdk import get_current_context, task

@task
def transform_clients() -> int:
    df = ...  # votre traitement
    get_current_context()["ti"].xcom_push(key="row_count", value=len(df))
    return len(df)
```

**PythonOperator classique** :

```python
def charger(ti, **_):
    n = ecrire_table(...)            # nombre de lignes écrites
    ti.xcom_push(key="row_count", value=n)
```

**Requête SQL (SQLExecuteQueryOperator)** : ajoutez une petite tâche qui compte
(`SELECT COUNT(*) FROM cible`) et publie le résultat avec la même clé.

Règles à retenir :
- une valeur **entière** = lignes **produites** par la tâche (pas lues) ;
- publiez **à la fin**, une fois l'écriture réussie : une tâche en échec n'affiche donc pas de volume ;
- coût négligeable : une ligne en base de métadonnées par tâche et par exécution.

## Historique et rejeu contrôlé

L'onglet **Historique** (`?vue=historique`) liste toutes les exécutions de tous les DAG, lues en
direct :

- indicateurs : DAG surveillés, en cours, en échec, taux de succès 24 h et 7 j, durée médiane ;
- activité sur 24 h ou 7 j ;
- filtres : DAG, état, type, période, recherche ;
- par exécution : tâches en échec, lignes chargées, durée anormale, avancement estimé si en cours.

**Rejouer** ouvre une fenêtre en trois temps :

1. choisir quoi rejouer : les tâches en échec, toute l'exécution, ou une tâche avec ou sans ses
   dépendantes ;
2. **simuler** : Airflow renvoie la liste exacte des tâches touchées, et rien n'est modifié ;
3. **confirmer** dans les 2 minutes ; le suivi se fait ensuite en direct jusqu'au résultat.

Comptes Airflow utilisés par Lens, chacun avec le minimum de droits :

| Compte | Rôle Airflow | Usage |
|---|---|---|
| `lens_lecture` | viewer | tout l'affichage (ne peut rien modifier) |
| `lens_ops` | op | **uniquement** le rejeu contrôlé |

Leurs mots de passe sont dans `config/passwords.json` et `.env` (ouvrez-les vous-même).
Lecture seule stricte : `LENS_ACTIONS=off` dans `.env`, puis relancez `npm run dev`.

Garde-fous, revérifiés par le serveur à chaque demande :

| Garde-fou | Effet |
|---|---|
| `LENS_ACTIONS=on` | sinon : lecture seule stricte |
| Tag `rejouable` sur le DAG | l'auteur déclare ses tâches idempotentes ; un test vérifie que `doc_md` le justifie |
| Simulation obligatoire | jeton de confirmation à usage unique, lié à la demande exacte, valable 2 min |
| Exécution terminée | pas de rejeu d'une exécution en cours |
| 1 rejeu / 5 s par exécution | évite les doubles clics |
| Compte `lens_ops` (rôle op) | le compte d'affichage ne peut pas rejouer |
| En-tête `X-Lens-Action` | une page tierce ne peut pas déclencher de rejeu |
| Journal `lens/audit/lens-audit.jsonl` | chaque simulation et chaque rejeu, avec le compte et le résultat |

Seul `product_etl_dag` est rejouable : il réécrit entièrement ses fichiers. `sales_daily_dag`
ne l'est pas, et Lens affiche la raison.

Les changements d'état détectés à l'actualisation (fin d'exécution, échec, nouvelle exécution)
s'affichent en notification.

## Tests

```bash
cd pipeline-lens-lab/lens
npm test             # unitaires, composants, proxy contre un faux Airflow, contrat d'API
npm run test:e2e     # parcours réels dans Edge contre le faux Airflow (dont les 8 obligatoires)
npm run test:live    # contre le vrai Airflow local : contrat, droits, volumes, rejeu simulé
# LENS_LIVE_REJEU=1 npm run test:live   ajoute un rejeu réel d'une tâche, attendu jusqu'au succès
```

Tests des DAG : voir la commande en tête de `tests/test_dags.py`.

## Monter de version

La version est réglée dans `.env` (`AIRFLOW_VERSION`). Une commande vérifie la compatibilité,
sauvegarde, migre, teste et sait revenir en arrière. Voir
[docs/MONTEE-DE-VERSION.md](docs/MONTEE-DE-VERSION.md).

```powershell
.\scripts\monter-version.ps1 -Version 3.4.1 -Simulation
```

## Et ensuite ?

Propositions pour l'exploitation d'Airflow : contrats de données, idempotence vérifiée, SLO de
fraîcheur, détection de dérives, temps réel, vue par jeu de données. Voir
[docs/VISION.md](docs/VISION.md).

## Ranger les tâches en Extract → Transform → Load

Lens classe les tâches dans cet ordre de priorité :
1. **groupe de tâches** nommé `extract`, `transform` ou `load` (recommandé, voir `dags/product_etl_dag.py`) ;
2. sinon **mots-clés** dans le nom (`extract`, `src`, `clean`, `agg`, `load`, `publish`…) ;
3. sinon **position** dans le graphe (premières tâches = Extract, dernières = Load).

Le volume d'une étape est la **somme** des volumes publiés par ses tâches. Dans le DAG de démo,
l'étape Transform compte donc aussi les 41 lignes de catégories et sous-catégories, qui sont
ensuite fusionnées dans le modèle produit : l'écart « −41 » vers Load est normal.
Si une étape ne s'est pas terminée avec succès, son volume est marqué « partiel » et aucun
écart n'est calculé.

## Les DAG de démonstration

| DAG | Planification | Ce qu'il montre |
|---|---|---|
| `product_etl_dag` | tous les jours à 9 h | 3 étapes, transformations en parallèle, doublons écartés, une tâche « non mesurée » |
| `sales_daily_dag` | du lundi au vendredi à 7 h 30 | agrégation (forte réduction de volume), tâche de publication sans volume |

Simuler un échec : dans Airflow, *Trigger DAG* avec la configuration `{"simuler_echec": true}`
sur `product_etl_dag`.

## Incidents : reprise automatique, alertes, historique

Tout part des **événements d'Airflow**, à la seconde où ils se produisent : chaque tâche porte
trois fonctions (dans `dags/product_etl_dag.py`) qu'Airflow appelle lui-même.

| Événement Airflow | Fonction | Ce qui se passe |
|---|---|---|
| Un essai échoue, Airflow va réessayer (`on_retry_callback`) | `alerte_essai` | historisé ; mail « Incident en cours, nouvel essai » |
| Le dernier essai échoue (`on_failure_callback`) | `alerte_echec` | historisé ; mail « Échec » |
| La tâche réussit après un incident (`on_success_callback`) | `signaler_reprise` | historisé avec la durée ; mail « Rétabli » |

**Reprise automatique.** `product_etl_dag`, déclaré idempotent, réessaie 3 fois avec un délai qui
double (30 s, 1 min, 2 min). `sales_daily_dag`, non déclaré rejouable, réessaie une fois après 10 s.

**Historique.** Chaque événement est une ligne de `logs/incidents/incidents.jsonl` : date et heure,
DAG, tâche, exécution, essai, raison (type et message de l'erreur), durée à la reprise. Le fichier
n'est jamais réécrit. Pipeline Lens le lit (volet Historique, panneau « Incidents ») : incidents
ouverts, rétablis, repris sans intervention, durée médiane de rétablissement, et pour chacun la
raison, la durée et le mode de reprise (nouvel essai automatique ou rejeu).

**Si Airflow lui-même tombe**, il ne peut plus rien signaler. Un chien de garde extérieur,
`scripts/veille_airflow.py`, tourne toutes les 5 minutes (tâche planifiée Windows « Veille Airflow
(lab) ») : API muette, base, planificateur ou lecteur de DAG en panne, DAG actif qui n'a pas démarré
à son heure (15 min de tolérance). Un mail au début du problème, un au rétablissement, jamais en
boucle. Si le mail ne peut pas partir (Mailpit tourne dans Docker), notification Windows et
`logs/veille.log`.

```powershell
.\scripts\installer-veille.ps1             # installer (déjà fait) ; -Retirer pour supprimer
python scripts\veille_airflow.py --essai     # diagnostic immédiat, sans mail
```

**Essayer.** Déclenchez `product_etl_dag` avec la configuration :

- `{"echecs_simules": 1}` : panne passagère ; mail d'incident, reprise automatique 30 s plus tard,
  mail « Rétabli » ;
- `{"simuler_echec": true}` : échec à chaque essai ; 3 mails d'incident puis le mail d'échec.

### Où partent les mails

| Réglage dans `.env` | Rôle |
|---|---|
| `ALERTE_EMAIL_A` | destinataire(s), séparés par des virgules ; vide = aucun mail |
| `ALERTE_EMAIL_ESSAIS` | `off` pour ne pas recevoir le mail à chaque nouvel essai |
| `ALERTE_LENS_URL` | adresse de Pipeline Lens pour le lien du mail |
| `AIRFLOW_CONN_SMTP_DEFAULT` | serveur d'envoi (connexion Airflow `smtp_default`), aussi utilisé par le chien de garde |

Par défaut, les mails vont dans **Mailpit** (http://localhost:8025) : rien ne sort du PC.
Pour les recevoir vraiment, remplacez vous-même la connexion dans `.env`. Pour une adresse
hébergée chez Hostinger  :

```
AIRFLOW_CONN_SMTP_DEFAULT='{"conn_type":"smtp","host":"smtp.hostinger.com","port":465,"login":"vous@votre-domaine.fr","password":"VOTRE_MOT_DE_PASSE_DE_BOITE","extra":{"from_email":"vous@votre-domaine.fr","disable_tls":true}}'
```

Pour Gmail : `"host":"smtp.gmail.com","port":587`, un mot de passe d'application, et
`"extra":{"from_email":"…","disable_ssl":true}`. Puis `docker compose up -d --force-recreate`.

## Contrôle automatique des DAG

`tests/test_dags.py` vérifie : chargement sans erreur, présence des groupes extract / transform / load,
alerte d'échec sur chaque tâche. Il s'exécute :
- en local (voir la commande en tête du fichier) ;
- sur GitHub via `.github/workflows/dags.yml`, **seulement si vous poussez ce dossier** sur un dépôt.

## Limites connues

- Volumes : uniquement ce que les tâches publient. Rien pour les tâches existantes tant qu'elles
  ne sont pas modifiées.
- Lens interroge Airflow toutes les 10 s par défaut (réglable, ou désactivé). Il ne remplace pas
  l'interface d'Airflow pour l'administration (déclencher, mettre en pause, effacer). Seul le
  rejeu y est possible, et seulement pour les DAG déclarés rejouables.
- Les lignes rejetées sont conservées dans un fichier côté serveur. Leur consultation n'est pas
  ouverte dans Lens : elle exige un contrôle des droits ligne à ligne.
- Le proxy fonctionne avec `npm run dev` ou `npm run preview` (pas avec un simple serveur
  de fichiers statiques sur `dist/`).
- Gestionnaire d'accès simple d'Airflow 3 (comptes et rôles dans un fichier) : suffisant pour un
  lab local, pas pour une équipe. Il faudrait le provider FAB ou un annuaire LDAP / OAuth, et le
  journal d'audit de Lens devrait alors porter le nom de la personne, pas seulement le compte
  technique.
- Pas de mise à l'échelle automatique (LocalExecutor, un seul poste).

## Arborescence

```
C:\airflow
├─ docker-compose.yaml             Airflow 3.4 (Postgres, api-server, scheduler, dag-processor)
├─ docker-compose.monitoring.yaml  option : StatsD exporter, Prometheus, Grafana
├─ dags/product_etl_dag.py         2 DAG de démo (données synthétiques)
├─ tests/test_dags.py              contrôle des DAG
├─ .github/workflows/              CI : DAG (dags.yml) et Pipeline Lens (lens.yml)
├─ scripts/monter-version.ps1      montée de version (et revenir-version.ps1)
├─ docs/                           VISION.md, MONTEE-DE-VERSION.md
├─ monitoring/                     configuration Prometheus, Grafana, mapping StatsD
├─ scripts/generer-secrets.py      secrets locaux, jamais affichés
└─ lens/                           Pipeline Lens (Vite, React, shadcn/ui, Motion)
   ├─ proxy/lens-server.ts         serveur : lecture, rejeu contrôlé, audit
   ├─ proxy/airflow-contract.ts    contrat d'API vérifié à chaque montée de version
   ├─ src/                         interface
   ├─ tests/                       faux Airflow, tests du proxy et du contrat, tests réels (live/)
   └─ e2e/                         parcours Playwright
```
