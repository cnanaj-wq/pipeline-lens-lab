---
name: pipeline-lens
description: Connaissance du lab Apache Airflow 3 + Pipeline Lens (http://localhost:5176) et de sa greffe Qlik Cloud. À utiliser quand l'utilisateur veut installer, démarrer, dépanner ou comprendre Pipeline Lens, ses ports, ses comptes lens_lecture et lens_ops, le rejeu contrôlé, la XCom row_count ou le lab qlik-airflow-lab.
---

# Pipeline Lens — lab Airflow 3

## Ce que c'est
- **Airflow 3.4** dans Docker (LocalExecutor + Postgres), API et interface sur http://localhost:8085.
- **Pipeline Lens** (React + Vite, `lens/`) sur http://localhost:5176 : étapes Extract → Transform →
  Load, volumes de lignes, écarts, contrôles qualité, incidents, rejeu contrôlé.
- **Mailpit** (http://localhost:8025) reçoit les mails d'alerte, rien ne sort du poste.
- Option `--supervision` : Prometheus (9091) et Grafana (3001).
- Tous les ports écoutent sur 127.0.0.1. Données 100 % synthétiques.

## Comment Lens lit Airflow
Navigateur → `/lens/*` → proxy local (plugin Vite, `lens/proxy/lens-server.ts`) → JWT → API REST
Airflow `/api/v2`. Le navigateur ne voit ni mot de passe ni jeton. Lecture seule, sauf
`POST /lens/actions/replay` (simulation obligatoire, journal d'audit, compte `lens_ops` au rôle `op`).
Volumes : chaque tâche publie une XCom `row_count`.

## Commandes du plugin
| Commande | Effet |
|---|---|
| `/pipeline-lens:installer [dossier]` | diagnostic, clone, secrets, images Docker, `npm ci` |
| `/pipeline-lens:demarrer [--supervision]` | Airflow puis Lens sur 5176, vérification |
| `/pipeline-lens:verifier` | Airflow, Lens, proxy authentifié, Mailpit |
| `/pipeline-lens:arreter [--tout]` | arrêt, données conservées |
| `/pipeline-lens:greffer-qlik` | ajoute le lab Qlik Cloud × Airflow de l'article |

Moteur : `${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py` (Python, bibliothèque standard).
Projet Docker Compose : `pipeline-lens`.

## Règles
- Ne jamais afficher, ouvrir ni recopier `.env`, `config/passwords.json`, `monitoring/grafana.env`
  ou le `.env` du dépôt Qlik. L'utilisateur les ouvre lui-même.
- Ne jamais demander de clé API Qlik dans la conversation.
- Ne jamais supprimer de volume Docker ni lancer `qlik_lab deploy` sans accord explicite.

## Dépannage rapide
- Port 5176 pris : `LENS_PORT=<autre>` avant `demarrer`.
- Proxy Lens en 401/403 : mots de passe `.env` et `config/passwords.json` désynchronisés →
  relancer `scripts/generer-secrets.py` (additif) puis `docker compose -p pipeline-lens restart airflow-apiserver`.
- Pas de DAG listé : attendre ~30 s (rafraîchissement du dag-processor).
