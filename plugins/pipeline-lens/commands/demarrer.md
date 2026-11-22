---
description: Démarre Airflow 3 (Docker) puis Pipeline Lens sur http://localhost:5176 et vérifie le tout
argument-hint: "[--supervision] [--dossier <chemin>]"
allowed-tools: Bash(python:*), Bash(python3:*), Bash(py:*)
---

Démarre le lab. Options transmises : `$ARGUMENTS`
(`--supervision` ajoute Prometheus et Grafana ; `--dossier` si le lab n'est pas dans `~/pipeline-lens-lab`).

1. Exécute `python "${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py" demarrer $ARGUMENTS`
   (délai d'au moins 6 minutes : le premier démarrage d'Airflow est long).
2. Le script attend qu'Airflow réponde, lance Pipeline Lens en arrière-plan sur le port 5176
   (journal : `<lab>/.pipeline-lens/lens.log`) puis vérifie que le proxy de Lens s'authentifie
   auprès d'Airflow.
3. Donne les adresses : Pipeline Lens http://localhost:5176, Airflow http://localhost:8085
   (compte `admin`, mot de passe dans `config/passwords.json`), Mailpit http://localhost:8025.
4. En cas d'échec, lis les 40 dernières lignes de `.pipeline-lens/lens.log` ou
   `docker compose -p pipeline-lens logs --tail 50 airflow-apiserver`, puis explique la cause.
   Si aucun DAG n'est encore listé, c'est normal pendant ~30 s : propose `/pipeline-lens:verifier`.
