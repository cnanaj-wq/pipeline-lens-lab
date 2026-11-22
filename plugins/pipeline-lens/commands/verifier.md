---
description: Vérifie qu'Airflow, Pipeline Lens (5176), son proxy et Mailpit répondent
argument-hint: "[--dossier <chemin>]"
allowed-tools: Bash(python:*), Bash(python3:*), Bash(py:*)
---

Exécute `python "${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py" verifier $ARGUMENTS` et
présente le résultat ligne par ligne. Pour chaque point en échec, donne la cause probable et la
commande pour la corriger (`/pipeline-lens:demarrer`, Docker Desktop arrêté, port déjà pris,
identifiants `.env` et `config/passwords.json` désynchronisés).
