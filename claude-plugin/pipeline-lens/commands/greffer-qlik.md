---
description: Ajoute le lab Qlik Cloud × Airflow (porte qualité, 3 apps Qlik) sur l'Airflow installé
argument-hint: "[--verifier-seulement] [--qlik <chemin>] [--dossier <chemin>]"
allowed-tools: Bash(python:*), Bash(python3:*), Bash(py:*)
---

Greffe le dépôt `qlik-airflow-lab` (celui de l'article) sur l'Airflow du lab. Options : `$ARGUMENTS`.

Prérequis à rappeler AVANT de lancer : un tenant Qlik Cloud et une clé API créée par
l'utilisateur dans ce tenant. Ne demande jamais la clé dans la conversation et ne l'écris jamais
toi-même : l'utilisateur la colle seul dans le fichier `.env` du dépôt Qlik.

1. Exécute `python "${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py" greffer-qlik $ARGUMENTS`.
2. Code 2 = valeurs manquantes : indique le chemin du `.env` à ouvrir et les variables à remplir
   (`QLIK_TENANT_URL`, `QLIK_API_KEY`, `QLIK_SUPERVISION_PASSWORD`), puis propose de relancer.
3. Succès : les DAG `qlik_sales_pipeline` et `qlik_supervision_dag` (tag `qlik`) apparaissent dans
   Airflow et Pipeline Lens après ~30 s. Les 3 apps Qlik ne sont PAS encore créées : explique
   l'étape 0.4 de `docs/SCENARIOS.md` du dépôt Qlik (`python -m qlik_lab deploy --dry-run`, puis
   `deploy`). Ne lance pas `deploy` sans l'accord explicite de l'utilisateur : il crée des apps
   dans son tenant.
4. Propose ensuite le scénario nominal : déclencher `qlik_sales_pipeline` avec `{"scenario": "clean"}`.
