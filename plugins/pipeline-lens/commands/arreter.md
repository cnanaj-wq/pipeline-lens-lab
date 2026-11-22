---
description: Arrête Pipeline Lens et les conteneurs du lab (données conservées)
argument-hint: "[--tout] [--dossier <chemin>]"
allowed-tools: Bash(python:*), Bash(python3:*), Bash(py:*)
---

Exécute `python "${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py" arreter $ARGUMENTS`.
Sans option, les conteneurs sont arrêtés ; `--tout` les supprime aussi. Dans les deux cas,
l'historique Airflow reste dans le volume Docker `airflow-pg`. Ne supprime jamais de volume :
si l'utilisateur veut tout effacer, donne-lui la commande et laisse-le la lancer.
