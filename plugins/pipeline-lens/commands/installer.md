---
description: Vérifie le poste, clone le lab, génère les secrets et prépare Airflow 3 + Pipeline Lens
argument-hint: "[dossier d'installation, défaut ~/pipeline-lens-lab]"
allowed-tools: Bash(python:*), Bash(python3:*), Bash(py:*)
---

Installe le lab Pipeline Lens sur ce poste. Dossier demandé : `$ARGUMENTS` (vide = `~/pipeline-lens-lab`).

1. Lance le diagnostic et montre le résultat :
   `python "${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py" diagnostic`
   (si `python` est introuvable, essaie `python3` puis `py`).
   S'il manque un prérequis (Git, Docker démarré, Node.js 22.12+, Python 3.10+), arrête-toi :
   explique quoi installer, avec le lien officiel, et n'installe rien toi-même.
2. Lance l'installation (plusieurs minutes la première fois : images Docker et `npm ci`) :
   `python "${CLAUDE_PLUGIN_ROOT}/scripts/pipeline_lens.py" installer` en ajoutant
   `--dossier "<dossier>"` si un dossier est fourni.
3. Résume : où est le lab, quels fichiers de secrets ont été créés (`.env`,
   `config/passwords.json`), et rappelle que le mot de passe du compte `admin` d'Airflow se lit
   en ouvrant soi-même `config/passwords.json`. N'ouvre ni n'affiche jamais ces fichiers.
4. Propose la suite : `/pipeline-lens:demarrer`.
