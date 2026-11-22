# Monter de version Airflow

La version d'Airflow du lab est réglée **à un seul endroit** : `AIRFLOW_VERSION` dans `.env`
(3.4.0 par défaut). `docker-compose.yaml` l'utilise pour l'image. La CI GitHub lit, elle, la
variable de dépôt du même nom.

## En une commande

```powershell
cd C:\airflow
.\scripts\monter-version.ps1 -Version 3.4.1 -Simulation   # 1. compatible ? rien n'est modifié
.\scripts\monter-version.ps1 -Version 3.4.1               # 2. montée réelle
```

### Ce que fait le script, dans l'ordre

| Étape | Action | En cas d'échec |
|---|---|---|
| 1 | Vérifie que l'image `apache/airflow:<version>` existe | Arrêt, rien n'est modifié |
| 2 | Lit la spécification OpenAPI de la **nouvelle** version dans un conteneur jetable et la compare au contrat de Pipeline Lens (`lens/proxy/airflow-contract.ts`) | Arrêt, rien n'est modifié ; la route, le paramètre ou le champ manquant est affiché |
| 3 | Sauvegarde la base de métadonnées (`pg_dump`) dans `backups\` | Arrêt, rien n'est modifié |
| 4 | Met à jour `.env` et conserve l'ancienne version et la sauvegarde | — |
| 5 | Télécharge l'image, migre la base (`airflow db migrate`), redémarre, attend que l'API réponde dans la bonne version | Commande de retour arrière affichée |
| 6 | Lance les tests des DAG (pytest dans le conteneur) | Commande de retour arrière affichée |
| 7 | Lance les tests réels de Pipeline Lens : contrat, lecture, droits, réconciliation des volumes, rejeu simulé | Commande de retour arrière affichée |

`-Simulation` s'arrête après l'étape 2 : c'est la question « puis-je monter ? », sans risque.

## Revenir en arrière

```powershell
.\scripts\revenir-version.ps1
```

Le script arrête Airflow, puis **restaure la base sauvegardée**. Une base migrée vers une version
plus récente n'est pas relisible par l'ancienne. Il remet ensuite l'ancienne version et redémarre.
Une confirmation est demandée ; `-Oui` la saute.

## Après une montée réussie

1. Enregistrez la spécification de la nouvelle version comme référence des tests hors ligne :
   `cd lens; npm run contrat:enregistrer`. Puis pointez `tests/contract.test.ts` vers le
   nouveau fichier.
2. Sur GitHub, mettez la variable de dépôt `AIRFLOW_VERSION` à jour : les DAG y seront testés
   dans la même version.
3. Gardez la sauvegarde quelques jours, puis supprimez-la de `backups\`. Ce dossier est exclu
   de Git.

## Si le contrat d'API est cassé

Le message indique précisément ce qui manque. Par exemple :
`Paramètre absent : /api/v2/dags/{dag_id}/dagRuns ?run_after_gte`.

1. Adaptez l'appel dans `lens/proxy/lens-server.ts` ou `lens/src/lib/api.ts`.
2. Mettez à jour `lens/proxy/airflow-contract.ts`.
3. Relancez la simulation.

Tant que la simulation échoue, rien n'a été touché.

## Répétition générale effectuée

Le 10 novembre 2026, sur Airflow 3.4.0 vers 3.4.0 :

- **Simulation** : contrat respecté.
- **Montée** : sauvegarde de 216 Ko, migration, redémarrage, 7 tests DAG et 6 tests réels réussis.
- **Retour arrière** : base restaurée, Airflow répond à nouveau.
