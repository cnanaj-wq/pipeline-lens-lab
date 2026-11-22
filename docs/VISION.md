# Ce que pourrait devenir l'exploitation d'Apache Airflow

Pipeline Lens part d'un constat simple : Airflow sait **exécuter** des traitements, mais
répond mal aux questions de ceux qui en dépendent. Quels traitements tournent ? Lesquels sont
en échec ou en retard ? Quelle tâche bloque, et pourquoi ? Les données sont-elles complètes ?
Que faut-il faire maintenant ?

Ce document distingue trois choses, sans les mélanger :

- ce que le lab **fait déjà**, testé ;
- ce qu'Airflow 3 **permet aujourd'hui** sans modifier son code ;
- ce que je **propose** pour la suite, avec le niveau de certitude de chaque idée.

## 1. Où en est le lab

| Capacité | Comment | État |
|---|---|---|
| Supervision en direct | Proxy local vers l'API REST v2, actualisation 10 s à 1 min, fraîcheur affichée | Réalisé, testé |
| Lecture par étapes E → T → L | Classement automatique des tâches, volumes publiés par les tâches (XCom `row_count`) | Réalisé, testé |
| Écarts de lignes expliqués | XCom `row_breakdown` : rejetées, dédupliquées, filtrées, non chargées, jointes | Réalisé sur le DAG produits |
| Contrôles qualité | XCom `quality_checks` ; « Non instrumenté » sinon | Réalisé sur le DAG produits |
| Historique global | Toutes les exécutions de tous les DAG, filtres, indicateurs, activité 24 h / 7 j | Réalisé, testé |
| Rejeu contrôlé | DAG déclaré idempotent, simulation, confirmation sous 2 min, compte dédié, journal d'audit | Réalisé, testé sur le vrai Airflow |
| Montée de version | Contrat d'API vérifié contre la nouvelle version **avant** migration, sauvegarde, retour arrière | Réalisé, répété sur 3.4.0 |

## 2. Propositions

### 2.1 Des contrats de données publiés par les tâches

**Constat.** Les volumes et les écarts n'existent que si la tâche les publie. Aujourd'hui, c'est
une convention du lab (`row_count`, `row_breakdown`, `quality_checks`).

**Proposition.** Faire de ce triplet un petit **format commun**. Il serait émis par un décorateur
partagé, par exemple `@publie_volumes`, et validé par les tests des DAG. Chaque nouveau DAG
deviendrait observable sans code spécifique dans l'interface.

**Pour aller plus loin.** Transposer ce format en *facets* OpenLineage. Ces facets sont gérées par
le provider OpenLineage d'Airflow, ce qui permettrait à un catalogue de données de lire les mêmes
faits.

*Certitude : forte pour le décorateur (pur Python, testable). Moyenne pour OpenLineage, à vérifier
sur le provider installé.*

### 2.2 L'idempotence comme propriété vérifiée, pas comme promesse

**Constat.** Le rejeu n'est proposé que pour les DAG portant le tag `rejouable`. C'est une
déclaration de l'auteur, justifiée dans `doc_md` et contrôlée par un test. Elle n'est pas prouvée
pour autant.

**Proposition.** Ajouter un **test d'idempotence automatique** : exécuter deux fois la même tâche
sur un jeu de données de test, puis comparer les sorties. Seuls les DAG qui réussissent ce test
recevraient le droit au rejeu.

Pour les DAG sensibles, le rejeu demanderait une **validation à quatre yeux** : une personne
simule, une autre confirme. Le jeton de confirmation et le journal d'audit existent déjà ; il
suffirait de lier le jeton à un second compte.

*Certitude : forte, aucune dépendance nouvelle.*

### 2.3 Superviser des engagements métier, pas des états techniques

**Constat.** « Réussi » ne dit pas si la donnée est arrivée à temps, ni si elle est complète.

**Proposition.** Déclarer pour chaque jeu de données des **objectifs de service** (SLO). Par
exemple : « référentiel produits chargé avant 9 h 30 », « moins de 1 % de lignes écartées ».
Lens afficherait alors l'écart à l'engagement, et non plus seulement l'état. Les DAG en retard
deviendraient visibles **avant** l'heure limite, grâce à l'avancement estimé déjà calculé sur la
durée médiane.

*Certitude : forte. La médiane et l'estimation de fin existent déjà dans le lab.*

### 2.4 Détecter les dérives avant l'échec

**Constat.** Une durée deux fois supérieure à la médiane est déjà signalée. Les volumes, eux, ne
sont comparés qu'à l'exécution précédente.

**Proposition.** Remplacer la comparaison simple par une **bande de normalité** calculée sur
30 exécutions (médiane et écart absolu médian). Elle porterait sur les durées, les volumes et le
taux de lignes écartées. Une chute de 40 % des lignes chargées serait signalée même si le DAG
« réussit ».

*Certitude : forte pour le calcul. Le seuil exact devra être réglé sur des données réelles :
celles du lab sont synthétiques.*

### 2.5 Un diagnostic qui sépare toujours faits et hypothèses

**Constat.** Le panneau d'écart montre les faits publiés et étiquette « Hypothèse à vérifier »
toute part non expliquée. C'est la bonne règle pour qu'un exploitant fasse confiance à l'outil.

**Proposition.** Ajouter un **résumé de journal** assisté (par exemple par un modèle de langage)
qui respecte la même règle :

- citer la ligne d'erreur exacte ;
- relier l'erreur aux faits de l'exécution (essai, tâche bloquée, volume) ;
- formuler une hypothèse clairement marquée comme telle ;
- ne jamais proposer de rejeu à la place de l'humain.

*Certitude : moyenne. La valeur dépend de la qualité des journaux. À évaluer sur de vrais
incidents avant toute mise en service.*

### 2.6 Du temps réel sans interroger l'API en boucle

**Constat.** Lens interroge l'API à intervalle régulier (10 s par défaut). C'est simple et
robuste. Cela reste toutefois du « polling » : il coûte des requêtes, et la latence est égale à
l'intervalle.

**Proposition.** Faire pousser les changements d'état par le proxy (Server-Sent Events). Le proxy
saurait les détecter :

- dans un premier temps, par une lecture unique et partagée entre tous les navigateurs ouverts ;
- à terme, par un flux d'événements d'Airflow s'il en expose un.

L'interface est prête : les notifications de changement d'état existent déjà.

*Certitude : forte pour la mutualisation dans le proxy. Pour un flux d'événements natif, à
vérifier dans les versions à venir d'Airflow.*

### 2.7 Une vue par jeu de données

**Constat.** Airflow 3 met les **assets** au premier plan : un DAG peut être déclenché par la mise
à jour d'un jeu de données. L'exploitant pense souvent « table ventes du jour » plutôt que
« DAG sales_daily_dag ».

**Proposition.** Créer un troisième volet, « Données ». Pour chaque asset, il montrerait :

- sa dernière mise à jour ;
- le DAG producteur ;
- les DAG consommateurs ;
- l'état de fraîcheur par rapport à son SLO (voir 2.3).

*Certitude : forte. L'API v2 expose les assets et leurs événements. À vérifier dans le contrat
d'API au moment de l'implémentation.*

### 2.8 Des montées de version sans appréhension

**Constat.** La procédure du lab vérifie le **contrat d'API**, c'est-à-dire chaque route,
paramètre et champ utilisé par Lens. Elle le fait contre la nouvelle version, avant de migrer.
Puis elle sauvegarde, migre, teste et sait revenir en arrière.

**Proposition.**

- Exécuter cette vérification **chaque semaine en intégration continue**, contre la dernière
  version publiée d'Airflow. Une incompatibilité serait connue avant même d'être planifiée.
- Répéter la migration sur une **copie de la base de production** restaurée dans un conteneur
  jetable. On mesurerait ainsi sa durée réelle avant la fenêtre de maintenance.

*Certitude : forte. Le script `monter-version.ps1 -Simulation` fait déjà la première moitié.*

## 3. Feuille de route suggérée

| Horizon | Contenu | Pourquoi d'abord |
|---|---|---|
| Prochain mois | Décorateur `@publie_volumes` (2.1), bande de normalité (2.4), contrat d'API hebdomadaire en CI (2.8) | Peu de code, beaucoup de confiance gagnée |
| Trimestre | SLO de fraîcheur et de complétude (2.3), vue Données (2.7), mutualisation des lectures et SSE (2.6) | Change la question posée : « la donnée est-elle là ? » |
| Ensuite | Test d'idempotence automatique et validation à quatre yeux (2.2), résumé de journal assisté (2.5) | Demande des incidents réels pour être réglé et évalué |

## 4. Limites assumées

- Les données du lab sont **synthétiques**. Les seuils (anomalies, SLO) devront être recalés sur
  des données réelles.
- Le rejeu n'est ouvert qu'à un DAG dont l'idempotence est **déclarée et justifiée**. L'outil ne
  la démontre pas encore (voir 2.2).
- Lens ne remplace pas l'interface d'Airflow pour l'administration : déclenchements, pauses,
  connexions et variables restent dans Airflow.
- La consultation des lignes rejetées n'est pas ouverte : elle exige un contrôle des droits ligne
  à ligne, hors du périmètre d'un prototype local.
