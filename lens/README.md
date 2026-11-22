# Pipeline Lens

Interface de suivi métier pour Apache Airflow 3 : étapes Extract → Transform → Load, volumes,
écarts expliqués, qualité, historique de toutes les exécutions et rejeu contrôlé.
Documentation complète : [../README.md](../README.md).

```bash
npm install
npm run dev          # http://localhost:5176 (lit ../.env côté serveur uniquement)
npm test             # unitaires, composants, proxy, contrat d'API
npm run test:e2e     # parcours Playwright dans Edge, contre un faux Airflow
npm run test:live    # contre le vrai Airflow local
npm run build:demo   # version statique de démonstration (instantané data.js)
```

| Dossier | Contenu |
|---|---|
| `proxy/` | serveur (lecture, rejeu, audit), contrat d'API, branchement Vite |
| `src/` | interface React, shadcn/ui, Motion |
| `tests/` | faux Airflow, tests du proxy, du contrat, tests réels (`live/`) |
| `e2e/` | parcours de bout en bout |
