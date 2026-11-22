"""Exporte un instantané de l'API Airflow 3 vers data.js (version de démonstration de Pipeline Lens).

Toutes les valeurs viennent de l'API REST d'Airflow (/api/v2) : DAG, exécutions, tâches,
instances, volumes publiés en XCom « row_count » et fin des journaux. Rien n'est saisi à la main.

    python scripts/exporter_instantane.py <dossier_de_sortie>

Les identifiants sont lus dans .env (jamais affichés ni écrits dans data.js).
"""
from __future__ import annotations

import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
BASE = "http://localhost:8085"
NB_RUNS = 25


def lire_env() -> dict[str, str]:
    env = {}
    for ligne in (RACINE / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in ligne and not ligne.startswith("#"):
            k, v = ligne.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def jeton(env: dict[str, str]) -> str:
    corps = json.dumps({"username": env["AIRFLOW_LENS_USER"], "password": env["AIRFLOW_LENS_PASSWORD"]}).encode()
    req = urllib.request.Request(f"{BASE}/auth/token", data=corps, headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req))["access_token"]


def main() -> None:
    sortie = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    tok = jeton(lire_env())
    q = urllib.parse.quote

    def api(chemin: str):
        req = urllib.request.Request(f"{BASE}/api/v2{chemin}", headers={"Authorization": f"Bearer {tok}"})
        try:
            return json.load(urllib.request.urlopen(req))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    version = api("/version")["version"]
    dags = api("/dags?limit=100&order_by=dag_id")["dags"]
    recent = api("/dags/~/dagRuns?limit=200&order_by=-run_after")["dag_runs"]
    per_dag, logs = {}, {}

    for d in dags:
        dag_id = d["dag_id"]
        runs = api(f"/dags/{q(dag_id)}/dagRuns?limit={NB_RUNS}&order_by=-run_after")["dag_runs"]
        run_ids = {r["dag_run_id"] for r in runs}
        tasks = api(f"/dags/{q(dag_id)}/tasks")["tasks"]
        tis = [t for t in api(f"/dags/{q(dag_id)}/dagRuns/~/taskInstances?limit=1000")["task_instances"]
               if t["dag_run_id"] in run_ids]
        volumes: dict[str, dict[str, int]] = {}
        entrees = api(f"/dags/{q(dag_id)}/dagRuns/~/taskInstances/~/xcomEntries?xcom_key=row_count&limit=1000")
        for e in entrees["xcom_entries"]:
            if e["run_id"] not in run_ids:
                continue
            v = api(f"/dags/{q(dag_id)}/dagRuns/{q(e['run_id'])}/taskInstances/{q(e['task_id'])}"
                    f"/xcomEntries/row_count?map_index={e['map_index']}")
            if v and isinstance(v.get("value"), (int, float)):
                volumes.setdefault(e["run_id"], {})[e["task_id"]] = int(v["value"])
        for t in tis:
            if t["try_number"] < 1:
                continue
            r = api(f"/dags/{q(dag_id)}/dagRuns/{q(t['dag_run_id'])}/taskInstances/{q(t['task_id'])}"
                    f"/logs/{t['try_number']}?full_content=true")
            if r:
                logs[f"{dag_id}|{t['dag_run_id']}|{t['task_id']}|{t['try_number']}"] = fin_journal(r.get("content"))
        per_dag[dag_id] = {"runs": runs, "tasks": tasks, "instances": tis, "volumes": volumes}

    # On ne garde que les champs utilisés par l'interface (pas de chemins, d'hôtes ni d'identifiants techniques).
    garder = lambda o, cles: {k: o.get(k) for k in cles}
    cles_dag = ["dag_id", "dag_display_name", "description", "is_paused", "timetable_summary",
                "timetable_description", "next_dagrun_run_after", "tags", "has_import_errors"]
    cles_run = ["dag_id", "dag_run_id", "state", "run_type", "start_date", "end_date", "run_after", "duration", "conf"]
    cles_task = ["task_id", "task_display_name", "downstream_task_ids", "operator_name"]
    cles_ti = ["task_id", "dag_run_id", "state", "start_date", "end_date", "duration", "try_number", "max_tries", "operator_name"]
    donnees = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "airflowVersion": version,
        "dags": [{**garder(d, cles_dag), "tags": [{"name": t["name"]} for t in d["tags"]]} for d in dags],
        "recentRuns": [garder(r, cles_run) for r in recent],
        "perDag": {k: {"runs": [garder(r, cles_run) for r in v["runs"]],
                       "tasks": [garder(t, cles_task) for t in v["tasks"]],
                       "instances": [garder(t, cles_ti) for t in v["instances"]],
                       "volumes": v["volumes"]} for k, v in per_dag.items()},
        "logs": logs,
    }
    texte = json.dumps(donnees, ensure_ascii=False, separators=(",", ":"))
    (sortie / "data.js").write_text(
        "// Instantané exporté de l'API d'un Airflow local par scripts/exporter_instantane.py.\n"
        f"// Données synthétiques. Ne pas modifier à la main.\nwindow.LENS_DATA = {texte};\n",
        encoding="utf-8",
    )
    nb_vol = sum(len(v) for d in per_dag.values() for v in d["volumes"].values())
    print(f"data.js : {len(dags)} DAG, {sum(len(d['runs']) for d in per_dag.values())} exécutions, "
          f"{nb_vol} volumes, {len(logs)} journaux")


def fin_journal(contenu) -> list[str]:
    lignes: list[str] = []

    def parcourir(c):
        if isinstance(c, str):
            lignes.extend(c.split("\n"))
        elif isinstance(c, list):
            for x in c:
                parcourir(x)
        elif isinstance(c, dict):
            if isinstance(c.get("event"), str):
                niveau = str(c.get("level", "")).upper()
                lignes.append(" - ".join(x for x in (niveau, c["event"]) if x))
            elif "content" in c:
                parcourir(c["content"])

    parcourir(contenu)
    propres = [l for l in lignes if l.strip() and not re.search(r"::(end)?group::", l)
               and not l.startswith("/opt/airflow/logs")]
    return propres[-14:]


if __name__ == "__main__":
    main()
