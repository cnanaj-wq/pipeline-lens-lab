"""DAG de démonstration — chargement quotidien du référentiel produits.

Données 100 % synthétiques, générées à chaque exécution (aucune source réelle).
Chaque tâche qui produit des données publie son volume en XCom sous la clé
« row_count » : c'est ce que lit l'application Pipeline Lens. La tâche
get_src_tables ne publie rien, volontairement, pour montrer l'état « non mesuré ».

Pour simuler un échec définitif : déclencher le DAG avec la configuration
{"simuler_echec": true}. Pour une panne passagère suivie d'une reprise automatique :
{"echecs_simules": 1} (le premier essai échoue, le suivant réussit).
"""
from __future__ import annotations

import csv
import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.parse import quote

from airflow.sdk import dag, get_current_context, task, task_group

DATA = Path("/opt/airflow/data/product_etl")
log = logging.getLogger(__name__)


def publier_volume(lignes: int) -> int:
    """Publie le nombre de lignes produit par la tâche (lu par Pipeline Lens)."""
    get_current_context()["ti"].xcom_push(key="row_count", value=lignes)
    log.info("Volume publié : %s lignes", lignes)
    return lignes


def publier_ecarts(entree: int, sortie: int, ecarts: list[dict], fichier_rejets: str | None = None) -> None:
    """Publie la répartition documentée de l'écart entrée → sortie (XCom « row_breakdown »).

    Chaque écart a une nature : « rejetee » (règle de qualité), « filtree » (hors périmètre, voulu),
    « dedupliquee », « non_chargee » (perte à l'écriture) ou « jointure » (lignes de référence
    devenues des colonnes : ce n'est pas une perte). Pipeline Lens rapproche la somme des écarts
    de l'écart observé et signale toute différence non expliquée.
    """
    get_current_context()["ti"].xcom_push(key="row_breakdown", value={
        "entree": entree, "sortie": sortie, "ecarts": ecarts, "fichier_rejets": fichier_rejets,
    })
    log.info("Écarts publiés : %s → %s, %s", entree, sortie, {e["motif"]: e["lignes"] for e in ecarts})


def publier_controles(controles: list[dict]) -> None:
    """Publie les contrôles qualité réellement exécutés (XCom « quality_checks »)."""
    for c in controles:
        c["statut"] = "ok" if c["valeur"] <= c["seuil"] else "ko"
    get_current_context()["ti"].xcom_push(key="quality_checks", value=controles)
    log.info("Contrôles qualité : %s", {c["controle"]: c["statut"] for c in controles})


def lire_csv(nom: str) -> list[dict]:
    with open(DATA / nom, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def ecrire_csv(nom: str, lignes: list[dict]) -> None:
    (DATA / nom).parent.mkdir(parents=True, exist_ok=True)
    with open(DATA / nom, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(lignes[0].keys()))
        w.writeheader()
        w.writerows(lignes)


# --- Incidents : alertes et historique, déclenchés par les événements d'Airflow ------------------
# Airflow appelle ces fonctions à l'instant où une tâche échoue, va être réessayée, ou réussit.
# Chaque événement est historisé (une ligne JSON) dans logs/incidents/incidents.jsonl :
# date et heure, DAG, tâche, exécution, essai, raison. Pipeline Lens en déduit chaque incident,
# sa durée et sa reprise (volet Historique). Fichier en ajout seul : rien n'est réécrit.
INCIDENTS = Path(os.environ.get("INCIDENTS_FICHIER", "/opt/airflow/logs/incidents/incidents.jsonl"))


def _raison(context) -> str:
    e = context.get("exception")
    return f"{type(e).__name__} : {e}"[:500] if e else "non transmise par Airflow"


def historiser(evenement: str, context, **extra) -> dict:
    """Ajoute un événement au journal des incidents ; ne fait jamais échouer la tâche."""
    ti = context["task_instance"]
    ligne = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "evenement": evenement,
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "essai": getattr(ti, "try_number", None),
        "essais_max": (getattr(ti, "max_tries", None) or 0) + 1,
        **extra,
    }
    try:
        INCIDENTS.parent.mkdir(parents=True, exist_ok=True)
        with INCIDENTS.open("a", encoding="utf-8") as f:
            f.write(json.dumps(ligne, ensure_ascii=False) + "\n")
    except OSError:
        log.exception("Historisation de l'incident impossible")
    return ligne


def incident_ouvert(dag_id: str, task_id: str, run_id: str) -> dict | None:
    """Premier échec non encore suivi d'une reprise, pour cette tâche de cette exécution."""
    debut = None
    try:
        lignes = INCIDENTS.read_text(encoding="utf-8").splitlines()[-5000:]
    except OSError:
        return None
    for brute in lignes:
        try:
            e = json.loads(brute)
        except ValueError:
            continue
        if (e.get("dag_id"), e.get("task_id"), e.get("run_id")) != (dag_id, task_id, run_id):
            continue
        if e.get("evenement") in ("echec_essai", "echec"):
            debut = debut or e
        elif e.get("evenement") == "reprise":
            debut = None
    return debut


def _duree(secondes: float) -> str:
    m, s = divmod(int(round(secondes)), 60)
    h, m = divmod(m, 60)
    return f"{h} h {m:02d} min" if h else f"{m} min {s:02d} s" if m else f"{s} s"


def composer_alerte(context, lens_url: str | None = None, mode: str = "echec", duree: float | None = None) -> tuple[str, str]:
    """Objet et corps HTML du mail. mode : « echec » (définitif), « essai » (Airflow réessaie), « reprise »."""
    ti = context["task_instance"]
    run_id = context["run_id"]
    essai = getattr(ti, "try_number", None)
    essais_max = getattr(ti, "max_tries", None)
    t, d = escape(ti.task_id), escape(ti.dag_id)
    if mode == "essai":
        sujet = f"[Airflow] Incident en cours, nouvel essai : {ti.dag_id} / {ti.task_id}"
        intro = (f"L'essai de la tâche <b>{t}</b> du DAG <b>{d}</b> a échoué. "
                 "Airflow réessaie automatiquement : rien à faire pour l'instant.")
        conseil = "Vous recevrez un mail de reprise, ou d'échec si le dernier essai échoue aussi."
    elif mode == "reprise":
        sujet = f"[Airflow] Rétabli : {ti.dag_id} / {ti.task_id}"
        intro = f"La tâche <b>{t}</b> du DAG <b>{d}</b> a réussi après un incident."
        conseil = "Incident clos. L'historique est dans Pipeline Lens, volet Historique."
    else:
        sujet = f"[Airflow] Échec : {ti.dag_id} / {ti.task_id}"
        intro = f"La tâche <b>{t}</b> du DAG <b>{d}</b> a échoué."
        conseil = "Avant un rejeu, vérifiez la cause dans le journal de la tâche."
    lignes = [
        ("DAG", ti.dag_id),
        ("Tâche", ti.task_id),
        ("Exécution", run_id),
        ("Essai", f"{essai} sur {essais_max + 1}" if essai and essais_max is not None else str(essai or "n/d")),
    ]
    if mode == "reprise":
        lignes.append(("Durée de l'incident", _duree(duree or 0)))
    else:
        lignes.append(("Erreur", str(context.get("exception") or "non transmise par Airflow")[:2000]))
    tableau = "".join(
        f'<tr><th align="left" style="padding:4px 12px 4px 0">{escape(k)}</th><td style="padding:4px 0">{escape(v)}</td></tr>'
        for k, v in lignes
    )
    lien = ""
    if lens_url:
        url = f"{lens_url.rstrip('/')}/?dag={quote(ti.dag_id)}&run={quote(run_id, safe='')}"
        lien = f'<p><a href="{escape(url)}">Ouvrir l’exécution dans Pipeline Lens</a></p>'
    corps = (
        f"<p>{intro}</p>"
        f'<table style="font-family:sans-serif;font-size:14px">{tableau}</table>{lien}'
        f'<p style="color:#666;font-size:12px">Alerte automatique du lab Airflow. {conseil}</p>'
    )
    return sujet, corps


def _envoyer_alerte(context, mode: str, duree: float | None = None) -> None:
    """Envoie le mail si ALERTE_EMAIL_A est défini ; un problème d'envoi est seulement journalisé."""
    destinataires = [a.strip() for a in os.environ.get("ALERTE_EMAIL_A", "").split(",") if a.strip()]
    if not destinataires:
        return
    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        sujet, corps = composer_alerte(context, os.environ.get("ALERTE_LENS_URL"), mode, duree)
        with SmtpHook(smtp_conn_id="smtp_default") as smtp:
            smtp.send_email_smtp(to=destinataires, subject=sujet, html_content=corps)
        log.info("Alerte envoyée par mail à %s", ", ".join(destinataires))
    except Exception:  # l'alerte ne doit jamais faire échouer autre chose
        log.exception("Envoi du mail d'alerte impossible")


def alerte_echec(context) -> None:
    """Échec définitif (tous les essais épuisés) : historisé, journalisé, mail."""
    ti = context["task_instance"]
    log.error("ALERTE — la tâche %s du DAG %s a échoué (exécution %s).", ti.task_id, ti.dag_id, context["run_id"])
    historiser("echec", context, raison=_raison(context))
    _envoyer_alerte(context, "echec")


def alerte_essai(context) -> None:
    """Un essai a échoué, Airflow va réessayer : historisé, mail « incident en cours »
    (ALERTE_EMAIL_ESSAIS=off pour couper ce mail)."""
    ti = context["task_instance"]
    log.warning("Essai échoué pour %s.%s : nouvel essai programmé.", ti.dag_id, ti.task_id)
    historiser("echec_essai", context, raison=_raison(context))
    if os.environ.get("ALERTE_EMAIL_ESSAIS", "on").lower() != "off":
        _envoyer_alerte(context, "essai")


def signaler_reprise(context) -> None:
    """Succès d'une tâche : si un incident était ouvert (essai automatique ou rejeu), il est clos
    avec sa durée, et un mail « rétabli » part."""
    ti = context["task_instance"]
    debut = incident_ouvert(ti.dag_id, ti.task_id, context["run_id"])
    if not debut:
        return
    duree = (datetime.now(timezone.utc) - datetime.fromisoformat(debut["ts"])).total_seconds()
    historiser("reprise", context, duree_s=round(duree), debut=debut["ts"])
    log.info("Incident clos pour %s.%s après %s", ti.dag_id, ti.task_id, _duree(duree))
    _envoyer_alerte(context, "reprise", duree)


# Reprise sur incident : Airflow relance seul une tâche en échec, avec un délai qui double
# (30 s, 1 min, 2 min). Réservé aux DAG idempotents : un nouvel essai ne crée pas de doublon.
REPRISE_IDEMPOTENTE = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=5),
    "on_retry_callback": alerte_essai,
    "on_failure_callback": alerte_echec,
    "on_success_callback": signaler_reprise,
}


@dag(
    dag_id="product_etl_dag",
    description="Référentiel produits : extraction, transformation, chargement (données synthétiques)",
    schedule="0 9 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    # « rejouable » : Pipeline Lens peut proposer le rejeu de ce DAG (voir doc_md).
    tags=["etl", "produits", "demo", "rejouable"],
    doc_md=(
        "Idempotent : chaque tâche réécrit entièrement ses fichiers (aucun ajout), "
        "un rejeu produit donc le même résultat sans doublon. Rejeu autorisé depuis Pipeline Lens."
    ),
    default_args=REPRISE_IDEMPOTENTE,
)
def product_etl_dag():
    @task_group(group_id="extract")
    def extract():
        @task
        def get_src_tables() -> list[str]:
            # Pas de publication de volume ici : Pipeline Lens affichera « non mesuré ».
            return ["produits", "categories", "sous_categories"]

        @task
        def load_src_data(tables: list[str]) -> int:
            ctx = get_current_context()
            rnd = random.Random(ctx["run_id"])  # volumes différents à chaque exécution
            nb_cat, nb_sous = 4, 37
            categories = [{"id": i, "nom": f"Catégorie {i}"} for i in range(1, nb_cat + 1)]
            sous = [{"id": i, "categorie_id": rnd.randint(1, nb_cat), "nom": f"Sous-catégorie {i}"}
                    for i in range(1, nb_sous + 1)]
            n = rnd.randint(11_500, 12_800)
            produits = [{
                "id": i if rnd.random() > 0.015 else rnd.randint(1, i),  # ~1,5 % de doublons
                "sous_categorie_id": rnd.randint(1, nb_sous),
                "nom": f"Produit {i}",
                "prix": round(rnd.uniform(1, 500), 2) if rnd.random() > 0.01 else "",
            } for i in range(1, n + 1)]
            ecrire_csv("src_categories.csv", categories)
            ecrire_csv("src_sous_categories.csv", sous)
            ecrire_csv("src_produits.csv", produits)
            return publier_volume(len(categories) + len(sous) + len(produits))

        load_src_data(get_src_tables())

    @task_group(group_id="transform")
    def transform():
        @task
        def transform_produits() -> int:
            ctx = get_current_context()
            conf = ctx["dag_run"].conf or {}
            if conf.get("simuler_echec"):
                raise ValueError("Échec simulé : prix illisibles dans src_produits.csv")
            # Panne passagère simulée : les N premiers essais échouent, puis la reprise automatique réussit.
            if ctx["ti"].try_number <= int(conf.get("echecs_simules", 0)):
                raise ConnectionError("Panne passagère simulée : source indisponible")
            source = lire_csv("src_produits.csv")
            vus, propres, rejets = set(), [], []
            for p in source:
                if p["id"] in vus:
                    rejets.append({**p, "motif": "doublon_id"})
                elif not p["prix"]:
                    rejets.append({**p, "motif": "prix_manquant"})
                else:
                    vus.add(p["id"])
                    propres.append(p)
            ecrire_csv("silver_produits.csv", propres)
            if rejets:  # conservé côté serveur ; consultation soumise à habilitation
                ecrire_csv("rejets_produits.csv", rejets)
            publier_ecarts(len(source), len(propres), [
                {"motif": "doublon_id", "libelle": "Identifiant produit en double", "nature": "dedupliquee",
                 "lignes": sum(r["motif"] == "doublon_id" for r in rejets)},
                {"motif": "prix_manquant", "libelle": "Prix manquant", "nature": "rejetee",
                 "lignes": sum(r["motif"] == "prix_manquant" for r in rejets)},
            ], "rejets_produits.csv" if rejets else None)
            return publier_volume(len(propres))

        @task
        def transform_categories() -> int:
            lignes = lire_csv("src_categories.csv")
            ecrire_csv("silver_categories.csv", lignes)
            return publier_volume(len(lignes))

        @task
        def transform_sous_categories() -> int:
            lignes = lire_csv("src_sous_categories.csv")
            ecrire_csv("silver_sous_categories.csv", lignes)
            return publier_volume(len(lignes))

        transform_produits()
        transform_categories()
        transform_sous_categories()

    @task_group(group_id="load")
    def load():
        @task
        def build_product_model() -> int:
            cats = {c["id"]: c["nom"] for c in lire_csv("silver_categories.csv")}
            sous = {s["id"]: s for s in lire_csv("silver_sous_categories.csv")}
            produits = lire_csv("silver_produits.csv")
            modele, orphelins = [], 0
            for p in produits:
                s = sous.get(p["sous_categorie_id"])
                if s is None or s["categorie_id"] not in cats:
                    orphelins += 1  # sans sous-catégorie connue : non chargé
                    continue
                modele.append({**p, "sous_categorie": s["nom"], "categorie": cats[s["categorie_id"]]})
            ecrire_csv("gold_modele_produit.csv", modele)
            publier_ecarts(len(produits) + len(cats) + len(sous), len(modele), [
                {"motif": "dimensions_jointes", "libelle": "Catégories et sous-catégories jointes au modèle",
                 "nature": "jointure", "lignes": len(cats) + len(sous)},
                {"motif": "sans_sous_categorie", "libelle": "Produit sans sous-catégorie connue",
                 "nature": "non_chargee", "lignes": orphelins},
            ])
            ids = [p["id"] for p in modele]
            publier_controles([
                {"controle": "product_id vide", "valeur": sum(1 for i in ids if not str(i).strip()), "seuil": 0},
                {"controle": "product_id en double", "valeur": len(ids) - len(set(ids)), "seuil": 0},
                {"controle": "Prix manquant", "valeur": sum(1 for p in modele if not p["prix"]), "seuil": 0},
                {"controle": "Produit sans catégorie", "valeur": orphelins, "seuil": 0},
            ])
            return publier_volume(len(modele))

        build_product_model()

    extract() >> transform() >> load()


product_etl_dag()


@dag(
    dag_id="sales_daily_dag",
    description="Ventes du jour : agrégation horaire (données synthétiques)",
    schedule="30 7 * * 1-5",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["etl", "ventes", "demo"],
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10), "on_retry_callback": alerte_essai,
                  "on_failure_callback": alerte_echec, "on_success_callback": signaler_reprise},
)
def sales_daily_dag():
    @task_group(group_id="extract")
    def extract():
        @task
        def extract_tickets() -> int:
            rnd = random.Random()
            lignes = [{"ticket": i, "heure": rnd.randint(8, 20), "montant": round(rnd.uniform(2, 90), 2)}
                      for i in range(rnd.randint(3_000, 4_500))]
            ecrire_csv("../sales/src_tickets.csv", lignes)
            return publier_volume(len(lignes))

        extract_tickets()

    @task_group(group_id="transform")
    def transform():
        @task
        def aggregate_by_hour() -> int:
            agg: dict[str, float] = {}
            for t in lire_csv("../sales/src_tickets.csv"):
                agg[t["heure"]] = agg.get(t["heure"], 0) + float(t["montant"])
            ecrire_csv("../sales/silver_ventes_heure.csv",
                       [{"heure": h, "ca": round(v, 2)} for h, v in sorted(agg.items())])
            return publier_volume(len(agg))

        aggregate_by_hour()

    @task_group(group_id="load")
    def load():
        @task
        def publish_report() -> None:
            # Pas de volume publié : simple notification de fin.
            log.info("Rapport des ventes prêt.")

        publish_report()

    extract() >> transform() >> load()


sales_daily_dag()
