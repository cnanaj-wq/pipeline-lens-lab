"""Chien de garde du lab Airflow, extérieur à Airflow (tâche planifiée Windows, toutes les 5 min).

Airflow envoie lui-même les alertes d'échec de tâche. Mais si Airflow tombe (planificateur
arrêté, Docker arrêté), plus personne ne prévient. Ce script le vérifie de l'extérieur :

- l'API d'Airflow répond-elle ?
- la base, le planificateur et le lecteur de DAG sont-ils vivants (battement récent) ?
- un DAG actif a-t-il manqué son heure (prévu depuis plus de RETARD_TOLERE, sans exécution) ?

Il envoie un mail au début d'un problème et à son rétablissement, jamais en boucle
(état gardé dans logs/veille-etat.json). Si le mail ne peut pas partir (la boîte de test
Mailpit tourne dans Docker), il affiche une notification Windows et l'écrit dans
logs/veille.log.

Réglages lus dans .env, sans jamais rien afficher : ALERTE_EMAIL_A, AIRFLOW_CONN_SMTP_DEFAULT,
AIRFLOW_LENS_USER / AIRFLOW_LENS_PASSWORD (compte en lecture seule), AIRFLOW_URL.

    python scripts/veille_airflow.py            # une vérification
    python scripts/veille_airflow.py --essai    # affiche le diagnostic, sans mail ni état
"""
from __future__ import annotations

import json
import smtplib
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
ETAT = RACINE / "logs" / "veille-etat.json"
JOURNAL = RACINE / "logs" / "veille.log"
BATTEMENT_MAX = timedelta(minutes=2)
RETARD_TOLERE = timedelta(minutes=15)
COMPOSANTS = {"metadatabase": "la base de métadonnées", "scheduler": "le planificateur", "dag_processor": "le lecteur de DAG"}


def lire_env(chemin: Path = RACINE / ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    if chemin.exists():
        for ligne in chemin.read_text(encoding="utf-8").splitlines():
            if "=" in ligne and not ligne.lstrip().startswith("#"):
                k, v = ligne.split("=", 1)
                v = v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                    v = v[1:-1]
                env[k.strip()] = v
    return env


def _date(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def diagnostiquer(sante: dict | None, dags: list[dict] | None, runs_actifs: set[str], maintenant: datetime) -> dict[str, str]:
    """Problèmes constatés, par clé stable (pour ne prévenir qu'une fois). Fonction pure, testée."""
    if sante is None:
        return {"api": "L'API d'Airflow ne répond pas (Airflow ou Docker arrêté ?)."}
    pb: dict[str, str] = {}
    for cle, nom in COMPOSANTS.items():
        c = sante.get(cle) or {}
        if c.get("status") != "healthy":
            pb[cle] = f"{nom.capitalize()} est en panne (état : {c.get('status', 'inconnu')})."
            continue
        battement = _date(c.get(f"latest_{cle}_heartbeat")) if cle != "metadatabase" else None
        if cle != "metadatabase" and (battement is None or maintenant - battement > BATTEMENT_MAX):
            pb[cle] = f"{nom.capitalize()} ne donne plus signe de vie depuis plus de {int(BATTEMENT_MAX.total_seconds() // 60)} min."
    for d in dags or []:
        prevu = _date(d.get("next_dagrun_run_after"))
        if d.get("is_paused") or prevu is None or d["dag_id"] in runs_actifs:
            continue
        retard = maintenant - prevu
        if retard > RETARD_TOLERE:
            pb[f"retard:{d['dag_id']}"] = (
                f"Le DAG {d['dag_id']} devait démarrer à {prevu.astimezone().strftime('%d/%m %H:%M')} "
                f"et n'a pas tourné ({int(retard.total_seconds() // 60)} min de retard)."
            )
    return pb


def comparer(avant: dict[str, str], maintenant: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """(nouveaux problèmes, problèmes résolus) depuis la dernière vérification."""
    nouveaux = {k: v for k, v in maintenant.items() if k not in avant}
    resolus = {k: v for k, v in avant.items() if k not in maintenant}
    return nouveaux, resolus


def composer(nouveaux: dict[str, str], resolus: dict[str, str], restants: dict[str, str]) -> tuple[str, str]:
    if nouveaux:
        sujet = f"[Veille Airflow] Problème : {next(iter(nouveaux.values()))[:90]}"
    else:
        sujet = "[Veille Airflow] Rétabli : Airflow fonctionne à nouveau" if not restants else "[Veille Airflow] Rétablissement partiel"
    parties = []
    if nouveaux:
        parties.append("Nouveau problème :\n" + "\n".join(f"- {v}" for v in nouveaux.values()))
    if resolus:
        parties.append("Résolu :\n" + "\n".join(f"- {v}" for v in resolus.values()))
    if restants and not nouveaux:
        parties.append("Toujours en cours :\n" + "\n".join(f"- {v}" for v in restants.values()))
    parties.append(
        "Que faire : vérifiez que Docker Desktop tourne, puis, dans C:\\airflow : docker compose up -d\n"
        "Les traitements manqués ne sont pas rattrapés automatiquement (catchup désactivé) : "
        "déclenchez-les depuis Airflow si besoin.\n\nMessage du chien de garde, extérieur à Airflow."
    )
    return sujet, "\n\n".join(parties)


def _get(url: str, jeton: str | None = None, timeout: int = 10) -> dict:
    r = urllib.request.Request(url, headers={"Authorization": f"Bearer {jeton}"} if jeton else {})
    with urllib.request.urlopen(r, timeout=timeout) as f:
        return json.loads(f.read())


def observer(env: dict[str, str]) -> tuple[dict | None, list[dict] | None, set[str]]:
    base = env.get("AIRFLOW_URL", "http://localhost:8085").rstrip("/")
    try:
        sante = _get(f"{base}/api/v2/monitor/health")
    except (urllib.error.URLError, OSError, ValueError):
        return None, None, set()
    try:
        corps = json.dumps({"username": env["AIRFLOW_LENS_USER"], "password": env["AIRFLOW_LENS_PASSWORD"]}).encode()
        r = urllib.request.Request(f"{base}/auth/token", data=corps, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(r, timeout=10) as f:
            jeton = json.loads(f.read())["access_token"]
        dags = _get(f"{base}/api/v2/dags?limit=100", jeton)["dags"]
        actifs = set()
        for etat in ("running", "queued"):
            for run in _get(f"{base}/api/v2/dags/~/dagRuns?state={etat}&limit=100", jeton)["dag_runs"]:
                actifs.add(run["dag_id"])
        return sante, dags, actifs
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        return sante, None, set()  # santé connue, détail des DAG indisponible


def envoyer_mail(env: dict[str, str], sujet: str, texte: str) -> None:
    a = [x.strip() for x in env.get("ALERTE_EMAIL_A", "").split(",") if x.strip()]
    if not a:
        raise RuntimeError("ALERTE_EMAIL_A vide")
    conn = json.loads(env["AIRFLOW_CONN_SMTP_DEFAULT"])
    extra = conn.get("extra") or {}
    if isinstance(extra, str):
        extra = json.loads(extra)
    hote = conn["host"]
    port = int(conn.get("port") or 25)
    if hote == "mailpit":  # nom interne à Docker ; vu de Windows, Mailpit écoute sur 127.0.0.1:1025
        hote = "127.0.0.1"
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = sujet, extra.get("from_email") or conn.get("login"), ", ".join(a)
    msg.set_content(texte)
    ctx = ssl.create_default_context()
    if not extra.get("disable_ssl"):
        client = smtplib.SMTP_SSL(hote, port, timeout=20, context=ctx)
    else:
        client = smtplib.SMTP(hote, port, timeout=20)
        if not extra.get("disable_tls"):
            client.starttls(context=ctx)
    with client:
        if conn.get("login") and conn.get("password"):
            client.login(conn["login"], conn["password"])
        client.send_message(msg)


def notifier_windows(titre: str, texte: str) -> None:
    """Notification Windows (repli si le mail ne part pas). Sans module à installer."""
    t = lambda s: s.replace("'", "''").replace("&", "et").replace("<", "").replace(">", "")[:200]
    ps = (
        "[Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime]>$null;"
        "$x=[Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02);"
        f"$x.GetElementsByTagName('text')[0].AppendChild($x.CreateTextNode('{t(titre)}'))>$null;"
        f"$x.GetElementsByTagName('text')[1].AppendChild($x.CreateTextNode('{t(texte)}'))>$null;"
        "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('Veille Airflow').Show([Windows.UI.Notifications.ToastNotification]::new($x))"
    )
    subprocess.run(["powershell.exe", "-NoProfile", "-Command", ps], timeout=30,
                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0), check=False)


def journaliser(texte: str) -> None:
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    with JOURNAL.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {texte}\n")


def main(argv: list[str]) -> int:
    env = lire_env()
    maintenant = datetime.now(timezone.utc)
    pb = diagnostiquer(*observer(env), maintenant)
    if "--essai" in argv:
        print("Aucun problème." if not pb else "\n".join(f"- {v}" for v in pb.values()))
        return 0 if not pb else 1
    avant = json.loads(ETAT.read_text(encoding="utf-8")) if ETAT.exists() else {}
    nouveaux, resolus = comparer(avant, pb)
    if nouveaux or resolus:
        sujet, texte = composer(nouveaux, resolus, pb)
        journaliser(sujet)
        try:
            envoyer_mail(env, sujet, texte)
            journaliser("  mail envoyé")
        except Exception as e:  # noqa: BLE001 — repli visible, jamais d'arrêt silencieux
            journaliser(f"  mail impossible ({type(e).__name__}) : notification Windows")
            notifier_windows(sujet.replace("[Veille Airflow] ", "Airflow : "), next(iter((nouveaux or resolus).values())))
    ETAT.parent.mkdir(parents=True, exist_ok=True)
    ETAT.write_text(json.dumps(pb, ensure_ascii=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
