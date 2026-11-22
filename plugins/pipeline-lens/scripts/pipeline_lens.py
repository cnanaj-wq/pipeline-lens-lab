#!/usr/bin/env python3
"""Pipeline Lens : installer, démarrer, vérifier et arrêter le lab Airflow 3 + Pipeline Lens.

Bibliothèque standard uniquement. Windows, macOS et Linux.

    python pipeline_lens.py diagnostic
    python pipeline_lens.py installer  [--dossier DIR] [--depot URL]
    python pipeline_lens.py demarrer   [--dossier DIR] [--supervision]
    python pipeline_lens.py verifier   [--dossier DIR]
    python pipeline_lens.py arreter    [--dossier DIR] [--tout]
    python pipeline_lens.py greffer-qlik [--dossier DIR] [--qlik DIR] [--verifier-seulement]

Aucun secret n'est affiché : les mots de passe sont générés par scripts/generer-secrets.py
dans des fichiers exclus de Git, que l'utilisateur ouvre lui-même.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEPOT_LAB = "https://github.com/cnanaj-wq/pipeline-lens-lab.git"
DEPOT_QLIK = "https://github.com/cnanaj-wq/qlik-airflow-lab.git"
DOSSIER_DEFAUT = Path.home() / "pipeline-lens-lab"
PROJET = "pipeline-lens"  # nom de projet Docker Compose, identique avec ou sans la greffe Qlik
PORT_LENS = int(os.environ.get("LENS_PORT", "5176"))
PORTS = {"Airflow": 8085, "Pipeline Lens": PORT_LENS, "Mailpit (web)": 8025, "Mailpit (SMTP)": 1025}
NODE_MIN = (22, 12)
PYTHON_MIN = (3, 10)

WIN = os.name == "nt"


# --------------------------------------------------------------------------- utilitaires
def ok(msg: str) -> None:
    print(f"  [OK]  {msg}")


def ko(msg: str) -> None:
    print(f"  [!!]  {msg}")


def info(msg: str) -> None:
    print(f"  ...   {msg}")


def run(cmd: list[str], cwd: Path | None = None, check: bool = True, quiet: bool = False,
        env: dict | None = None) -> subprocess.CompletedProcess:
    exe = shutil.which(cmd[0]) or cmd[0]
    res = subprocess.run([exe, *cmd[1:]], cwd=cwd, env=env, text=True,
                         capture_output=quiet, encoding="utf-8", errors="replace")
    if check and res.returncode != 0:
        if quiet:
            sys.stderr.write((res.stdout or "") + (res.stderr or ""))
        raise SystemExit(f"Échec de la commande : {' '.join(cmd)} (code {res.returncode})")
    return res


def version(cmd: list[str]) -> str | None:
    if not shutil.which(cmd[0]):
        return None
    try:
        res = run(cmd, check=False, quiet=True)
    except OSError:
        return None
    return (res.stdout or res.stderr or "").strip().splitlines()[0] if res.returncode == 0 else None


def parse_version(txt: str) -> tuple[int, ...]:
    chiffres = "".join(c if (c.isdigit() or c == ".") else " " for c in txt).split()
    return tuple(int(x) for x in chiffres[0].split(".")[:3] if x) if chiffres else ()


def port_libre(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) != 0


def http_get(url: str, timeout: float = 5) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 (localhost uniquement)
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return 0, ""


def attendre(url: str, delai: int, libelle: str) -> bool:
    debut = time.time()
    while time.time() - debut < delai:
        code, _ = http_get(url, timeout=3)
        if code == 200:
            ok(f"{libelle} répond ({int(time.time() - debut)} s)")
            return True
        time.sleep(3)
    ko(f"{libelle} ne répond pas après {delai} s : {url}")
    return False


def compose(dossier: Path, *args: str, qlik: Path | None = None, supervision: bool = False,
            check: bool = True, quiet: bool = False) -> subprocess.CompletedProcess:
    fichiers = ["-f", str(dossier / "docker-compose.yaml")]
    if supervision:
        fichiers += ["-f", str(dossier / "docker-compose.monitoring.yaml")]
    envs = ["--env-file", str(dossier / ".env")]
    if qlik:
        fichiers += ["-f", str(qlik / "docker" / "compose.qlik.yaml")]
        envs += ["--env-file", str(qlik / ".env")]
    cmd = ["docker", "compose", "-p", PROJET, "--project-directory", str(dossier), *envs, *fichiers, *args]
    return run(cmd, cwd=dossier, check=check, quiet=quiet)


def dossier_lab(arg: str | None) -> Path:
    return Path(arg).expanduser().resolve() if arg else DOSSIER_DEFAUT


def exiger_lab(dossier: Path) -> None:
    if not (dossier / "docker-compose.yaml").exists() or not (dossier / "lens").exists():
        raise SystemExit(f"Lab introuvable dans {dossier}. Lancez d'abord : installer --dossier \"{dossier}\"")


def qlik_actif(dossier: Path) -> Path | None:
    marque = dossier / ".pipeline-lens" / "qlik-dir"
    if marque.exists():
        q = Path(marque.read_text(encoding="utf-8").strip())
        if (q / "docker" / "compose.qlik.yaml").exists() and (q / ".env").exists():
            return q
    return None


# --------------------------------------------------------------------------- commandes
def diagnostic(_: argparse.Namespace) -> int:
    print("Diagnostic du poste")
    erreurs = 0
    v = version(["git", "--version"])
    (ok if v else ko)(f"Git : {v or 'absent (https://git-scm.com)'}")
    erreurs += not v

    v = version(["docker", "--version"])
    if v:
        ok(f"Docker : {v}")
        c = version(["docker", "compose", "version"])
        (ok if c else ko)(f"Docker Compose v2 : {c or 'absent'}")
        erreurs += not c
        moteur = run(["docker", "info", "--format", "{{.ServerVersion}}"], check=False, quiet=True)
        if moteur.returncode == 0:
            ok(f"Moteur Docker démarré (serveur {moteur.stdout.strip()})")
        else:
            ko("Moteur Docker arrêté : lancez Docker Desktop (ou le service docker), puis relancez")
            erreurs += 1
    else:
        ko("Docker : absent (Docker Desktop : https://www.docker.com/products/docker-desktop)")
        erreurs += 1

    v = version(["node", "--version"])
    if v and parse_version(v) >= NODE_MIN:
        ok(f"Node.js : {v}")
    else:
        ko(f"Node.js {'.'.join(map(str, NODE_MIN))}+ requis (trouvé : {v or 'absent'}) : https://nodejs.org")
        erreurs += 1
    v = version(["npm", "--version"])
    (ok if v else ko)(f"npm : {v or 'absent'}")
    erreurs += not v

    if sys.version_info >= PYTHON_MIN:
        ok(f"Python : {sys.version.split()[0]}")
    else:
        ko(f"Python {'.'.join(map(str, PYTHON_MIN))}+ requis (trouvé : {sys.version.split()[0]})")
        erreurs += 1

    print("Ports locaux (127.0.0.1)")
    for nom, port in PORTS.items():
        if port_libre(port):
            ok(f"{port} libre ({nom})")
        else:
            info(f"{port} déjà utilisé ({nom}) : normal si le lab tourne déjà, sinon libérez-le")
    print("Diagnostic : " + ("tout est prêt." if not erreurs else f"{erreurs} prérequis manquant(s)."))
    return 1 if erreurs else 0


def installer(a: argparse.Namespace) -> int:
    dossier = dossier_lab(a.dossier)
    print(f"Installation du lab dans {dossier}")
    if not (dossier / "docker-compose.yaml").exists():
        if dossier.exists() and any(dossier.iterdir()):
            raise SystemExit(f"{dossier} existe et n'est pas le lab : choisissez un autre --dossier.")
        run(["git", "clone", "--depth", "1", a.depot, str(dossier)])
        ok("Dépôt cloné")
    else:
        ok("Dépôt déjà présent (aucun fichier écrasé)")

    for sous in ("logs", "plugins", "data", "config", ".pipeline-lens"):
        (dossier / sous).mkdir(exist_ok=True)
    ok("Dossiers logs, plugins, data, config prêts")

    run([sys.executable, str(dossier / "scripts" / "generer-secrets.py")], cwd=dossier)
    ok("Secrets locaux générés (.env, config/passwords.json) : aucun affiché")

    info("Téléchargement des images Docker (plusieurs minutes la première fois)")
    compose(dossier, "pull", "--quiet")
    ok("Images Docker prêtes")

    info("Installation des dépendances de Pipeline Lens (npm ci)")
    run(["npm", "ci", "--no-audit", "--no-fund"], cwd=dossier / "lens")
    ok("Dépendances installées")
    print(f"Installation terminée. Suite : demarrer --dossier \"{dossier}\"")
    return 0


def demarrer(a: argparse.Namespace) -> int:
    dossier = dossier_lab(a.dossier)
    exiger_lab(dossier)
    qlik = qlik_actif(dossier)
    print(f"Démarrage du lab ({dossier})" + (" avec la greffe Qlik" if qlik else ""))
    compose(dossier, "up", "-d", qlik=qlik, supervision=a.supervision)
    if not attendre("http://127.0.0.1:8085/api/v2/version", 300, "Airflow (http://localhost:8085)"):
        return 1

    etat = dossier / ".pipeline-lens"
    etat.mkdir(exist_ok=True)
    if http_get(f"http://127.0.0.1:{PORT_LENS}/", 2)[0] == 200:
        ok(f"Pipeline Lens tourne déjà sur http://localhost:{PORT_LENS}")
    else:
        if not port_libre(PORT_LENS):
            raise SystemExit(f"Le port {PORT_LENS} est pris par une autre application.")
        journal = open(etat / "lens.log", "w", encoding="utf-8")  # noqa: SIM115 (gardé ouvert par le processus)
        env = {**os.environ, "LENS_PORT": str(PORT_LENS)}
        npm = shutil.which("npm") or "npm"
        options: dict = {"cwd": dossier / "lens", "stdout": journal, "stderr": subprocess.STDOUT,
                         "stdin": subprocess.DEVNULL, "env": env}
        if WIN:
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        else:
            options["start_new_session"] = True
        proc = subprocess.Popen([npm, "run", "dev"], **options)
        (etat / "lens.pid").write_text(str(proc.pid), encoding="utf-8")
        if not attendre(f"http://127.0.0.1:{PORT_LENS}/", 90, f"Pipeline Lens (http://localhost:{PORT_LENS})"):
            ko(f"Journal : {etat / 'lens.log'}")
            return 1
    return verifier(a)


def verifier(a: argparse.Namespace) -> int:
    dossier = dossier_lab(a.dossier)
    print("Vérification")
    erreurs = 0
    code, corps = http_get("http://127.0.0.1:8085/api/v2/version")
    if code == 200:
        ok(f"Airflow {json.loads(corps).get('version', '?')} : http://localhost:8085")
    else:
        ko("Airflow ne répond pas sur http://localhost:8085")
        erreurs += 1
    code, _ = http_get(f"http://127.0.0.1:{PORT_LENS}/")
    if code == 200:
        ok(f"Pipeline Lens : http://localhost:{PORT_LENS}")
    else:
        ko(f"Pipeline Lens ne répond pas sur http://localhost:{PORT_LENS}")
        erreurs += 1
    code, corps = http_get(f"http://127.0.0.1:{PORT_LENS}/lens/api/dags?limit=100")
    if code == 200:
        dags = [d.get("dag_id") for d in json.loads(corps).get("dags", [])]
        ok(f"Proxy Lens authentifié auprès d'Airflow : {len(dags)} DAG ({', '.join(sorted(dags)) or 'aucun encore'})")
        if not dags:
            info("Les DAG apparaissent environ 30 s après le premier démarrage : relancez la vérification.")
    elif code:
        ko(f"Proxy Lens : HTTP {code} (identifiants .env / config/passwords.json désynchronisés ?)")
        erreurs += 1
    code, _ = http_get("http://127.0.0.1:8025/")
    (ok if code == 200 else info)("Mailpit (mails d'alerte) : http://localhost:8025" if code == 200
                                  else "Mailpit ne répond pas (alertes mail indisponibles)")
    if qlik_actif(dossier):
        ok("Greffe Qlik active : DAG qlik_sales_pipeline et qlik_supervision_dag (tag qlik)")
    print("Vérification : " + ("tout fonctionne." if not erreurs else f"{erreurs} problème(s)."))
    return 1 if erreurs else 0


def arreter(a: argparse.Namespace) -> int:
    dossier = dossier_lab(a.dossier)
    exiger_lab(dossier)
    pid_f = dossier / ".pipeline-lens" / "lens.pid"
    if pid_f.exists():
        pid = pid_f.read_text(encoding="utf-8").strip()
        if WIN:
            run(["taskkill", "/PID", pid, "/T", "/F"], check=False, quiet=True)
        else:
            try:
                os.killpg(int(pid), 15)
            except (ProcessLookupError, PermissionError, ValueError):
                pass
        pid_f.unlink()
        ok("Pipeline Lens arrêté")
    compose(dossier, "down" if a.tout else "stop", qlik=qlik_actif(dossier), check=False)
    ok("Conteneurs arrêtés" + (" et supprimés (les données restent dans le volume airflow-pg)" if a.tout else ""))
    return 0


def greffer_qlik(a: argparse.Namespace) -> int:
    dossier = dossier_lab(a.dossier)
    exiger_lab(dossier)
    qlik = Path(a.qlik).expanduser().resolve() if a.qlik else dossier.parent / "qlik-airflow-lab"
    print(f"Greffe du lab Qlik ({qlik}) sur Airflow ({dossier})")
    if not (qlik / "docker" / "compose.qlik.yaml").exists():
        run(["git", "clone", "--depth", "1", DEPOT_QLIK, str(qlik)])
        ok("qlik-airflow-lab cloné")
    env = qlik / ".env"
    if not env.exists():
        shutil.copy(qlik / ".env.example", env)
        ok(f"{env} créé depuis .env.example")
    # Seule ligne écrite par le script : le chemin du dépôt vu par Docker.
    lignes = env.read_text(encoding="utf-8").splitlines()
    chemin = qlik.as_posix()
    lignes = [f"QLIK_LAB_DIR={chemin}" if l.startswith("QLIK_LAB_DIR=") else l for l in lignes]
    if not any(l.startswith("QLIK_LAB_DIR=") for l in lignes):
        lignes.append(f"QLIK_LAB_DIR={chemin}")
    env.write_text("\n".join(lignes) + "\n", encoding="utf-8")

    valeurs = {l.split("=", 1)[0]: l.split("=", 1)[1].strip() for l in lignes if "=" in l and not l.startswith("#")}
    manquants = [k for k in ("QLIK_TENANT_URL", "QLIK_API_KEY", "QLIK_SUPERVISION_PASSWORD")
                 if not valeurs.get(k) or "votre-alias" in valeurs.get(k, "")]
    if manquants:
        ko(f"À remplir VOUS-MÊME dans {env} : {', '.join(manquants)}")
        info("Ne collez jamais la clé API dans une conversation. Relancez ensuite cette commande.")
        return 2

    res = compose(dossier, "config", "--no-interpolate", "--quiet", qlik=qlik, check=False, quiet=True)
    if res.returncode != 0:
        ko("Configuration fusionnée invalide")
        return 1
    ok("Configuration fusionnée valide (clé non affichée)")
    if a.verifier_seulement:
        return 0
    (dossier / ".pipeline-lens").mkdir(exist_ok=True)
    (dossier / ".pipeline-lens" / "qlik-dir").write_text(str(qlik), encoding="utf-8")
    compose(dossier, "up", "-d", qlik=qlik)
    ok("Greffe Qlik démarrée : les DAG tag « qlik » apparaissent dans ~30 s")
    info("Les 3 apps Qlik se créent dans VOTRE tenant avec : python -m qlik_lab deploy (voir docs/SCENARIOS.md, étape 0.4)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Lab Airflow 3 + Pipeline Lens")
    s = p.add_subparsers(dest="cmd", required=True)
    s.add_parser("diagnostic").set_defaults(f=diagnostic)
    i = s.add_parser("installer")
    i.add_argument("--dossier")
    i.add_argument("--depot", default=DEPOT_LAB)
    i.set_defaults(f=installer)
    d = s.add_parser("demarrer")
    d.add_argument("--dossier")
    d.add_argument("--supervision", action="store_true", help="ajoute Prometheus et Grafana")
    d.set_defaults(f=demarrer)
    v = s.add_parser("verifier")
    v.add_argument("--dossier")
    v.set_defaults(f=verifier)
    r = s.add_parser("arreter")
    r.add_argument("--dossier")
    r.add_argument("--tout", action="store_true", help="supprime aussi les conteneurs")
    r.set_defaults(f=arreter)
    q = s.add_parser("greffer-qlik")
    q.add_argument("--dossier")
    q.add_argument("--qlik")
    q.add_argument("--verifier-seulement", action="store_true")
    q.set_defaults(f=greffer_qlik)
    a = p.parse_args()
    if WIN:
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except AttributeError:
            pass
    return a.f(a)


if __name__ == "__main__":
    sys.exit(main())
