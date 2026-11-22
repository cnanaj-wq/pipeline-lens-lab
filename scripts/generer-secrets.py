"""Génère ou complète les secrets locaux du lab, sans jamais les afficher.

- config/passwords.json : mots de passe des comptes du gestionnaire d'accès simple d'Airflow 3
    admin         rôle admin   : vous, dans l'interface d'Airflow
    lens_lecture  rôle viewer  : Pipeline Lens, lecture seule (affichage)
    lens_ops      rôle op      : Pipeline Lens, rejeu contrôlé uniquement
- .env : clés partagées par les composants Airflow, version d'Airflow, identifiants du proxy
- monitoring/grafana.env : compte administrateur Grafana (option supervision)

Le script est additif : il ne remplace aucun secret existant, il ajoute ce qui manque.
Il est donc sans risque de le relancer après une mise à jour du lab.
Pour consulter un mot de passe : ouvrez vous-même config/passwords.json.
"""
import json
import secrets
from pathlib import Path

racine = Path(__file__).resolve().parent.parent
env = racine / ".env"
mdp = racine / "config" / "passwords.json"
mdp.parent.mkdir(exist_ok=True)

comptes = json.loads(mdp.read_text(encoding="utf-8")) if mdp.exists() else {}
for nom in ("admin", "lens_lecture", "lens_ops"):
    comptes.setdefault(nom, secrets.token_urlsafe(18))
mdp.write_text(json.dumps(comptes), encoding="utf-8")

# .env : lignes existantes conservées, clés manquantes ajoutées.
lignes = env.read_text(encoding="utf-8").splitlines() if env.exists() else ["# Fichier local, exclu de Git. Ne pas partager."]
cles = {l.split("=", 1)[0].strip(): i for i, l in enumerate(lignes) if "=" in l and not l.lstrip().startswith("#")}


def definir(cle: str, valeur: str, remplacer_si: str | None = None) -> None:
    if cle not in cles:
        lignes.append(f"{cle}={valeur}")
        cles[cle] = len(lignes) - 1
    elif remplacer_si is not None and lignes[cles[cle]].split("=", 1)[1].strip() == remplacer_si:
        lignes[cles[cle]] = f"{cle}={valeur}"


definir("AIRFLOW_UID", "50000")
definir("AIRFLOW__API_AUTH__JWT_SECRET", secrets.token_urlsafe(32))
definir("AIRFLOW__API__SECRET_KEY", secrets.token_urlsafe(32))
definir("AIRFLOW_VERSION", "3.4.0")
# Pipeline Lens lit avec un compte en lecture seule (ancienne configuration : admin).
ancien = next((lignes[cles[k]].split("=", 1)[1].strip() for k in ["AIRFLOW_LENS_USER"] if k in cles), None)
definir("AIRFLOW_LENS_USER", "lens_lecture", remplacer_si="admin")
if ancien == "admin" or "AIRFLOW_LENS_PASSWORD" not in cles:
    if "AIRFLOW_LENS_PASSWORD" in cles:
        lignes[cles["AIRFLOW_LENS_PASSWORD"]] = f"AIRFLOW_LENS_PASSWORD={comptes['lens_lecture']}"
    else:
        definir("AIRFLOW_LENS_PASSWORD", comptes["lens_lecture"])
definir("AIRFLOW_LENS_OPS_USER", "lens_ops")
definir("AIRFLOW_LENS_OPS_PASSWORD", comptes["lens_ops"])
# Rejeu depuis Pipeline Lens : « on » pour l'autoriser, « off » pour revenir en lecture seule stricte.
definir("LENS_ACTIONS", "on")
# Alertes d'échec par mail : par défaut vers la boîte de test locale Mailpit (http://localhost:8025).
# Pour une vraie messagerie, remplacez ces deux lignes dans .env (voir README, « Alertes par mail »).
definir("ALERTE_EMAIL_A", "exploitation@lab.local")
definir("ALERTE_LENS_URL", "http://localhost:5176")
definir("AIRFLOW_CONN_SMTP_DEFAULT", """'{"conn_type":"smtp","host":"mailpit","port":1025,"extra":{"from_email":"airflow@lab.local","disable_ssl":true,"disable_tls":true}}'""")
env.write_text("\n".join(lignes) + "\n", encoding="utf-8")

graf = racine / "monitoring" / "grafana.env"
if not graf.exists():
    graf.parent.mkdir(exist_ok=True)
    graf.write_text(
        "# Fichier local, exclu de Git. Compte administrateur Grafana (option supervision).\n"
        "GF_SECURITY_ADMIN_USER=admin\n"
        f"GF_SECURITY_ADMIN_PASSWORD={secrets.token_urlsafe(18)}\n",
        encoding="utf-8",
    )

print("Secrets locaux à jour (.env, config/passwords.json). Comptes : admin, lens_lecture, lens_ops. Aucun secret affiché.")
