"""Contrôle des DAG : chargement sans erreur et conventions du lab.

Lancé par GitHub Actions (.github/workflows/dags.yml) ou en local :
    docker compose run --rm --no-deps -v "C:/airflow/tests:/opt/airflow/tests" --entrypoint "" airflow-scheduler \
        bash -c "pip install -q pytest && python -m pytest /opt/airflow/tests -q -p no:cacheprovider"
"""
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS))


def test_aucune_erreur_import(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, f"Erreurs d'import : {dagbag.import_errors}"


def test_au_moins_un_dag(dagbag: DagBag) -> None:
    assert dagbag.dags, "Aucun DAG trouvé dans dags/"


@pytest.mark.parametrize("etape", ["extract", "transform", "load"])
def test_groupes_etl(dagbag: DagBag, etape: str) -> None:
    """Convention Pipeline Lens : chaque DAG range ses tâches dans extract / transform / load."""
    for dag_id, dag in dagbag.dags.items():
        assert any(t.task_id.startswith(f"{etape}.") for t in dag.tasks), f"{dag_id} : aucun groupe « {etape} »"


def test_alerte_echec(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        for t in dag.tasks:
            assert t.on_failure_callback, f"{dag_id}.{t.task_id} : pas d'alerte d'échec"


def test_rejouable_documente(dagbag: DagBag) -> None:
    """Un DAG proposé au rejeu (tag « rejouable ») doit justifier son idempotence."""
    for dag_id, dag in dagbag.dags.items():
        if "rejouable" in dag.tags:
            assert dag.doc_md and "dempoten" in dag.doc_md, f"{dag_id} : idempotence non documentée"


# --- Alertes d'échec par mail -------------------------------------------------------------
import sys
from types import SimpleNamespace

sys.path.insert(0, str(DAGS))
import product_etl_dag as etl  # noqa: E402


@pytest.fixture(autouse=True)
def _journal_isole(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Les tests n'écrivent jamais dans le vrai journal des incidents."""
    monkeypatch.setattr(etl, "INCIDENTS", tmp_path / "incidents-test.jsonl")


def _contexte(exception: Exception | None = ValueError("Prix illisibles <ligne 3>")) -> dict:
    ti = SimpleNamespace(dag_id="product_etl_dag", task_id="transform.clean_prices", try_number=1, max_tries=0)
    return {"task_instance": ti, "run_id": "manual__2026-11-11T10:00:00+00:00", "exception": exception}


def test_mail_alerte_contenu() -> None:
    sujet, corps = etl.composer_alerte(_contexte(), "http://localhost:5176")
    assert sujet == "[Airflow] Échec : product_etl_dag / transform.clean_prices"
    assert "1 sur 1" in corps
    assert "Prix illisibles &lt;ligne 3&gt;" in corps, "l'erreur doit être échappée"
    assert "?dag=product_etl_dag&amp;run=manual__2026-11-11T10%3A00%3A00%2B00%3A00" in corps


class _FauxSmtp:
    envois: list[dict] = []
    erreur: Exception | None = None

    def __init__(self, smtp_conn_id: str) -> None:
        assert smtp_conn_id == "smtp_default"

    def __enter__(self):
        if _FauxSmtp.erreur:
            raise _FauxSmtp.erreur
        return self

    def __exit__(self, *a) -> None:
        pass

    def send_email_smtp(self, **kw) -> None:
        _FauxSmtp.envois.append(kw)


@pytest.fixture
def faux_smtp(monkeypatch: pytest.MonkeyPatch):
    import airflow.providers.smtp.hooks.smtp as mod

    _FauxSmtp.envois, _FauxSmtp.erreur = [], None
    monkeypatch.setattr(mod, "SmtpHook", _FauxSmtp)
    return _FauxSmtp


def test_mail_envoye_si_destinataire(monkeypatch: pytest.MonkeyPatch, faux_smtp) -> None:
    monkeypatch.setenv("ALERTE_EMAIL_A", "a@lab.local, b@lab.local")
    etl.alerte_echec(_contexte())
    assert len(faux_smtp.envois) == 1
    assert faux_smtp.envois[0]["to"] == ["a@lab.local", "b@lab.local"]


def test_pas_de_mail_sans_destinataire(monkeypatch: pytest.MonkeyPatch, faux_smtp) -> None:
    monkeypatch.delenv("ALERTE_EMAIL_A", raising=False)
    etl.alerte_echec(_contexte())
    assert faux_smtp.envois == []


def test_panne_smtp_sans_exception(monkeypatch: pytest.MonkeyPatch, faux_smtp) -> None:
    monkeypatch.setenv("ALERTE_EMAIL_A", "a@lab.local")
    faux_smtp.erreur = ConnectionRefusedError("serveur mail arrêté")
    etl.alerte_echec(_contexte())  # ne doit rien lever


# --- Reprise sur incident ---------------------------------------------------------------
def test_reprise_dags_rejouables(dagbag: DagBag) -> None:
    """Un DAG idempotent (tag « rejouable ») réessaie seul, avec un délai croissant."""
    for dag_id, dag in dagbag.dags.items():
        for t in dag.tasks:
            assert t.on_retry_callback, f"{dag_id}.{t.task_id} : pas d'alerte de nouvel essai"
            if "rejouable" in dag.tags:
                assert t.retries >= 2, f"{dag_id}.{t.task_id} : pas de reprise automatique"
                assert t.retry_exponential_backoff > 1


def test_mail_nouvel_essai(monkeypatch: pytest.MonkeyPatch, faux_smtp) -> None:
    monkeypatch.setenv("ALERTE_EMAIL_A", "a@lab.local")
    monkeypatch.delenv("ALERTE_EMAIL_ESSAIS", raising=False)
    etl.alerte_essai(_contexte())
    assert faux_smtp.envois[0]["subject"].startswith("[Airflow] Incident en cours, nouvel essai")
    assert "réessaie automatiquement" in faux_smtp.envois[0]["html_content"]


def test_mail_nouvel_essai_desactivable(monkeypatch: pytest.MonkeyPatch, faux_smtp) -> None:
    monkeypatch.setenv("ALERTE_EMAIL_A", "a@lab.local")
    monkeypatch.setenv("ALERTE_EMAIL_ESSAIS", "off")
    etl.alerte_essai(_contexte())
    assert faux_smtp.envois == []


# --- Historique des incidents (événements) ----------------------------------------------
def test_incident_historise_puis_clos(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, faux_smtp) -> None:
    import json

    monkeypatch.setattr(etl, "INCIDENTS", tmp_path / "incidents.jsonl")
    monkeypatch.setenv("ALERTE_EMAIL_A", "a@lab.local")
    ctx = _contexte(ConnectionError("source indisponible"))
    etl.alerte_essai(ctx)
    ouvert = etl.incident_ouvert("product_etl_dag", "transform.clean_prices", ctx["run_id"])
    assert ouvert and ouvert["raison"] == "ConnectionError : source indisponible"
    ctx["task_instance"].try_number = 2
    etl.signaler_reprise(ctx)
    evts = [json.loads(l) for l in (tmp_path / "incidents.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [e["evenement"] for e in evts] == ["echec_essai", "reprise"]
    assert evts[1]["debut"] == evts[0]["ts"] and evts[1]["duree_s"] >= 0
    assert etl.incident_ouvert("product_etl_dag", "transform.clean_prices", ctx["run_id"]) is None
    assert [m["subject"].split(" :")[0] for m in faux_smtp.envois] == ["[Airflow] Incident en cours, nouvel essai", "[Airflow] Rétabli"]


def test_succes_sans_incident_silencieux(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, faux_smtp) -> None:
    monkeypatch.setattr(etl, "INCIDENTS", tmp_path / "incidents.jsonl")
    monkeypatch.setenv("ALERTE_EMAIL_A", "a@lab.local")
    etl.signaler_reprise(_contexte(None))
    assert faux_smtp.envois == [] and not (tmp_path / "incidents.jsonl").exists()


def test_historisation_ne_bloque_jamais(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "bloque").write_text("x")
    monkeypatch.setattr(etl, "INCIDENTS", tmp_path / "bloque" / "incidents.jsonl")  # dossier impossible
    etl.historiser("echec", _contexte(), raison="x")  # ne lève rien
