"""Tests du chien de garde : python -m unittest scripts/test_veille.py (bibliothèque standard seule)."""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parent))
import veille_airflow as v  # noqa: E402

T = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
iso = lambda d: d.isoformat()


def sante(sched=T - timedelta(seconds=10), proc=T - timedelta(seconds=10), etat_sched="healthy"):
    return {
        "metadatabase": {"status": "healthy"},
        "scheduler": {"status": etat_sched, "latest_scheduler_heartbeat": iso(sched) if sched else None},
        "dag_processor": {"status": "healthy", "latest_dag_processor_heartbeat": iso(proc)},
        "triggerer": {"status": "unhealthy"},  # non déployé dans le lab : ignoré
    }


class Diagnostic(unittest.TestCase):
    def test_tout_va_bien(self):
        dags = [{"dag_id": "a", "is_paused": False, "next_dagrun_run_after": iso(T + timedelta(hours=2))}]
        self.assertEqual(v.diagnostiquer(sante(), dags, set(), T), {})

    def test_api_muette(self):
        self.assertIn("api", v.diagnostiquer(None, None, set(), T))

    def test_planificateur_en_panne(self):
        self.assertIn("scheduler", v.diagnostiquer(sante(etat_sched="unhealthy"), [], set(), T))

    def test_battement_trop_ancien(self):
        self.assertIn("scheduler", v.diagnostiquer(sante(sched=T - timedelta(minutes=5)), [], set(), T))

    def test_dag_en_retard(self):
        dags = [{"dag_id": "ventes", "is_paused": False, "next_dagrun_run_after": iso(T - timedelta(minutes=40))}]
        pb = v.diagnostiquer(sante(), dags, set(), T)
        self.assertIn("retard:ventes", pb)
        self.assertIn("40 min de retard", pb["retard:ventes"])

    def test_retard_ignore_si_en_pause_en_cours_ou_tolere(self):
        tard = iso(T - timedelta(minutes=40))
        dags = [
            {"dag_id": "pause", "is_paused": True, "next_dagrun_run_after": tard},
            {"dag_id": "encours", "is_paused": False, "next_dagrun_run_after": tard},
            {"dag_id": "juste", "is_paused": False, "next_dagrun_run_after": iso(T - timedelta(minutes=5))},
        ]
        self.assertEqual(v.diagnostiquer(sante(), dags, {"encours"}, T), {})


class Transitions(unittest.TestCase):
    def test_un_seul_mail_par_probleme(self):
        n, r = v.comparer({}, {"api": "x"})
        self.assertEqual((n, r), ({"api": "x"}, {}))
        n, r = v.comparer({"api": "x"}, {"api": "x"})  # vérification suivante : rien de neuf
        self.assertEqual((n, r), ({}, {}))

    def test_retablissement(self):
        n, r = v.comparer({"api": "x"}, {})
        sujet, texte = v.composer(n, r, {})
        self.assertIn("Rétabli", sujet)
        self.assertIn("Résolu", texte)

    def test_mail_de_probleme(self):
        sujet, texte = v.composer({"scheduler": "Le planificateur est en panne."}, {}, {"scheduler": "…"})
        self.assertTrue(sujet.startswith("[Veille Airflow] Problème"))
        self.assertIn("docker compose up -d", texte)


class Env(unittest.TestCase):
    def test_lecture_env_guillemets(self):
        with TemporaryDirectory() as d:
            p = Path(d) / ".env"
            p.write_text("# c\nA=1\nB='{\"x\":1}'\n", encoding="utf-8")
            self.assertEqual(v.lire_env(p), {"A": "1", "B": '{"x":1}'})


if __name__ == "__main__":
    unittest.main()
