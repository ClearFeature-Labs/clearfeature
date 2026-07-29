"""Runbooks exist and are practical (metrics + commands + safe remediation)."""

from pathlib import Path

_RUNBOOKS = Path(__file__).resolve().parents[1] / "docs" / "runbooks"
_EXPECTED = (
    "dlq_triage.md",
    "replay_rerun.md",
    "shadow_diff.md",
    "propagation_wave_triage.md",
    "mode2_guarded_refresh.md",
)


def test_all_runbooks_exist():
    for name in _EXPECTED:
        assert (_RUNBOOKS / name).exists(), f"missing runbook {name}"


def test_runbooks_are_practical():
    for name in _EXPECTED:
        text = (_RUNBOOKS / name).read_text(encoding="utf-8").lower()
        assert "symptom" in text
        assert "metric" in text
        assert "what not to do" in text
        assert "escalat" in text


def test_mode2_runbook_mentions_copy_postgres_smoke():
    text = (_RUNBOOKS / "mode2_guarded_refresh.md").read_text(encoding="utf-8").lower()
    assert "copy" in text
    assert "postgres" in text
    assert "run_local_backend_smoke.sh" in text
