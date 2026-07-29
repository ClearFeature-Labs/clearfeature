"""Runtime-contract consolidation guarantees.

Structural + behavioral proofs that the platform keeps exactly ONE implementation of
each consolidated seam — feature-view lookup, UDF provider loading, and the readiness
role domain — and that removed legacy pool settings stay removed with no aliases.
"""

import inspect

import pytest

from fintech_feature_platform.api.settings import Settings, load_settings
from fintech_feature_platform.fs_core.compute.udf_provider import (
    UdfProviderLoadError,
    load_udf_provider,
)

# --- ONE authoritative feature-view lookup ------------------------------

def test_no_production_duplicate_of_find_view_remains():
    """The (name, version) scan exists once on Registry (+ ComputeCore's intentionally
    richer version-mismatch variant); the five former copies are delegates."""
    import fintech_feature_platform.api.app as app
    import fintech_feature_platform.api.batch_worker as bw
    import fintech_feature_platform.api.model_score_writer as msw
    import fintech_feature_platform.api.online_worker as ow
    import fintech_feature_platform.fs_core.feature_store as fs

    for module in (app, bw, msw, ow, fs):
        source = inspect.getsource(module)
        assert "registry.find_view(" in source or "._registry.find_view(" in source, (
            module.__name__
        )
        # the old inline scan body must be gone:
        assert "and view.view_version == version:\n            return view" not in source, (
            module.__name__
        )


def test_find_view_behavior_unchanged():
    from fintech_feature_platform.api.backend import build_registry_and_udfs

    registry, _ = build_registry_and_udfs()
    view = registry.find_view("user_credit_risk", 1)
    assert view.name == "user_credit_risk" and view.view_version == 1
    with pytest.raises(ValueError, match="unknown view 'nope' version 9"):
        registry.find_view("nope", 9)
    with pytest.raises(ValueError, match="unknown view 'user_credit_risk' version 99"):
        registry.find_view("user_credit_risk", 99)  # wrong version -> same category


# --- ONE UDF provider loader --------------------------------------------

def test_cli_and_runtime_delegate_to_the_shared_loader():
    import fintech_feature_platform.api.backend as backend
    import fintech_feature_platform.cli.harness as harness

    assert "load_udf_provider" in inspect.getsource(harness.load_udfs)
    assert "load_udf_provider" in inspect.getsource(backend._load_udf_provider)
    # the duplicated import/getattr mechanics are gone from both callers:
    for func in (harness.load_udfs, backend._load_udf_provider):
        assert "importlib.import_module" not in inspect.getsource(func)


def test_shared_loader_spec_labels_preserve_both_contracts():
    with pytest.raises(UdfProviderLoadError, match="--udfs must be 'module.path"):
        load_udf_provider("no-colon", spec_label="--udfs")
    with pytest.raises(UdfProviderLoadError, match="FSP_UDF_PROVIDER must be 'module.path"):
        load_udf_provider("no-colon", spec_label="FSP_UDF_PROVIDER")


def test_runtime_provider_failures_stay_fail_closed(monkeypatch):
    monkeypatch.setenv("FSP_UDF_PROVIDER", "definitely.not.installed:build")
    monkeypatch.setenv("FSP_REGISTRY_PATH", "")
    from fintech_feature_platform.api.backend import build_registry_and_udfs

    with pytest.raises(ValueError, match="cannot be imported"):
        build_registry_and_udfs()


# --- readiness roles keyed to the canonical domain -----------------------

def test_readiness_roles_equal_canonical_worker_roles():
    from fintech_feature_platform.api.readiness import ROLE_DEPENDENCIES
    from fintech_feature_platform.fs_core.observability.catalog import WORKER_ROLES

    assert set(ROLE_DEPENDENCIES) == set(WORKER_ROLES)


def test_readiness_module_carries_the_import_time_drift_guard():
    import fintech_feature_platform.api.readiness as readiness

    source = inspect.getsource(readiness)
    assert "set(ROLE_DEPENDENCIES) != set(WORKER_ROLES)" in source
    assert "raise RuntimeError" in source


# --- dead pool settings removed -------------------------------------------

def test_removed_pool_settings_are_gone(monkeypatch):
    for field in ("online_db_pool_size", "batch_db_pool_size",
                  "metadata_db_pool_size", "offline_writer_db_pool_size"):
        assert not hasattr(Settings(), field), field
    # setting the old env vars has no effect and no alias was introduced:
    monkeypatch.setenv("FSP_ONLINE_DB_POOL_SIZE", "99")
    settings = load_settings()
    assert settings.db_pool_size == 4  # the ONE consumed pool setting, unchanged


def test_no_hidden_alias_for_removed_settings():
    import fintech_feature_platform.api.settings as settings_module

    source = inspect.getsource(settings_module)
    for name in ("FSP_ONLINE_DB_POOL_SIZE", "FSP_BATCH_DB_POOL_SIZE",
                 "FSP_METADATA_DB_POOL_SIZE", "FSP_OFFLINE_WRITER_DB_POOL_SIZE"):
        assert name not in source, name
