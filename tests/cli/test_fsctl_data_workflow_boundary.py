"""Import boundary for the fsctl data-workflow verbs.

The three commands are public-API adapters ONLY. They must work from a base
install (stdlib HTTP; no fastapi/httpx) and must never import backend, store,
worker, compute, or example-orchestration code.
"""

import ast
import os
import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
DATA_WORKFLOW = SRC / "fintech_feature_platform" / "cli" / "data_workflow.py"
FSCTL = SRC / "fintech_feature_platform" / "cli" / "fsctl.py"

# Everything the adapter module is allowed to import.
ALLOWED_PREFIXES = (
    "__future__", "argparse", "hashlib", "json", "os", "pathlib", "tempfile",
    "time", "typing", "urllib",
)

FORBIDDEN_MARKERS = (
    "fintech_feature_platform.api",
    "fintech_feature_platform.fs_core",
    "local_backend",
    "FeatureStore",
    "ComputeCore",
    "compute_dependent_from_offline",
    "psycopg",
    "minio",
    "redis",
    "confluent_kafka",
    "examples.",
)


def _imports(tree: ast.Module, module_level_only: bool = False):
    nodes = tree.body if module_level_only else list(ast.walk(tree))
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom):
            yield node.module or ""


def test_data_workflow_imports_stdlib_only():
    tree = ast.parse(DATA_WORKFLOW.read_text(encoding="utf-8"))
    for name in _imports(tree):
        assert name.startswith(ALLOWED_PREFIXES), f"forbidden import in data_workflow: {name}"


def test_data_workflow_source_has_no_platform_internals():
    source = DATA_WORKFLOW.read_text(encoding="utf-8")
    for marker in FORBIDDEN_MARKERS:
        assert marker not in source, f"forbidden marker in data_workflow: {marker}"


def test_fsctl_imports_data_workflow_lazily_only():
    # No module-level import: `fsctl --help` and the existing verbs must not pay
    # for (or depend on) the data-workflow module.
    tree = ast.parse(FSCTL.read_text(encoding="utf-8"))
    module_level = list(_imports(tree, module_level_only=True))
    assert not any("data_workflow" in name for name in module_level)
    assert any(
        "data_workflow" in name for name in _imports(tree)
    ), "fsctl no longer wires the data-workflow verbs"


def test_importing_data_workflow_pulls_no_api_or_http_frameworks():
    code = (
        "import sys\n"
        "import fintech_feature_platform.cli.data_workflow\n"
        "bad = [m for m in sys.modules if m.startswith('fintech_feature_platform.api')\n"
        "       or m.startswith('fintech_feature_platform.fs_core')\n"
        "       or m in ('fastapi', 'httpx', 'requests')]\n"
        "assert not bad, bad\n"
    )
    env = dict(os.environ, PYTHONPATH=str(SRC))
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env
    )
    assert proc.returncode == 0, proc.stderr
