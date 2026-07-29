#!/usr/bin/env bash
set -euo pipefail

echo "==> Python version"
python --version

echo "==> Checking package import"
python - <<'PY'
import fintech_feature_platform
print("import ok:", fintech_feature_platform.__name__)
PY

echo "==> Ruff check"
if python -c "import ruff" >/dev/null 2>&1; then
  python -m ruff check .
else
  echo "ruff is not installed; skipping. Install dev dependencies to enable linting."
fi

echo "==> Pytest"
if python -c "import pytest" >/dev/null 2>&1; then
  python -m pytest -q
else
  echo "pytest is not installed; skipping. Install dev dependencies to enable tests."
fi

echo "==> Verification script completed"
