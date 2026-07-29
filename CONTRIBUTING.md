# Contributing to ClearFeature

Thank you for considering a contribution.

## Development environment

- Python **3.12** and [uv](https://docs.astral.sh/uv/).
- Install the supported development extras (note: the `kafka` extra is intentionally
  NOT part of the development baseline — several tests assert the graceful
  missing-extra behavior):

```bash
uv sync --extra dev --extra api --extra storage --extra postgres --extra online
```

## Tests and checks

```bash
uv run make verify          # ruff + the full Docker-free test suite
uv run python -m pytest tests/cli -q          # focused example
docker compose config -q    # compose validation
bash scripts/run_local_backend_smoke.sh       # full-stack smoke (Docker required)
```

A change is ready when `make verify` passes and any behavior change carries tests.

## Code style

- `ruff` (configured in `pyproject.toml`) is the linter/formatter gate.
- Prefer simple, explicit Python: type hints, small functions, boring names,
  invalid states made explicit, loud failures over silent fallbacks.
- Comments state constraints the code cannot express — not narration.

## Pull requests

- One focused change per PR with a clear description of behavior before/after.
- Include tests for new behavior and regression tests for fixes.
- Keep diffs minimal; avoid drive-by refactors.

## Backward compatibility

- Public contracts (HTTP API shapes, CLI JSON output, `feature_project.yaml`
  schema, registry YAML schema, bundle/artifact identity, metric names/labels,
  structured log event names, environment variables) are stable; breaking changes
  need explicit discussion first and a documented migration.
- New environment variables must define default, precedence, and failure mode.

## Security-sensitive changes

Changes touching authentication, promotion/approvals, artifact verification, PIT
eligibility, or the D9 write guard require: fail-closed behavior preserved, negative
tests (the attack must fail), and no secrets in logs/metrics/errors. If in doubt,
open a discussion before coding — see also `SECURITY.md`.
