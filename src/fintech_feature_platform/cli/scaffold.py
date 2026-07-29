"""Domain-neutral Feature Project scaffold for ``fsctl init``.

The scaffold is deliberately NOT credit-specific: it models a generic ``customer`` entity
with an ``event`` source, one windowed count feature (F1) and one dependent score feature
(F2). Templates are plain Python strings substituted with the project ``name`` and Python
``package`` so no package-data wiring is needed — the module ships as ordinary source.
"""

from __future__ import annotations

from pathlib import Path

_FEATURES_PY = '''\
"""Feature functions for {name} — pure ``(sources, deps) -> value``; public core only."""

from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry


def event_count_7d(sources, deps):
    """F1: a windowed event count read straight from the source payload."""
    return int(sources["event"]["count_7d"])


def activity_score(sources, deps):
    """F2: a bounded score derived from F1 (declares a dependency on event_count_7d)."""
    return round(min(deps["event_count_7d"] / 10.0, 1.0), 6)


_UDFS = {{
    "udf.{package}.event_count_7d": event_count_7d,
    "udf.{package}.activity_score": activity_score,
}}


def build_udfs() -> UdfRegistry:
    """The UDF provider entry point referenced by feature_project.yaml / --udfs."""
    return UdfRegistry(dict(_UDFS))
'''

_REGISTRY_YAML = '''\
registry_version: "{package}-v1"
entities:
  customer:
    key_fields: ["customer_id"]
sources:
  event:
    type: "raw_report"
    report_type: "event"
    ts_field: "event_ts"
feature_views:
  activity:
    entity: "customer"
    key_fields: ["customer_id"]
    view_version: 1
    owner: "{package}_team"
    status: "active"
    features:
      event_count_7d:
        kind: "udf"
        feature_version: 1
        udf: "udf.{package}.event_count_7d"
        inputs: ["event"]
        dtype: "int"
        status: "live"
      activity_score:
        kind: "udf"
        feature_version: 1
        udf: "udf.{package}.activity_score"
        inputs: []
        deps:
          - {{feature: event_count_7d, version: 1}}
        dtype: "float"
        status: "live"
'''

_GOLDEN_YAML = '''\
cases:
  - name: count_basic
    feature: event_count_7d
    sources: {{event: {{count_7d: 42, event_ts: "2026-01-01T00:00:00+00:00"}}}}
    expected: {{value: 42}}
  - name: score_dependent
    feature: activity_score
    deps: {{event_count_7d: 42}}
    expected: {{value: 1.0}}
'''

_ENTITY_JSON = '{"sources": {"event": {"count_7d": 42, "event_ts": "2026-01-01T00:00:00+00:00"}}}\n'

_MANIFEST_YAML = '''\
# Feature Project manifest ({name}). Defaults for fsctl; NEVER put secrets or approvers here.
project: {name}
requires_core: ">=0.1,<0.2"
package: {package}
registry: {package}/registry/features_v1.yaml
udf_provider: {package}.features:build_udfs
tests: {package}/tests/golden.yaml
bundle_store: ./.bundles
pointer_store: ./.pointers
wheel_store: ./.wheels
'''

_PYPROJECT_TOML = '''\
[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = []

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["{package}"]
'''

_README_MD = '''\
# {name}

An external Feature Project for the FinTech Feature Platform.

## Local development (editable install, compatibility mode)

```bash
uv pip install -e .            # editable install (local dev ONLY)
fsctl validate                 # registry defaults from feature_project.yaml
fsctl test                     # golden cases through the real ComputeCore
```

## Pilot / production (immutable wheel + prebuilt worker image)

```bash
fsctl publish --project .      # builds + isolates + golden-tests the EXACT wheel,
                               # uploads it, writes the artifact-bound bundle
fsctl image-context --project . --dir build-context \\
    --base-image fsp-app:<pinned-core-version>
docker build -t <registry>/{name}-worker:<git-sha> build-context
```

Then promote the bundle (governed: approvers + shadow soak) and have the operator
deploy/restart workers on the new image. Running workers do **not** hot-swap; the
restarted worker verifies the promoted bundle + wheel before computing. Production
deployments set `FSP_ARTIFACT_BINDING=required`. A full pipeline example is in
`ci/feature-project-ci.example.yml` (vendor-neutral).

Nothing here is platform source. Feature code is pure `(sources, deps) -> value` Python in
`{package}/features.py`; definitions live in `{package}/registry/features_v1.yaml`. Do not
put credentials or datasets in this repository.
'''


_CI_EXAMPLE = '''\
# Feature Project CI pipeline — vendor-neutral EXAMPLE ({name}).
#
# Adapt the stage list to your CI system (GitHub Actions, GitLab CI, Jenkins, ...);
# each stage is plain shell over the public fsctl CLI. Placeholders only — supply
# credentials via your CI secret store, never in this file or the repository.
#
# Stage separation (deliberate):
#   1) Data Scientist validation      (validate, test)
#   2) Artifact publication           (build_wheel, test_exact_wheel, publish)
#   3) Image construction             (image, verify_image)
#   4) Governed promotion             (promote_shadow; promote_live is MANUAL)
#   5) Operator deployment            (deploy: restart workers on the new image)

stages:
  validate:
    # DS validation: manifest + registry through the installed CLI (pinned version).
    - pip install "fintech-feature-platform==<PINNED_CORE_VERSION>"
    - fsctl validate

  test:
    - fsctl test

  build_wheel:
    - uv build --wheel --out-dir dist .
    - "sha256sum dist/*.whl   # record feature_artifact_sha256 in the CI log"

  test_exact_wheel:
    # publish --project rebuilds/uses the exact wheel, installs it in isolation,
    # golden-tests THAT wheel, uploads it, then writes the artifact-bound bundle.
    # NOTE: an s3:// wheel store needs the storage extra:
    #   pip install "fintech-feature-platform[storage]==<PINNED_CORE_VERSION>"
    - export FSP_WHEEL_STORE="s3://<YOUR_BUCKET>/wheels"
    - fsctl publish --project . --bundle-store <BUNDLE_STORE_PATH_OR_MOUNT>
    # Retain from the JSON output: registry_definition_digest,
    # feature_artifact_sha256, final_bundle_digest.

  image:
    - fsctl image-context --project . --dir build-context
        --base-image "clearfeature/fsp-app:<PINNED_CORE_VERSION>"
    - docker build -t "<REGISTRY>/<PROJECT>-worker:<GIT_SHA>" build-context
    - docker push "<REGISTRY>/<PROJECT>-worker:<GIT_SHA>"
    # Record the IMMUTABLE image digest, not only the tag:
    - 'docker inspect --format "{{{{index .RepoDigests 0}}}}"
        "<REGISTRY>/<PROJECT>-worker:<GIT_SHA>"'

  verify_image:
    # The manifest inside the image must match the published artifact.
    - docker run --rm "<REGISTRY>/<PROJECT>-worker:<GIT_SHA>"
        cat /etc/clearfeature/feature-artifact.json

  promote_shadow:
    # Optional automatic shadow promotion of the artifact-bound bundle.
    - fsctl promote --bundle-store <BUNDLE_STORE> --pointer-store <POINTER_STORE>
        --bundle-digest <FINAL_BUNDLE_DIGEST> --env <ENV> --to shadow
        --actor "$CI_ACTOR" --reason "CI shadow of $GIT_SHA"

  promote_live:
    # MANUAL, governed stage — never automatic. Requires the approval profile's
    # unique approvers and shadow soak (see fsctl promote --help).
    - echo "manual stage: fsctl promote --to live ... --profile <bank|energy> --approved-by ..."

  deploy:
    # Operator deployment: point the worker services at the new image digest and
    # restart them. Running workers do NOT hot-swap; the restarted workers verify
    # the promoted final_bundle_digest + artifact before computing.
    - echo "operator stage: update image digest in deployment config; restart workers"

# Provenance an operator retains per release (all safe identifiers, no secrets):
#   registry_definition_digest, feature_artifact_sha256, final_bundle_digest,
#   worker image DIGEST (immutable) + tag, package name, package version, provider,
#   pinned ClearFeature core version.
'''

_GITIGNORE = '''\
# Python environments and build output — never commit these.
.venv/
venv/
dist/
build/
.wheels/
.bundles/
.pointers/

# Caches and package metadata.
__pycache__/
*.py[cod]
.pytest_cache/
.ruff_cache/
.mypy_cache/
*.egg-info/
*.egg
'''


def render_scaffold(name: str, package: str) -> dict[str, str]:
    """Return the scaffold as ``{relative_path: file_contents}`` (domain-neutral)."""
    ctx = {"name": name, "package": package}
    return {
        "feature_project.yaml": _MANIFEST_YAML.format(**ctx),
        "pyproject.toml": _PYPROJECT_TOML.format(**ctx),
        "README.md": _README_MD.format(**ctx),
        ".gitignore": _GITIGNORE,
        "ci/feature-project-ci.example.yml": _CI_EXAMPLE.format(**ctx),
        f"{package}/__init__.py": "",
        f"{package}/features.py": _FEATURES_PY.format(**ctx),
        f"{package}/registry/features_v1.yaml": _REGISTRY_YAML.format(**ctx),
        f"{package}/tests/golden.yaml": _GOLDEN_YAML.format(**ctx),
        f"{package}/tests/entity.json": _ENTITY_JSON,
    }


def package_name_from(name: str) -> str:
    """Derive an import-safe package name from a project name (dashes -> underscores)."""
    return name.strip().replace("-", "_").replace(" ", "_")


def write_scaffold(target: Path, name: str, package: str) -> list[str]:
    """Write the scaffold under ``target`` (must be empty/new). Returns written rel-paths."""
    target = Path(target)
    if target.exists() and any(target.iterdir()):
        raise FileExistsError(f"target directory {target} is not empty")
    written: list[str] = []
    for rel, content in render_scaffold(name, package).items():
        path = target / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written.append(rel)
    return sorted(written)
