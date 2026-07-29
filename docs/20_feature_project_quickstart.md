# Feature Project Quickstart

How a customer team ships features from an **external, customer-owned repository** —
without editing or cloning platform source for everyday work. Two clearly separated paths:

- **Local development** — editable install, compatibility mode, fast loop.
- **Pilot / production** — immutable wheel, artifact-bound bundle, prebuilt worker image,
  `required` binding, worker restart to adopt a release.

## 1. Install the CLI and scaffold a project

```bash
pip install "fintech-feature-platform==<pinned-core-version>"   # brings the fsctl CLI
fsctl init --name my-features
cd my-features
```

The generated project is domain-neutral: a registry (`<pkg>/registry/features_v1.yaml`),
pure-Python feature functions (`<pkg>/features.py`), golden tests, `feature_project.yaml`
(CLI defaults incl. `requires_core`), a wheel-buildable `pyproject.toml`, a `.gitignore`,
and a vendor-neutral CI example (`ci/feature-project-ci.example.yml`).

## 2. Local development (editable install, compatibility mode)

```bash
uv pip install -e .        # editable install — local development ONLY
fsctl validate             # registry -> prints registry_definition_digest
fsctl test                 # golden cases through the real ComputeCore
```

Edit a **direct** feature by changing its function in `<pkg>/features.py` and its inputs
in the registry; edit a **dependent** feature by changing its function and its `deps`
entry. Re-run `fsctl test` after each change. Local serving may use the environment seams
(`FSP_REGISTRY_PATH`, `FSP_UDF_PROVIDER`) with `FSP_ENVIRONMENT=development` and
`FSP_ARTIFACT_BINDING=legacy-compatible` (unbound bundles allowed **only** here).

## 3. Pilot / production (immutable wheel → image → governed promotion)

```bash
# Build + isolate + golden-test the EXACT wheel, upload it, write the bound bundle.
# NOTE : an s3:// wheel store needs the storage extra —
#   pip install "fintech-feature-platform[storage]==<pinned-core-version>"
export FSP_WHEEL_STORE="s3://<bucket>/wheels"        # or feature_project.yaml wheel_store
fsctl publish --project . --bundle-store <bundle-store>
#   -> registry_definition_digest, feature_artifact_sha256, final_bundle_digest

# Generate the worker-image build context from the exact verified wheel:
fsctl image-context --project . --dir build-context \
    --base-image fsp-app:<pinned-core-version>
docker build -t <registry>/my-features-worker:<git-sha> build-context
docker push  <registry>/my-features-worker:<git-sha>
#   record the IMMUTABLE image digest (RepoDigests), not only the tag

# Governed promotion (approvers + shadow soak; see fsctl promote --help):
fsctl promote --bundle-digest <final_bundle_digest> --env <env> --to shadow ...
fsctl promote --bundle-digest <final_bundle_digest> --env <env> --to live \
    --profile <bank|energy> --approved-by <a> [--approved-by <b>] ...

# Operator deployment: point worker services at the new image digest and restart them.
```

Production worker environment (every computing service):

```text
FSP_ARTIFACT_BINDING=required
FSP_BUNDLE_STORE / FSP_POINTER_STORE / FSP_BUNDLE_ENV / FSP_BUNDLE_STAGE
(the image itself carries FSP_REGISTRY_PATH, FSP_UDF_PROVIDER,
 FSP_FEATURE_ARTIFACT_MANIFEST — set at image build by fsctl image-context)
```

Requesting features (online + batch) and lineage use the same public API as any
deployment; nothing feature-specific is added to the platform.

## 4. The build/runtime contract (what is guaranteed)

- **No runtime `pip install`; no automatic execution of downloaded wheels.** Workers never
  fetch code at request time.
- **The wheel is tested before publication** — installed into an isolated target and
  golden-tested; publication is refused otherwise.
- **The wheel is installed during the image build** (`uv pip install --no-deps --offline`),
  and **the original wheel is retained** in the image at
  `/opt/clearfeature/feature-artifacts/<sha256>/<filename>`.
- **`feature-artifact.json` is build-generated** by `fsctl image-context` from verified
  metadata (never handwritten), at `/etc/clearfeature/feature-artifact.json`; the image
  build re-verifies the wheel SHA (`sha256sum -c`) and **fails on mismatch**.
- **The runtime re-hashes the wheel**, checks the installed files against the wheel
  RECORD (byte-level), checks provider ownership, and checks `requires_core` against the
  installed core version — failing closed with structured categories.
- **Promotion changes `final_bundle_digest`** (definitions + exact artifact); the same
  definitions with a different wheel are a different release.
- **Running workers require a restart** to adopt a new bundle/image. There is **no hot
  reload**; the restarted worker verifies the new release before computing.

## 5. Dependency policy (Community MVP)

One trusted Feature Project (or one operator-approved compatible package set) per worker
image. Project dependencies are declared in `pyproject.toml`, reviewed, and resolved at
CI/image-build time — the MVP default installs the feature wheel with `--no-deps` into the
base image's environment (dependencies must already be present there). Projects with extra
dependencies need an explicit locked dependency build stage in their image build.
Shared-environment version conflicts are a **known Community limitation** (one env per
worker image); per-team isolated environments are out of MVP scope.

## 6. Provenance an operator keeps per release

`registry_definition_digest`, `feature_artifact_sha256`, `final_bundle_digest`, worker
**image digest** (immutable) + tag, package name + version, provider, pinned core version.
Chain of proof: image digest → in-image `feature-artifact.json` → wheel SHA →
`final_bundle_digest`.

## 7. Migration / compatibility

`FSP_REGISTRY_PATH`, `FSP_UDF_PROVIDER`, `legacy-compatible` development mode, and the
existing credit demo (synthetic; development compatibility mode; unbound bundle allowed
only there) all keep working. **New pilot/production Feature Projects should use the
artifact-bound image workflow above.** Compatibility mode is not appropriate for pilot or
production.
