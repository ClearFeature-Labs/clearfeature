# Lightweight runtime image for the FinTech Feature Platform.
#
# One image serves the API and every worker: compose picks the command per service
# (uvicorn for the API, `python -m ...runner --forever` for workers). Runtime deps only
# (no dev extras), locked install, non-root user, editable project layout so the demo
# registry under /app/examples resolves exactly as in local dev.

FROM python:3.12-slim-bookworm

# uv (pinned) — the project's package manager; no pip usage below.
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependency layer first (cache-friendly): resolve from the committed lockfile only.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev \
        --extra api --extra storage --extra postgres --extra online --extra kafka

# Project layer: source + the demo registry + SQL schemas (referenced at runtime/docs).
COPY src ./src
COPY examples ./examples
COPY infra ./infra
COPY README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev \
        --extra api --extra storage --extra postgres --extra online --extra kafka

# Non-root runtime user.
RUN useradd --create-home --uid 10001 fsp && chown -R fsp:fsp /app
USER fsp

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000

# Default command: the API. Workers override `command` in compose.
CMD ["uvicorn", "fintech_feature_platform.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
