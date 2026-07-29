#!/usr/bin/env python
"""Documentation-only validation for docs/product/.

Checks, without any framework:
  1. every relative markdown link in docs/product/*.md resolves to a real file;
  2. every referenced repository script exists;
  3. every /v1/... endpoint mentioned exists in the executable endpoint policy;
  4. no banned marketing phrase appears in the product docs.

Exit 0 when clean; prints a JSON report.
"""

from __future__ import annotations

# ruff: noqa: E402  (CLI bootstrap: make the repo root importable for `python <script>.py`)
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json
import os
import re

REPO = Path(__file__).resolve().parents[1]
PRODUCT = REPO / "docs" / "product"

BANNED = ["revolutionary", "unlimited scalab", "enterprise-ready out of the box",
          "zero downtime", "exactly-once kafka", "fully production-ready"]

os.environ.setdefault("FSP_SECURITY_MODE", "disabled")
os.environ.setdefault("FSP_ENVIRONMENT", "development")
from examples.credit_decision_demo import model_service  # noqa: E402

from fintech_feature_platform.api import app as api_app  # noqa: E402

KNOWN_ENDPOINTS = set(api_app.ENDPOINT_POLICY) | set(model_service.ENDPOINT_POLICY)


def main() -> int:
    report = {"files": [], "broken_links": [], "missing_scripts": [],
              "unknown_endpoints": [], "banned_phrases": []}
    for path in sorted(PRODUCT.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        report["files"].append(path.name)

        for match in re.finditer(r"\]\(([^)#]+?)(#[^)]*)?\)", text):
            target = match.group(1).strip()
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            if not (path.parent / target).resolve().exists():
                report["broken_links"].append(f"{path.name}: {target}")

        for script in set(re.findall(r"scripts/[A-Za-z0-9_./-]+\.(?:sh|py)", text)):
            if not (REPO / script).exists():
                report["missing_scripts"].append(f"{path.name}: {script}")

        for endpoint in set(re.findall(r"/v1/[A-Za-z0-9\-_/{}]+", text)):
            candidate = endpoint.rstrip("/")
            # Normalize concrete ids in examples to their template forms.
            normalized = re.sub(r"/(freq_[a-z0-9]+|nope|[a-z0-9]{8,})$",
                                "/{request_id}", candidate)
            if candidate in KNOWN_ENDPOINTS or normalized in KNOWN_ENDPOINTS:
                continue
            if candidate + "/{request_id}" in KNOWN_ENDPOINTS:
                continue
            if any(candidate.startswith(k.split("{")[0].rstrip("/"))
                   and "{" in k for k in KNOWN_ENDPOINTS):
                continue
            report["unknown_endpoints"].append(f"{path.name}: {endpoint}")

        # The claims ledger QUOTES the banned wording in its "Never say" column —
        # it defines the ban and is exempt from it.
        if path.name != "claims_and_evidence.md":
            lowered = text.lower()
            for phrase in BANNED:
                if phrase in lowered:
                    report["banned_phrases"].append(f"{path.name}: {phrase}")

    problems = sum(len(report[k]) for k in
                   ("broken_links", "missing_scripts", "unknown_endpoints",
                    "banned_phrases"))
    report["status"] = "PASS" if problems == 0 else "FAIL"
    print(json.dumps(report, indent=2))
    return 0 if problems == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
