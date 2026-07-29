#!/usr/bin/env python
"""Write the synthetic credit population to disk.

Usage:
    python examples/credit_decision_demo/generate_data.py \
        --clients 30000 --seed 42 --currency USD --output.demo-data/credit_decision

Outputs one ingestion-ready JSONL per source (each line is the platform's JsonlReportRow
shape), labels.csv, and manifest.json with generation metadata. Deterministic: same
arguments -> byte-identical files. Do NOT commit large generated datasets (.demo-data/ is
gitignored); the committed fixture lives in examples/credit_decision_demo/fixtures/.
"""

from __future__ import annotations

# ruff: noqa: E402  (CLI bootstrap: make the repo root importable for `python <script>.py`)
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse
import csv
import hashlib
import json
from pathlib import Path

from examples.credit_decision_demo.generator import (
    GENERATOR_VERSION,
    SOURCES,
    generate_population,
    ingestion_row,
)

FILES = {
    "application_request": "applications.jsonl",
    "tax_report": "tax_reports.jsonl",
    "credit_bureau_report": "credit_bureau_reports.jsonl",
    "telco_report": "telco_reports.jsonl",
    "socdem_report": "socdem_reports.jsonl",
}


def write_dataset(clients: int, seed: int, currency: str, output: Path) -> dict:
    population = generate_population(clients, seed=seed, currency_code=currency)
    output.mkdir(parents=True, exist_ok=True)

    hashes: dict[str, str] = {}
    for source_name, filename in FILES.items():
        path = output / filename
        with path.open("w", encoding="utf-8") as handle:
            for client in population:
                handle.write(json.dumps(ingestion_row(client, source_name),
                                        sort_keys=True) + "\n")
        hashes[filename] = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()

    with (output / "labels.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["user_id", "application_id", "label_default", "segment"])
        for client in population:
            writer.writerow([client.user_id, client.application_id,
                             client.label_default, client.segment])

    manifest = {
        "generator_version": GENERATOR_VERSION,
        "clients": clients,
        "seed": seed,
        "currency_code": currency,
        "sources": list(SOURCES),
        "files": hashes,
        "default_rate": round(sum(c.label_default for c in population) / clients, 4),
        "note": "SYNTHETIC data for a platform demo; not suitable for real lending.",
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the synthetic credit dataset.")
    parser.add_argument("--clients", type=int, default=30000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--output", default=".demo-data/credit_decision")
    args = parser.parse_args(argv)

    manifest = write_dataset(args.clients, args.seed, args.currency, Path(args.output))
    print(json.dumps({k: manifest[k] for k in
                      ("clients", "seed", "currency_code", "default_rate")}, indent=2))
    print(f"written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
