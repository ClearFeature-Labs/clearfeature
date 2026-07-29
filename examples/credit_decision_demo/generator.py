"""Deterministic synthetic credit population.

Generates internally-coherent applications + four external reports (tax, credit bureau,
telco, socdem) for N synthetic clients, plus a default label drawn from a hidden latent
risk score with noise. Everything is driven by ``random.Random(seed)`` — same seed, same
population, bit for bit. **All data is synthetic and unsuitable for real lending.**

Values are internationally neutral: ``user_id``/``application_id`` counters, configurable
ISO ``currency_code`` (default USD), region codes ``R01..R08``, timezone-aware UTC
timestamps, integer amounts. No names, no cities, no institutions, no sensitive
demographics.

Segments (correlated, not independent noise):

    LOW_RISK               stable income, clean bureau, low debt
    MEDIUM_RISK            moderate everything
    HIGH_RISK              low/uneven income, weak bureau, high debt appetite
    THIN_FILE              little credit + telco history -> uncertainty, not doom
    RECENT_DELINQUENCY     otherwise mid profile with a fresh 30-90 DPD event
    HIGH_INCOME_HIGH_DEBT  high income but heavily levered
    UNSTABLE_INCOME        decent average income, high variance, gap months

The label is P(default) = sigmoid(latent risk) with noise — deliberately not a copy of
any single report field.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

GENERATOR_VERSION = "credit-demo-gen-v1"

# Fixed demo clock: the application date every client applies around (determinism).
BASE_APPLICATION_TS = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)

SEGMENTS = (
    ("LOW_RISK", 0.30),
    ("MEDIUM_RISK", 0.25),
    ("HIGH_RISK", 0.12),
    ("THIN_FILE", 0.10),
    ("RECENT_DELINQUENCY", 0.08),
    ("HIGH_INCOME_HIGH_DEBT", 0.08),
    ("UNSTABLE_INCOME", 0.07),
)

_REGIONS = tuple(f"R{i:02d}" for i in range(1, 9))


@dataclass(frozen=True)
class Client:
    """One synthetic client: ids, the five payloads, segment, and the default label."""

    user_id: str
    application_id: str
    segment: str
    application: dict[str, Any]
    tax_report: dict[str, Any]
    credit_bureau_report: dict[str, Any]
    telco_report: dict[str, Any]
    socdem_report: dict[str, Any]
    label_default: int
    latent_risk: float

    def entity_key(self) -> dict[str, str]:
        return {"user_id": self.user_id, "application_id": self.application_id}


def _sigmoid(x: float) -> float:
    if x < -60:
        return 0.0
    if x > 60:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def _pick_segment(rng: random.Random) -> str:
    roll = rng.random()
    cumulative = 0.0
    for name, weight in SEGMENTS:
        cumulative += weight
        if roll < cumulative:
            return name
    return SEGMENTS[-1][0]


def _month_periods(end: datetime, months: int = 12) -> list[str]:
    """The ``months`` calendar periods ending the month BEFORE ``end`` (e.g. 2025-06..2026-05)."""
    year, month = end.year, end.month
    periods: list[str] = []
    for _ in range(months):
        month -= 1
        if month == 0:
            month, year = 12, year - 1
        periods.append(f"{year:04d}-{month:02d}")
    return list(reversed(periods))


def generate_client(index: int, rng: random.Random, currency_code: str) -> Client:
    """Generate one internally-coherent client (index is 1-based)."""
    segment = _pick_segment(rng)
    user_id = f"user_{index:06d}"
    application_id = f"app_{index:06d}"
    application_ts = BASE_APPLICATION_TS + timedelta(minutes=index % 1440)
    report_ts = application_ts - timedelta(days=1)

    # --- income profile (drives tax report + affordability) -----------------------
    base_income = {
        "LOW_RISK": rng.uniform(3500, 9000),
        "MEDIUM_RISK": rng.uniform(2200, 5000),
        "HIGH_RISK": rng.uniform(900, 2600),
        "THIN_FILE": rng.uniform(1200, 3200),
        "RECENT_DELINQUENCY": rng.uniform(1800, 4500),
        "HIGH_INCOME_HIGH_DEBT": rng.uniform(7000, 16000),
        "UNSTABLE_INCOME": rng.uniform(2500, 6000),
    }[segment]
    volatility = {
        "LOW_RISK": 0.05, "MEDIUM_RISK": 0.12, "HIGH_RISK": 0.25, "THIN_FILE": 0.15,
        "RECENT_DELINQUENCY": 0.15, "HIGH_INCOME_HIGH_DEBT": 0.10, "UNSTABLE_INCOME": 0.45,
    }[segment]
    gap_probability = {"UNSTABLE_INCOME": 0.25, "HIGH_RISK": 0.12, "THIN_FILE": 0.10}.get(
        segment, 0.02
    )
    trend = rng.uniform(-0.02, 0.03)  # slight monthly drift either way

    periods = []
    monthly_incomes: list[int] = []
    for month_index, period in enumerate(_month_periods(application_ts)):
        if rng.random() < gap_probability:
            income = 0
        else:
            drifted = base_income * (1 + trend) ** month_index
            income = max(0, int(drifted * (1 + rng.gauss(0, volatility))))
        monthly_incomes.append(income)
        periods.append({"period": period, "taxable_income": income})
    active_months = sum(1 for value in monthly_incomes if value > 0)
    avg_income = (sum(monthly_incomes) / 12) or 1.0

    # --- credit bureau (debt burden + history quality) ----------------------------
    debt_appetite = {
        "LOW_RISK": rng.uniform(0.05, 0.20), "MEDIUM_RISK": rng.uniform(0.15, 0.40),
        "HIGH_RISK": rng.uniform(0.35, 0.85), "THIN_FILE": rng.uniform(0.0, 0.15),
        "RECENT_DELINQUENCY": rng.uniform(0.20, 0.55),
        "HIGH_INCOME_HIGH_DEBT": rng.uniform(0.45, 0.90),
        "UNSTABLE_INCOME": rng.uniform(0.15, 0.45),
    }[segment]
    active_loans = 0 if segment == "THIN_FILE" and rng.random() < 0.7 else rng.randint(
        0 if segment == "THIN_FILE" else 1,
        {"LOW_RISK": 3, "MEDIUM_RISK": 4, "HIGH_RISK": 6, "THIN_FILE": 1,
         "RECENT_DELINQUENCY": 4, "HIGH_INCOME_HIGH_DEBT": 7, "UNSTABLE_INCOME": 4}[segment],
    )
    monthly_debt_payment = int(avg_income * debt_appetite) if active_loans else 0
    outstanding = monthly_debt_payment * rng.randint(8, 30)

    if segment == "RECENT_DELINQUENCY":
        max_dpd = rng.randint(30, 90)
        delinquency_days_ago = rng.randint(15, 150)
    elif segment == "HIGH_RISK" and rng.random() < 0.5:
        max_dpd = rng.randint(10, 60)
        delinquency_days_ago = rng.randint(60, 360)
    elif segment in ("MEDIUM_RISK", "UNSTABLE_INCOME") and rng.random() < 0.15:
        max_dpd = rng.randint(5, 29)
        delinquency_days_ago = rng.randint(120, 360)
    else:
        max_dpd = 0
        delinquency_days_ago = None
    last_delinquency_date = (
        (report_ts - timedelta(days=delinquency_days_ago)).date().isoformat()
        if delinquency_days_ago is not None
        else None
    )
    inquiries_30d = rng.randint(0, 1) if segment == "LOW_RISK" else rng.randint(
        0, {"HIGH_RISK": 6, "RECENT_DELINQUENCY": 4}.get(segment, 3)
    )
    # Bureau score 300..850, inversely tied to burden/dpd, uncertain for thin files.
    score = 720.0
    score -= 220 * debt_appetite
    score -= 1.2 * max_dpd
    score -= 15 * inquiries_30d
    score -= 40 if segment == "THIN_FILE" else 0
    score += 40 if segment == "LOW_RISK" else 0
    bureau_score = int(max(300, min(850, score + rng.gauss(0, 25))))

    # --- telco (weak stability signal) ---------------------------------------------
    sim_age_days = rng.randint(90, 700) if segment == "THIN_FILE" else rng.randint(400, 4000)
    active_days_30d = max(0, min(30, int(rng.gauss(26, 4)))) if segment != "HIGH_RISK" \
        else max(0, min(30, int(rng.gauss(20, 7))))
    avg_monthly_topup = max(5, int(avg_income * rng.uniform(0.004, 0.012)))
    telco_score = int(max(1, min(100, 45 + sim_age_days / 80 + active_days_30d
                                 - 25 * debt_appetite + rng.gauss(0, 8))))

    # --- socdem ----------------------------------------------------------------------
    age = rng.randint(21, 27) if segment == "THIN_FILE" else rng.randint(22, 64)
    region_code = rng.choice(_REGIONS)
    residence_tenure_months = rng.randint(2, 30) if segment == "THIN_FILE" else rng.randint(6, 240)

    # --- application (ask correlated with income; risky segments over-ask) -----------
    ask_multiple = {
        "LOW_RISK": rng.uniform(2, 6), "MEDIUM_RISK": rng.uniform(3, 8),
        "HIGH_RISK": rng.uniform(5, 14), "THIN_FILE": rng.uniform(2, 6),
        "RECENT_DELINQUENCY": rng.uniform(3, 9),
        "HIGH_INCOME_HIGH_DEBT": rng.uniform(4, 10), "UNSTABLE_INCOME": rng.uniform(3, 9),
    }[segment]
    requested_amount = max(500, int(avg_income * ask_multiple / 100) * 100)
    term_months = rng.choice((6, 12, 18, 24, 36, 48, 60))
    requested_monthly = requested_amount / term_months

    # --- hidden latent risk -> default label (never a copy of one field) -------------
    payment_to_income = (monthly_debt_payment + requested_monthly) / avg_income
    stability = (1.0 / (1.0 + volatility * 3)) * (active_months / 12)
    latent = (
        -2.1
        + 2.6 * min(payment_to_income, 2.0)
        + 1.3 * (max_dpd / 90)
        + (0.9 if delinquency_days_ago is not None and delinquency_days_ago <= 180 else 0.0)
        - 1.6 * stability
        - 1.1 * (bureau_score - 300) / 550
        + 0.5 * (inquiries_30d / 6)
        + 0.35 * (1 if active_loans == 0 else 0)      # thin-file uncertainty
        - 0.15 * (telco_score / 100)
        + rng.gauss(0, 0.45)                          # irreducible noise
    )
    label_default = 1 if rng.random() < _sigmoid(latent) else 0

    return Client(
        user_id=user_id,
        application_id=application_id,
        segment=segment,
        application={
            "user_id": user_id, "application_id": application_id,
            "application_ts": application_ts.isoformat(),
            "requested_amount": requested_amount, "term_months": term_months,
            "currency_code": currency_code,
        },
        tax_report={
            "user_id": user_id, "report_ts": report_ts.isoformat(),
            "currency_code": currency_code, "periods": periods,
        },
        credit_bureau_report={
            "user_id": user_id, "report_ts": report_ts.isoformat(),
            "bureau_score": bureau_score, "active_loans": active_loans,
            "total_outstanding_amount": outstanding,
            "total_monthly_payment": monthly_debt_payment,
            "max_dpd_12m": max_dpd, "last_delinquency_date": last_delinquency_date,
            "inquiries_30d": inquiries_30d, "currency_code": currency_code,
        },
        telco_report={
            "user_id": user_id, "report_ts": report_ts.isoformat(),
            "sim_age_days": sim_age_days, "active_days_30d": active_days_30d,
            "avg_monthly_topup": avg_monthly_topup, "telco_score": telco_score,
        },
        socdem_report={
            "user_id": user_id, "report_ts": report_ts.isoformat(),
            "age": age, "region_code": region_code,
            "residence_tenure_months": residence_tenure_months,
        },
        label_default=label_default,
        latent_risk=round(latent, 6),
    )


def generate_population(
    clients: int, seed: int = 42, currency_code: str = "USD"
) -> list[Client]:
    """Deterministic population: same (clients, seed, currency) -> same clients."""
    rng = random.Random(seed)
    return [generate_client(i, rng, currency_code) for i in range(1, clients + 1)]


# Source name -> (payload attribute, report_type). The application request is landed as a
# report too (it is an input source), but is not one of the four EXTERNAL reports.
SOURCES = {
    "application_request": ("application", "application_request"),
    "tax_report": ("tax_report", "tax_report"),
    "credit_bureau_report": ("credit_bureau_report", "credit_bureau_report"),
    "telco_report": ("telco_report", "telco_report"),
    "socdem_report": ("socdem_report", "socdem_report"),
}


def ingestion_row(client: Client, source_name: str) -> dict[str, Any]:
    """One JSONL ingestion row (the platform's ``JsonlReportRow`` shape) for a source."""
    attribute, _report_type = SOURCES[source_name]
    payload = getattr(client, attribute)
    event_ts = payload.get("application_ts") or payload["report_ts"]
    return {"entity_key": client.entity_key(), "event_ts": event_ts, "payload": payload}
