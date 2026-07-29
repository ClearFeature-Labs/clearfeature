"""Credit-decision demo feature UDFs + registry/UDF provider.

The F1 UDFs each read exactly ONE source payload (required by manifest-scoped batch jobs,
which carry one source_ref per item). The F2 UDFs read only feature dependencies and are
computed by the platform's real DAG (topological ComputeCore / recompute-from-offline) —
never duplicated in demo scripts. Derived floats are rounded to 6dp as part of the
contract, so goldens are stable and readable.

Provider (the FSP_UDF_PROVIDER / fsctl --udfs contract):

    FSP_REGISTRY_PATH=examples/credit_decision_demo/registry/credit_decision_v1.yaml
    FSP_UDF_PROVIDER=examples.credit_decision_demo.features:build_registry_and_udfs
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.registry.loader import load_registry_file

REGISTRY_PATH = Path(__file__).resolve().parent / "registry" / "credit_decision_v1.yaml"

# Sentinel for "no delinquency on record" (large but finite; comparable in UDFs/models).
NO_DELINQUENCY_DAYS = 9999


def _round6(value: float) -> float:
    return round(float(value), 6)


def _incomes(sources, months: int) -> list[int]:
    periods = sorted(sources["tax_report"]["periods"], key=lambda p: p["period"])
    return [p["taxable_income"] for p in periods[-months:]]


# --- F1: application_request ---------------------------------------------------

def requested_amount(sources, deps):
    return sources["application_request"]["requested_amount"]


def term_months(sources, deps):
    return sources["application_request"]["term_months"]


# --- F1: tax_income --------------------------------------------------------------

def income_avg_3m(sources, deps):
    return _round6(sum(_incomes(sources, 3)) / 3)


def income_avg_6m(sources, deps):
    return _round6(sum(_incomes(sources, 6)) / 6)


def income_avg_12m(sources, deps):
    return _round6(sum(_incomes(sources, 12)) / 12)


def income_std_12m(sources, deps):
    values = _incomes(sources, 12)
    mean = sum(values) / len(values)
    return _round6((sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5)


def income_trend_6m(sources, deps):
    """(mean of last 3 months - mean of the 3 before) / overall 6m mean; 0 if no income."""
    values = _incomes(sources, 6)
    older, recent = values[:3], values[3:]
    mean6 = sum(values) / 6
    if mean6 <= 0:
        return 0.0
    return _round6((sum(recent) / 3 - sum(older) / 3) / mean6)


def months_with_income_12m(sources, deps):
    return sum(1 for v in _incomes(sources, 12) if v > 0)


# --- F1: bureau_risk ---------------------------------------------------------------

def bureau_score(sources, deps):
    return sources["credit_bureau_report"]["bureau_score"]


def active_loans_count(sources, deps):
    return sources["credit_bureau_report"]["active_loans"]


def total_outstanding_amount(sources, deps):
    return sources["credit_bureau_report"]["total_outstanding_amount"]


def total_monthly_debt_payment(sources, deps):
    return sources["credit_bureau_report"]["total_monthly_payment"]


def max_dpd_12m(sources, deps):
    return sources["credit_bureau_report"]["max_dpd_12m"]


def days_since_last_delinquency(sources, deps):
    report = sources["credit_bureau_report"]
    last = report["last_delinquency_date"]
    if last is None:
        return NO_DELINQUENCY_DAYS
    report_day = datetime.fromisoformat(report["report_ts"]).date()
    return (report_day - date.fromisoformat(last)).days


def inquiries_30d(sources, deps):
    return sources["credit_bureau_report"]["inquiries_30d"]


# --- F1: telco_profile ----------------------------------------------------------------

def sim_age_days(sources, deps):
    return sources["telco_report"]["sim_age_days"]


def active_days_30d(sources, deps):
    return sources["telco_report"]["active_days_30d"]


def avg_monthly_topup(sources, deps):
    return sources["telco_report"]["avg_monthly_topup"]


def telco_score(sources, deps):
    return sources["telco_report"]["telco_score"]


# --- F1: socdem_profile ------------------------------------------------------------------

def age(sources, deps):
    return sources["socdem_report"]["age"]


def region_code(sources, deps):
    return sources["socdem_report"]["region_code"]


def residence_tenure_months(sources, deps):
    return sources["socdem_report"]["residence_tenure_months"]


# --- F2: credit affordability (deps only; computed by the real DAG) ----------------------

def requested_monthly_payment(sources, deps):
    return _round6(deps["requested_amount"] / deps["term_months"])


def existing_payment_to_income(sources, deps):
    income = deps["income_avg_12m"]
    if income <= 0:
        return 10.0  # capped "no income" burden
    return _round6(min(deps["total_monthly_debt_payment"] / income, 10.0))


def total_payment_to_income(sources, deps):
    income = deps["income_avg_12m"]
    if income <= 0:
        return 10.0
    total = deps["requested_monthly_payment"] + deps["total_monthly_debt_payment"]
    return _round6(min(total / income, 10.0))


def income_stability_index(sources, deps):
    """0..1: penalizes variance (coefficient of variation) and inactive months."""
    avg = deps["income_avg_12m"]
    if avg <= 0:
        return 0.0
    cv = deps["income_std_12m"] / avg
    return _round6((1.0 / (1.0 + cv)) * (deps["months_with_income_12m"] / 12))


def recent_delinquency_flag(sources, deps):
    recent = deps["days_since_last_delinquency"] <= 180
    return 1 if recent and deps["max_dpd_12m"] >= 30 else 0


def thin_file_flag(sources, deps):
    return 1 if deps["active_loans_count"] == 0 or deps["sim_age_days"] < 365 else 0


def affordability_index(sources, deps):
    """0..1: stable income left after all payments; 0 when payments swallow income."""
    burden = min(deps["total_payment_to_income"], 1.0)
    return _round6(deps["income_stability_index"] * (1.0 - burden))


def combined_risk_index(sources, deps):
    """0..1 heuristic risk blend — the rule-based counterpart the PD model out-ranks."""
    score_risk = 1.0 - (deps["bureau_score"] - 300) / 550
    value = (
        0.50 * (1.0 - deps["affordability_index"])
        + 0.25 * deps["recent_delinquency_flag"]
        + 0.10 * deps["thin_file_flag"]
        + 0.15 * max(0.0, min(score_risk, 1.0))
    )
    return _round6(value)


_UDFS = {
    "udf.credit_demo.requested_amount": requested_amount,
    "udf.credit_demo.term_months": term_months,
    "udf.credit_demo.income_avg_3m": income_avg_3m,
    "udf.credit_demo.income_avg_6m": income_avg_6m,
    "udf.credit_demo.income_avg_12m": income_avg_12m,
    "udf.credit_demo.income_std_12m": income_std_12m,
    "udf.credit_demo.income_trend_6m": income_trend_6m,
    "udf.credit_demo.months_with_income_12m": months_with_income_12m,
    "udf.credit_demo.bureau_score": bureau_score,
    "udf.credit_demo.active_loans_count": active_loans_count,
    "udf.credit_demo.total_outstanding_amount": total_outstanding_amount,
    "udf.credit_demo.total_monthly_debt_payment": total_monthly_debt_payment,
    "udf.credit_demo.max_dpd_12m": max_dpd_12m,
    "udf.credit_demo.days_since_last_delinquency": days_since_last_delinquency,
    "udf.credit_demo.inquiries_30d": inquiries_30d,
    "udf.credit_demo.sim_age_days": sim_age_days,
    "udf.credit_demo.active_days_30d": active_days_30d,
    "udf.credit_demo.avg_monthly_topup": avg_monthly_topup,
    "udf.credit_demo.telco_score": telco_score,
    "udf.credit_demo.age": age,
    "udf.credit_demo.region_code": region_code,
    "udf.credit_demo.residence_tenure_months": residence_tenure_months,
    "udf.credit_demo.requested_monthly_payment": requested_monthly_payment,
    "udf.credit_demo.existing_payment_to_income": existing_payment_to_income,
    "udf.credit_demo.total_payment_to_income": total_payment_to_income,
    "udf.credit_demo.income_stability_index": income_stability_index,
    "udf.credit_demo.recent_delinquency_flag": recent_delinquency_flag,
    "udf.credit_demo.thin_file_flag": thin_file_flag,
    "udf.credit_demo.affordability_index": affordability_index,
    "udf.credit_demo.combined_risk_index": combined_risk_index,
}

# Model input feature order is owned by the trained artifact (model/artifact.json);
# these names must exist in the registry and are asserted by tests.
MODEL_FEATURES = (
    "income_avg_12m",
    "income_stability_index",
    "bureau_score",
    "existing_payment_to_income",
    "total_payment_to_income",
    "recent_delinquency_flag",
    "thin_file_flag",
    "inquiries_30d",
    "telco_score",
    "affordability_index",
)


def build_udfs() -> UdfRegistry:
    return UdfRegistry(dict(_UDFS))


def build_registry_and_udfs():
    """The FSP_UDF_PROVIDER entrypoint (tuple shape; the UDF half is used)."""
    return load_registry_file(REGISTRY_PATH), build_udfs()
