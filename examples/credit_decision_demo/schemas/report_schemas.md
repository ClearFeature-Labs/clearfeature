# Credit-decision demo — report contracts

All data is **synthetic** and internationally neutral (counter ids, ISO currency code,
`R01..R08` regions, UTC timestamps, integer amounts). Not suitable for real lending.

Every JSONL line is the platform ingestion row shape:
`{"entity_key": {"user_id", "application_id"}, "event_ts": iso8601, "payload": {...}}`.

## application_request (the request — not an external report)

| field | type | notes |
|---|---|---|
| user_id / application_id | str | `user_000001` / `app_000001` |
| application_ts | iso8601 UTC | |
| requested_amount | int | whole currency units |
| term_months | int | 6..60 |
| currency_code | str | ISO 4217, default USD |

## tax_report (external report 1)

| field | type | notes |
|---|---|---|
| user_id, report_ts, currency_code | | report_ts = application_ts − 1 day |
| periods | list | 12 entries: `{period: "YYYY-MM", taxable_income: int}` (0 = inactive month) |

## credit_bureau_report (external report 2)

| field | type | notes |
|---|---|---|
| bureau_score | int | 300..850 |
| active_loans | int | |
| total_outstanding_amount | int | |
| total_monthly_payment | int | existing debt service |
| max_dpd_12m | int | worst days-past-due in 12m |
| last_delinquency_date | date or null | null = clean record |
| inquiries_30d | int | |

## telco_report (external report 3)

| field | type |
|---|---|
| sim_age_days | int |
| active_days_30d | int (0..30) |
| avg_monthly_topup | int |
| telco_score | int (1..100) |

## socdem_report (external report 4)

| field | type | notes |
|---|---|---|
| age | int | 21..64 |
| region_code | str | neutral `R01..R08` |
| residence_tenure_months | int | |

## labels.csv (training/evaluation only — never ingested as a feature)

`user_id, application_id, label_default (0/1), segment` — the label is drawn from a
hidden latent-risk score plus noise (see `generator.py`), not copied from any one field.
