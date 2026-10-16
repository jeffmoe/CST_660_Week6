# Synthetic incident telemetry: claims-severity dashboard, 2026-10-16

Synthetic observability data for the Brightline Insurance claims pipeline: 91 nightly
runs (`claims_nightly`, 2026-07-18 to 2026-10-16), ending with the run behind the
dashboard the VP of Claims Operations opened at 6:40 a.m.

```
python -m venv .venv && .venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python synthetic\generate_telemetry.py [--seed 660] [--out data\telemetry]
```

Output is deterministic for a given seed. All timestamps are local (CDT, -05:00).

| File | Grain | What it holds |
|---|---|---|
| `table_freshness.csv` | run x table | `last_loaded_at`, `data_watermark`, SLA `expected_by`, `freshness_status` |
| `row_volume_history.csv` | run x table x attempt | `status`, `rows_affected`, `table_row_count`, `pct_change_vs_prev_run`, errors |
| `schema_change_log.csv` | change event | Source releases, loader schema-drift events, dbt and Tableau changes, with ticket, team and declared impact |
| `column_profiles.csv` | run x profiled column | `null_rate`, `distinct_count`, `mean`/`p50`/`p95`, `category_mix` (JSON shares; `__NULL__` is the null bucket) |
| `lineage_graph.json` / `.graphml` | table DAG | 24 nodes (source → raw → staging → intermediate → mart → exposure), with owner, SLA, and join/filter logic on edges |
| `lineage_column_level.csv` | column edge | Column-level lineage, including which columns are used as join keys or filters |
| `dashboard_claims_severity_daily.csv` | close date x LOB | Closed count and mean paid severity, which is what the dashboard shows |

Lines of business: PA Personal Auto, HO Homeowners, CP Commercial Property,
CA Commercial Auto, GL General Liability, IM Inland Marine.
Policy status codes: IF in force, EX expired, CX cancelled, QT quote.

The injected root cause is in the module docstring of `generate_telemetry.py`.
Don't hand that file to anyone working the incident.
