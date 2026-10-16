"""Generate synthetic data-observability telemetry for the Brightline Insurance
claims-severity incident (dashboard run of 2026-10-16).

Outputs (written to --out, default data/telemetry/):
    table_freshness.csv          per run x table: load time, watermark, SLA, status
    row_volume_history.csv       per run x table: rows affected, table row count, status
    schema_change_log.csv        source, loader and dbt schema changes
    column_profiles.csv          per run x profiled column: null rate, category mix, mean
    lineage_graph.json           node-link lineage graph (networkx)
    lineage_graph.graphml        same graph, GraphML
    lineage_column_level.csv     column-level lineage, including filter/join usage
    dashboard_claims_severity_daily.csv   what the VP is looking at

Injected causal chain (answer key -- do not share with students before the exercise):
    1. 2026-10-15 21:47 CDT  PolicyCenter release PC-2026.10.2 (CHG-48213) moves the
       Homeowners (HO) and Commercial Property (CP) product lines onto extract API v4,
       which renames pol_status_cd -> policy_status_code. Declared non-breaking.
    2. 2026-10-16 02:14 CDT  The ingest loader sees a *new* column on
       raw.policy_snapshot and auto-adds it. The old column is retained, so for the
       migrated rows pol_status_cd is now NULL. No drop/rename is ever logged downstream.
    3. stg.policy still maps pol_status_cd -> policy_status, so ~38% of policies have a
       NULL status. Every run still reports success.
    4. int.claim_policy_enriched inner-joins claims to policies with
       policy_status IN ('IF','CX','EX'). NULL fails the filter, silently removing HO
       and CP claims (a few legacy-form HO policies stay on API v3 and survive).
    5. mart.fct_claim_closed is incremental, so history is intact and only today's
       partition is short ~38% -- which reads as a business decline on the dashboard.
    Red herrings: ext_noaa.storm_events is late (the only red freshness check);
    Claims Analytics merged a dashboard model change the afternoon before; ClaimCenter
    added litigation_flag the day before; a prior PolicyCenter rename broke nothing.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd

CDT = timezone(timedelta(hours=-5))
START_DATE = date(2026, 7, 18)
INCIDENT_DATE = date(2026, 10, 16)
RENAME_AT = datetime(2026, 10, 15, 21, 47, tzinfo=CDT)
DEFAULT_SEED = 660

LOBS = ["PA", "HO", "CP", "CA", "GL", "IM"]
LOB_NAMES = {
    "PA": "Personal Auto",
    "HO": "Homeowners",
    "CP": "Commercial Property",
    "CA": "Commercial Auto",
    "GL": "General Liability",
    "IM": "Inland Marine",
}
# Share of daily closures by line of business.
CLOSURE_MIX = {"PA": 0.37, "HO": 0.32, "CP": 0.075, "CA": 0.095, "GL": 0.08, "IM": 0.06}
# Share of the in-force policy book by line of business.
POLICY_MIX = {"PA": 0.45, "HO": 0.33, "CP": 0.05, "CA": 0.06, "GL": 0.06, "IM": 0.05}
# Lognormal paid-severity parameters: (median paid, sigma).
SEVERITY = {
    "PA": (4200, 0.9),
    "HO": (7500, 1.0),
    "CP": (15000, 1.2),
    "CA": (6000, 1.0),
    "GL": (9000, 1.3),
    "IM": (3000, 0.9),
}
CLOSED_WITHOUT_PAYMENT = 0.18
POLICY_STATUS_MIX = {"IF": 0.775, "EX": 0.152, "CX": 0.061, "QT": 0.012}
VALID_STATUSES = ("IF", "CX", "EX")

# LOBs moved to PolicyCenter extract API v4 by CHG-48213, and the share of each
# LOB's policies that actually moved (legacy HO forms stay on v3).
MIGRATED_SHARE = {"HO": 0.96, "CP": 1.0}

DOW_FACTOR = [1.06, 1.05, 1.00, 1.00, 0.96, 0.24, 0.14]  # Mon..Sun
HOLIDAYS = {date(2026, 9, 7): 0.30}  # Labor Day
HAIL_EVENT = date(2026, 8, 24)  # spring-in-autumn hail; HO/CP closures surge ~1 week later
WEEKDAY_CLOSURES = 1000


@dataclass(frozen=True)
class TableSpec:
    name: str
    layer: str
    system: str
    owner: str
    materialization: str
    duration_min: float
    expected_by: time  # freshness SLA (local, on run date)


TABLES = [
    TableSpec("src_policycenter.policy", "source", "PolicyCenter", "Policy Platform", "external", 0, time(2, 0)),
    TableSpec("src_claimcenter.claim", "source", "ClaimCenter", "Claims Systems", "external", 0, time(2, 0)),
    TableSpec("src_claimcenter.claim_payment", "source", "ClaimCenter", "Claims Systems", "external", 0, time(2, 0)),
    TableSpec("src_claimcenter.claim_status_history", "source", "ClaimCenter", "Claims Systems", "external", 0, time(2, 0)),
    TableSpec("src_workday.adjuster_roster", "source", "Workday", "HR Systems", "external", 0, time(2, 0)),
    TableSpec("ext_noaa.storm_events", "source", "NOAA API", "External", "external", 0, time(2, 0)),
    TableSpec("raw.policy_snapshot", "raw", "Ingest loader", "Data Engineering", "full_refresh", 14, time(3, 0)),
    TableSpec("raw.claim", "raw", "Ingest loader", "Data Engineering", "incremental", 9, time(3, 0)),
    TableSpec("raw.claim_payment", "raw", "Ingest loader", "Data Engineering", "incremental", 7, time(3, 0)),
    TableSpec("raw.claim_status_history", "raw", "Ingest loader", "Data Engineering", "incremental", 6, time(3, 0)),
    TableSpec("raw.adjuster_roster", "raw", "Ingest loader", "Data Engineering", "full_refresh", 1, time(3, 0)),
    TableSpec("raw.storm_events", "raw", "Ingest loader", "Data Engineering", "incremental", 2, time(4, 0)),
    TableSpec("stg.policy", "staging", "dbt", "Data Engineering", "table", 6, time(3, 30)),
    TableSpec("stg.claim", "staging", "dbt", "Data Engineering", "table", 5, time(3, 30)),
    TableSpec("stg.claim_payment", "staging", "dbt", "Data Engineering", "table", 4, time(3, 30)),
    TableSpec("stg.claim_status", "staging", "dbt", "Data Engineering", "incremental", 3, time(3, 30)),
    TableSpec("stg.adjuster", "staging", "dbt", "Data Engineering", "table", 1, time(3, 30)),
    TableSpec("stg.storm_events", "staging", "dbt", "Data Engineering", "incremental", 1, time(4, 30)),
    TableSpec("int.claim_policy_enriched", "intermediate", "dbt", "Data Engineering", "table", 11, time(4, 0)),
    TableSpec("int.claim_financials", "intermediate", "dbt", "Data Engineering", "table", 7, time(4, 0)),
    TableSpec("mart.fct_claim_closed", "mart", "dbt", "Claims Analytics", "incremental", 5, time(4, 30)),
    TableSpec("mart.dim_adjuster", "mart", "dbt", "Claims Analytics", "table", 1, time(4, 30)),
    TableSpec("mart.agg_claim_severity_daily", "mart", "dbt", "Claims Analytics", "incremental", 2, time(5, 0)),
    TableSpec("bi.claims_severity_dashboard", "exposure", "Tableau", "Claims Analytics", "extract", 4, time(6, 0)),
]
SPEC = {t.name: t for t in TABLES}

EDGES = [
    ("src_policycenter.policy", "raw.policy_snapshot", "extract API v3 + v4 (merged)"),
    ("src_claimcenter.claim", "raw.claim", "CDC"),
    ("src_claimcenter.claim_payment", "raw.claim_payment", "CDC"),
    ("src_claimcenter.claim_status_history", "raw.claim_status_history", "CDC"),
    ("src_workday.adjuster_roster", "raw.adjuster_roster", "SFTP file"),
    ("ext_noaa.storm_events", "raw.storm_events", "REST API"),
    ("raw.policy_snapshot", "stg.policy", "select/rename"),
    ("raw.claim", "stg.claim", "select/dedupe"),
    ("raw.claim_payment", "stg.claim_payment", "select/cast"),
    ("raw.claim_status_history", "stg.claim_status", "select/dedupe"),
    ("raw.adjuster_roster", "stg.adjuster", "select"),
    ("raw.storm_events", "stg.storm_events", "select/geo-filter"),
    ("stg.claim", "int.claim_policy_enriched", "base"),
    ("stg.policy", "int.claim_policy_enriched",
     "INNER JOIN ON claim.policy_id = policy.policy_id AND policy.policy_status IN ('IF','CX','EX')"),
    ("stg.claim_payment", "int.claim_financials", "aggregate by claim_id"),
    ("int.claim_policy_enriched", "mart.fct_claim_closed", "base"),
    ("int.claim_financials", "mart.fct_claim_closed", "LEFT JOIN ON claim_id"),
    ("stg.claim_status", "mart.fct_claim_closed", "INNER JOIN latest status = 'CLOSED'"),
    ("stg.adjuster", "mart.dim_adjuster", "base"),
    ("mart.fct_claim_closed", "mart.agg_claim_severity_daily", "GROUP BY close_date, lob_cd"),
    ("mart.dim_adjuster", "mart.agg_claim_severity_daily", "LEFT JOIN ON adjuster_id"),
    ("stg.storm_events", "mart.agg_claim_severity_daily", "LEFT JOIN cat tagging"),
    ("mart.agg_claim_severity_daily", "bi.claims_severity_dashboard", "extract refresh"),
]

COLUMN_LINEAGE = [
    # (downstream table, downstream column, upstream table, upstream column, usage)
    ("raw.policy_snapshot", "pol_status_cd", "src_policycenter.policy", "pol_status_cd", "projection (API v3)"),
    ("raw.policy_snapshot", "policy_status_code", "src_policycenter.policy", "policy_status_code", "projection (API v4)"),
    ("raw.policy_snapshot", "producer_cd", "src_policycenter.policy", "producer_cd", "projection"),
    ("raw.policy_snapshot", "lob_cd", "src_policycenter.policy", "lob_cd", "projection"),
    ("raw.policy_snapshot", "policy_id", "src_policycenter.policy", "policy_id", "projection"),
    ("stg.policy", "policy_id", "raw.policy_snapshot", "policy_id", "projection"),
    ("stg.policy", "lob_cd", "raw.policy_snapshot", "lob_cd", "projection"),
    ("stg.policy", "policy_status", "raw.policy_snapshot", "pol_status_cd", "projection (renamed)"),
    ("stg.claim", "claim_id", "raw.claim", "claim_id", "projection"),
    ("stg.claim", "policy_id", "raw.claim", "policy_id", "projection"),
    ("stg.claim", "lob_cd", "raw.claim", "lob_cd", "projection"),
    ("stg.claim", "litigation_flag", "raw.claim", "litigation_flag", "projection"),
    ("int.claim_policy_enriched", "claim_id", "stg.claim", "claim_id", "projection"),
    ("int.claim_policy_enriched", "lob_cd", "stg.claim", "lob_cd", "projection"),
    ("int.claim_policy_enriched", "policy_id", "stg.policy", "policy_id", "join key"),
    ("int.claim_policy_enriched", "policy_status", "stg.policy", "policy_status", "join filter + projection"),
    ("int.claim_financials", "total_paid", "stg.claim_payment", "amount", "aggregate SUM"),
    ("mart.fct_claim_closed", "claim_id", "int.claim_policy_enriched", "claim_id", "projection"),
    ("mart.fct_claim_closed", "lob_cd", "int.claim_policy_enriched", "lob_cd", "projection"),
    ("mart.fct_claim_closed", "total_paid", "int.claim_financials", "total_paid", "projection"),
    ("mart.fct_claim_closed", "close_date", "stg.claim_status", "status_ts", "filter status = 'CLOSED'"),
    ("mart.agg_claim_severity_daily", "closed_count", "mart.fct_claim_closed", "claim_id", "aggregate COUNT"),
    ("mart.agg_claim_severity_daily", "mean_severity", "mart.fct_claim_closed", "total_paid", "aggregate AVG where > 0"),
    ("mart.agg_claim_severity_daily", "cat_event_id", "stg.storm_events", "event_id", "left join"),
    ("bi.claims_severity_dashboard", "closed_count", "mart.agg_claim_severity_daily", "closed_count", "SUM"),
    ("bi.claims_severity_dashboard", "mean_severity", "mart.agg_claim_severity_daily", "mean_severity", "weighted AVG"),
]


def ts(d: date, t: time) -> datetime:
    return datetime.combine(d, t, tzinfo=CDT)


def iso(dt: datetime | None) -> str | None:
    return dt.isoformat(timespec="seconds") if dt else None


def run_dates() -> list[date]:
    return [START_DATE + timedelta(days=i) for i in range((INCIDENT_DATE - START_DATE).days + 1)]


def run_id(d: date) -> str:
    return f"claims_nightly__{d:%Y%m%d}T0200"


def is_incident(d: date) -> bool:
    return d >= INCIDENT_DATE


# --------------------------------------------------------------------------- business


def simulate_business(rng: np.random.Generator, days: list[date]) -> dict[date, dict]:
    """True (pre-pipeline) business activity per day, plus what survives the pipeline."""
    out = {}
    for i, d in enumerate(days):
        level = WEEKDAY_CLOSURES * (1 + 0.0004 * i) * DOW_FACTOR[d.weekday()] * HOLIDAYS.get(d, 1.0)
        days_since_hail = (d - HAIL_EVENT).days - 7
        hail = 1 + 0.45 * np.exp(-days_since_hail / 11) if days_since_hail >= 0 else 1.0

        true_closed, surviving, severities = {}, {}, {}
        for lob in LOBS:
            lam = level * CLOSURE_MIX[lob] * (hail if lob in ("HO", "CP") else 1.0)
            n = int(rng.poisson(lam))
            true_closed[lob] = n
            keep = n
            if is_incident(d) and lob in MIGRATED_SHARE:
                keep = n - int(rng.binomial(n, MIGRATED_SHARE[lob]))
            surviving[lob] = keep
            median, sigma = SEVERITY[lob]
            paid = rng.lognormal(np.log(median), sigma, size=keep)
            paid[rng.random(keep) < CLOSED_WITHOUT_PAYMENT] = 0.0
            severities[lob] = np.round(paid, 2)

        new_claims = int(rng.poisson(level * 1.02 * (1 + 0.6 * (hail - 1))))
        out[d] = {
            "true_closed": true_closed,
            "closed": surviving,
            "severities": severities,
            "new_claims": new_claims,
            "payments": int(rng.poisson(level * 2.4)),
            "status_events": int(rng.poisson(level * 3.1)),
            "policies": int(412_000 + 55 * i + rng.normal(0, 40)),
            "storm_events": int(rng.poisson(40 + (900 if d == HAIL_EVENT else 0))),
            "adjusters": int(412 + i // 30 + rng.integers(-2, 3)),
        }
    return out


# --------------------------------------------------------------------------- lineage


def build_lineage() -> nx.DiGraph:
    g = nx.DiGraph(name="brightline_claims_lineage")
    for t in TABLES:
        g.add_node(t.name, layer=t.layer, system=t.system, owner=t.owner,
                   materialization=t.materialization, freshness_sla=t.expected_by.isoformat("minutes"))
    for up, down, how in EDGES:
        g.add_edge(up, down, relationship=how)
    assert nx.is_directed_acyclic_graph(g)
    return g


# --------------------------------------------------------------------------- runs


def schedule_runs(rng: np.random.Generator, g: nx.DiGraph, days: list[date]) -> pd.DataFrame:
    """Start/end times per table per run, respecting DAG dependencies."""
    order = list(nx.topological_sort(g))
    rows = []
    for d in days:
        finished: dict[str, datetime] = {}
        for name in order:
            spec = SPEC[name]
            attempt, status, err = 1, "success", None
            if spec.layer == "source":
                # Source extract cutoff for the nightly window.
                end = ts(d, time(1, 55)) + timedelta(seconds=int(rng.integers(0, 240)))
                start = end
            else:
                parents = list(g.predecessors(name))
                ready = max(finished[p] for p in parents) if parents else ts(d, time(2, 0))
                start = max(ready, ts(d, time(2, 0))) + timedelta(seconds=int(rng.integers(20, 120)))
                if name == "raw.storm_events" and is_incident(d):
                    # NOAA API outage: loader retries until it gets through.
                    for attempt, retry_at in enumerate([start, ts(d, time(3, 31, 40)), ts(d, time(4, 46, 2))], 1):
                        rows.append(dict(run_date=d, table=name, attempt=attempt, started_at=retry_at,
                                         ended_at=retry_at + timedelta(seconds=31), status="failed",
                                         error="HTTP 503 Service Unavailable from api.weather.gov"))
                    start = ts(d, time(5, 58, 11))
                    attempt = 4
                if name == "raw.claim" and d == date(2026, 8, 29):
                    # ClaimCenter maintenance window held the CDC slot.
                    start = ts(d, time(3, 41, 5))
                dur = spec.duration_min * float(rng.lognormal(0, 0.15))
                if name == "raw.policy_snapshot" and is_incident(d):
                    dur *= 1.12  # schema-drift handling adds a few minutes
                if name == "stg.claim_payment" and d == date(2026, 9, 3):
                    rows.append(dict(run_date=d, table=name, attempt=1, started_at=start,
                                     ended_at=start + timedelta(minutes=30), status="failed",
                                     error="Warehouse statement timeout (1800s) on WH_TRANSFORM_M"))
                    start = start + timedelta(minutes=33)
                    attempt = 2
                end = start + timedelta(minutes=dur)
                if name == "bi.claims_severity_dashboard":
                    start = max(ts(d, time(5, 30)), ready)
                    end = start + timedelta(minutes=dur)
            finished[name] = end
            rows.append(dict(run_date=d, table=name, attempt=attempt, started_at=start,
                             ended_at=end, status=status, error=err))
    return pd.DataFrame(rows)


def table_counts(d: date, b: dict, cum: dict) -> dict[str, tuple[int, int]]:
    """(rows_affected, table_row_count) for each non-source table on a run date."""
    closed = sum(b["closed"].values())
    lob_rows = sum(1 for v in b["closed"].values() if v > 0)
    cum["claims"] += b["new_claims"]
    cum["payments"] += b["payments"]
    cum["status"] += b["status_events"]
    cum["storm"] += b["storm_events"]
    cum["closed"] += closed
    cum["agg"] += lob_rows
    claims_total = cum["claims"]
    # Claims on valid policies (~0.3% are on quote/draft policies and drop by design).
    enriched = int(claims_total * 0.997)
    if is_incident(d):
        lost = sum(CLOSURE_MIX[l] * s for l, s in MIGRATED_SHARE.items())
        enriched = int(enriched * (1 - lost))
    with_payments = int(claims_total * 0.81)
    changed_claims = int(b["new_claims"] + b["status_events"] * 0.55)
    return {
        "raw.policy_snapshot": (b["policies"], b["policies"]),
        "raw.claim": (changed_claims, claims_total),
        "raw.claim_payment": (b["payments"], cum["payments"]),
        "raw.claim_status_history": (b["status_events"], cum["status"]),
        "raw.adjuster_roster": (b["adjusters"], b["adjusters"]),
        "raw.storm_events": (b["storm_events"], cum["storm"]),
        "stg.policy": (b["policies"] - 212, b["policies"] - 212),
        "stg.claim": (claims_total, claims_total),
        "stg.claim_payment": (cum["payments"], cum["payments"]),
        "stg.claim_status": (b["status_events"], cum["status"]),
        "stg.adjuster": (b["adjusters"], b["adjusters"]),
        "stg.storm_events": (int(b["storm_events"] * 0.18), int(cum["storm"] * 0.18)),
        "int.claim_policy_enriched": (enriched, enriched),
        "int.claim_financials": (with_payments, with_payments),
        "mart.fct_claim_closed": (closed, cum["closed"]),
        "mart.dim_adjuster": (b["adjusters"], b["adjusters"]),
        "mart.agg_claim_severity_daily": (lob_rows, cum["agg"]),
        "bi.claims_severity_dashboard": (cum["agg"], cum["agg"]),
    }


def build_row_volume(runs: pd.DataFrame, biz: dict) -> pd.DataFrame:
    cum = {"claims": 296_400, "payments": 1_874_000, "status": 2_312_000,
           "storm": 61_200, "closed": 248_900, "agg": 4_800}
    counts = {d: table_counts(d, biz[d], cum) for d in sorted(biz)}
    rows = []
    for r in runs.itertuples():
        spec = SPEC[r.table]
        if spec.layer == "source":
            continue
        affected, total = counts[r.run_date][r.table]
        if r.status == "failed":
            affected, total = 0, None
        rows.append(dict(
            run_id=run_id(r.run_date), run_date=r.run_date.isoformat(), table=r.table,
            layer=spec.layer, materialization=spec.materialization, attempt=r.attempt,
            started_at=iso(r.started_at), ended_at=iso(r.ended_at), status=r.status,
            rows_affected=affected, table_row_count=total, error=r.error,
        ))
    df = pd.DataFrame(rows)
    ok = df[df.status == "success"].copy()
    ok["prev"] = ok.groupby("table")["rows_affected"].shift(1)
    df.loc[ok.index, "pct_change_vs_prev_run"] = ((ok.rows_affected - ok.prev) / ok.prev * 100).round(2)
    return df


def build_freshness(runs: pd.DataFrame) -> pd.DataFrame:
    ok = runs[runs.status == "success"]
    by_day = {d: grp.set_index("table") for d, grp in ok.groupby("run_date")}
    g = build_lineage()
    rows = []
    for d, grp in by_day.items():
        watermark: dict[str, datetime] = {}
        for name in nx.topological_sort(g):
            spec = SPEC[name]
            r = grp.loc[name]
            parents = list(g.predecessors(name))
            # Data watermark = latest source event covered; inherits the oldest upstream.
            watermark[name] = r.ended_at if not parents else min(watermark[p] for p in parents)
            if spec.layer == "source":
                continue
            loaded = r.ended_at
            expected = ts(d, spec.expected_by)
            lag = (loaded - watermark[name]).total_seconds() / 60
            rows.append(dict(
                run_id=run_id(d), run_date=d.isoformat(), table=name, layer=spec.layer,
                owner=spec.owner, last_loaded_at=iso(loaded), data_watermark=iso(watermark[name]),
                expected_by=iso(expected), minutes_late=max(0, round((loaded - expected).total_seconds() / 60, 1)),
                load_lag_minutes=round(lag, 1),
                freshness_status="on_time" if loaded <= expected else "late",
            ))
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- schema log


def build_schema_changes() -> pd.DataFrame:
    def c(at, system, obj, ctype, col, old, new, ticket, team, detected_by, declared_breaking, notes):
        return dict(detected_at=iso(at), system=system, object=obj, change_type=ctype, column_name=col,
                    old_value=old, new_value=new, change_ticket=ticket, author_team=team,
                    detected_by=detected_by, declared_breaking=declared_breaking, notes=notes)

    D = lambda y, m, d, hh, mm: datetime(y, m, d, hh, mm, tzinfo=CDT)  # noqa: E731
    events = [
        c(D(2026, 7, 22, 14, 5), "dbt", "stg.claim", "ADD_MODEL_COLUMN", "reported_channel", None, "VARCHAR",
          "PR #741", "Data Engineering", "dbt_manifest_diff", False, "Expose FNOL channel for digital intake KPI"),
        c(D(2026, 7, 30, 21, 10), "ClaimCenter", "src_claimcenter.claim_payment", "ALTER_TYPE", "check_memo",
          "VARCHAR(80)", "VARCHAR(255)", "CHG-46551", "Claims Systems", "source_release_notes", False,
          "Widen memo field"),
        c(D(2026, 7, 31, 2, 9), "Ingest loader", "raw.claim_payment", "ALTER_TYPE", "check_memo",
          "VARCHAR(80)", "VARCHAR(255)", None, "Data Engineering", "loader_schema_drift", False,
          "Auto-applied widening"),
        c(D(2026, 8, 5, 22, 0), "PolicyCenter", "src_policycenter.policy", "ADD_COLUMN", "paperless_ind",
          None, "BOOLEAN", "CHG-46802", "Policy Platform", "source_release_notes", False, "E-delivery preference"),
        c(D(2026, 8, 6, 2, 11), "Ingest loader", "raw.policy_snapshot", "ADD_COLUMN", "paperless_ind",
          None, "BOOLEAN", None, "Data Engineering", "loader_schema_drift", False, "Auto-added (schema drift: allow)"),
        c(D(2026, 8, 12, 10, 42), "dbt", "mart.fct_claim_closed", "ADD_MODEL_COLUMN", "days_open", None, "INTEGER",
          "PR #758", "Claims Analytics", "dbt_manifest_diff", False, "Cycle-time metric for adjuster scorecards"),
        c(D(2026, 8, 19, 9, 15), "Workday", "src_workday.adjuster_roster", "ADD_COLUMN", "licensed_states",
          None, "VARCHAR", "HR-2290", "HR Systems", "source_release_notes", False, "Multi-state licensing"),
        c(D(2026, 8, 20, 2, 4), "Ingest loader", "raw.adjuster_roster", "ADD_COLUMN", "licensed_states",
          None, "VARCHAR", None, "Data Engineering", "loader_schema_drift", False, "Auto-added (schema drift: allow)"),
        c(D(2026, 8, 27, 15, 30), "dbt", "stg.storm_events", "MODIFY_MODEL_LOGIC", None, None, None,
          "PR #771", "Data Engineering", "dbt_manifest_diff", False, "Expand geo filter to include IA counties"),
        c(D(2026, 9, 10, 21, 30), "ClaimCenter", "src_claimcenter.claim_payment", "ALTER_TYPE", "amount",
          "NUMERIC(12,2)", "NUMERIC(14,2)", "CHG-47011", "Claims Systems", "source_release_notes", False,
          "Large-loss headroom after hail event"),
        c(D(2026, 9, 11, 2, 8), "Ingest loader", "raw.claim_payment", "ALTER_TYPE", "amount",
          "NUMERIC(12,2)", "NUMERIC(14,2)", None, "Data Engineering", "loader_schema_drift", False,
          "Auto-applied widening"),
        c(D(2026, 9, 22, 21, 15), "PolicyCenter", "src_policycenter.policy", "RENAME_COLUMN", "agt_cd",
          "agt_cd", "producer_cd", "CHG-47102", "Policy Platform", "source_release_notes", False,
          "ACORD naming alignment, all product lines. Loader handles schema drift."),
        c(D(2026, 9, 23, 2, 13), "Ingest loader", "raw.policy_snapshot", "ADD_COLUMN", "producer_cd",
          None, "VARCHAR(12)", None, "Data Engineering", "loader_schema_drift", False,
          "Auto-added (schema drift: allow). agt_cd retained, NULL going forward."),
        c(D(2026, 9, 29, 11, 2), "dbt", "int.claim_financials", "MODIFY_MODEL_LOGIC", None, None, None,
          "PR #790", "Data Engineering", "dbt_manifest_diff", False, "Exclude voided payments"),
        c(D(2026, 10, 6, 13, 48), "dbt", "mart.dim_adjuster", "ADD_MODEL_COLUMN", "licensed_states", None,
          "VARCHAR", "PR #799", "Claims Analytics", "dbt_manifest_diff", False, None),
        c(D(2026, 10, 14, 20, 30), "ClaimCenter", "src_claimcenter.claim", "ADD_COLUMN", "litigation_flag",
          None, "BOOLEAN", "CHG-48170", "Claims Systems", "source_release_notes", False,
          "Litigation tracking for GL and CA"),
        c(D(2026, 10, 15, 2, 12), "Ingest loader", "raw.claim", "ADD_COLUMN", "litigation_flag", None, "BOOLEAN",
          None, "Data Engineering", "loader_schema_drift", False, "Auto-added (schema drift: allow)"),
        c(D(2026, 10, 15, 9, 20), "dbt", "stg.claim", "ADD_MODEL_COLUMN", "litigation_flag", None, "BOOLEAN",
          "PR #806", "Data Engineering", "dbt_manifest_diff", False, None),
        c(D(2026, 10, 15, 16, 20), "dbt", "mart.agg_claim_severity_daily", "ADD_MODEL_COLUMN", "cat_event_id",
          None, "VARCHAR", "PR #812", "Claims Analytics", "dbt_manifest_diff", False,
          "Tag CAT-driven closures on severity dashboard"),
        c(D(2026, 10, 15, 16, 22), "Tableau", "bi.claims_severity_dashboard", "MODIFY_EXPOSURE", None, None, None,
          "PR #812", "Claims Analytics", "manual", False, "Add CAT filter (default: All)"),
        c(RENAME_AT, "PolicyCenter", "src_policycenter.policy", "RENAME_COLUMN", "pol_status_cd",
          "pol_status_cd", "policy_status_code", "CHG-48213", "Policy Platform", "source_release_notes", False,
          "PC-2026.10.2: HO and CP product lines move to extract API v4 (product model v4). "
          "Status field renamed per ACORD alignment. Loader handles schema drift."),
        c(D(2026, 10, 16, 2, 14), "Ingest loader", "raw.policy_snapshot", "ADD_COLUMN", "policy_status_code",
          None, "VARCHAR(4)", None, "Data Engineering", "loader_schema_drift", False,
          "Auto-added (schema drift: allow)"),
    ]
    df = pd.DataFrame(events).sort_values("detected_at").reset_index(drop=True)
    df.insert(0, "change_id", [f"SC-{4100 + i:05d}" for i in range(len(df))])
    return df


# --------------------------------------------------------------------------- profiles


def mix(counts: dict[str, float]) -> str:
    total = sum(counts.values())
    shares = {k: round(v / total, 4) for k, v in sorted(counts.items(), key=lambda kv: -kv[1]) if v > 0}
    return json.dumps(shares)


def profile(d, table, column, dtype, row_count, null_count, cats=None, values=None):
    p = dict(run_id=run_id(d), profile_date=d.isoformat(), table=table, column=column, data_type=dtype,
             row_count=row_count, null_count=int(null_count),
             null_rate=round(null_count / row_count, 5) if row_count else None,
             distinct_count=None, mean=None, p50=None, p95=None, category_mix=None)
    if cats is not None:
        p["distinct_count"] = sum(1 for k, v in cats.items() if v > 0 and k != "__NULL__")
        p["category_mix"] = mix(cats)
    if values is not None and len(values):
        p.update(mean=round(float(values.mean()), 2), p50=round(float(np.median(values)), 2),
                 p95=round(float(np.percentile(values, 95)), 2))
    return p


def build_profiles(rng: np.random.Generator, biz: dict, volume: pd.DataFrame) -> pd.DataFrame:
    totals = volume[volume.status == "success"].set_index(["run_date", "table"])["table_row_count"]
    raw_claim = volume[(volume.status == "success") & (volume.table == "raw.claim")]
    changed_claims = {date.fromisoformat(r): n for r, n in zip(raw_claim.run_date, raw_claim.rows_affected)}
    rows = []
    for d, b in biz.items():
        n_pol = b["policies"]
        pol_lob = {l: n_pol * POLICY_MIX[l] * (1 + rng.normal(0, 0.002)) for l in LOBS}
        status = {s: n_pol * p * (1 + rng.normal(0, 0.003)) for s, p in POLICY_STATUS_MIX.items()}
        base_nulls = int(rng.poisson(n_pol * 0.0004))
        migrated = sum(pol_lob[l] * s for l, s in MIGRATED_SHARE.items()) if is_incident(d) else 0
        legacy_status = {s: v * (1 - migrated / n_pol) for s, v in status.items()}
        legacy_status["__NULL__"] = migrated + base_nulls
        rows.append(profile(d, "raw.policy_snapshot", "pol_status_cd", "VARCHAR(4)", n_pol,
                            migrated + base_nulls, legacy_status))
        if is_incident(d):
            new_status = {s: v * migrated / n_pol for s, v in status.items()}
            new_status["__NULL__"] = n_pol - migrated
            rows.append(profile(d, "raw.policy_snapshot", "policy_status_code", "VARCHAR(4)", n_pol,
                                n_pol - migrated, new_status))
        rows.append(profile(d, "raw.policy_snapshot", "lob_cd", "VARCHAR(2)", n_pol, 0, pol_lob))

        n_stg = n_pol - 212
        rows.append(profile(d, "stg.policy", "policy_status", "VARCHAR(4)", n_stg,
                            migrated + base_nulls, legacy_status))
        rows.append(profile(d, "stg.policy", "lob_cd", "VARCHAR(2)", n_stg, 0, pol_lob))

        ev = b["status_events"]
        # The source still records every true closure -- upstream business is normal.
        other = ev - b["new_claims"] - sum(b["true_closed"].values())
        ev_mix = {"OPEN": b["new_claims"], "CLOSED": sum(b["true_closed"].values()),
                  "PAYMENT_ISSUED": other * 0.62, "RESERVE_CHANGE": other * 0.17,
                  "NOTE_ADDED": other * 0.18, "REOPENED": other * 0.03}
        rows.append(profile(d, "raw.claim_status_history", "status_cd", "VARCHAR(16)", ev, 0, ev_mix))

        n_claims = int(totals[(d.isoformat(), "stg.claim")])
        claim_lob = {l: n_claims * CLOSURE_MIX[l] * (1 + rng.normal(0, 0.002)) for l in LOBS}
        rows.append(profile(d, "stg.claim", "lob_cd", "VARCHAR(2)", n_claims, 0, claim_lob))
        if d >= date(2026, 10, 15):
            # Only claims touched since ClaimCenter added the column carry a value.
            touched = int(changed_claims[d] * (d - date(2026, 10, 14)).days * 0.9)
            rows.append(profile(d, "stg.claim", "litigation_flag", "BOOLEAN", n_claims, n_claims - touched,
                                {"false": touched * 0.97, "true": touched * 0.03, "__NULL__": n_claims - touched}))

        n_enr = int(totals[(d.isoformat(), "int.claim_policy_enriched")])
        enr_lob = dict(claim_lob)
        if is_incident(d):
            for l, s in MIGRATED_SHARE.items():
                enr_lob[l] *= 1 - s
        rows.append(profile(d, "int.claim_policy_enriched", "lob_cd", "VARCHAR(2)", n_enr, 0, enr_lob))
        rows.append(profile(d, "int.claim_policy_enriched", "policy_status", "VARCHAR(4)", n_enr, 0,
                            {"IF": 0.71 * n_enr, "EX": 0.21 * n_enr, "CX": 0.08 * n_enr}))

        closed = b["closed"]
        n_closed = sum(closed.values())
        sev = np.concatenate([b["severities"][l] for l in LOBS])
        paid = sev[sev > 0]
        rows.append(profile(d, "mart.fct_claim_closed", "lob_cd", "VARCHAR(2)", n_closed, 0, closed))
        rows.append(profile(d, "mart.fct_claim_closed", "total_paid", "NUMERIC(14,2)", n_closed, 0, values=paid))
        rows.append(profile(d, "mart.fct_claim_closed", "policy_status", "VARCHAR(4)", n_closed, 0,
                            {"IF": 0.83 * n_closed, "EX": 0.12 * n_closed, "CX": 0.05 * n_closed}))
    return pd.DataFrame(rows)


def build_dashboard(biz: dict) -> pd.DataFrame:
    rows = []
    for d, b in biz.items():
        for l in LOBS:
            n = b["closed"][l]
            if n == 0:
                continue  # GROUP BY emits no row for an empty LOB
            paid = b["severities"][l]
            paid = paid[paid > 0]
            rows.append(dict(close_date=d.isoformat(), lob_cd=l, lob_name=LOB_NAMES[l], closed_count=n,
                             paid_claim_count=len(paid), total_paid=round(float(paid.sum()), 2),
                             mean_severity=round(float(paid.mean()), 2) if len(paid) else None))
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parent.parent / "data" / "telemetry")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)
    days = run_dates()
    biz = simulate_business(rng, days)
    g = build_lineage()
    runs = schedule_runs(rng, g, days)
    volume = build_row_volume(runs, biz)

    outputs = {
        "table_freshness.csv": build_freshness(runs),
        "row_volume_history.csv": volume,
        "schema_change_log.csv": build_schema_changes(),
        "column_profiles.csv": build_profiles(rng, biz, volume),
        "dashboard_claims_severity_daily.csv": build_dashboard(biz),
        "lineage_column_level.csv": pd.DataFrame(
            COLUMN_LINEAGE, columns=["table", "column", "upstream_table", "upstream_column", "usage"]),
    }
    for name, df in outputs.items():
        df.to_csv(args.out / name, index=False)
    (args.out / "lineage_graph.json").write_text(json.dumps(nx.node_link_data(g, edges="edges"), indent=2))
    nx.write_graphml(g, args.out / "lineage_graph.graphml")

    for name, df in outputs.items():
        print(f"{name:40s} {len(df):>7,d} rows")
    print(f"{'lineage_graph.json / .graphml':40s} {g.number_of_nodes():>7,d} nodes, {g.number_of_edges()} edges")


if __name__ == "__main__":
    main()
