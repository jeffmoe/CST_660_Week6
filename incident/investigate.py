"""Reproduce every piece of evidence cited in INC-20261016 from the telemetry alone.

Reads only data/telemetry/ (never the generator) and prints one section per check,
in the order the investigation ran them.

    python incident/investigate.py [--telemetry data/telemetry] [--date 2026-10-16]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import networkx as nx
import pandas as pd

def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def load(tdir: Path) -> dict:
    g = nx.node_link_graph(json.loads((tdir / "lineage_graph.json").read_text()), edges="edges")
    return dict(
        dash=pd.read_csv(tdir / "dashboard_claims_severity_daily.csv", parse_dates=["close_date"]),
        fresh=pd.read_csv(tdir / "table_freshness.csv"),
        vol=pd.read_csv(tdir / "row_volume_history.csv"),
        schema=pd.read_csv(tdir / "schema_change_log.csv"),
        prof=pd.read_csv(tdir / "column_profiles.csv"),
        col_lineage=pd.read_csv(tdir / "lineage_column_level.csv"),
        graph=g,
        topo={n: i for i, n in enumerate(nx.topological_sort(g))},
    )


def symptom(t: dict, day: pd.Timestamp) -> pd.Series:
    section("S1  Symptom: what the dashboard shows")
    d = t["dash"]
    daily = d.groupby("close_date").agg(closed=("closed_count", "sum"), paid=("total_paid", "sum"),
                                        paid_n=("paid_claim_count", "sum"))
    daily["mean_severity"] = daily.paid / daily.paid_n
    same_dow = daily[daily.index.dayofweek == day.dayofweek]
    prior = same_dow[same_dow.index < day].tail(4)
    today = daily.loc[day]
    print(f"Closed claims {day.date()}: {today.closed:,.0f}")
    print(f"  prior same weekday: {prior.closed.iloc[-1]:,.0f} -> {1 - today.closed / prior.closed.iloc[-1]:.1%} drop")
    print(f"  prior 4 same-weekday avg: {prior.closed.mean():,.0f} -> {1 - today.closed / prior.closed.mean():.1%} drop")
    print(f"Mean paid severity: ${today.mean_severity:,.0f} vs ${prior.mean_severity.mean():,.0f} "
          f"({today.mean_severity / prior.mean_severity.mean() - 1:+.1%})")

    by_lob = d.pivot_table(index="close_date", columns="lob_cd", values="closed_count", aggfunc="sum").fillna(0)
    base = by_lob.loc[prior.index].mean()
    ratio = (by_lob.loc[day] / base).sort_values()
    # Historical band: each weekday vs the average of its own prior 4 same-weekdays.
    trailing = by_lob.groupby(by_lob.index.dayofweek).transform(lambda x: x.shift(1).rolling(4).mean())
    hist = (by_lob / trailing)[(by_lob.index < day) & (by_lob.index.dayofweek < 5)].dropna()
    band = pd.DataFrame({"today": ratio, "hist_p01": hist.quantile(0.01), "hist_p99": hist.quantile(0.99)})
    print("\nToday / prior-4-same-weekday average, by LOB (historical weekday 1st-99th percentile band):")
    print(band.round(2).to_string())
    return ratio


def business_decline(t: dict, day: str) -> None:
    section("C1  Real business decline?  (source events + upstream category mix)")
    p = t["prof"]
    ev = p[(p.table == "raw.claim_status_history") & (p.column == "status_cd")].copy()
    ev["closed_events"] = [round(r * json.loads(m).get("CLOSED", 0)) for r, m in zip(ev.row_count, ev.category_mix)]
    ev = ev.set_index("profile_date")
    dow = pd.to_datetime(ev.index).dayofweek
    same = ev[(dow == pd.Timestamp(day).dayofweek) & (ev.index < day)].tail(4)
    src = ev.loc[day, "closed_events"]
    print(f"ClaimCenter CLOSED status events {day}: {src:,} (prior 4 same-weekday avg {same.closed_events.mean():,.0f})")

    dash_closed = t["dash"][t["dash"].close_date == day].closed_count.sum()
    print(f"Dashboard closed count {day}: {dash_closed:,}  -> pipeline delivered {dash_closed / src:.1%} of source closures")
    for table, col in [("raw.policy_snapshot", "lob_cd"), ("stg.claim", "lob_cd")]:
        m = p[(p.table == table) & (p.column == col)].set_index("profile_date").category_mix
        prev = pd.Series(json.loads(m.iloc[-2])); cur = pd.Series(json.loads(m.iloc[-1]))
        print(f"{table}.{col} max LOB share shift vs prior run: {(cur - prev).abs().max():.4f}")


def freshness(t: dict, day: str) -> None:
    section("C2  Late / stale data?  (table_freshness)")
    f = t["fresh"][t["fresh"].run_date == day]
    late = f[f.freshness_status == "late"]
    print(late[["table", "last_loaded_at", "expected_by", "minutes_late"]].to_string(index=False))
    g = t["graph"]
    counts_path = nx.ancestors(g, "mart.fct_claim_closed") | {"mart.fct_claim_closed"}
    late_on_path = sorted(set(late.table) & counts_path)
    fct = f[f.table == "mart.fct_claim_closed"].iloc[0]
    print(f"\nLate tables upstream of mart.fct_claim_closed (where counts are computed): {late_on_path or 'none'}")
    print(f"mart.fct_claim_closed loaded {fct.last_loaded_at} -> {fct.freshness_status}")
    used = t["col_lineage"]
    print("Columns the storm feed contributes downstream:",
          used[used.upstream_table == "stg.storm_events"][["table", "column", "usage"]].to_dict("records"))


def job_failures(t: dict, day: str) -> None:
    section("C3  Failed or partial job runs?  (row_volume_history status)")
    v = t["vol"][t["vol"].run_date == day]
    print(v.groupby(["table", "status"]).size().unstack(fill_value=0).query("failed > 0").to_string())
    print(f"Final status of every table: {sorted(v.drop_duplicates('table', keep='last').status.unique())}")


def volume_anomalies(t: dict, day: str) -> None:
    section("C3b Volume anomalies, in lineage order  (row_volume_history)")
    v = t["vol"][t["vol"].status == "success"].drop_duplicates(["run_date", "table"], keep="last")
    hist = v[v.run_date < day].groupby("table").pct_change_vs_prev_run
    today = v[v.run_date == day].set_index("table").pct_change_vs_prev_run
    z = ((today - hist.mean()) / hist.std()).rename("z")
    out = pd.concat([today.rename("pct_change_today"), hist.std().rename("hist_std_pct"), z], axis=1)
    out["topo"] = out.index.map(t["topo"])
    out = out[(out.z.abs() > 4) & (out.pct_change_today.abs() > 1)].sort_values("topo").drop(columns="topo")
    print(out.round(2).to_string())
    print("\nNote: incremental tables (fct_claim_closed, stg.claim_status) swing with weekday mix;")
    print("full-rebuild tables (int.claim_policy_enriched) should move ~0.3%/day.")


def null_anomalies(t: dict, day: str) -> None:
    section("C4  Distribution drift, in lineage order  (column_profiles)")
    p = t["prof"]
    rows = []
    for (table, col), grp in p.groupby(["table", "column"]):
        grp = grp.set_index("profile_date")
        if day not in grp.index:
            continue
        hist = grp[grp.index < day]
        prev_mix = json.loads(hist.category_mix.iloc[-1]) if len(hist) and isinstance(hist.category_mix.iloc[-1], str) else {}
        cur_mix = json.loads(grp.loc[day, "category_mix"]) if isinstance(grp.loc[day, "category_mix"], str) else {}
        mix_shift = max((abs(cur_mix.get(k, 0) - prev_mix.get(k, 0)) for k in set(cur_mix) | set(prev_mix)), default=0)
        rows.append(dict(table=table, column=col, topo=t["topo"][table],
                         null_rate_prev=hist.null_rate.iloc[-1] if len(hist) else None,
                         null_rate_today=grp.loc[day, "null_rate"],
                         mean_prev=hist["mean"].iloc[-1] if len(hist) else None, mean_today=grp.loc[day, "mean"],
                         max_category_shift=round(mix_shift, 4), new_column=len(hist) == 0))
    df = pd.DataFrame(rows).sort_values(["topo", "table"])
    flagged = df[(df.max_category_shift > 0.05) | df.new_column
                 | ((df.null_rate_today - df.null_rate_prev).abs() > 0.01)]
    print(flagged.drop(columns="topo").to_string(index=False))


def schema_changes(t: dict, day: str) -> None:
    section("C5  Schema changes in the 48h before the run  (schema_change_log)")
    s = t["schema"]
    s = s[(s.detected_at >= f"{pd.Timestamp(day) - pd.Timedelta(days=2):%Y-%m-%d}") & (s.detected_at < f"{day}T06:00")]
    print(s[["change_id", "detected_at", "system", "object", "change_type", "column_name",
             "old_value", "new_value", "change_ticket", "author_team"]].to_string(index=False))


def column_impact(t: dict) -> None:
    section("C6  Which changed columns feed a join/filter on the counts path?  (lineage_column_level)")
    cl = t["col_lineage"]
    for table, col in [("raw.policy_snapshot", "pol_status_cd"), ("raw.policy_snapshot", "policy_status_code"),
                       ("raw.claim", "litigation_flag"), ("mart.agg_claim_severity_daily", "cat_event_id")]:
        frontier, chain = [(table, col)], []
        while frontier:
            tb, c = frontier.pop()
            nxt = cl[(cl.upstream_table == tb) & (cl.upstream_column == c)]
            for r in nxt.itertuples():
                chain.append(f"{tb}.{c} -> {r.table}.{r.column} [{r.usage}]")
                frontier.append((r.table, r.column))
        print(f"\n{table}.{col}:")
        print("\n".join(f"  {x}" for x in chain) or "  (no downstream consumers)")
    edge = t["graph"].edges["stg.policy", "int.claim_policy_enriched"]["relationship"]
    print(f"\nstg.policy -> int.claim_policy_enriched: {edge}")


def reconcile(t: dict, day: str, ratio: pd.Series) -> None:
    section("R1  Magnitude reconciliation")
    p = t["prof"]
    pol = p[(p.table == "raw.policy_snapshot") & (p.profile_date == day)].set_index("column")
    null_today = pol.loc["pol_status_cd", "null_rate"]
    new_pop = 1 - pol.loc["policy_status_code", "null_rate"]
    print(f"raw.policy_snapshot: pol_status_cd went NULL on {null_today:.2%} of rows; "
          f"new policy_status_code is populated on {new_pop:.2%}  (same rows: values moved columns)")
    lob_mix = pd.Series(json.loads(pol.loc["lob_cd", "category_mix"]))
    impacted = ratio[ratio < 0.5].index.tolist()
    print(f"Impacted LOBs (from S1): {impacted}; their share of policy book: {lob_mix[impacted].sum():.2%}")

    enr = p[(p.table == "int.claim_policy_enriched") & (p.column == "lob_cd")].set_index("profile_date")
    pre = pd.Series(json.loads(enr.category_mix.iloc[-2]))
    v = t["vol"][(t["vol"].table == "int.claim_policy_enriched") & (t["vol"].status == "success")]
    pre_rows, post_rows = v.table_row_count.iloc[-2], v.table_row_count.iloc[-1]
    post = pd.Series(json.loads(enr.category_mix.iloc[-1])).reindex(pre.index).fillna(0)
    survive = post * post_rows / (pre * pre_rows)
    print(f"int.claim_policy_enriched: {pre_rows:,.0f} -> {post_rows:,.0f} rows ({post_rows / pre_rows - 1:+.1%})")
    print("Share of each LOB's claims surviving the join:", survive.round(3).to_dict())
    expected = (pre[impacted] * (1 - survive[impacted])).sum()
    print(f"Expected loss from impacted LOBs x their drop-out rate: {expected:.1%} (observed {1 - post_rows / pre_rows:.1%})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--telemetry", type=Path, default=Path(__file__).resolve().parent.parent / "data" / "telemetry")
    ap.add_argument("--date", default="2026-10-16")
    a = ap.parse_args()
    t = load(a.telemetry)
    ratio = symptom(t, pd.Timestamp(a.date))
    business_decline(t, a.date)
    freshness(t, a.date)
    job_failures(t, a.date)
    volume_anomalies(t, a.date)
    null_anomalies(t, a.date)
    schema_changes(t, a.date)
    column_impact(t)
    reconcile(t, a.date, ratio)


if __name__ == "__main__":
    main()
