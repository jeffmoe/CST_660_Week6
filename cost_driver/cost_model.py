"""Cost drivers for the incident class "silent upstream schema change -> partial load".

Quantities that the telemetry can measure (compute minutes, rework minutes, missing
claims, detection latency, adjuster headcount) are read from data/telemetry/ and from
the value-stream outputs. Everything else is an explicit assumption in ASSUMPTIONS,
with low / base / high values. Writes:
    cost_driver/assumptions.csv
    cost_driver/cost_per_incident.csv     cost driver table (one incident)
    cost_driver/annual_cost.csv           annualised, unmitigated
    cost_driver/countermeasures.csv       cost, effect and payback of each countermeasure

    python cost_driver/cost_model.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
TELEMETRY = ROOT / "data" / "telemetry"
INCIDENT = "2026-10-16"

sys.path.insert(0, str(ROOT / "value_stream"))
from vsm import defective_tables, load  # noqa: E402

SCENARIOS = ["low", "base", "high"]


def A(key, low, base, high, unit, basis):
    return dict(key=key, low=low, base=base, high=high, unit=unit, basis=basis)


# Every number not measured from telemetry. "basis" says where the value comes from.
ASSUMPTIONS = pd.DataFrame([
    # Compute
    A("credit_price", 2.00, 3.00, 4.00, "$/credit", "Cloud warehouse list price range, Standard to Business Critical"),
    A("credits_per_hour_m", 4, 4, 4, "credits/h", "Medium warehouse (WH_TRANSFORM_M, named in the 09-03 error)"),
    A("adhoc_query_hours", 2, 4, 8, "warehouse-h", "Ad hoc investigation and reconciliation queries on a Medium warehouse"),
    A("full_refresh_factor", 1, 3, 6, "x rebuild", "Times the targeted rebuild is repeated (failed fixes, full-refresh of fct instead of one partition)"),
    # Rework labour (hours by role during response, plus post-incident)
    A("eng_rate", 85, 100, 125, "$/h", "Loaded rate, data/platform engineer"),
    A("mgr_rate", 110, 130, 160, "$/h", "Loaded rate, engineering/analytics manager"),
    A("exec_rate", 180, 220, 280, "$/h", "Loaded rate, VP and claims operations leaders"),
    A("eng_hours_response", 10, 16, 28, "h", "Data Reliability, Data Eng (x2), Policy Platform, Claims Analytics (x2) during triage, fix and backfill"),
    A("mgr_hours_response", 3, 5, 9, "h", "Three team leads in the 'whose fault' bridge"),
    A("exec_hours_response", 1, 2, 4, "h", "VP Claims Ops and staffing leads re-planning"),
    A("eng_hours_postmortem", 6, 10, 16, "h", "Postmortem, action items, verification"),
    A("analyst_trust_hours", 10, 20, 40, "h", "Manual dashboard cross-checks by analysts for ~2 weeks after"),
    # Business impact
    A("p_acted_on", 0.25, 0.50, 0.75, "prob", "Chance a staffing decision is made on the bad figure before detection (no gate)"),
    A("flex_capacity_share", 0.10, 0.15, 0.20, "share", "Share of adjuster capacity that is flexible (overtime + independent adjusters)"),
    A("days_misstaffed", 2, 5, 10, "business days", "Until the next weekly staffing review or a reconciliation catches it"),
    A("closures_per_adj_day", None, None, None, "claims", "Measured: normal Friday closures / adjuster roster"),
    A("cost_per_claim_delay_day", 25, 60, 120, "$/claim-day", "Blended extra LAE, rental (PA) and additional living expense (HO, displaced only) per claim-day of delay"),
    A("avg_delay_days", 2, 3, 6, "days", "Average extra days a deferred claim waits for capacity"),
    A("catchup_premium_per_adj_day", 180, 270, 360, "$/adjuster-day", "Overtime premium (0.5x of ~$45/h x 8h) to recover the backlog"),
    A("ops_replan_hours", 3, 6, 12, "h", "Claims ops managers redoing the staffing plan on corrected data"),
    # Frequency of the incident class
    A("source_renames_per_year", 4, 8, 12, "renames/yr", "Schema log: 2 PolicyCenter renames in 90 days (CHG-47102, CHG-48213)"),
    A("p_rename_hits_consumed_col", 0.25, 0.50, 0.75, "prob", "Schema log + column lineage: 1 of 2 renames hit a consumed column"),
])


def measured() -> dict:
    """Quantities taken from the telemetry and value-stream outputs (single values, no range)."""
    g, vol, fresh = load(TELEMETRY)
    prof = pd.read_csv(TELEMETRY / "column_profiles.csv")
    dash = pd.read_csv(TELEMETRY / "dashboard_claims_severity_daily.csv")
    rework = pd.read_csv(ROOT / "value_stream" / "vsm_rework_loops.csv")
    stages = pd.read_csv(ROOT / "value_stream" / "vsm_stage_metrics.csv")

    day = vol[(vol.run_date == INCIDENT) & (vol.status == "success")].set_index("table")
    bad = defective_tables(g, vol, INCIDENT)
    wasted_min = sum((day.loc[t].ended_at - day.loc[t].started_at).total_seconds() / 60 for t in bad)

    ev = prof[(prof.table == "raw.claim_status_history") & (prof.profile_date == INCIDENT)].iloc[0]
    source_closed = round(ev.row_count * json.loads(ev.category_mix)["CLOSED"])
    shown_closed = int(dash[dash.close_date == INCIDENT].closed_count.sum())

    defect = rework[rework.loop == "Defect escaped to consumer"].iloc[0]
    fridays = dash.assign(d=pd.to_datetime(dash.close_date)).query("d.dt.dayofweek == 4 and close_date < @INCIDENT")
    normal_friday = fridays.groupby("close_date").closed_count.sum().tail(4).mean()
    adjusters = int(day.loc["raw.adjuster_roster"].rows_affected)
    nightly_pt = stages.process_median_min.sum()
    return dict(
        defective_tables=len(bad), wasted_compute_min=round(wasted_min, 1),
        rebuild_compute_min=float(defect.rebuild_compute_min),
        detection_latency_min=float(defect.detection_latency_min),
        source_closed=source_closed, shown_closed=shown_closed, missing_closed=source_closed - shown_closed,
        normal_friday_closed=round(normal_friday, 1), adjusters=adjusters,
        closures_per_adj_day=round(normal_friday / adjusters, 2),
        nightly_critical_path_min=round(nightly_pt, 1),
    )


def per_incident(a: dict, m: dict) -> list[dict]:
    """Cost lines for one incident under one scenario. a = assumption values for that scenario."""
    wh = a["credits_per_hour_m"] * a["credit_price"] / 60  # $ per Medium-warehouse minute
    flex_adj = m["adjusters"] * a["flex_capacity_share"]
    adj_days_cut = flex_adj * 0.38 * a["days_misstaffed"]  # cut in proportion to the apparent 38% drop
    deferred_claims = adj_days_cut * m["closures_per_adj_day"]
    rows = [
        ("Compute", "Wasted nightly build on bad data",
         m["wasted_compute_min"] * wh, f"{m['wasted_compute_min']} min on {m['defective_tables']} tables x ${wh:.2f}/min"),
        ("Compute", "Rebuild and backfill",
         m["rebuild_compute_min"] * a["full_refresh_factor"] * wh,
         f"{m['rebuild_compute_min']} min x {a['full_refresh_factor']} attempts x ${wh:.2f}/min"),
        ("Compute", "Ad hoc investigation queries", a["adhoc_query_hours"] * 60 * wh,
         f"{a['adhoc_query_hours']} warehouse-h"),
        ("Rework", "Incident response: engineers", a["eng_hours_response"] * a["eng_rate"],
         f"{a['eng_hours_response']} h x ${a['eng_rate']}"),
        ("Rework", "Incident response: team leads", a["mgr_hours_response"] * a["mgr_rate"],
         f"{a['mgr_hours_response']} h x ${a['mgr_rate']}"),
        ("Rework", "Postmortem and action items", a["eng_hours_postmortem"] * a["eng_rate"],
         f"{a['eng_hours_postmortem']} h x ${a['eng_rate']}"),
        ("Rework", "Analyst trust tax (manual cross-checks)", a["analyst_trust_hours"] * a["eng_rate"],
         f"{a['analyst_trust_hours']} h x ${a['eng_rate']}"),
        ("Business impact", "Executive and ops re-planning",
         a["exec_hours_response"] * a["exec_rate"] + a["ops_replan_hours"] * a["mgr_rate"],
         f"{a['exec_hours_response']} h exec + {a['ops_replan_hours']} h ops mgr"),
        ("Business impact", "Claim delay from under-staffing (expected)",
         a["p_acted_on"] * deferred_claims * a["avg_delay_days"] * a["cost_per_claim_delay_day"],
         f"p={a['p_acted_on']} x {deferred_claims:,.0f} deferred claims x {a['avg_delay_days']} d x "
         f"${a['cost_per_claim_delay_day']}"),
        ("Business impact", "Overtime to clear backlog (expected)",
         a["p_acted_on"] * adj_days_cut * a["catchup_premium_per_adj_day"],
         f"p={a['p_acted_on']} x {adj_days_cut:,.0f} adjuster-days x ${a['catchup_premium_per_adj_day']}"),
    ]
    return [dict(category=c, driver=d, cost=round(v, 2), calc=calc) for c, d, v, calc in rows]


def countermeasures(per: dict[str, pd.DataFrame], freq: dict, a_base: dict) -> pd.DataFrame:
    """Expected annual savings and payback. Effects are stated as share of each category removed."""
    eng = a_base["eng_rate"]
    wh = a_base["credits_per_hour_m"] * a_base["credit_price"] / 60
    cms = [
        dict(priority=1, countermeasure="Pre-publish quality gate + data-ready publish",
             what="Block publish on join row-loss > 1%, volume |z| > 5 on full-rebuild models, not_null on "
                  "stg.policy.policy_status, fct vs ClaimCenter CLOSED within 2%; publish on aggregate success "
                  "with 05:30 as deadline; storm feed non-blocking",
             owner="Data Engineering + Claims Analytics",
             build_cost=80 * eng, run_cost=5 * 365 * wh + 4 * 12 * eng,
             # Detects this class at build: removes expected mis-staffing, most re-planning, most response time.
             effect={"Compute": 0.30, "Rework": 0.60, "Business impact": 0.95}, prevents=0.0,
             time_to_value="2-3 weeks"),
        dict(priority=2, countermeasure="Source data contract + rename detection at ingest",
             what="Contract for src_policycenter.policy listing consumed columns (from lineage); "
                  "CI check on PolicyCenter releases; loader flags 'new column + null spike' as a probable "
                  "rename and blocks downstream builds",
             owner="Policy Platform + Data Engineering",
             build_cost=120 * eng, run_cost=a_base["source_renames_per_year"] * 2 * 2 * eng + 2 * 12 * eng,
             effect={"Compute": 0.0, "Rework": 0.0, "Business impact": 0.0}, prevents=0.80,
             time_to_value="6-8 weeks (cross-team)"),
    ]
    def saved(cm, cats: pd.Series) -> float:
        """Per-incident cost avoided: prevented share outright, plus the effect on what still occurs."""
        residual = 1 - cm["prevents"]
        return cm["prevents"] * cats.sum() + residual * sum(cats[c] * e for c, e in cm["effect"].items())

    # Both together: the contract prevents 80%, the gate mitigates the 20% that slips through.
    both = dict(priority="1+2", countermeasure="Both (gate catches what the contract misses)",
                what="", owner="", build_cost=cms[0]["build_cost"] + cms[1]["build_cost"],
                run_cost=cms[0]["run_cost"] + cms[1]["run_cost"], prevents=cms[1]["prevents"],
                effect=cms[0]["effect"], time_to_value="")
    rows = []
    for cm in cms + [both]:
        sv = {s: saved(cm, per[s].groupby("category").cost.sum()) for s in per}
        annual = {s: sv[s] * freq[s] for s in sv}
        rows.append(dict(
            priority=cm["priority"], countermeasure=cm["countermeasure"], what=cm["what"], owner=cm["owner"],
            build_cost=round(cm["build_cost"]), annual_run_cost=round(cm["run_cost"]),
            saved_per_incident_base=round(sv["base"]),
            **{f"annual_savings_{s}": round(annual[s]) for s in sv},
            **{f"net_year1_{s}": round(annual[s] - cm["build_cost"] - cm["run_cost"]) for s in sv},
            payback_months_low=round(12 * cm["build_cost"] / (annual["low"] - cm["run_cost"]), 1)
            if annual["low"] > cm["run_cost"] else None,
            payback_months_base=round(12 * cm["build_cost"] / (annual["base"] - cm["run_cost"]), 1),
            time_to_value=cm["time_to_value"],
        ))
    return pd.DataFrame(rows)


def main() -> None:
    m = measured()
    a_all = ASSUMPTIONS.set_index("key")
    a_all.loc["closures_per_adj_day", SCENARIOS] = m["closures_per_adj_day"]

    tables = {}
    for s in SCENARIOS:
        a = a_all[s].to_dict()
        tables[s] = pd.DataFrame(per_incident(a, m))
    per = tables["base"][["category", "driver", "calc"]].copy()
    for s in SCENARIOS:
        per[s] = tables[s].cost.values
    total = per.groupby("category")[SCENARIOS].sum()
    per["share_of_base"] = (per.base / per.base.sum()).round(3)

    freq = {s: float(a_all.loc["source_renames_per_year", s] * a_all.loc["p_rename_hits_consumed_col", s])
            for s in SCENARIOS}
    annual = total.copy()
    for s in SCENARIOS:
        annual[s] = total[s] * freq[s]
    annual.loc["Total"] = annual.sum()

    cms = countermeasures(tables, freq, a_all["base"].to_dict())

    # One-at-a-time sensitivity of base annual cost: swing each assumption to its low and high.
    def annual_total(a: dict) -> float:
        cost = pd.DataFrame(per_incident(a, m)).cost.sum()
        return cost * a["source_renames_per_year"] * a["p_rename_hits_consumed_col"]

    base_a = a_all["base"].to_dict()
    sens = []
    for key in a_all.index:
        lo, hi = a_all.loc[key, "low"], a_all.loc[key, "high"]
        if lo == hi:
            continue
        sens.append(dict(assumption=key, low_value=lo, high_value=hi,
                         annual_at_low=round(annual_total({**base_a, key: lo})),
                         annual_at_high=round(annual_total({**base_a, key: hi}))))
    sens = pd.DataFrame(sens)
    sens["swing"] = sens.annual_at_high - sens.annual_at_low
    sens = sens.sort_values("swing", ascending=False)
    sens.to_csv(HERE / "sensitivity.csv", index=False)

    ASSUMPTIONS.assign(**{s: a_all[s].values for s in SCENARIOS}).to_csv(HERE / "assumptions.csv", index=False)
    per.to_csv(HERE / "cost_per_incident.csv", index=False)
    annual.round(0).to_csv(HERE / "annual_cost.csv")
    cms.to_csv(HERE / "countermeasures.csv", index=False)

    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 90)
    print("Measured from telemetry:", json.dumps(m, indent=1))
    print("\nPer incident:\n", per.round(0).to_string(index=False))
    print("\nPer incident by category:\n", total.round(0).assign(base_share=(total.base / total.base.sum()).round(3)))
    print(f"\nTotal per incident: " + ", ".join(f"{s} ${total[s].sum():,.0f}" for s in SCENARIOS))
    print("Incidents/yr:", {s: round(f, 2) for s, f in freq.items()})
    print("\nAnnual (unmitigated):\n", annual.round(0))
    print("\nCountermeasures:\n", cms.drop(columns=["what"]).to_string(index=False))
    print("\nSensitivity of base annual cost:\n", sens.to_string(index=False))


if __name__ == "__main__":
    main()
