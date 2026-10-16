"""Value stream map of the claims-severity analytics delivery flow.

Measures lead time, process time, waits, first-pass quality and rework from
data/telemetry/ only, then writes:
    value_stream/vsm_stage_metrics.csv   per-stage medians over normal runs
    value_stream/vsm_rework_loops.csv    every rework event found in the telemetry
    value_stream/vsm.svg                 the map

    python value_stream/vsm.py [--telemetry data/telemetry] [--incident 2026-10-16]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import networkx as nx  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Polygon  # noqa: E402

HERE = Path(__file__).resolve().parent

# Stage of the stream each pipeline table belongs to.
STAGE_OF_LAYER = {"raw": "Ingest", "staging": "Stage", "intermediate": "Enrich", "exposure": "Publish"}
STAGE_OF_TABLE = {"mart.agg_claim_severity_daily": "Aggregate"}  # other marts -> "Model"
STAGES = ["Ingest", "Stage", "Enrich", "Model", "Aggregate", "Publish"]

# Facts that are not in the telemetry. Each is labelled where it is used.
ASSUMED_ACCRUAL_MIN = 13 * 60  # closures centred ~13:00 on D-1 -> 01:55 cutoff on D
VP_OPENED = "06:40"  # scenario: VP reviews the dashboard (observed 10-16 only)
DECISION_AT = "08:40"  # scenario: adjuster staffing decision

# Reference palette (dataviz skill), three categorical slots validated all-pairs.
C_PROCESS, C_WAIT, C_REWORK = "#2a78d6", "#eb6834", "#4a3aa7"
INK, INK_2, INK_3, RULE, SURFACE = "#0b0b0b", "#52514e", "#8a8984", "#e4e3df", "#fcfcfb"


# --------------------------------------------------------------------------- data


def load(tdir: Path):
    g = nx.node_link_graph(json.loads((tdir / "lineage_graph.json").read_text()), edges="edges")
    vol = pd.read_csv(tdir / "row_volume_history.csv")
    for c in ("started_at", "ended_at"):
        vol[c] = pd.to_datetime(vol[c])
    fresh = pd.read_csv(tdir / "table_freshness.csv")
    for c in ("last_loaded_at", "data_watermark", "expected_by"):
        fresh[c] = pd.to_datetime(fresh[c])
    return g, vol, fresh


def stage_of(g: nx.DiGraph, table: str) -> str | None:
    layer = g.nodes[table]["layer"]
    if layer == "source":
        return None
    return STAGE_OF_TABLE.get(table) or STAGE_OF_LAYER.get(layer, "Model")


def minutes(a, b) -> float:
    return (b - a).total_seconds() / 60


def critical_path(g: nx.DiGraph, runs: pd.DataFrame, fresh: pd.DataFrame, sink: str) -> list[dict]:
    """Walk back from the sink, always taking the parent that finished last."""
    ok = runs[runs.status == "success"].set_index("table")
    src_end = fresh.set_index("table").data_watermark  # source extract cutoff, inherited by raw
    path, node = [], sink
    while True:
        r = ok.loc[node]
        parents = [p for p in g.predecessors(node) if g.nodes[p]["layer"] != "source"]
        if not parents:
            ready = src_end.loc[node]
            prev = next(g.predecessors(node))
        else:
            prev = max(parents, key=lambda p: ok.loc[p].ended_at)
            ready = ok.loc[prev].ended_at
        first_start = runs[runs.table == node].started_at.min()
        path.append(dict(table=node, stage=stage_of(g, node), ready=ready, started=first_start,
                         ended=r.ended_at, wait_min=minutes(ready, first_start),
                         process_min=minutes(r.started_at, r.ended_at),
                         rework_min=minutes(first_start, r.started_at), upstream=prev))
        if not parents:
            return path[::-1]
        node = prev


def defective_tables(g: nx.DiGraph, vol: pd.DataFrame, day: str) -> set[str]:
    """Full-rebuild tables with a volume anomaly (|z|>4 and |change|>1%), plus everything downstream."""
    ok = vol[vol.status == "success"].drop_duplicates(["run_date", "table"], keep="last")
    ok = ok[ok.materialization.isin(["table", "full_refresh"])]
    hist = ok[ok.run_date < day].groupby("table").pct_change_vs_prev_run
    today = ok[ok.run_date == day].set_index("table").pct_change_vs_prev_run
    z = (today - hist.mean()) / hist.std()
    bad = set(z[(z.abs() > 4) & (today.abs() > 1)].index)
    return bad | {d for t in bad for d in nx.descendants(g, t)}


# --------------------------------------------------------------------------- metrics


def measure(g, vol, fresh, incident: str):
    sink = "bi.claims_severity_dashboard"
    days = sorted(vol.run_date.unique())
    paths = {d: critical_path(g, vol[vol.run_date == d], fresh[fresh.run_date == d], sink) for d in days}

    # Per-stage first-pass quality across every table in the stage, every run.
    late = fresh[fresh.freshness_status == "late"].groupby("run_date").table.apply(set)
    failed = vol[vol.status == "failed"].groupby("run_date").table.apply(set)
    fpy = {s: 0 for s in STAGES}
    for d in days:
        bad = defective_tables(g, vol, d) | late.get(d, set()) | failed.get(d, set())
        for s in STAGES:
            fpy[s] += not any(stage_of(g, t) == s for t in bad)

    rows = []
    normal = [d for d in days if d != incident]
    for s in STAGES:
        recs = [[p for p in paths[d] if p["stage"] == s] for d in normal]
        pt = pd.Series([sum(x["process_min"] for x in r) for r in recs])
        wt = pd.Series([sum(x["wait_min"] for x in r) for r in recs])
        inc = [p for p in paths[incident] if p["stage"] == s]
        tables = sorted({x["table"] for r in recs for x in r})
        rows.append(dict(
            stage=s, critical_path_tables_seen=", ".join(tables),
            owner=g.nodes[tables[0]]["owner"] if tables else None,
            wait_before_median_min=round(wt.median(), 1), wait_before_p90_min=round(wt.quantile(0.9), 1),
            process_median_min=round(pt.median(), 1), process_p90_min=round(pt.quantile(0.9), 1),
            first_pass_runs=fpy[s], runs=len(days), pct_complete_accurate=round(100 * fpy[s] / len(days), 1),
            incident_wait_min=round(sum(x["wait_min"] + x["rework_min"] for x in inc), 1),
            incident_process_min=round(sum(x["process_min"] for x in inc), 1),
            incident_path=" > ".join(x["table"] for x in inc),
        ))
    stages = pd.DataFrame(rows)

    def lead(d):
        p = paths[d]
        return dict(cutoff=p[0]["ready"], published=p[-1]["ended"],
                    lead_min=minutes(p[0]["ready"], p[-1]["ended"]),
                    process_min=sum(x["process_min"] for x in p))
    leads = pd.DataFrame({d: lead(d) for d in days}).T
    leads["pce"] = leads.process_min / leads.lead_min

    rework = rework_loops(g, vol, fresh, paths, incident)
    return stages, leads, rework, paths


def rework_loops(g, vol, fresh, paths, incident) -> pd.DataFrame:
    out = []
    for (d, t), grp in vol.groupby(["run_date", "table"]):
        fails = grp[grp.status == "failed"]
        if fails.empty:
            continue
        good = grp[grp.status == "success"].iloc[0]
        on_path = t in {p["table"] for p in paths[d]}
        out.append(dict(run_date=d, loop="Retry after failed attempt", stage=stage_of(g, t), table=t,
                        failed_attempts=len(fails), error=fails.error.iloc[0],
                        wasted_process_min=round(sum(minutes(a, b) for a, b in zip(fails.started_at, fails.ended_at)), 1),
                        delay_to_success_min=round(minutes(fails.started_at.min(), good.started_at), 1),
                        on_critical_path=on_path))
    for d in sorted(vol.run_date.unique()):
        bad = defective_tables(g, vol, d)
        if not bad:
            continue
        ok = vol[(vol.run_date == d) & (vol.status == "success")].set_index("table")
        first = min(bad, key=lambda t: ok.loc[t].ended_at)
        produced = ok.loc[first].ended_at
        detected = pd.Timestamp(f"{d}T{VP_OPENED}:00-05:00")
        # Rebuild = the defective table's parents in its stage plus everything it feeds.
        normal = vol[(vol.run_date != incident) & (vol.status == "success")]
        med = (normal.ended_at - normal.started_at).dt.total_seconds().groupby(normal.table).median() / 60
        rebuild = ["stg.policy"] + sorted(bad, key=lambda t: ok.loc[t].ended_at)
        out.append(dict(run_date=d, loop="Defect escaped to consumer", stage=stage_of(g, first), table=first,
                        failed_attempts=0, error="Silent row loss (job status: success)",
                        wasted_process_min=round(sum(minutes(ok.loc[t].started_at, ok.loc[t].ended_at) for t in bad), 1),
                        delay_to_success_min=None, on_critical_path=True,
                        detection_latency_min=round(minutes(produced, detected), 1),
                        rebuild_compute_min=round(med[rebuild].sum(), 1), rebuild_tables=" > ".join(rebuild)))
    late_src = fresh[(fresh.layer == "raw") & (fresh.freshness_status == "late")]
    for r in late_src.itertuples():
        if r.table == "raw.storm_events":
            continue  # already counted as a retry loop
        out.append(dict(run_date=r.run_date, loop="Late source (wait, no rework)", stage="Ingest", table=r.table,
                        failed_attempts=0, error=None, wasted_process_min=0,
                        delay_to_success_min=r.minutes_late, on_critical_path=False))
    return pd.DataFrame(out).sort_values("run_date").reset_index(drop=True)


# --------------------------------------------------------------------------- drawing


def fmt(m: float) -> str:
    m = round(m)
    return f"{m // 60} h {m % 60:02d} m" if m >= 60 else f"{m} m"


def draw(stages: pd.DataFrame, leads: pd.DataFrame, rework: pd.DataFrame, incident: str, out: Path,
         preview: Path | None = None) -> None:
    normal = leads.drop(index=incident)
    pub_med = normal.published.map(lambda t: t.hour * 60 + t.minute + t.second / 60).median()
    decision_min = int(DECISION_AT[:2]) * 60 + int(DECISION_AT[3:])
    consume_wait = decision_min - pub_med

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "svg.fonttype": "none"})
    fig, ax = plt.subplots(figsize=(17, 9.4))
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_xlim(0, 17)
    ax.set_ylim(0, 9.4)
    ax.axis("off")

    ax.text(0.3, 9.05, "Value stream map: closed-claims severity, ClaimCenter → staffing decision",
            fontsize=14, fontweight="bold", color=INK)
    ax.text(0.3, 8.72, f"Medians over {len(normal)} normal nightly runs (2026-07-18 to 2026-10-15), "
            f"critical path through the lineage DAG. Incident run {incident} shown in brackets.",
            fontsize=9.5, color=INK_2)

    # Boxes: customer/supplier ends plus the six pipeline stages.
    boxes = [("ClaimCenter\n+ PolicyCenter", "supplier")] + [(s, "stage") for s in STAGES] + \
            [("Claims Ops\nstaffing", "customer")]
    w, h, y = 1.38, 0.95, 5.9
    xs = [0.35 + i * 2.06 for i in range(len(boxes))]
    st = stages.set_index("stage")
    for (label, kind), x in zip(boxes, xs):
        edge = INK_2 if kind != "stage" else C_PROCESS
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                    fc="white", ec=edge, lw=1.6 if kind == "stage" else 1.1))
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center", fontsize=10.5 if kind == "stage" else 9,
                fontweight="bold", color=INK)
        if kind == "stage":
            r = st.loc[label]
            info = (f"PT  {fmt(r.process_median_min)}  (p90 {fmt(r.process_p90_min)})\n"
                    f"%C&A  {r.pct_complete_accurate:.1f}%\n"
                    f"Owner  {r.owner}")
            ax.add_patch(FancyBboxPatch((x, y - 1.35), w, 1.18, boxstyle="square,pad=0", fc=SURFACE,
                                        ec=RULE, lw=0.8))
            ax.text(x + 0.07, y - 0.24, info, va="top", fontsize=7.6, color=INK_2, linespacing=1.55)

    # Wait triangles between boxes.
    waits = [ASSUMED_ACCRUAL_MIN + float(st.loc["Ingest"].wait_before_median_min)] + \
            [float(st.loc[s].wait_before_median_min) for s in STAGES[1:]] + [consume_wait]
    inc_waits = [None] + [float(st.loc[s].incident_wait_min) for s in STAGES[1:]] + [None]
    for i, wmin in enumerate(waits):
        cx = xs[i] + w + (xs[i + 1] - xs[i] - w) / 2
        ax.add_patch(FancyArrowPatch((xs[i] + w + 0.02, y + h / 2), (xs[i + 1] - 0.02, y + h / 2),
                                     arrowstyle="-|>", mutation_scale=11, color=INK_3, lw=1))
        big = wmin >= 60
        s = 0.2 if big else 0.13
        ax.add_patch(Polygon([(cx - s, y + h + 0.12), (cx + s, y + h + 0.12), (cx, y + h + 0.12 + 1.6 * s)],
                             closed=True, fc=C_WAIT, ec="none"))
        ax.text(cx, y + h + 0.12 + 0.6 * s, "I", ha="center", va="center", fontsize=7, color="white",
                fontweight="bold")
        label = fmt(wmin) + (" *" if i in (0, len(waits) - 1) else "")
        if inc_waits[i] is not None and abs(inc_waits[i] - wmin) > 5:
            label += f"\n[{fmt(inc_waits[i])}]"
        ax.text(cx, y + h + 0.2 + 1.6 * s, label, ha="center", va="bottom", fontsize=8, color=INK,
                fontweight="bold" if big else "normal")

    # Bottleneck call-outs.
    def callout(x, yy, text):
        ax.text(x, yy, text, ha="center", va="center", fontsize=8.5, color=INK, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.35", fc="#fff3ec", ec=C_WAIT, lw=1.3))

    pub_x = xs[6] - (xs[6] - xs[5] - w) / 2
    callout(pub_x, 8.2, "⚠ B1  Scheduled publish wait")
    detect = rework[rework.loop == "Defect escaped to consumer"].iloc[0]
    callout((xs[7] + 0.5 + xs[2] + 0.7) / 2, 2.95, "⚠ B2  Defects detected by the consumer, not the pipeline")

    # Rework loops (violet), drawn under the data boxes.
    def loop(x0, x1, yy, text, rad):
        ax.add_patch(FancyArrowPatch((x0, y - 1.4), (x1, y - 1.4), connectionstyle=f"arc3,rad={rad}",
                                     arrowstyle="-|>", mutation_scale=12, color=C_REWORK, lw=1.6,
                                     linestyle=(0, (4, 2))))
        ax.text((x0 + x1) / 2, yy, text, ha="center", va="center", fontsize=7.8, color=INK,
                bbox=dict(boxstyle="round,pad=0.25", fc="white", ec=C_REWORK, lw=0.8))

    retries = rework[rework.loop == "Retry after failed attempt"]
    noaa = retries[retries.table == "raw.storm_events"].iloc[0]
    tmo = retries[retries.table != "raw.storm_events"].iloc[0]
    loop(xs[1] + 0.25, xs[1] + 1.1, 3.95,
         f"R1 NOAA retry ×{noaa.failed_attempts} ({noaa.run_date[5:]})\n+{fmt(noaa.delay_to_success_min)} to success", 1.2)
    loop(xs[2] + 0.25, xs[2] + 1.1, 3.95,
         f"R2 warehouse timeout\nrerun ({tmo.run_date[5:]}) +{fmt(tmo.delay_to_success_min)}", 1.2)
    loop(xs[7] + 0.5, xs[2] + 0.7, 3.5,
         f"R3 defect escape ({detect.run_date[5:]}): detected {fmt(detect.detection_latency_min)} after it was produced → "
         f"diagnose → hotfix → rebuild {fmt(detect.rebuild_compute_min)} compute → reconcile → republish", -0.18)

    # Timeline ladder.
    ly, lh = 1.35, 0.42
    ax.text(0.3, ly + lh + 0.55, "Lead-time ladder (normal run)", fontsize=10, fontweight="bold", color=INK)
    seg_x = xs[0] + w
    total_w = xs[-1] - seg_x
    seq = []
    for i, wmin in enumerate(waits):
        seq.append(("wait", wmin))
        if i < len(STAGES):
            seq.append(("process", float(st.loc[STAGES[i]].process_median_min)))
    # Accrual dominates: draw it on a broken scale so the in-pipeline steps stay readable.
    unit = total_w / (sum(m for _, m in seq[1:]) + 120)
    cur = seg_x
    for k, (kind, m) in enumerate(seq):
        width = 120 * unit if k == 0 else max(m * unit, 0.03)
        yy = ly + lh if kind == "wait" else ly
        ax.plot([cur, cur + width], [yy, yy], color=C_WAIT if kind == "wait" else C_PROCESS, lw=3,
                solid_capstyle="butt")
        ax.plot([cur + width, cur + width], [ly, ly + lh], color=RULE, lw=0.8)
        if k == 0:
            ax.text(cur + width / 2, yy + 0.12, "≈13 h accrual* (axis broken)", ha="center", fontsize=7.5,
                    color=INK_2)
        elif kind == "wait" and m >= 60:
            ax.text(cur + width / 2, yy + 0.12, fmt(m), ha="center", fontsize=8, color=INK, fontweight="bold")
        cur += width

    in_pipe = normal.lead_min.median()
    pt = normal.process_min.median()
    decision_lead = in_pipe + consume_wait
    summary = (f"Cutoff → dashboard (measured):  lead {fmt(in_pipe)}  ·  process {fmt(pt)}  ·  "
               f"PCE {pt / in_pipe:.0%}\n"
               f"Cutoff → staffing decision:  lead {fmt(decision_lead)}  ·  PCE {pt / decision_lead:.0%}      "
               f"Event → decision incl. accrual*:  ≈{fmt(decision_lead + ASSUMED_ACCRUAL_MIN)}  ·  "
               f"PCE {pt / (decision_lead + ASSUMED_ACCRUAL_MIN):.1%}\n"
               f"[{incident}: cutoff → dashboard {fmt(leads.loc[incident].lead_min)}, critical path via NOAA "
               f"storm feed; data was wrong]")
    ax.text(0.3, 0.55, summary, fontsize=9, color=INK, va="center", linespacing=1.6)

    # Legend.
    lx, lgy = 12.7, 1.0
    for i, (c, t, style) in enumerate([(C_PROCESS, "Process time", "-"), (C_WAIT, "Wait / queue", "-"),
                                       (C_REWORK, "Rework loop", "--")]):
        ax.plot([lx, lx + 0.35], [lgy - i * 0.25] * 2, color=c, lw=3 if style == "-" else 1.6, linestyle=style)
        ax.text(lx + 0.45, lgy - i * 0.25, t, va="center", fontsize=8.5, color=INK)
    ax.text(lx + 2.0, lgy, "* not in telemetry:", fontsize=7.5, color=INK_2, va="center")
    ax.text(lx + 2.0, lgy - 0.25, "accrual assumed; consume", fontsize=7.5, color=INK_2, va="center")
    ax.text(lx + 2.0, lgy - 0.5, f"wait to {DECISION_AT} decision", fontsize=7.5, color=INK_2, va="center")

    fig.savefig(out, format="svg", facecolor=SURFACE, bbox_inches="tight")
    if preview:
        fig.savefig(preview, dpi=110, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--telemetry", type=Path, default=HERE.parent / "data" / "telemetry")
    ap.add_argument("--incident", default="2026-10-16")
    ap.add_argument("--preview", type=Path, help="also write a PNG of the map here")
    a = ap.parse_args()
    g, vol, fresh = load(a.telemetry)
    stages, leads, rework, paths = measure(g, vol, fresh, a.incident)
    stages.to_csv(HERE / "vsm_stage_metrics.csv", index=False)
    rework.to_csv(HERE / "vsm_rework_loops.csv", index=False)
    draw(stages, leads, rework, a.incident, HERE / "vsm.svg", a.preview)

    pd.set_option("display.width", 220)
    print(stages.drop(columns=["critical_path_tables_seen", "incident_path"]).to_string(index=False))
    normal = leads.drop(index=a.incident)
    print(f"\nCutoff -> dashboard: lead median {normal.lead_min.median():.1f} min (p90 "
          f"{normal.lead_min.astype(float).quantile(0.9):.1f}), process median {normal.process_min.median():.1f} min, "
          f"PCE {normal.pce.median():.1%}")
    print(f"Incident: lead {leads.loc[a.incident].lead_min:.1f} min; path "
          + " > ".join(p['table'] for p in paths[a.incident]))
    print("\n" + rework.to_string(index=False))


if __name__ == "__main__":
    main()
