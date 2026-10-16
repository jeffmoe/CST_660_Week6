# Cost drivers: silent upstream schema change → partial load

**Incident class.** A source system renames or retypes a column that a downstream join or filter
depends on. The loader absorbs it as schema drift, every job reports success, and a
decision-grade metric is silently under-counted. INC-20261016 is the reference instance: the
PolicyCenter rename of `pol_status_cd` dropped 364 of 984 closed claims from the
claims-severity dashboard on the morning of a staffing decision.

All figures are reproduced by `python cost_driver/cost_model.py`. It reads the measured inputs
from `data/telemetry/` and `value_stream/`, and takes every other input from the assumptions
table in section 4. It writes `cost_per_incident.csv`, `annual_cost.csv`, `countermeasures.csv`,
`sensitivity.csv` and `assumptions.csv`.

## 1. Cost driver table (per incident)

Costs are in USD. *Low / base / high* use the matching column of the assumptions table for every
input at once, so *high* is a stacked worst case, not a likely outcome.

| Category | Driver | How it is calculated (base) | Low | Base | High | Share of base |
|---|---|---|---:|---:|---:|---:|
| Compute | Wasted nightly build on bad data | 23.6 min on 4 defective tables × $0.20/min | 3 | 5 | 6 | 0.0% |
| Compute | Rebuild and backfill | 28.0 min rebuild × 3 attempts × $0.20/min | 4 | 17 | 45 | 0.0% |
| Compute | Ad hoc investigation queries | 4 warehouse-hours (Medium) | 16 | 48 | 128 | 0.1% |
| **Compute subtotal** | | | **23** | **70** | **179** | **0.1%** |
| Rework | Incident response: engineers | 16 h × $100 | 850 | 1,600 | 3,500 | 3.3% |
| Rework | Incident response: team leads | 5 h × $130 | 330 | 650 | 1,440 | 1.4% |
| Rework | Postmortem and action items | 10 h × $100 | 510 | 1,000 | 2,000 | 2.1% |
| Rework | Analyst trust tax (manual cross-checks for ~2 weeks) | 20 h × $100 | 850 | 2,000 | 5,000 | 4.2% |
| **Rework subtotal** | | | **2,540** | **5,250** | **11,940** | **10.9%** |
| Business impact | Executive and ops re-planning | 2 h exec × $220 + 6 h ops manager × $130 | 510 | 1,220 | 3,040 | 2.5% |
| Business impact | Claim delay from under-staffing *(expected value)* | p = 0.5 × 283 deferred claims × 3 days × $60 per claim-day | 944 | 25,486 | 407,773 | 53.1% |
| Business impact | Overtime to clear the backlog *(expected value)* | p = 0.5 × 118 adjuster-days × $270 premium | 1,416 | 15,929 | 84,953 | 33.2% |
| **Business impact subtotal** | | | **2,870** | **42,634** | **495,766** | **88.9%** |
| **Total per incident** | | | **5,433** | **47,954** | **507,885** | 100% |

**What 10-16 actually cost.** The VP caught the anomaly before the staffing decision, so the two
expected-value lines were not realized this time. The realized cost is compute, rework and
re-planning: **about $6,540 (base)**. The other ~$41k (base) is the exposure the organization
carries on every occurrence where nobody happens to notice in time.

### How the under-staffing exposure is derived

- **Flexible capacity:** 414 adjusters (measured from `raw.adjuster_roster`) × 15% flexible (overtime and independent adjusters) = 62 adjuster-days per day.
- **Cut taken on the bad figure:** proportional to the apparent drop, 62 × 38% × 5 business days until the next weekly review = 118 adjuster-days.
- **Claims deferred:** 118 × 2.40 closures per adjuster-day (measured: 994 normal Friday closures ÷ 414 adjusters) = 283 claims.
- **Cost:** deferred claim-days × the blended cost of delay (LAE, rental for Personal Auto, additional living expenses for displaced Homeowners), plus the overtime premium to recover the lost adjuster-days. Both are weighted by the probability that someone acts on the bad number before it is caught.

## 2. Annual cost of the incident class (unmitigated)

Frequency = source renames per year × probability a rename hits a consumed column. The schema log
shows 2 PolicyCenter renames in 90 days. One of them (CHG-48213) hit a join filter; the other
(CHG-47102) had no downstream consumer.

| | Low | Base | High |
|---|---:|---:|---:|
| Incidents per year | 1.0 (4 × 0.25) | 4.0 (8 × 0.5) | 9.0 (12 × 0.75) |
| Compute | 23 | 278 | 1,612 |
| Rework | 2,540 | 21,000 | 107,460 |
| Business impact | 2,870 | 170,538 | 4,461,896 |
| **Total** | **5,433** | **191,816** | **4,570,968** |

### Key reading

- **Compute is not a cost driver.** It is at most 0.4% of the total in any scenario, and the base
  rebuild costs about $17. Any proposal justified by "saving compute" on this
  incident class is chasing the wrong number.
- **Business impact dominates (89%), and it comes from a decision made on bad data,** not from
  the data being wrong. That cost is avoided whenever the bad number never reaches the decision-maker.
- **Rework (11%) is mostly people time spread across three teams,** including the hidden
  "trust tax": analysts double-checking the dashboard for weeks afterwards.

### What the estimate is most sensitive to

One-at-a-time swings on base annual cost (`sensitivity.csv`). Each assumption is moved to its
low and high value while the others stay at base.

| Assumption | Low → high | Annual cost range |
|---|---|---:|
| Days mis-staffed before correction | 2 → 10 | $92k → $357k |
| Renames per year | 4 → 12 | $96k → $288k |
| P(rename hits a consumed column) | 0.25 → 0.75 | $96k → $288k |
| P(decision made on bad data) | 0.25 → 0.75 | $109k → $275k |
| Cost per claim-day of delay | $25 → $120 | $132k → $294k |
| All labor-rate and labor-hour assumptions | each | ≤ $12k swing each |

The top five are all about **how long a wrong number survives and how often one is created.**
Those two levers are what the countermeasures target.

## 3. Prioritized countermeasures

| | **#1 Pre-publish quality gate + data-ready publish** | **#2 Source data contract + rename detection at ingest** |
|---|---|---|
| What | Block the dashboard publish if: join row-loss > 1%, volume \|z\| > 5 on full-rebuild models, `not_null` fails on `stg.policy.policy_status`, or `fct_claim_closed` is more than 2% off ClaimCenter `CLOSED` events. Publish when the aggregate succeeds (05:30 becomes a deadline, not a start time). Make the storm feed non-blocking. If held, show the prior day's figure with a "data held" banner | A contract for `src_policycenter.policy` listing consumed columns, generated from the column lineage. PolicyCenter release CI fails on a rename or drop of a contracted column without a dual-write period. The loader flags "new column + null spike in a sibling column" as a probable rename and blocks downstream builds |
| Type | Detect and contain (stops a defect reaching a decision) | Prevent (stops the defect being created) |
| Owner | Data Engineering + Claims Analytics | Policy Platform + Data Engineering |
| Build | 80 h → **$8,000** | 120 h → **$12,000** |
| Run per year | **$5,165** (5 min/night of gate queries + 4 h/month upkeep) | **$5,600** (2 people × 2 h per source rename review + 2 h/month) |
| Effect assumed | Removes 95% of business impact, 60% of rework, 30% of compute on every occurrence | Prevents 80% of occurrences outright |
| Saved per incident (base) | $43,674 | $38,363 |
| Annual savings (low / base / high) | $4,257 / **$174,694** / $4.30M | $4,346 / **$153,453** / $3.66M |
| Year-1 net (base) | **+$161,529** | +$135,853 |
| Payback (base) | **0.6 months** | 1.0 month |
| Break-even frequency (base costs) | 0.3 incidents/yr | 0.5 incidents/yr |
| Time to value | 2–3 weeks, mostly one team | 6–8 weeks, cross-team |

### Why this order

1. **Gate first.** It has the largest savings, the fastest payback and the shortest time to value,
   and one team can ship it. It also covers *every* cause of a silent partial load, not just
   renames: bad joins, upstream truncation, late partitions, dedupe bugs. It directly removes
   bottleneck B2 from the value stream map. With publish triggered when data is ready (B1), a
   defect is caught at about 02:35 with roughly 6 hours to fix it before 08:40.
2. **Contract second.** It fixes the root cause rather than the symptom, and it is the only
   countermeasure that reduces the frequency term, which is the second-largest sensitivity. But
   *on top of* the gate its marginal economics are thin. The gate already removes most of each
   incident's cost, so the contract adds **$13.7k/yr of savings (base) for $17.6k in year 1, and
   about +$8.1k/yr from year 2**. It is still worth doing because:
   - The gate's 5% residual is a *held* dashboard, so on an incident day the staffing decision is
     made on yesterday's data.
   - The rework, war room and cross-team blame still happen with the gate alone.
   - In the high scenario the contract adds about $214k/yr.
   - It protects every other consumer of PolicyCenter, not just this dashboard.

   If frequency turns out to be at the *low* end (about 1 per year), defer #2. The gate alone
   covers the exposure.

| Combined (#1 + #2) | Low | Base | High |
|---|---:|---:|---:|
| Annual savings | 5,198 | 188,392 | 4,517,527 |
| Year-1 net | −25,567 | +157,627 | +4,486,762 |

**Low-scenario caveat.** At 1 incident per year with low unit costs, neither countermeasure pays
back in year 1: the gate needs about 3.1 incidents per year at low costs. The case for acting
rests on the base estimate of 4 per year, which is supported by the schema log (2 renames in 90
days, 1 of which hit a consumed column). That is a small sample, so re-check the frequency term
after two more quarters of schema-log data.

## 4. Stated assumptions

### Measured (from telemetry, no range)

| Quantity | Value | Source |
|---|---|---|
| Defective tables on the incident run | 4 | `row_volume_history` volume anomaly + lineage descendants |
| Wasted compute on defective tables | 23.6 min | `row_volume_history` start/end times, 10-16 |
| Rebuild compute (stg.policy → int → fct → agg → dashboard) | 28.0 min | `value_stream/vsm_rework_loops.csv` (median task durations) |
| Detection latency | 4 h 08 m | `int.claim_policy_enriched` built 02:32, VP report 06:40 |
| Closed claims: source vs dashboard | 984 vs 620 (364 missing) | `column_profiles` status mix, dashboard data |
| Normal Friday closures | 994 | Prior 4 Fridays, dashboard data |
| Adjuster roster | 414 | `row_volume_history` `raw.adjuster_roster` |
| Closures per adjuster-day | 2.40 | 994 ÷ 414 |
| Warehouse size | Medium (4 credits/h) | `WH_TRANSFORM_M`, named in the 09-03 timeout error |

### Assumed (low / base / high)

| Assumption | Low | Base | High | Basis |
|---|---:|---:|---:|---|
| Warehouse credit price ($/credit) | 2.00 | 3.00 | 4.00 | Cloud warehouse list-price range |
| Ad hoc investigation queries (warehouse-h) | 2 | 4 | 8 | Engineer estimate |
| Rebuild attempts (× targeted rebuild) | 1 | 3 | 6 | Failed fixes; full refresh of the incremental fact instead of one partition |
| Engineer loaded rate ($/h) | 85 | 100 | 125 | Regional carrier, loaded |
| Manager loaded rate ($/h) | 110 | 130 | 160 | Regional carrier, loaded |
| Executive loaded rate ($/h) | 180 | 220 | 280 | VP and operations leaders |
| Engineer hours, incident response | 10 | 16 | 28 | 6 engineers across 3 teams, triage → backfill |
| Team-lead hours, incident response | 3 | 5 | 9 | Three leads on the bridge |
| Executive hours | 1 | 2 | 4 | VP and staffing leads |
| Engineer hours, postmortem | 6 | 10 | 16 | Postmortem and verification |
| Analyst trust-tax hours | 10 | 20 | 40 | Manual cross-checks for ~2 weeks |
| Ops re-planning hours | 3 | 6 | 12 | Claims ops managers redo the staffing plan |
| P(decision made on bad data before detection) | 0.25 | 0.50 | 0.75 | No gate; caught this time only because the VP noticed |
| Flexible share of adjuster capacity | 0.10 | 0.15 | 0.20 | Overtime + independent adjusters |
| Business days mis-staffed | 2 | 5 | 10 | Until the next weekly review or reconciliation |
| Cost per claim-day of delay ($) | 25 | 60 | 120 | Blended LAE, rental (PA), additional living expenses (HO, displaced only) |
| Average extra delay per deferred claim (days) | 2 | 3 | 6 | Backlog recovery time |
| Overtime premium per adjuster-day ($) | 180 | 270 | 360 | 0.5× of ~$45/h × 8 h |
| Source renames per year | 4 | 8 | 12 | Schema log: 2 in 90 days |
| P(rename hits a consumed column) | 0.25 | 0.50 | 0.75 | Schema log + column lineage: 1 of 2 |
| Countermeasure #1 effect (business / rework / compute removed) | | 95% / 60% / 30% | | The gate blocks publish; diagnosis starts from a named failing test |
| Countermeasure #2 effect (occurrences prevented) | | 80% | | Releases that skip CI or contract review still slip through |

### Not quantified

- **Reserving and pricing contamination.** The 28.5% artificial drop in mean severity would
  understate loss trends if it fed a weekly actuarial extract.
- **Regulatory prompt-payment exposure** on claims delayed past statutory deadlines.
- **Policyholder experience and retention** effects of slower closures.
- **Opportunity cost** of the three teams' time beyond the hours counted above.

Each of these would increase the business-impact share. None changes the ranking of the two
countermeasures.
