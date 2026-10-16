# INC-20261016: Closed-claims drop on the claims-severity dashboard

| | |
|---|---|
| **Reported** | 2026-10-16 06:40 CDT by the VP of Claims Operations |
| **Impact** | The dashboard shows 620 closed claims for Fri 10-16, 37.6% below the prior four Fridays (avg 994). Adjuster staffing decisions were due at 08:40. |
| **Severity** | SEV-2: a decision-grade metric is wrong, and the pipeline reports success |
| **Status** | Root cause identified. Fix and backfill are described in section 7 |
| **Evidence** | `data/telemetry/*`. Every number in this document is reproduced by `python incident/investigate.py` (section codes such as S1 and C3 refer to that script's output) |

## 1. Summary for Claims Operations

**Closures did not fall. The dashboard is missing two lines of business.** ClaimCenter recorded
**984** claim closures on 10-16, in line with the prior-Friday average of 994 (-1.0%). Only 620
reached the dashboard. Homeowners (HO) and Commercial Property (CP) closures are almost entirely
absent: HO is at 5% of its normal volume and CP at 0%. The other four lines are within their normal range.

**Don't staff from today's figure.** Use 984 or the trailing trend for 10-16. The 28.5% drop in
mean severity ($11,647 to $8,331) is the same defect, not good news: HO and CP are the
higher-severity lines, so removing them pulls the average down.

**Root cause:** PolicyCenter release PC-2026.10.2 (CHG-48213, 10-15 21:47) moved HO and CP onto
extract API v4, which renames `pol_status_cd` to `policy_status_code`. The ingest loader treated
the rename as a new column, so `pol_status_cd` became NULL for those policies.
`int.claim_policy_enriched` inner-joins claims to policies with
`policy_status IN ('IF','CX','EX')`, and that filter silently discarded every claim on an
HO or CP policy. Every job finished with status `success`.

## 2. Event timeline (what happened)

All times are CDT. The source column shows which telemetry file records each event.

| Time | System | Event | Source |
|---|---|---|---|
| 10-14 20:30 | ClaimCenter | `litigation_flag` added to `claim` (CHG-48170) | schema_change_log SC-04115 |
| 10-15 02:12 | Ingest loader | Auto-adds `raw.claim.litigation_flag` | SC-04116 |
| 10-15 ~02:49 | Nightly run | Normal run: 1,028 closures, 6 LOB rows in the severity aggregate | row_volume_history |
| 10-15 09:20 | dbt | `stg.claim.litigation_flag` exposed (PR #806) | SC-04117 |
| 10-15 16:20 | dbt / Tableau | Claims Analytics adds `cat_event_id` to `mart.agg_claim_severity_daily` and a CAT filter (default *All*) to the dashboard (PR #812) | SC-04118, SC-04119 |
| **10-15 21:47** | **PolicyCenter** | **RENAME `pol_status_cd` → `policy_status_code` for HO/CP (API v4), CHG-48213, declared non-breaking** | **SC-04120** |
| 10-16 01:55 | Sources | Nightly extract cutoff | table_freshness (source watermarks) |
| 10-16 02:01 | NOAA API | `raw.storm_events` attempt 1 fails (HTTP 503); retries at 03:31 and 04:46 also fail | row_volume_history |
| 10-16 02:01–02:15 | Ingest loader | `raw.policy_snapshot` full refresh, 416,964 rows, status `success` | row_volume_history |
| **10-16 02:14** | **Ingest loader** | **Logs only `ADD_COLUMN policy_status_code`. The old column is kept and is NULL for migrated rows. No rename or drop is recorded** | **SC-04121** |
| 10-16 02:20 | dbt | `stg.policy` built, `policy_status` null rate 0.04% → **36.6%** | column_profiles |
| **10-16 02:32** | **dbt** | **`int.claim_policy_enriched` rebuilt: 369,835 → 229,113 rows (-38.0%), success** | **row_volume_history** |
| 10-16 02:38 | dbt | `mart.fct_claim_closed` appends **620** rows for 10-16 (on time) | row_volume_history, table_freshness |
| 10-16 06:00 | NOAA API | `raw.storm_events` attempt 4 succeeds, 120 minutes past SLA | table_freshness |
| 10-16 06:06 | dbt | `mart.agg_claim_severity_daily` writes **5** LOB rows instead of 6 (CP absent), 67 minutes late because it waits on the storm feed | row_volume_history, table_freshness |
| 10-16 06:11 | Tableau | Dashboard extract refreshed, 11.5 minutes late | table_freshness |
| 10-16 06:40 | Claims Ops | VP reports the 38% drop | incident report |

## 3. Symptoms (observed effects, not causes)

These are what people observed. None of them shows a cause on its own.

| # | Symptom | Measured | Evidence |
|---|---|---|---|
| S1 | Closed claims down 37.6% vs prior Fridays | 620 vs avg 994 | dashboard_claims_severity_daily (S1) |
| S2 | Mean paid severity down 28.5% | $8,331 vs $11,647 | S1 |
| S3 | Drop is confined to two LOBs | CP 0.00×, HO 0.05× normal; PA 0.95, IM 0.95, CA 1.20, GL 1.23, all inside their historical 1st–99th percentile bands | S1 |
| S4 | Dashboard and severity aggregate refreshed late | 06:11 and 06:06 | table_freshness (C2) |
| S5 | Severity aggregate wrote 5 rows instead of 6 | CP row missing | row_volume_history (C3b) |
| S6 | Every pipeline task ended in `success` | Only `raw.storm_events` had failed attempts | row_volume_history (C3) |

S3 is the most useful symptom. A real business slowdown (weather, staffing, a holiday) would
reduce every line somewhat. Two lines going to zero while the other four stay normal points to a data defect.

## 4. Candidate causes and what each signal ruled in or out

Each team's working theory is listed as a hypothesis and tested against the telemetry.

| # | Candidate cause | Raised by | Verdict |
|---|---|---|---|
| H1 | Real business decline in closures | Claims Ops (initial read) | **Ruled out** |
| H2 | Late or stale data: the NOAA feed failure left the dashboard incomplete | Data Engineering on-call (the only red freshness check) | **Ruled out** as cause of the counts. A real but separate issue |
| H3 | Failed or partial job run | Data Engineering | **Ruled out** for job failure. A *partial load* is confirmed, but it came from logic, not a failed job (see H6) |
| H4 | Claims Analytics' PR #812 changed dashboard or aggregate logic | Policy Platform, Data Engineering | **Ruled out** |
| H5 | ClaimCenter `litigation_flag` change broke claim ingestion | Claims Analytics | **Ruled out** |
| H6 | Upstream policy schema change broke the claim-policy join | (found by investigation) | **Ruled in. Root cause** |

### Signal-by-signal findings

| Signal | What it showed | Ruled in | Ruled out |
|---|---|---|---|
| **Source events** (`raw.claim_status_history.status_cd` profile) | 984 `CLOSED` events on 10-16 vs 994 average; the pipeline delivered 63.0% of them (C1) | Loss happens *inside* the pipeline | H1 |
| **Upstream category mix** (`raw.policy_snapshot.lob_cd`, `stg.claim.lob_cd`) | LOB shares moved ≤0.11 percentage points vs the prior run (C1) | Inputs are intact up to staging | H1, H5 (claims arrive at normal volume and mix) |
| **Freshness** (`table_freshness`) | Late: storm_events (raw and stg), agg_claim_severity_daily, dashboard. **Nothing upstream of `mart.fct_claim_closed` was late.** `fct_claim_closed` loaded on time at 02:38 with 620 rows (C2) | S4 is explained by the NOAA outage | H2: the count was already 620 at 02:38, before any late table. The storm feed contributes only `cat_event_id` through a LEFT JOIN, which can't remove rows |
| **Job status** (`row_volume_history.status`) | All 18 tables finished `success`; only `raw.storm_events` had failed attempts (C3) | Nothing errored, so a test or monitor would have to catch this | H3 as a crash or failed job |
| **Row volume** (`row_volume_history`, lineage-ordered) | The first anomalous table in lineage order is **`int.claim_policy_enriched` at -38.05%**, versus a historical day-to-day std dev of 0.13% (z ≈ -306). All of its inputs (`stg.claim` +0.28%, `stg.policy` 0.00%) are normal (C3b) | The loss happens in the `int.claim_policy_enriched` model | H4 and H5: the loss is two layers upstream of PR #812 and downstream of where `litigation_flag` lands |
| **Column profiles** (`column_profiles`, lineage-ordered) | First drift: `raw.policy_snapshot.pol_status_cd` null rate **0.04% → 36.59%**, alongside a brand-new `policy_status_code` populated on 36.56% of rows. Carried into `stg.policy.policy_status` (36.61% null). Then `int.claim_policy_enriched.lob_cd` loses HO/CP (C4) | A column's values moved to a new name. The NULLs start at ingestion | H5 (`litigation_flag` profile is unremarkable) |
| **Schema change log** (`schema_change_log`) | Seven records covering three changes in the 48 hours before the run (`litigation_flag`, PR #812, CHG-48213). Only CHG-48213 touches a column on the counts path. The loader's own record (SC-04121) calls it `ADD_COLUMN`, not a rename (C5) | H6: a rename landed about 4 hours before the bad run | — |
| **Column-level lineage** (`lineage_column_level`, edge attributes) | `raw.policy_snapshot.pol_status_cd → stg.policy.policy_status → int.claim_policy_enriched` **[join filter]**. `policy_status_code` has **no downstream consumers**. `litigation_flag` stops at `stg.claim` (projection only). `cat_event_id` has no consumers beyond the aggregate (C6) | H6: the renamed column is a filter on the exact model that lost rows | H4, H5: their columns can't filter rows |
| **Magnitude reconciliation** | Book share of HO (96% migrated) + CP = 36.6%, which matches the 36.59% null rate. Expected claim loss from the impacted LOBs is **38.2%** vs 38.0% observed. Survival through the join: HO 4%, CP 0%, all other LOBs ≈100% (R1) | H6 explains the full magnitude with no residual | Any second contributing cause of material size |

## 5. Investigation sequence

1. **Confirm the symptom and break it down.** The total is down 37.6%, but by LOB it is two lines at roughly 0 and four lines normal (S3). This looks like a data defect, not a business change.
2. **Check the source of truth.** ClaimCenter `CLOSED` events are normal (984). H1 is ruled out, and the loss is inside the pipeline.
3. **Check the obvious alarm.** The only red freshness checks trace back to the NOAA outage. Lineage shows the storm feed is a LEFT JOIN into the aggregate, and `fct_claim_closed` already had 620 rows at 02:38. H2 is ruled out as the cause of the counts.
4. **Check job health.** Everything succeeded. A crash is ruled out, so this is a silent logic failure.
5. **Follow the volume signal in lineage order.** The first table with abnormal volume is `int.claim_policy_enriched` (-38%), while both of its inputs are normal. The defect is in that model's logic or in a column it depends on.
6. **Follow the distribution signal in lineage order.** The first column with abnormal drift is `raw.policy_snapshot.pol_status_cd` (36.6% NULL), with a new sibling column holding the same values.
7. **Find the change.** The schema log shows CHG-48213 renamed exactly that column at 21:47. The loader recorded only an `ADD_COLUMN`.
8. **Confirm the mechanism.** Column lineage shows that column is a join filter in `int.claim_policy_enriched`, and `NULL IN (...)` evaluates to unknown, so the row is dropped.
9. **Reconcile the size.** Predicted loss is 38.2% vs 38.0% observed. No other cause is needed.
10. **Clear the remaining suspects.** PR #812 and `litigation_flag` sit on columns that can't filter rows, and are downstream of the loss or stop short of it.

## 6. Root cause

**The PolicyCenter column rename (CHG-48213) broke the claim-to-policy join filter in
`int.claim_policy_enriched`. The result was a partial load that looked like a business decline.**

```
PolicyCenter API v4 (HO, CP)      pol_status_cd  ──renamed──▶  policy_status_code
        │
raw.policy_snapshot               loader schema drift = "allow": ADD COLUMN policy_status_code,
        │                         keep pol_status_cd → NULL on 36.6% of policies
stg.policy                        policy_status := pol_status_cd     (still the old name)
        │
int.claim_policy_enriched         INNER JOIN ... AND policy.policy_status IN ('IF','CX','EX')
        │                         NULL IN (...) → row dropped: HO 96%, CP 100% of claims
mart.fct_claim_closed (incr.)     today's partition short by 38%; history untouched
        │
bi.claims_severity_dashboard      "38% drop in closures"; severity looks 28.5% better
```

### Why nothing caught it

These are contributing factors. No single team is at fault.

1. **The change was declared non-breaking** on the grounds that "the loader handles schema
   drift." The loader handles *additive* drift. It can't recognize a rename, so the change
   reached the warehouse as a new column plus a column that went quietly NULL. CHG-47102
   (`agt_cd` → `producer_cd`, 09-22) did the same thing without consequence, only because
   no downstream model uses that column. That precedent made the pattern look safe.
2. **No data contract** between Policy Platform and Data Engineering lists `pol_status_cd` as a
   consumed column, so Policy Platform's change review had no way to see the dependency.
3. **The filter fails closed and silently.** An `INNER JOIN` with an `IN` list on a nullable
   column drops rows without an error. No test checks row loss across the join.
4. **Monitoring watched freshness and job status, and both looked fine.** No volume-anomaly
   or null-rate monitor runs on `stg.policy` or `int.claim_policy_enriched`. A -38% change on a
   table whose daily std dev is 0.13% would have been an easy alert.
5. **The only red signal pointed the wrong way.** The NOAA outage made the dashboard late and
   drew attention to freshness, which was a separate issue.
6. **The incremental mart hides the defect in history.** Only the latest partition is short, so
   the series looks like a cliff on one day, which is what a real business shock also looks like.

## 7. Remediation

### Immediate (before the 08:40 staffing decision)

1. **Tell Claims Ops now:** the 10-16 figure is invalid. Actual closures were 984 per ClaimCenter.
   HO and CP severity is unavailable until the backfill completes. Annotate the dashboard tile.
2. **Hotfix `stg.policy`** to read both column names:
   ```sql
   coalesce(policy_status_code, pol_status_cd) as policy_status
   ```
3. **Rebuild** `stg.policy` → `int.claim_policy_enriched`, then **reprocess the 10-16 partition** of
   `mart.fct_claim_closed`. It is incremental, so a normal run won't restate it: use
   `--full-refresh` or delete and re-insert the partition. Then rebuild
   `mart.agg_claim_severity_daily` for 10-16 and refresh the extract.
4. **Verify:** `int.claim_policy_enriched` returns to about 370k rows. `fct_claim_closed` for
   10-16 is within ±2% of the 984 ClaimCenter `CLOSED` events. The aggregate has 6 LOB rows, and
   the `stg.policy.policy_status` null rate is back near 0.04%.

### Preventive

| Action | Owner |
|---|---|
| Data contract for `src_policycenter.policy`, listing consumed columns (`policy_id`, `lob_cd`, status). Renaming or dropping a contracted column requires Data Engineering sign-off and a dual-write period | Policy Platform + Data Engineering |
| Loader rule: when a new column appears and an existing column's null rate jumps in the same load, flag a probable rename and block downstream builds | Data Engineering |
| `not_null` (severity **error**) and `accepted_values` tests on `stg.policy.policy_status` | Data Engineering |
| Join row-loss test: `count(int.claim_policy_enriched) / count(stg.claim) ≥ 0.99` | Data Engineering |
| Volume anomaly monitor on full-rebuild models (alert at \|Δ\| > 5σ) | Data Engineering |
| Source-reconciliation check: daily `fct_claim_closed` count vs ClaimCenter `CLOSED` events within ±2%, and a minimum row count per LOB in the aggregate | Claims Analytics |
| Decouple `mart.agg_claim_severity_daily` from the storm feed (publish with `cat_event_id` NULL, backfill later) so an optional input can't delay the dashboard | Claims Analytics |
| Add "consumed columns" from the lineage graph to the change-review template for source systems | Policy Platform |

## 8. Reproducing the evidence

```
.venv\Scripts\python incident\investigate.py
```

The script reads only `data/telemetry/` and prints each check in the order of section 5.
