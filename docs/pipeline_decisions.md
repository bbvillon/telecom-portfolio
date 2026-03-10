# Pipeline Design Decisions
### Telecom Fault Ticket Analysis — Cleaning and Modeling Rationale

This document explains the reasoning behind each major decision in `pipeline.py`.
It is a companion to the code for anyone reviewing the project, and a record of
the thought process behind each step.

---

## Why a 5-Phase Pipeline?

The raw fault ticket data could not be used directly for analysis. It had duplicates,
incomplete fields, inconsistent text, no geographic structure, and personally
identifiable information. A single-pass cleaning approach would have made it impossible
to audit what changed and why. The five phases were designed so that each one has a
clear, independent purpose:

| Phase | Purpose |
|-------|---------|
| 1 – Structural Filtering | Remove records that are structurally unusable |
| 2 – Data Repair | Recover usable information from incomplete fields |
| 3 – Geographic Enrichment | Assign zones and cities from a site reference database |
| 4 – Operational Modeling | Derive SLA, MTTR, resolution path, and time metrics |
| 5 – Privacy Protection | Anonymize personnel data before any output is saved |

---

## Phase 1 — Structural Filtering

### Step 1: Duplicate Ticket IDs

Tickets with the same `TICKETID` appear in the raw data because the source system
exports all activity updates per ticket. Keeping duplicates would inflate volumes,
distort MTTR calculations, and double-count SLA breaches. Only the first occurrence
is retained.

### Step 2: Blank Priority

A ticket with no priority cannot be classified by SLA tier and cannot contribute
meaningfully to any metric. These rows were dropped rather than imputed since
priority is a required operational field. A blank value indicates a data entry
failure, not something recoverable.

### Step 3: Priority 1–3 Only

The raw data includes Priority 4–6 tickets, which represent preventive maintenance
(scheduled work orders). These are not reactive fault tickets. They are planned
activities with no outage to restore. Including them would corrupt MTTR, SLA
compliance rates, and fault density calculations. Only Priority 1–3 reactive faults
were retained.

- **P1** — National Core (3h SLA) — outages affecting core network elements
- **P2** — Regional Core (6h SLA) — outages affecting regional infrastructure
- **P3** — Zone Equipment (9–24h SLA) — BTS-level faults at specific sites

### Step 4: Valid Urgency (0–3)

Urgency combined with Priority defines the SLA threshold. Values outside 0–3 have
no corresponding SLA mapping and are likely data entry errors. These were dropped
to ensure every retained ticket has a calculable SLA target.

### Step 5: FT Status — CLOSED, RESOLVED, PENRESOLVE Only

Only tickets with a terminal status have a resolution time that can be measured.
Retaining open or in-progress tickets would produce incomplete MTTR and incorrect
SLA assessments. The three valid statuses are:

- **CLOSED** — fully resolved and closed in the system
- **RESOLVED** — technically resolved, pending administrative closure
- **PENRESOLVE** — resolution confirmed, pending final system update

Tickets with statuses like `INPROG`, `CANCELLED`, or `OPEN` were excluded because
they represent incomplete events with no usable resolution timestamp.

### Step 6: Drop Fault Type = INVALID

The `INVALID` fault type flag is used in the source system to mark test entries,
system-generated records, and administratively voided tickets. These are not real
network events and were removed entirely.

---

## Phase 2 — Data Repair and Field Reconstruction

### Why Repair Instead of Drop?

Dropping every ticket with a missing field would have removed a significant portion
of otherwise usable data. The approach instead was to attempt recovery in a defined
order of confidence, log what was inferred, flag low-confidence results, and only
drop records that could not be recovered.

### Step 7: TF-IDF for Missing NEType

**The problem.** About 6,700 tickets had no `NEType` value. NEType (Network Element
Type) is required to assign `NE_Category`, which classifies whether a fault is an
Access, Transport, Core, or IP/Network fault. This is a key dimension in all
downstream analysis.

**Why TF-IDF.** The ticket `DESCRIPTION` field contains free text written by NOC
engineers describing the fault. This text typically names the equipment involved,
e.g. "CELL site down", "Router unreachable", "BTS antenna issue". TF-IDF measures
how characteristic a word is for a given NEType by comparing its frequency in that
NEType's tickets against its frequency across all tickets. This makes it well suited
for short, domain-specific technical text where exact keyword matching fails due to
variations in phrasing.

For a more detailed discussion on why TF-IDF was chosen over BERT, BM25, Doc2Vec,
and other approaches, see `docs/tfidf_rationale.md`.

**How it works:**
1. Build a corpus from all tickets where NEType is known
2. Group the corpus by NEType and concatenate descriptions
3. Vectorize using TF-IDF
4. For each unknown ticket, compute cosine similarity against every NEType group
5. Assign the NEType with the highest similarity score
6. Categorize confidence based on the score

**Confidence thresholds.** TF-IDF cosine similarity scores in this corpus top out
at around 0.50, far below the conventional 0.75 threshold, because telecom fault
descriptions are short and technically sparse. The thresholds were calibrated to
the actual score distribution after manual validation confirmed that low-scoring
inferences were still accurate:

| Tier | Score Range | Meaning |
|------|-------------|---------|
| high | >= 0.40 | Strong match |
| medium | >= 0.28 | Reliable match |
| low | >= 0.15 | Usable but flagged |
| very_low | < 0.15 | Spot-check recommended |

### Steps 9–12: RFO Standardization

**The problem.** The `RFODescription` field (Reason for Outage) was entered as free
text by multiple engineers over multiple years, producing hundreds of variations for
what should be a fixed set of categories. For example:

"FOC CUT LINEAR", "FOC CUT - LINEAR", "FIBER OPTIC CABLE CUT", "SINGLE FIBER BREAK"

All four refer to the same fault type.

**Why not just a keyword map.** A simple keyword map fails on abbreviated or
misspelled entries, entries where the same keyword appears in multiple categories
(e.g. "power" appears in both `FACILITIES-Power Failure` and unrelated contexts),
and entries where the RFO field is blank but the cause can be inferred from other
fields.

**The 9-step resolution cascade** handles these cases in order of confidence.
Each step is only reached if the previous one did not produce a match:

1. **Exact match** — handles all well-formed entries immediately
2. **INVALID flag** — catches test entries and administrative voids
3. **FOC description lists** — dedicated handling for fiber cut variants, with urgency-based disambiguation between linear and redundant cuts
4. **ActionTaken inference** — if RFO is blank, the action taken often implies the cause (e.g. "SPLICED" implies fiber cut, "REPLACED" implies hardware failure)
5. **Keyword map scan** — 25-category map scanned across RFO, RootCause, and ActionTaken combined
6. **UNKNOWN/OTHERS description fallback** — for entries logged as unknown, the `DESCRIPTION` field is checked for diagnostic clues
7. **NEType fallback** — if no text-based match is found, the equipment type implies a probable cause (e.g. `POWER FACILITY` implies power failure)
8. **Final description scan** — broad keyword check on power, fiber, and hardware
9. **Fuzzy match** — last resort using rapidfuzz WRatio scorer; returns `Uncategorized` if no match exceeds 85% confidence

### Steps 10–11: RFO Backfill from RootCause and ActionTaken

Before TF-IDF is applied, blank `RFODescription` fields are first filled from
`RootCause` (Step 10) since these fields often contain the same information entered
by different teams. Only entries still blank after this proceed to the `ActionTaken`
inference (Step 11) and then TF-IDF (Step 12).

---

## Phase 3 — Geographic and Site Enrichment

### Step 16: Region Assignment — Why Three Fallbacks?

The raw data does not have a clean region column. Region had to be derived from
three fields in priority order:

1. **ContactGroup** — the NOC team that raised the ticket (most reliable; team names encode region, e.g. `GMA_NOC` maps to Region 3)
2. **Area** — the area code of the site (e.g. `NCR_001` maps to Region 3)
3. **DESCRIPTION** — free text scanned for geographic keywords as a last resort

This three-tier fallback was necessary because no single field was consistently
populated. Tickets where `ContactGroup` pointed to one region but the `Area` code
pointed to another were resolved by giving `ContactGroup` priority, since it
reflects the operational ownership of the ticket rather than the physical site code.

Tickets that could not be assigned to any region were logged to
`unmatched_areas_region3.csv` and then dropped. These were predominantly tickets
from decommissioned sites or administrative entries with no meaningful location.

### Step 17: Zone and City Assignment via Site Database

Region 3 (NCR) tickets required zone-level granularity for the core analysis. The
site database (`site_database.csv`) maps each `Area` code (PLAID) to a `ZONE` and
`CITY`. This merge was only performed for Region 3 tickets. All other regions
received `Unknown` as their zone since the analysis scope was NCR.

Unmatched PLAIDs (Area codes with no entry in the site database) were retained with
`ZONE = Unknown` and logged for review rather than dropped. The ticket data itself
was valid; only the geographic enrichment was missing.

---

## Phase 4 — Operational Modeling

### SLA Thresholds

SLA targets are defined by the Priority-Urgency combination. The thresholds reflect
operational commitments:

| Tier | SLA Target | Description |
|------|-----------|-------------|
| P1 (any urgency) | 3 hours | National core outage |
| P2 (any urgency) | 6 hours | Regional core outage |
| P3.1 | 9 hours | Zone equipment outage |
| P3.2 | 12 hours | Zone equipment degradation |
| P3.3 | 24 hours | Zone equipment management alarm |

### Timestamp Integrity Flag

`DISPATCH_DELAY_HOURS` and `FIELD_TIME_HOURS` are derived from timestamps that
engineers enter manually. In some cases engineers log the dispatch timestamp after
the ticket is already closed, which produces negative field times. Rather than
dropping these tickets, a `Timestamp_Integrity` boolean flag was added:

- **True** — both delay and field time are non-negative, and dispatch does not exceed total MTTR
- **False** — timestamp anomaly detected (data entry artefact)

Tickets with `Timestamp_Integrity = False` are retained in the dataset but excluded
from NOC time and field time aggregations. They still contribute to volume counts,
SLA compliance rates, and MTTR calculations.

### Resolution Path Classification

Each ticket is classified into one of three resolution paths based on the
`WOOwnerGroup` category of the assigned work order:

- **Auto_Self_Restored** — resolved without field dispatch or NOC intervention
- **NOC_Remote_Restored** — resolved remotely by NOC or ROC team
- **Field_Dispatch_Restored** — required a field engineer to attend the site

This classification was central to Project 2's finding that 61.8% of NCR faults
require field dispatch, making field efficiency the primary operational lever.

---

## Phase 5 — Privacy Protection

### Why Anonymize?

The raw data contains the real names of NOC engineers and field technicians
(`FT_Owner`, `WOLeadName`). Publishing or sharing this data without anonymization
would expose individuals' performance records without their consent.

Personnel fields were replaced with SHA-256 derived codes (truncated to 8 hex
characters) in the format `ENG_xxxxxxxx`. The mapping between real names and codes
is saved to `ft_owner_anonymization_map.csv` and `wo_lead_anonymization_map.csv`
for internal use but is not committed to the repository.

`ContactGroup` and `Area` fields were similarly hashed since they can be used to
infer individual team membership.

---

## Testing vs. Validation

### Pipeline Validation (`validate_cleaned_data`)

Validation runs inside the pipeline after cleaning is complete. It checks that the
output meets the expected contract: no duplicates, valid regions, non-negative
durations, correct SLA values, valid resolution path labels. It is a sanity check
that the pipeline produced what it was supposed to produce, on every run.

Validation answers: did this specific run produce correct output?

### Unit Testing (`tests/`)

Unit tests run outside the pipeline, in isolation from the real dataset. They test
individual functions: confidence thresholds, SLA boundary conditions, RFO
standardization logic, metric calculations. They use small synthetic fixtures and
are designed to catch regressions. If a future code change breaks existing behaviour,
the tests fail before the pipeline is ever run.

Unit testing answers: is the logic of this function correct, regardless of what
data it receives?

| | Validation | Unit Tests |
|--|-----------|-----------|
| When it runs | Every pipeline execution | Explicitly via pytest |
| What it checks | Pipeline output correctness | Function logic correctness |
| Data used | Real cleaned output | Synthetic 20–30 row fixtures |
| Purpose | Catch bad outputs | Catch code regressions |
| Tool | Custom validate_cleaned_data() | pytest |

Both are necessary. Validation without testing means broken logic can pass undetected
if the real data happens not to trigger the failure. Testing without validation means
a correctly coded function could still produce wrong outputs if the pipeline wiring
is wrong.

---

*Last updated: Phase 1 — Foundation Build*
