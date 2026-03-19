# Data Dictionary
**Telecom Fault Ticket Analysis — NCR**
_Last updated: 2026-03-03_

---

## Table of Contents
1. [cleaned_fault_ticket.csv](#1-cleaned_fault_ticketcsv) — Master cleaned dataset
2. [ncr_summary.csv](#2-ncr_summarycsv) — Zone-level KPI summary
3. [resolution_path_analysis.csv](#3-resolution_path_analysiscsv) — Resolution path breakdown
4. [top_100_sites.csv](#4-top_100_sitescsv) — Highest-volume sites
5. [Notes & Derivation Logic](#5-notes--derivation-logic)

---

## 1. `cleaned_fault_ticket.csv`
**Row grain:** One fault ticket (work order) per row.
**Scope:** NCR Priority 1–3 closed/resolved tickets after pipeline cleaning.

| Column | Type | Description |
|--------|------|-------------|
| `TICKETID` | string | Unique fault ticket identifier. Duplicates removed in Phase 1. |
| `REPORTDATE` | datetime | Timestamp when the fault was first reported / ticket opened (NOC receipt). |
| `Priority` | int8 | Ticket priority level. Values: `1` (critical), `2` (major), `3` (minor). Pipeline retains Priority 1–3 only. |
| `Urgency` | int8 | Urgency classification. Values: `0–3`. Combined with Priority to determine SLA threshold. |
| `Priority_Urgency` | string | Composite key in `P.U` format (e.g. `1.1`, `3.2`). Maps to SLA threshold via `SLA_THRESHOLDS` config. |
| `OUTAGEDURATION` | float32 | Total ticket resolution time in **hours** (MTTR). Source field; equals `RESOLVEDDATE − REPORTDATE`. |
| `RESOLVEDDATE` | datetime | Computed: `REPORTDATE + OUTAGEDURATION`. Timestamp when the ticket was closed. |
| `DISPATCHDATE` | datetime | Field dispatch / WO issuance timestamp (`StartDateTime` in raw source). Time when NOC endorsed the ticket to field. |
| `RESOLUTION_TIME_HOURS` | float32 | Alias of `OUTAGEDURATION`. Retained for backwards compatibility. |
| `SLA_Compliant` | bool | `True` if `OUTAGEDURATION ≤ SLA threshold` for the ticket's `Priority_Urgency`. Determined by `determine_sla_compliance()`. |
| `SLA_Compliance_Rate` | float | `SLA_Compliant × 100`. Per-ticket value (0 or 100). Used for group-level averaging. |
| `SLA_Breach_Rate` | float | `100 − SLA_Compliance_Rate`. Per-ticket value. |
| `DISPATCH_DELAY_HOURS` | float32 | **NOC/ROC phase duration** = `DISPATCHDATE − REPORTDATE` in hours. Time from ticket open to field endorsement. Zero is valid (immediate dispatch). Previously labelled `Avg_Dispatch_Delay`. |
| `FIELD_TIME_HOURS` | float32 | **Field engineer phase duration** = `OUTAGEDURATION − DISPATCH_DELAY_HOURS` in hours. Time from WO issuance to ticket close. Upper bound in high-CBD zones (building permits not separately tagged). |
| `Timestamp_Integrity` | bool | `True` if both phase times are non-negative and `DISPATCH_DELAY_HOURS ≤ OUTAGEDURATION`. Tickets flagged `False` are retained in the dataset but **excluded from NOC/field time aggregations** (late dispatch entry artefact in source system). |
| `Resolution_Path` | string | Operational resolution category. Values: `Field_Dispatch_Restored` (both FT_Owner and WOLeadName present), `NOC_Remote_Restored` (FT_Owner present, no WOLeadName), `Auto_Self_Restored` (no FT_Owner). |
| `ZONE` | string | Operational zone (e.g. `ZONE 1` … `ZONE 6`). Derived from `AssignArea` in raw source. |
| `CITY` | string | City within the zone. Derived from `AssignCity`. |
| `Region` | string | Broader administrative region (e.g. `Region 1`). |
| `Area` | string | Sub-city site area code. Used for fault density and site-level aggregations. |
| `SiteName` | string | Synthetic site identifier: `{Region}_{City}_{Area[:8]}`. Privacy-safe; no raw site names. |
| `NEType` | string | Network Element type (raw). Missing values inferred via TF-IDF in Phase 2. |
| `NE_Category` | string | Standardised NE grouping. Values include: `Access`, `Transport`, `Core Network`, `IP/Network Infra`, `Network Management`. |
| `Standardized RFO` | string | Reason For Outage, normalised. Top categories: `FACILITIES-Power Failure`, `EQUIPMENT-Defective Hardware`, `FOC CUT - LINEAR`, etc. |
| `ContactGroup` | string | Group that reported or owns the ticket contact. |
| `Issuer_Team` | string | Normalised issuer team. Values: `NOC` (includes ROC), `Field Operations`, `Customer Support`, `Other`, `Unknown`. |
| `WOOwnerGroup` | string | Privacy-safe functional category of the WO owner group (replaces raw team name). Values: `Field Operations`, `NOC`, `Fiber Restoration`, `Core Operations`, `Engineering`, `Facilities Operations`, `Tier 2 Support`, `Enterprise Operations`, `Operations Support`, `Third Party`, `Other`, `Unknown`. |
| `Engineer_ID` | string | Anonymised field engineer ID. Format: `ENG_` + 8-char SHA-256 hash of raw `FT_Owner` name. `Unknown` if unattributed. |
| `Field_Lead_ID` | string | Anonymised WO lead ID. Format: `ENG_` + 8-char SHA-256 hash of raw `WOLeadName`. `Unknown` if unattributed. |
| `Urgency` | int8 | _(see above)_ |

---

## 2. `ncr_summary.csv`
**Row grain:** One row per operational zone (6 rows total: ZONE 1–6).
**Source:** Aggregated from `cleaned_fault_ticket.csv` via `calculate_zone_summary()`.

| Column | Type | Description |
|--------|------|-------------|
| `ZONE` | string | Operational zone identifier (e.g. `ZONE 1`). |
| `Ticket_Count` | int | Total number of fault tickets in the zone. |
| `MTTR` | float | Mean Time To Repair in **hours**. Mean of `OUTAGEDURATION` across all zone tickets. |
| `SLA_Compliance_Rate` | float | Percentage of tickets resolved within SLA threshold. Range: 0–100. |
| `Total_Faults` | int | Total fault events (same as `Ticket_Count` at zone level). |
| `Unique_Sites` | int | Count of distinct `Area` values (sites) within the zone. |
| `Fault_Density` | float | `Total_Faults / Unique_Sites`. Average faults per site; higher values indicate chronic hotspots. |
| `Avg_NOC_Time` | float | Average NOC/ROC phase duration in **hours** (`DISPATCH_DELAY_HOURS`). Excludes `Timestamp_Integrity=False` tickets. |
| `Avg_Field_Time` | float | Average field engineer phase duration in **hours** (`FIELD_TIME_HOURS`). Excludes `Timestamp_Integrity=False` tickets. |

---

## 3. `resolution_path_analysis.csv`
**Row grain:** One row per `Resolution_Path` value (3 rows).
**Source:** Aggregated from `cleaned_fault_ticket.csv` in pipeline Step 22.

| Column | Type | Description |
|--------|------|-------------|
| `Resolution_Path` | string | Resolution category. Values: `Auto_Self_Restored`, `NOC_Remote_Restored`, `Field_Dispatch_Restored`. |
| `Count` | int | Number of tickets following this path. |
| `Avg_Duration` | float | Mean `OUTAGEDURATION` in **hours** for tickets on this path. |
| `SLA_Rate` | float | Mean `SLA_Compliant` (0–1 scale). Multiply by 100 for percentage. |
| `Avg_Dispatch` | float | Mean `DISPATCH_DELAY_HOURS` for tickets on this path. |

---

## 4. `top_100_sites.csv`
**Row grain:** One row per site (top 100 by ticket volume).
**Source:** Aggregated from `cleaned_fault_ticket.csv` in pipeline Step 24.

| Column | Type | Description |
|--------|------|-------------|
| `SiteName` | string | Synthetic site key: `{Region}_{City}_{Area[:8]}`. Unique identifier; no raw infrastructure names exposed. |
| `Tickets` | int | Total ticket count for the site. |
| `SLA_Rate` | float | Mean `SLA_Compliant` for the site (0–1 scale). |
| `Avg_Duration` | float | Mean `OUTAGEDURATION` in **hours** for the site. |
| `Region` | string | Administrative region the site belongs to. |
| `City` | string | City the site is located in. |

---

## 5. Notes & Derivation Logic

### SLA Thresholds (hours)
| Priority \ Urgency | 1 | 2 | 3 |
|--------------------|---|---|---|
| **1** | 3h | 3h | 3h |
| **2** | 6h | 6h | 6h |
| **3** | 9h | 12h | 24h |

### Resolution Path Logic
```
FT_Owner = NaN                          → Auto_Self_Restored
FT_Owner present, WOLeadName = NaN      → NOC_Remote_Restored
FT_Owner present, WOLeadName present    → Field_Dispatch_Restored
```

### Timestamp Integrity Rule
A ticket is flagged `Timestamp_Integrity = False` when:
- `DISPATCH_DELAY_HOURS < 0`, OR
- `FIELD_TIME_HOURS < 0`, OR
- `DISPATCH_DELAY_HOURS > OUTAGEDURATION`

These tickets are **retained** in the dataset for volume/SLA counts but **excluded** from `Avg_NOC_Time` and `Avg_Field_Time` aggregations.

### Privacy Controls
- Individual engineer names (`FT_Owner`, `WOLeadName`) are replaced with `ENG_<hash>` identifiers.
- Raw `WOOwnerGroup` team names are replaced with functional category labels.
- Raw personnel columns are dropped from the final output in pipeline Phase 5.

### Field Time Caveat (CBD Zones)
In high-CBD areas (Makati, BGC, Ortigas — primarily ZONE 5), `FIELD_TIME_HOURS` may include building work permit wait time, which has no distinct RFO tag. Treat as an **upper bound** on true engineering resolution time for those zones.
