## Overview
This repository documents an end-to-end analysis of ~37,000 Priority 1–3 fault tickets
across six operational zones in the National Capital Region (NCR). The work spans pipeline engineering, KPI modelling, zone benchmarking, site risk
profiling, and field engineer workload analysis.

All personal and site-identifying data has been anonymised. Raw ticket data is not
included in this repository.

## Project Series

| # | Project | Focus |
|---|---------|-------|
| P1 | NCR Baseline | Zone-level MTTR, SLA, fault density |
| P2 | Resolution Paths | Field dispatch anatomy, RFO breakdown, Zone 1 site risk |
| P3 | Zone Benchmarking | Priority-adjusted scorecard, P3.2 breach deep-dive |
| P4 | City Intelligence | City-level risk scoring, composite index |
| P5 | Site & Engineer Risk | Site risk profiling, field engineer load equity |

## Status
🔧 Pipeline and source modules — complete  
📓 Notebooks — being published in stages

## Setup
```bash
pip install -r requirements.txt
```

> Notebooks expect `output/cleaned_fault_ticket.csv` to be generated locally
> by running the pipeline. See `src/fault_ticket/pipeline.py`.