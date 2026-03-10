"""
conftest.py — Shared fixtures and path setup for all test modules.
Adds project root and src/fault_ticket to sys.path so both
config.py (root) and pipeline.py (src/fault_ticket) are importable.
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np

# ── PATH SETUP ────────────────────────────────────────────────────────────────
# Project root  →  config.py, loading.py
# src/fault_ticket  →  pipeline.py, metrics.py
_ROOT     = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_FT_SRC   = os.path.join(_ROOT, "src", "fault_ticket")

for _p in [_ROOT, _FT_SRC]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ── FIXTURES ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_cleaned_df():
    """
    Minimal cleaned DataFrame mirroring pipeline output schema.
    30 tickets across 3 zones — sufficient for all metric calculations.
    Ticket mix: P1-1 (2h, SLA pass), P2-2 (5h, SLA pass), P3-3 (30h, SLA fail).
    """
    np.random.seed(42)
    n = 30
    df = pd.DataFrame({
        "TICKETID"             : [f"IC-{10000+i}" for i in range(n)],
        "REPORTDATE"           : pd.date_range("2024-01-01", periods=n, freq="6h"),
        "Priority"             : list(np.tile([1, 2, 3], n // 3).astype("int8")),
        "Urgency"              : list(np.tile([1, 2, 3], n // 3).astype("int8")),
        "Priority_Urgency"     : list(np.tile(["1.1", "2.2", "3.3"], n // 3)),
        "OUTAGEDURATION"       : list(np.tile([2.0, 5.0, 30.0], n // 3).astype("float32")),
        "SLA_Compliant"        : list(np.tile([1, 1, 0], n // 3)),
        "SLA_Compliance_Rate"  : list(np.tile([100.0, 100.0, 0.0], n // 3)),
        "SLA_Breach_Rate"      : list(np.tile([0.0, 0.0, 100.0], n // 3)),
        "DISPATCH_DELAY_HOURS" : list(np.tile([0.5, 1.0, 2.0], n // 3)),
        "FIELD_TIME_HOURS"     : list(np.tile([1.5, 4.0, 28.0], n // 3).astype("float32")),
        "Timestamp_Integrity"  : [True] * n,
        "ZONE"                 : list(np.tile(["ZONE 1", "ZONE 2", "ZONE 3"], n // 3)),
        "CITY"                 : list(np.tile(["MAKATI", "QUEZON CITY", "PASIG"], n // 3)),
        "Region"               : ["Region 3"] * n,
        "Area"                 : list(np.tile(["AREA_A", "AREA_B", "AREA_C"], n // 3)),
        "NEType"               : list(np.tile(["CELL", "ROUTER", "MSS"], n // 3)),
        "NE_Category"          : list(np.tile(["Access", "IP/Network Infra", "Core Network"], n // 3)),
        "Standardized RFO"     : list(np.tile(["FOC CUT - LINEAR", "FACILITIES-Power Failure", "UNKNOWN-Under Investigation"], n // 3)),
        "Resolution_Path"      : list(np.tile(["Field_Dispatch_Restored", "NOC_Remote_Restored", "Auto_Self_Restored"], n // 3)),
        "WOOwnerGroup"         : list(np.tile(["Field Operations", "NOC", "Unknown"], n // 3)),
        "RESOLUTION_TIME_HOURS": list(np.tile([2.0, 5.0, 30.0], n // 3)),
    })
    return df


@pytest.fixture
def sample_raw_df():
    """
    Minimal raw DataFrame mirroring pre-cleaning input schema.
    Used for pipeline and validation tests.
    """
    n = 20
    df = pd.DataFrame({
        "TICKETID"      : [f"IC-{20000+i}" for i in range(n)],
        "REPORTDATE"    : [str(d) for d in pd.date_range("2024-01-01", periods=n, freq="4h")],
        "Priority"      : list(np.tile([1, 2, 3, 4], n // 4)),
        "Urgency"       : list(np.tile([1, 2, 3, 0], n // 4)),
        "FT Status"     : list(np.tile(["CLOSED", "RESOLVED", "PENRESOLVE", "CLOSED"], n // 4)),
        "Fault Type"    : ["REACTIVE"] * n,
        "OUTAGEDURATION": list(np.tile([2.0, 5.0, 15.0, 72.0], n // 4)),
        "RFODescription": list(np.tile(["FOC CUT LINEAR", "FACILITIES POWER FAILURE", "EQUIPMENT DEFECTIVE HARDWARE", "UNKNOWN"], n // 4)),
        "RootCause"     : ["Test root cause"] * n,
        "ActionTaken"   : ["Test action"] * n,
        "ContactGroup"  : ["GMA_NOC"] * n,
        "Area"          : list(np.tile(["NCR_001", "NCR_002", "NCR_003", "NCR_004"], n // 4)),
        "DESCRIPTION"   : ["Test ticket description for site"] * n,
        "NEType"        : list(np.tile(["CELL", "ROUTER", "MSS", None], n // 4)),
        "StartDateTime" : [str(d) for d in pd.date_range("2024-01-01 01:00", periods=n, freq="4h")],
        "FT_Owner"      : list(np.tile(["John Doe", "Jane Smith", None, "John Doe"], n // 4)),
        "WOLeadName"    : list(np.tile(["Tech A", None, None, "Tech B"], n // 4)),
        "Issuer Team"   : ["NOC-GMA"] * n,
    })
    return df


@pytest.fixture
def single_zone_df(sample_cleaned_df):
    return sample_cleaned_df[sample_cleaned_df["ZONE"] == "ZONE 1"].copy()
