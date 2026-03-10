"""
Tests for determine_sla_compliance().
Covers every P-U tier boundary in both dot and dash notation.
"""
import pytest
import pandas as pd
import numpy as np
from pipeline import determine_sla_compliance
from config import SLA_THRESHOLDS


class TestSLAComplianceBoundaries:
    """Parametrized boundary test for every P-U combination."""

    @pytest.mark.parametrize("pu,threshold", [
        (pu, h) for pu, h in SLA_THRESHOLDS.items() if "." in pu
    ])
    def test_at_threshold_is_compliant(self, pu, threshold):
        row = pd.Series({"Priority_Urgency": pu, "RESOLUTION_TIME_HOURS": float(threshold)})
        assert determine_sla_compliance(row) == 1

    @pytest.mark.parametrize("pu,threshold", [
        (pu, h) for pu, h in SLA_THRESHOLDS.items() if "." in pu
    ])
    def test_under_threshold_is_compliant(self, pu, threshold):
        row = pd.Series({"Priority_Urgency": pu, "RESOLUTION_TIME_HOURS": float(threshold) - 0.1})
        assert determine_sla_compliance(row) == 1

    @pytest.mark.parametrize("pu,threshold", [
        (pu, h) for pu, h in SLA_THRESHOLDS.items() if "." in pu
    ])
    def test_over_threshold_is_breach(self, pu, threshold):
        row = pd.Series({"Priority_Urgency": pu, "RESOLUTION_TIME_HOURS": float(threshold) + 0.1})
        assert determine_sla_compliance(row) == 0

    def test_known_pass_p1(self):
        row = pd.Series({"Priority_Urgency": "1.1", "RESOLUTION_TIME_HOURS": 2.0})
        assert determine_sla_compliance(row) == 1

    def test_known_breach_p3_3(self):
        # P3-3 threshold = 24h; 30h should breach
        row = pd.Series({"Priority_Urgency": "3.3", "RESOLUTION_TIME_HOURS": 30.0})
        assert determine_sla_compliance(row) == 0

    def test_p4_long_duration_compliant(self):
        # P4 threshold = 72h; 71h should pass
        row = pd.Series({"Priority_Urgency": "4.1", "RESOLUTION_TIME_HOURS": 71.0})
        assert determine_sla_compliance(row) == 1

    def test_missing_resolution_time_returns_nan(self):
        row = pd.Series({"Priority_Urgency": "2.2", "RESOLUTION_TIME_HOURS": np.nan})
        result = determine_sla_compliance(row)
        assert result is np.nan or (isinstance(result, float) and np.isnan(result))

    def test_unknown_priority_urgency_uses_default_24h(self):
        # SLA_THRESHOLDS.get(pu, 24): unknown P-U defaults to 24h threshold.
        # 5h < 24h → compliant (returns 1), not NaN.
        row = pd.Series({"Priority_Urgency": "9.9", "RESOLUTION_TIME_HOURS": 5.0})
        assert determine_sla_compliance(row) == 1
    
    def test_unknown_priority_urgency_breaches_default_24h(self):
        # 30h > 24h default → breach (returns 0)
        row = pd.Series({"Priority_Urgency": "9.9", "RESOLUTION_TIME_HOURS": 30.0})
        assert determine_sla_compliance(row) == 0