"""
Tests for standardize_rfo_description() — the 9-step resolution cascade.
Each test class covers one step in the cascade.
"""
import pytest
import pandas as pd
from pipeline import standardize_rfo_description


def _row(rfo="", root_cause="", action="", desc="", ne_type="", urgency=3):
    return pd.Series({
        "RFODescription": rfo,
        "RootCause"     : root_cause,
        "ActionTaken"   : action,
        "DESCRIPTION"   : desc,
        "NEType"        : ne_type,
        "Urgency"       : urgency,
    })


class TestStep1ExactMatch:
    def test_foc_cut_linear_exact(self):
        assert standardize_rfo_description(_row(rfo="FOC CUT - LINEAR")) == "FOC CUT - LINEAR"

    def test_facilities_power_failure_exact(self):
        assert standardize_rfo_description(_row(rfo="FACILITIES-Power Failure")) == "FACILITIES-Power Failure"

    def test_case_insensitive_does_not_exact_match(self):
        # Exact match normalises case, so uppercase variant should still resolve
        result = standardize_rfo_description(_row(rfo="facilities-power failure"))
        assert result == "FACILITIES-Power Failure"


class TestStep3FOCDescriptionLists:
    def test_linear_keyword_in_rfo(self):
        assert standardize_rfo_description(_row(rfo="foc cut linear")) == "FOC CUT - LINEAR"

    def test_redundancy_keyword_in_rfo(self):
        assert standardize_rfo_description(_row(rfo="foc cut with redundancy")) == "FOC CUT WITH REDUNDANCY"

    def test_generic_fiber_cut_resolves_by_urgency(self):
        # Generic FOC + urgency 1 → linear
        result = standardize_rfo_description(_row(rfo="fiber cut", urgency=1))
        assert result == "FOC CUT - LINEAR"

    def test_generic_fiber_cut_redundant_by_urgency(self):
        # Generic FOC + urgency > 1 → redundancy
        result = standardize_rfo_description(_row(rfo="fiber cut", urgency=2))
        assert result == "FOC CUT WITH REDUNDANCY"


class TestStep4ActionTakenInference:
    def test_replaced_action_maps_to_defective_hardware(self):
        result = standardize_rfo_description(_row(rfo="unknown", action="replaced faulty module"))
        assert result == "EQUIPMENT-Defective Hardware"

    def test_reconfigured_action_maps_to_config_problem(self):
        result = standardize_rfo_description(_row(rfo="unknown", action="reconfigured the device"))
        assert result == "EQUIPMENT-Configuration Problem"

    def test_power_restored_action_maps_to_power_failure(self):
        result = standardize_rfo_description(_row(
            rfo="unknown",
            action="self restored commercial power resumed"
        ))
        assert result == "FACILITIES-Power Failure"


class TestStep5KeywordMapScan:
    def test_power_keyword_in_rfo(self):
        result = standardize_rfo_description(_row(rfo="facilities power failure"))
        assert result == "FACILITIES-Power Failure"

    def test_equipment_defective_keyword(self):
        result = standardize_rfo_description(_row(rfo="equipment defective hardware"))
        assert result == "EQUIPMENT-Defective Hardware"


class TestStep7NETypeFallback:
    """
    NEType fallback (step 7) only triggers when no earlier step matches.
    Using rfo="unknown" hits the keyword map at step 5 first.
    A blank RFO with no keywords bypasses steps 1-6 and reaches step 7.
    """
    def test_cell_ne_type_fallback(self):
        result = standardize_rfo_description(_row(rfo="", ne_type="CELL"))
        assert result == "EQUIPMENT-Defective Hardware"

    def test_power_facility_ne_type_fallback(self):
        result = standardize_rfo_description(_row(rfo="", ne_type="POWER FACILITY"))
        assert result == "FACILITIES-Power Failure"

    def test_router_ne_type_fallback(self):
        result = standardize_rfo_description(_row(rfo="", ne_type="ROUTER"))
        assert result == "TRANSMISSION-IP Network Problem"

    def test_unknown_rfo_keyword_is_caught_before_ne_fallback(self):
        # "unknown" in rfo hits keyword map at step 5 — NEType fallback never reached
        result = standardize_rfo_description(_row(rfo="unknown", ne_type="CELL"))
        assert result == "UNKNOWN-Under Investigation"
