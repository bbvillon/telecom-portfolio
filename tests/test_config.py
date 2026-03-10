"""
Tests for config.py — sanity checks on constants and logger setup.
"""
import logging
import pytest
from config import SLA_THRESHOLDS, ZONE_ORDER, REGION_ORDER, EXPECTED_RFO_VALUES, get_logger


class TestSLAThresholds:
    EXPECTED_PU_COMBOS = [
        "1.1","1.2","1.3",
        "2.1","2.2","2.3",
        "3.1","3.2","3.3",
        "4.1","4.2","4.3",
    ]

    def test_all_dot_notation_present(self):
        for pu in self.EXPECTED_PU_COMBOS:
            assert pu in SLA_THRESHOLDS, f"Missing P-U combo: {pu}"

    def test_p1_threshold_is_3h(self):
        assert SLA_THRESHOLDS["1.1"] == 3
        assert SLA_THRESHOLDS["1.2"] == 3
        assert SLA_THRESHOLDS["1.3"] == 3

    def test_p2_threshold_is_6h(self):
        assert SLA_THRESHOLDS["2.1"] == 6
        assert SLA_THRESHOLDS["2.2"] == 6
        assert SLA_THRESHOLDS["2.3"] == 6

    def test_p3_2_threshold_is_12h(self):
        # P3.2 is the degradation SLA — critical for breach analysis
        assert SLA_THRESHOLDS["3.2"] == 12

    def test_p4_threshold_is_72h(self):
        assert SLA_THRESHOLDS["4.1"] == 72

    def test_all_values_are_positive(self):
        for pu, h in SLA_THRESHOLDS.items():
            assert h > 0, f"Non-positive threshold for {pu}: {h}"


class TestZoneAndRegionOrder:
    def test_zone_order_has_6_zones(self):
        assert len(ZONE_ORDER) == 6

    def test_zone_order_named_correctly(self):
        assert ZONE_ORDER == [f"ZONE {i}" for i in range(1, 7)]

    def test_region_order_has_5_regions(self):
        assert len(REGION_ORDER) == 5

    def test_region_order_named_correctly(self):
        assert REGION_ORDER == [f"Region {i}" for i in range(1, 6)]


class TestExpectedRFOValues:
    def test_rfo_values_is_not_empty(self):
        assert len(EXPECTED_RFO_VALUES) > 0

    def test_foc_linear_present(self):
        assert "FOC CUT - LINEAR" in EXPECTED_RFO_VALUES

    def test_foc_redundant_present(self):
        assert "FOC CUT WITH REDUNDANCY" in EXPECTED_RFO_VALUES

    def test_unknown_categories_present(self):
        rfo_str = " ".join(EXPECTED_RFO_VALUES)
        assert "UNKNOWN" in rfo_str


class TestGetLogger:
    def test_returns_logger_instance(self):
        log = get_logger("test_module")
        assert isinstance(log, logging.Logger)

    def test_has_two_handlers(self):
        log = get_logger("test_handler_check")
        assert len(log.handlers) == 2

    def test_no_duplicate_handlers_on_reimport(self):
        log1 = get_logger("test_dedup")
        log2 = get_logger("test_dedup")
        assert len(log2.handlers) == 2  # Not 4
