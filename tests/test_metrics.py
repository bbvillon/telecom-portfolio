"""
Tests for KPI calculation functions in src/fault_ticket/metrics.py.
Uses the sample_cleaned_df fixture (30 tickets, 3 zones, known values).
"""
import pytest
import pandas as pd
import numpy as np

# Import from project src layout
try:
    from src.fault_ticket.metrics import (
        calculate_mttr, calculate_sla_compliance,
        calculate_fault_density, calculate_noc_time,
        calculate_field_time, calculate_zone_summary,
    )
except ImportError:
    from metrics import (
        calculate_mttr, calculate_sla_compliance,
        calculate_fault_density, calculate_noc_time,
        calculate_field_time, calculate_zone_summary,
    )


class TestCalculateMTTR:
    def test_returns_dataframe(self, sample_cleaned_df):
        result = calculate_mttr(sample_cleaned_df, group_by="ZONE")
        assert isinstance(result, pd.DataFrame)

    def test_has_mttr_column(self, sample_cleaned_df):
        result = calculate_mttr(sample_cleaned_df, group_by="ZONE")
        assert "MTTR" in result.columns

    def test_correct_number_of_zones(self, sample_cleaned_df):
        result = calculate_mttr(sample_cleaned_df, group_by="ZONE")
        assert len(result) == 3  # ZONE 1, 2, 3 in fixture

    def test_ungrouped_returns_single_row(self, sample_cleaned_df):
        result = calculate_mttr(sample_cleaned_df)
        assert len(result) == 1

    def test_known_mttr_value(self, sample_cleaned_df):
        # Each zone has tiles [2.0, 5.0, 30.0] mapped to zones 1/2/3 in rotation
        # ZONE 1 gets all Priority=1 tickets (OUTAGEDURATION=2.0)
        z1 = sample_cleaned_df[sample_cleaned_df["ZONE"] == "ZONE 1"]
        expected = z1["OUTAGEDURATION"].mean()
        result = calculate_mttr(sample_cleaned_df, group_by="ZONE")
        actual = result[result["ZONE"] == "ZONE 1"]["MTTR"].values[0]
        assert abs(actual - expected) < 0.01


class TestCalculateSLACompliance:
    def test_returns_dataframe(self, sample_cleaned_df):
        result = calculate_sla_compliance(sample_cleaned_df, group_by="ZONE")
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, sample_cleaned_df):
        result = calculate_sla_compliance(sample_cleaned_df, group_by="ZONE")
        assert "SLA_Compliance_Rate" in result.columns

    def test_rate_between_0_and_100(self, sample_cleaned_df):
        result = calculate_sla_compliance(sample_cleaned_df, group_by="ZONE")
        assert (result["SLA_Compliance_Rate"] >= 0).all()
        assert (result["SLA_Compliance_Rate"] <= 100).all()

    def test_zone1_is_100_percent(self, sample_cleaned_df):
        # Fixture: ZONE 1 = P1 tickets, all SLA_Compliant=1
        result = calculate_sla_compliance(sample_cleaned_df, group_by="ZONE")
        z1_rate = result[result["ZONE"] == "ZONE 1"]["SLA_Compliance_Rate"].values[0]
        assert z1_rate == 100.0

    def test_zone3_is_0_percent(self, sample_cleaned_df):
        # Fixture: ZONE 3 = P3 tickets, all SLA_Compliant=0
        result = calculate_sla_compliance(sample_cleaned_df, group_by="ZONE")
        z3_rate = result[result["ZONE"] == "ZONE 3"]["SLA_Compliance_Rate"].values[0]
        assert z3_rate == 0.0


class TestCalculateFaultDensity:
    def test_returns_dataframe(self, sample_cleaned_df):
        result = calculate_fault_density(sample_cleaned_df, group_by="ZONE")
        assert isinstance(result, pd.DataFrame)

    def test_has_fault_density_column(self, sample_cleaned_df):
        result = calculate_fault_density(sample_cleaned_df, group_by="ZONE")
        assert "Fault_Density" in result.columns

    def test_density_positive(self, sample_cleaned_df):
        result = calculate_fault_density(sample_cleaned_df, group_by="ZONE")
        assert (result["Fault_Density"] > 0).all()

    def test_density_formula(self, sample_cleaned_df):
        # Density = Total_Faults / Unique_Sites
        result = calculate_fault_density(sample_cleaned_df, group_by="ZONE")
        row = result[result["ZONE"] == "ZONE 1"].iloc[0]
        assert abs(row["Fault_Density"] - row["Total_Faults"] / row["Unique_Sites"]) < 0.001


class TestCalculateNOCTime:
    def test_returns_dataframe(self, sample_cleaned_df):
        result = calculate_noc_time(sample_cleaned_df, group_by="ZONE")
        assert isinstance(result, pd.DataFrame)

    def test_has_avg_noc_time_column(self, sample_cleaned_df):
        result = calculate_noc_time(sample_cleaned_df, group_by="ZONE")
        assert "Avg_NOC_Time" in result.columns

    def test_excludes_bad_timestamp_integrity(self, sample_cleaned_df):
        df = sample_cleaned_df.copy()
        df.loc[df["ZONE"] == "ZONE 1", "Timestamp_Integrity"] = False
        result = calculate_noc_time(df, group_by="ZONE")
        # ZONE 1 rows excluded — result should not have ZONE 1 or have NaN
        z1 = result[result["ZONE"] == "ZONE 1"]
        assert len(z1) == 0 or z1["Avg_NOC_Time"].isna().all()


class TestCalculateFieldTime:
    def test_returns_dataframe(self, sample_cleaned_df):
        result = calculate_field_time(sample_cleaned_df, group_by="ZONE")
        assert isinstance(result, pd.DataFrame)

    def test_has_avg_field_time_column(self, sample_cleaned_df):
        result = calculate_field_time(sample_cleaned_df, group_by="ZONE")
        assert "Avg_Field_Time" in result.columns

    def test_uses_precomputed_column(self, sample_cleaned_df):
        # Fixture has FIELD_TIME_HOURS — should use it directly
        result = calculate_field_time(sample_cleaned_df, group_by="ZONE")
        expected_z1 = sample_cleaned_df[sample_cleaned_df["ZONE"] == "ZONE 1"]["FIELD_TIME_HOURS"].mean()
        actual_z1 = result[result["ZONE"] == "ZONE 1"]["Avg_Field_Time"].values[0]
        assert abs(actual_z1 - expected_z1) < 0.01


class TestCalculateZoneSummary:
    def test_returns_dataframe(self, sample_cleaned_df):
        result = calculate_zone_summary(sample_cleaned_df)
        assert isinstance(result, pd.DataFrame)

    def test_has_all_required_columns(self, sample_cleaned_df):
        result = calculate_zone_summary(sample_cleaned_df)
        for col in ["ZONE", "Ticket_Count", "MTTR", "SLA_Compliance_Rate",
                    "Fault_Density", "Avg_NOC_Time", "Avg_Field_Time"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_excludes_unknown_zone_by_default(self, sample_cleaned_df):
        df = sample_cleaned_df.copy()
        df.loc[0, "ZONE"] = "Unknown"
        result = calculate_zone_summary(df, exclude_unknown=True)
        assert "Unknown" not in result["ZONE"].values

    def test_includes_unknown_when_flag_false(self, sample_cleaned_df):
        df = sample_cleaned_df.copy()
        df.loc[0, "ZONE"] = "Unknown"
        result = calculate_zone_summary(df, exclude_unknown=False)
        assert "Unknown" in result["ZONE"].values

    def test_sorted_by_ticket_count_descending(self, sample_cleaned_df):
        result = calculate_zone_summary(sample_cleaned_df)
        counts = result["Ticket_Count"].tolist()
        assert counts == sorted(counts, reverse=True)
