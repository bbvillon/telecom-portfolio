"""
Tests for the clean_data() pipeline and its helper validation functions.
Uses sample_raw_df fixture — does not touch the real dataset.
"""
import pytest
import pandas as pd
import numpy as np
from pipeline import (
    validate_initial_data_quality,
    validate_cleaned_data,
    assign_real_area,
    categorize_inference_confidence,
)


class TestValidateInitialDataQuality:
    def test_returns_dict(self, sample_raw_df):
        result = validate_initial_data_quality(sample_raw_df)
        assert isinstance(result, dict)

    def test_has_quality_score(self, sample_raw_df):
        result = validate_initial_data_quality(sample_raw_df)
        assert "quality_score" in result
        assert 0 <= result["quality_score"] <= 100

    def test_detects_duplicates(self, sample_raw_df):
        df_dup = pd.concat([sample_raw_df, sample_raw_df.iloc[:2]], ignore_index=True)
        result = validate_initial_data_quality(df_dup)
        assert result["duplicate_tickets"] == 2

    def test_clean_df_has_zero_duplicates(self, sample_raw_df):
        result = validate_initial_data_quality(sample_raw_df)
        assert result["duplicate_tickets"] == 0


class TestValidateCleanedData:
    def test_passes_on_valid_fixture(self, sample_cleaned_df):
        result = validate_cleaned_data(sample_cleaned_df)
        assert result["overall_status"] == "PASS"

    def test_fails_on_duplicate_tickets(self, sample_cleaned_df):
        df_dup = pd.concat([sample_cleaned_df, sample_cleaned_df.iloc[:3]], ignore_index=True)
        result = validate_cleaned_data(df_dup)
        assert result["overall_status"] == "FAIL"

    def test_fails_on_invalid_region(self, sample_cleaned_df):
        df_bad = sample_cleaned_df.copy()
        df_bad.loc[0, "Region"] = "Region 99"
        result = validate_cleaned_data(df_bad)
        assert result["overall_status"] == "FAIL"

    def test_fails_on_negative_duration(self, sample_cleaned_df):
        df_bad = sample_cleaned_df.copy()
        df_bad.loc[0, "OUTAGEDURATION"] = -1.0
        result = validate_cleaned_data(df_bad)
        assert result["overall_status"] == "FAIL"

    def test_result_has_validations_list(self, sample_cleaned_df):
        result = validate_cleaned_data(sample_cleaned_df)
        assert "validations" in result
        assert len(result["validations"]) == 7


class TestAssignRealArea:
    def test_gma_contact_group_returns_region3(self):
        row = pd.Series({"ContactGroup": "GMA_NOC", "Area": "", "DESCRIPTION": ""})
        assert assign_real_area(row) == "Region 3"

    def test_vis_contact_group_returns_region4(self):
        row = pd.Series({"ContactGroup": "VIS_OPS", "Area": "", "DESCRIPTION": ""})
        assert assign_real_area(row) == "Region 4"

    def test_ncr_area_returns_region3(self):
        row = pd.Series({"ContactGroup": "", "Area": "NCR_001", "DESCRIPTION": ""})
        assert assign_real_area(row) == "Region 3"

    def test_nlz_area_returns_region1(self):
        row = pd.Series({"ContactGroup": "", "Area": "NL_SITE", "DESCRIPTION": ""})
        assert assign_real_area(row) == "Region 1"

    def test_description_keyword_fallback(self):
        row = pd.Series({"ContactGroup": "", "Area": "", "DESCRIPTION": "GMA CORE LINK DOWN"})
        assert assign_real_area(row) == "Region 3"

    def test_unknown_returns_unknown_area(self):
        row = pd.Series({"ContactGroup": "", "Area": "", "DESCRIPTION": ""})
        assert assign_real_area(row) == "Unknown Area"

    def test_nan_values_handled_gracefully(self):
        row = pd.Series({"ContactGroup": None, "Area": None, "DESCRIPTION": None})
        result = assign_real_area(row)
        assert isinstance(result, str)
