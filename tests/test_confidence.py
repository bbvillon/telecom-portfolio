"""
Tests for categorize_inference_confidence().
Verifies the recalibrated thresholds (corpus ceiling ~0.50).
"""
from pipeline import categorize_inference_confidence


class TestConfidenceThresholds:
    def test_high_at_exact_boundary(self):
        assert categorize_inference_confidence(0.40) == "high"

    def test_high_above_boundary(self):
        assert categorize_inference_confidence(0.50) == "high"

    def test_medium_at_exact_boundary(self):
        assert categorize_inference_confidence(0.28) == "medium"

    def test_medium_just_below_high(self):
        assert categorize_inference_confidence(0.399) == "medium"

    def test_low_at_exact_boundary(self):
        assert categorize_inference_confidence(0.15) == "low"

    def test_low_just_below_medium(self):
        assert categorize_inference_confidence(0.279) == "low"

    def test_very_low_just_below_low(self):
        assert categorize_inference_confidence(0.149) == "very_low"

    def test_very_low_at_zero(self):
        assert categorize_inference_confidence(0.0) == "very_low"

    def test_very_low_near_zero(self):
        assert categorize_inference_confidence(0.001) == "very_low"

    def test_old_thresholds_no_longer_produce_high(self):
        # Old high threshold was 0.75 — that should now be high under new thresholds too
        # but 0.60 (old medium) should now be high
        assert categorize_inference_confidence(0.60) == "high"
        assert categorize_inference_confidence(0.51) == "high"
