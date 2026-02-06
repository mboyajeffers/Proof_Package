"""
Tests for data quality validation (quality gates).
Validates completeness, uniqueness, and range checks.

Author: Mboya Jeffers
"""

import pytest
import pandas as pd
import numpy as np


class DataValidator:
    """
    Data quality validator for ETL pipelines.
    Implements quality gates: completeness, uniqueness, range validation.
    """

    @staticmethod
    def check_completeness(df: pd.DataFrame, required_columns: list) -> dict:
        """Check for null values in required columns."""
        results = {}
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isna().sum()
                total = len(df)
                results[col] = {
                    'null_count': int(null_count),
                    'completeness_pct': round((total - null_count) / total * 100, 2) if total > 0 else 0,
                    'passed': null_count == 0
                }
        return results

    @staticmethod
    def check_uniqueness(df: pd.DataFrame, key_columns: list) -> dict:
        """Check for duplicate values in key columns."""
        if not all(col in df.columns for col in key_columns):
            return {'error': 'Missing columns', 'passed': False}

        duplicates = df.duplicated(subset=key_columns, keep=False).sum()
        return {
            'duplicate_count': int(duplicates),
            'unique_count': len(df) - duplicates,
            'passed': duplicates == 0
        }

    @staticmethod
    def check_range(df: pd.DataFrame, column: str, min_val: float, max_val: float) -> dict:
        """Check if values fall within expected range."""
        if column not in df.columns:
            return {'error': f'Column {column} not found', 'passed': False}

        values = df[column].dropna()
        out_of_range = ((values < min_val) | (values > max_val)).sum()

        return {
            'out_of_range_count': int(out_of_range),
            'min_found': float(values.min()) if len(values) > 0 else None,
            'max_found': float(values.max()) if len(values) > 0 else None,
            'passed': out_of_range == 0
        }


class TestCompletenessValidation:
    """Test suite for completeness checks."""

    def test_complete_data_passes(self, sample_gaming_data):
        """Data with no nulls should pass completeness check."""
        result = DataValidator.check_completeness(
            sample_gaming_data,
            ['app_id', 'name', 'developer']
        )
        assert all(r['passed'] for r in result.values())

    def test_null_detection(self, messy_data):
        """Nulls should be detected and reported."""
        result = DataValidator.check_completeness(messy_data, ['id', 'email'])
        assert result['id']['null_count'] == 1
        assert result['email']['null_count'] == 1
        assert result['id']['passed'] is False

    def test_completeness_percentage(self):
        """Completeness percentage should be calculated correctly."""
        df = pd.DataFrame({'col': [1, 2, None, 4, None]})
        result = DataValidator.check_completeness(df, ['col'])
        assert result['col']['completeness_pct'] == 60.0


class TestUniquenessValidation:
    """Test suite for uniqueness checks."""

    def test_unique_data_passes(self, sample_gaming_data):
        """Data with unique keys should pass."""
        result = DataValidator.check_uniqueness(sample_gaming_data, ['app_id'])
        assert result['passed'] is True
        assert result['duplicate_count'] == 0

    def test_duplicate_detection(self, messy_data):
        """Duplicates should be detected."""
        result = DataValidator.check_uniqueness(messy_data, ['id'])
        assert result['passed'] is False
        assert result['duplicate_count'] == 2  # Both duplicate rows flagged

    def test_composite_key_uniqueness(self):
        """Composite keys should be validated together."""
        df = pd.DataFrame({
            'game_id': [1, 1, 2],
            'platform': ['steam', 'epic', 'steam']
        })
        result = DataValidator.check_uniqueness(df, ['game_id', 'platform'])
        assert result['passed'] is True


class TestRangeValidation:
    """Test suite for range validation."""

    def test_valid_range_passes(self, sample_gaming_data):
        """Values within range should pass."""
        result = DataValidator.check_range(sample_gaming_data, 'price', 0, 1000)
        assert result['passed'] is True

    def test_out_of_range_detection(self, messy_data):
        """Out-of-range values should be detected."""
        result = DataValidator.check_range(messy_data, 'score', 0, 100)
        assert result['passed'] is False
        assert result['out_of_range_count'] == 2  # 120 and -5

    def test_range_bounds_reported(self):
        """Min and max found values should be reported."""
        df = pd.DataFrame({'value': [10, 50, 90]})
        result = DataValidator.check_range(df, 'value', 0, 100)
        assert result['min_found'] == 10
        assert result['max_found'] == 90


class TestQualityGateIntegration:
    """Integration tests for full quality gate pipeline."""

    def test_all_gates_pass(self, sample_financial_data):
        """Clean data should pass all quality gates."""
        completeness = DataValidator.check_completeness(
            sample_financial_data,
            ['cik', 'company_name', 'revenue']
        )
        uniqueness = DataValidator.check_uniqueness(
            sample_financial_data,
            ['cik']
        )
        range_check = DataValidator.check_range(
            sample_financial_data,
            'revenue',
            0,
            1e12
        )

        assert all(r['passed'] for r in completeness.values())
        assert uniqueness['passed']
        assert range_check['passed']

    def test_gate_failure_blocks_pipeline(self, messy_data):
        """Any gate failure should be catchable."""
        gates_passed = []

        completeness = DataValidator.check_completeness(messy_data, ['id'])
        gates_passed.append(all(r['passed'] for r in completeness.values()))

        uniqueness = DataValidator.check_uniqueness(messy_data, ['id'])
        gates_passed.append(uniqueness['passed'])

        # At least one gate should fail
        assert not all(gates_passed)
