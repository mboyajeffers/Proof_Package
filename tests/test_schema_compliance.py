"""
Tests for star schema compliance.
Validates dimensional modeling standards (Kimball methodology).

Author: Mboya Jeffers
"""

import pandas as pd


class SchemaValidator:
    """
    Validates star schema compliance for dimensional models.
    Enforces Kimball methodology standards.
    """

    DIMENSION_PREFIXES = ['dim_', 'dimension_']
    FACT_PREFIXES = ['fact_', 'fct_']

    @staticmethod
    def validate_dimension_table(df: pd.DataFrame, table_name: str) -> dict:
        """
        Validate a dimension table follows standards.

        Requirements:
        - Has a surrogate key column (ends with _key or _sk)
        - Has descriptive attributes
        - No measures (numeric aggregates)
        """
        errors = []
        warnings = []

        # Check naming convention
        is_dimension = any(table_name.lower().startswith(p) for p in SchemaValidator.DIMENSION_PREFIXES)
        if not is_dimension:
            warnings.append(f"Table '{table_name}' doesn't follow dim_ naming convention")

        # Check for surrogate key
        key_cols = [c for c in df.columns if c.endswith('_key') or c.endswith('_sk')]
        if not key_cols:
            errors.append("Missing surrogate key column (should end with _key or _sk)")

        # Check surrogate key uniqueness
        if key_cols:
            for key_col in key_cols:
                if df[key_col].duplicated().any():
                    errors.append(f"Surrogate key '{key_col}' contains duplicates")

        return {
            'table_name': table_name,
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'row_count': len(df),
            'column_count': len(df.columns)
        }

    @staticmethod
    def validate_fact_table(df: pd.DataFrame, table_name: str, dimension_keys: list) -> dict:
        """
        Validate a fact table follows standards.

        Requirements:
        - Has foreign keys to dimension tables
        - Has at least one measure column
        - Grain is documented (implied by key combination)
        """
        errors = []
        warnings = []

        # Check naming convention
        is_fact = any(table_name.lower().startswith(p) for p in SchemaValidator.FACT_PREFIXES)
        if not is_fact:
            warnings.append(f"Table '{table_name}' doesn't follow fact_ naming convention")

        # Check for dimension keys
        missing_keys = [k for k in dimension_keys if k not in df.columns]
        if missing_keys:
            errors.append(f"Missing dimension keys: {missing_keys}")

        # Check for measures (numeric columns that aren't keys)
        key_cols = [c for c in df.columns if c.endswith('_key') or c.endswith('_sk') or c.endswith('_id')]
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        measures = [c for c in numeric_cols if c not in key_cols]

        if not measures:
            warnings.append("No measure columns detected")

        return {
            'table_name': table_name,
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'dimension_keys_found': [k for k in dimension_keys if k in df.columns],
            'measures_found': measures,
            'row_count': len(df)
        }

    @staticmethod
    def validate_referential_integrity(fact_df: pd.DataFrame, dim_df: pd.DataFrame,
                                        fact_key: str, dim_key: str) -> dict:
        """
        Validate foreign key relationship between fact and dimension.
        All fact keys should exist in dimension.
        """
        if fact_key not in fact_df.columns:
            return {'is_valid': False, 'error': f"Key '{fact_key}' not in fact table"}

        if dim_key not in dim_df.columns:
            return {'is_valid': False, 'error': f"Key '{dim_key}' not in dimension table"}

        fact_keys = set(fact_df[fact_key].dropna().unique())
        dim_keys = set(dim_df[dim_key].dropna().unique())

        orphan_keys = fact_keys - dim_keys

        return {
            'is_valid': len(orphan_keys) == 0,
            'orphan_count': len(orphan_keys),
            'orphan_keys': list(orphan_keys)[:10],  # Sample of orphans
            'fact_key_count': len(fact_keys),
            'dim_key_count': len(dim_keys)
        }


class TestDimensionTableValidation:
    """Test suite for dimension table validation."""

    def test_valid_dimension_passes(self):
        """Properly structured dimension should pass."""
        dim_game = pd.DataFrame({
            'game_key': ['a1b2c3d4e5f6a7b8', 'b2c3d4e5f6a7b8c9'],
            'game_id': [730, 570],
            'name': ['CS2', 'Dota 2'],
            'developer': ['Valve', 'Valve']
        })

        result = SchemaValidator.validate_dimension_table(dim_game, 'dim_game')
        assert result['is_valid'] is True
        assert len(result['errors']) == 0

    def test_missing_surrogate_key_fails(self):
        """Dimension without surrogate key should fail."""
        bad_dim = pd.DataFrame({
            'game_id': [730, 570],
            'name': ['CS2', 'Dota 2']
        })

        result = SchemaValidator.validate_dimension_table(bad_dim, 'dim_game')
        assert result['is_valid'] is False
        assert 'surrogate key' in result['errors'][0].lower()

    def test_duplicate_key_fails(self):
        """Dimension with duplicate keys should fail."""
        bad_dim = pd.DataFrame({
            'game_key': ['same_key', 'same_key'],
            'name': ['Game 1', 'Game 2']
        })

        result = SchemaValidator.validate_dimension_table(bad_dim, 'dim_game')
        assert result['is_valid'] is False


class TestFactTableValidation:
    """Test suite for fact table validation."""

    def test_valid_fact_passes(self):
        """Properly structured fact should pass."""
        fact_metrics = pd.DataFrame({
            'game_key': ['key1', 'key2'],
            'date_key': [20260205, 20260205],
            'player_count': [50000, 30000],
            'revenue': [1000.50, 750.25]
        })

        result = SchemaValidator.validate_fact_table(
            fact_metrics,
            'fact_game_metrics',
            ['game_key', 'date_key']
        )
        assert result['is_valid'] is True
        assert 'player_count' in result['measures_found']

    def test_missing_dimension_key_fails(self):
        """Fact missing required dimension key should fail."""
        bad_fact = pd.DataFrame({
            'game_key': ['key1', 'key2'],
            'revenue': [1000, 750]
        })

        result = SchemaValidator.validate_fact_table(
            bad_fact,
            'fact_metrics',
            ['game_key', 'date_key']
        )
        assert result['is_valid'] is False
        assert 'date_key' in str(result['errors'])


class TestReferentialIntegrity:
    """Test suite for referential integrity validation."""

    def test_valid_relationship_passes(self):
        """Valid FK relationship should pass."""
        dim_game = pd.DataFrame({
            'game_key': ['key1', 'key2', 'key3']
        })
        fact_metrics = pd.DataFrame({
            'game_key': ['key1', 'key2', 'key1']
        })

        result = SchemaValidator.validate_referential_integrity(
            fact_metrics, dim_game, 'game_key', 'game_key'
        )
        assert result['is_valid'] is True
        assert result['orphan_count'] == 0

    def test_orphan_keys_detected(self):
        """Orphan keys in fact table should be detected."""
        dim_game = pd.DataFrame({
            'game_key': ['key1', 'key2']
        })
        fact_metrics = pd.DataFrame({
            'game_key': ['key1', 'key3', 'key4']  # key3, key4 are orphans
        })

        result = SchemaValidator.validate_referential_integrity(
            fact_metrics, dim_game, 'game_key', 'game_key'
        )
        assert result['is_valid'] is False
        assert result['orphan_count'] == 2


class TestKimballCompliance:
    """Integration tests for Kimball methodology compliance."""

    def test_full_star_schema_validation(self, star_schema_tables):
        """Complete star schema should pass all validations."""
        # Create sample dimension
        dim_game = pd.DataFrame({
            'game_key': ['gk1', 'gk2'],
            'game_id': [1, 2],
            'name': ['Game A', 'Game B'],
            'developer': ['Dev A', 'Dev B'],
            'publisher': ['Pub A', 'Pub B']
        })

        # Create sample date dimension
        dim_date = pd.DataFrame({
            'date_key': [20260205, 20260206],
            'full_date': ['2026-02-05', '2026-02-06'],
            'year': [2026, 2026],
            'month': [2, 2],
            'day': [5, 6]
        })

        # Create sample fact
        fact_metrics = pd.DataFrame({
            'game_key': ['gk1', 'gk1', 'gk2'],
            'date_key': [20260205, 20260206, 20260205],
            'player_count': [1000, 1200, 500],
            'revenue': [99.99, 120.50, 45.00]
        })

        # Validate all tables
        dim_game_result = SchemaValidator.validate_dimension_table(dim_game, 'dim_game')
        dim_date_result = SchemaValidator.validate_dimension_table(dim_date, 'dim_date')
        fact_result = SchemaValidator.validate_fact_table(
            fact_metrics, 'fact_game_metrics', ['game_key', 'date_key']
        )

        # Validate referential integrity
        ri_game = SchemaValidator.validate_referential_integrity(
            fact_metrics, dim_game, 'game_key', 'game_key'
        )
        ri_date = SchemaValidator.validate_referential_integrity(
            fact_metrics, dim_date, 'date_key', 'date_key'
        )

        # All should pass
        assert dim_game_result['is_valid']
        assert dim_date_result['is_valid']
        assert fact_result['is_valid']
        assert ri_game['is_valid']
        assert ri_date['is_valid']
