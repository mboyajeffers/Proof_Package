"""
Tests for surrogate key generation.
Validates deterministic key generation for dimensional modeling.

Author: Mboya Jeffers
"""

from datetime import datetime, date
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.etl_framework.core.surrogate_keys import (
    generate_surrogate_key,
    generate_date_key,
    generate_time_key,
    generate_composite_key,
    validate_surrogate_key,
)


class TestSurrogateKeyGeneration:
    """Test suite for surrogate key functions."""

    def test_basic_key_generation(self):
        """Key should be 16-character hex string."""
        key = generate_surrogate_key('test_value')
        assert len(key) == 16
        assert all(c in '0123456789abcdef' for c in key)

    def test_deterministic_keys(self):
        """Same inputs should always produce same key."""
        key1 = generate_surrogate_key('game_123', 'steam')
        key2 = generate_surrogate_key('game_123', 'steam')
        assert key1 == key2

    def test_different_inputs_different_keys(self):
        """Different inputs should produce different keys."""
        key1 = generate_surrogate_key('game_123')
        key2 = generate_surrogate_key('game_456')
        assert key1 != key2

    def test_null_handling(self):
        """Null values should be handled consistently."""
        key1 = generate_surrogate_key(None, 'value')
        key2 = generate_surrogate_key(None, 'value')
        assert key1 == key2
        assert len(key1) == 16

    def test_case_insensitivity(self):
        """Keys should be case-insensitive."""
        key1 = generate_surrogate_key('APPLE')
        key2 = generate_surrogate_key('apple')
        assert key1 == key2

    def test_whitespace_normalization(self):
        """Leading/trailing whitespace should be trimmed."""
        key1 = generate_surrogate_key('  value  ')
        key2 = generate_surrogate_key('value')
        assert key1 == key2


class TestDateKeyGeneration:
    """Test suite for date key functions."""

    def test_date_key_format(self):
        """Date key should be YYYYMMDD integer."""
        key = generate_date_key(date(2026, 2, 5))
        assert key == 20260205

    def test_datetime_to_date_key(self):
        """Datetime should convert to date key."""
        key = generate_date_key(datetime(2026, 2, 5, 14, 30, 0))
        assert key == 20260205

    def test_string_date_parsing(self):
        """ISO string should parse to date key."""
        key = generate_date_key('2026-02-05')
        assert key == 20260205

    def test_time_key_format(self):
        """Time key should be HHMMSS integer."""
        key = generate_time_key(datetime(2026, 2, 5, 14, 30, 45))
        assert key == 143045


class TestCompositeKeys:
    """Test suite for composite key generation."""

    def test_prefixed_key(self):
        """Composite key should include prefix."""
        key = generate_composite_key('123', prefix='GAME_')
        assert key.startswith('GAME_')
        assert len(key) == 16

    def test_no_prefix(self):
        """Without prefix, should behave like regular key."""
        key1 = generate_composite_key('value')
        key2 = generate_surrogate_key('value')
        assert key1 == key2


class TestKeyValidation:
    """Test suite for key validation."""

    def test_valid_key(self):
        """Valid 16-char hex should pass."""
        assert validate_surrogate_key('a1b2c3d4e5f6a7b8') is True

    def test_invalid_length(self):
        """Wrong length should fail."""
        assert validate_surrogate_key('abc123') is False
        assert validate_surrogate_key('a' * 20) is False

    def test_invalid_characters(self):
        """Non-hex characters should fail."""
        assert validate_surrogate_key('ghijklmnopqrstuv') is False

    def test_non_string(self):
        """Non-string input should fail."""
        assert validate_surrogate_key(12345) is False
        assert validate_surrogate_key(None) is False
