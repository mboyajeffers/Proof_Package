"""
Enterprise ETL Framework - Surrogate Key Generator
Utilities for generating deterministic surrogate keys for dimensional modeling.

Uses MD5 hashing to create consistent, reproducible keys from natural key values.
This ensures the same natural key always produces the same surrogate key.
"""

import hashlib
from datetime import datetime, date
from typing import Union, Any


def generate_surrogate_key(*values: Any) -> str:
    """
    Generate a 16-character surrogate key from one or more values.

    Uses MD5 hash truncated to 16 hex characters. This provides:
    - Deterministic: same inputs always produce same key
    - Unique enough: 16^16 = 18 quintillion possible values
    - Compact: fits in a CHAR(16) column
    - Fast: MD5 is very fast for this use case

    Args:
        *values: One or more values to hash (natural key components)

    Returns:
        16-character hexadecimal string

    Example:
        >>> generate_surrogate_key('game_123', 'steam')
        'a1b2c3d4e5f6a7b8'
        >>> generate_surrogate_key('game_123', 'steam')  # Same inputs
        'a1b2c3d4e5f6a7b8'  # Same output
    """
    # Normalize values to strings
    normalized = []
    for v in values:
        if v is None:
            normalized.append('__NULL__')
        elif isinstance(v, (datetime, date)):
            normalized.append(v.isoformat())
        else:
            normalized.append(str(v).strip().lower())

    # Join with separator and hash
    key_string = '|'.join(normalized)
    hash_bytes = hashlib.md5(key_string.encode('utf-8')).hexdigest()

    # Return first 16 characters
    return hash_bytes[:16]


def generate_date_key(dt: Union[datetime, date, str]) -> int:
    """
    Generate an integer date key in YYYYMMDD format.

    This is the standard format for date dimension surrogate keys,
    allowing easy date arithmetic and human readability.

    Args:
        dt: Date as datetime, date, or string (YYYY-MM-DD)

    Returns:
        Integer in YYYYMMDD format

    Example:
        >>> generate_date_key(datetime(2026, 2, 4))
        20260204
        >>> generate_date_key('2026-02-04')
        20260204
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if isinstance(dt, datetime):
        dt = dt.date()

    return int(dt.strftime('%Y%m%d'))


def generate_time_key(dt: Union[datetime, str]) -> int:
    """
    Generate an integer time key in HHMMSS format.

    Useful for time-of-day dimension or grain finer than daily.

    Args:
        dt: Datetime or string with time component

    Returns:
        Integer in HHMMSS format

    Example:
        >>> generate_time_key(datetime(2026, 2, 4, 14, 30, 0))
        143000
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return int(dt.strftime('%H%M%S'))


def generate_composite_key(*values: Any, prefix: str = '') -> str:
    """
    Generate a surrogate key with an optional prefix.

    Useful when you need to distinguish keys from different sources
    or want human-readable prefixes.

    Args:
        *values: Values to hash
        prefix: Optional prefix (e.g., 'GAME_', 'TEAM_')

    Returns:
        Prefixed 16-character key

    Example:
        >>> generate_composite_key('123', prefix='GAME_')
        'GAME_a1b2c3d4e5f6'
    """
    base_key = generate_surrogate_key(*values)

    if prefix:
        # Truncate key to fit prefix
        max_key_len = 16 - len(prefix)
        return f"{prefix}{base_key[:max_key_len]}"

    return base_key


def validate_surrogate_key(key: str) -> bool:
    """
    Validate that a string is a valid surrogate key.

    Args:
        key: String to validate

    Returns:
        True if valid 16-character hex string
    """
    if not isinstance(key, str):
        return False

    if len(key) != 16:
        return False

    try:
        int(key, 16)
        return True
    except ValueError:
        return False


def generate_hash_key(df_row: dict, key_columns: list) -> str:
    """
    Generate a surrogate key from a DataFrame row dictionary.

    Convenience function for use with DataFrame.apply().

    Args:
        df_row: Row as dictionary (from df.to_dict('records'))
        key_columns: List of column names to use as natural key

    Returns:
        16-character surrogate key

    Example:
        >>> df['game_key'] = df.apply(
        ...     lambda row: generate_hash_key(row.to_dict(), ['game_id', 'platform']),
        ...     axis=1
        ... )
    """
    values = [df_row.get(col) for col in key_columns]
    return generate_surrogate_key(*values)


# Aliases for convenience
sk = generate_surrogate_key
dk = generate_date_key
