"""
Pytest fixtures for ETL framework tests.
Author: Mboya Jeffers
"""

import pytest
import pandas as pd
from datetime import datetime, date


@pytest.fixture
def sample_gaming_data():
    """Sample gaming data for testing transformations."""
    return pd.DataFrame({
        'app_id': [730, 570, 440],
        'name': ['Counter-Strike 2', 'Dota 2', 'Team Fortress 2'],
        'developer': ['Valve', 'Valve', 'Valve'],
        'publisher': ['Valve', 'Valve', 'Valve'],
        'price': [0.0, 0.0, 0.0],
        'release_date': ['2023-09-27', '2013-07-09', '2007-10-10'],
        'positive_reviews': [7500000, 2100000, 950000],
        'negative_reviews': [500000, 300000, 50000],
    })


@pytest.fixture
def sample_financial_data():
    """Sample SEC financial data for testing."""
    return pd.DataFrame({
        'cik': ['0000320193', '0000789019', '0001018724'],
        'company_name': ['Apple Inc.', 'Microsoft Corp', 'Amazon.com Inc'],
        'ticker': ['AAPL', 'MSFT', 'AMZN'],
        'revenue': [394328000000, 211915000000, 513983000000],
        'net_income': [99803000000, 72738000000, 30425000000],
        'filing_date': ['2023-10-27', '2023-10-24', '2023-10-26'],
    })


@pytest.fixture
def messy_data():
    """Data with quality issues for validation testing."""
    return pd.DataFrame({
        'id': [1, 2, 2, 4, None],  # Duplicate and null
        'name': ['Alice', 'Bob', 'Bob', '', 'Eve'],  # Empty string
        'score': [85, 120, 75, -5, 90],  # Out of range (0-100)
        'email': ['a@test.com', 'invalid', 'b@test.com', 'c@test.com', None],
    })


@pytest.fixture
def star_schema_tables():
    """Expected structure for star schema validation."""
    return {
        'dim_game': ['game_key', 'game_id', 'name', 'developer', 'publisher'],
        'dim_date': ['date_key', 'full_date', 'year', 'month', 'day'],
        'fact_game_metrics': ['game_key', 'date_key', 'player_count', 'revenue'],
    }
