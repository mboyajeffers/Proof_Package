"""
Enterprise ETL Framework - Crypto Star Schema
Dimensional model definitions for cryptocurrency analytics.

Schema:
    Dimensions:
        - dim_coin: Cryptocurrency master data
        - dim_exchange: Exchange information
        - dim_category: Crypto categories (DeFi, Layer1, etc.)
        - dim_date: Standard date dimension

    Facts:
        - fact_coin_metrics: Daily coin market metrics
        - fact_exchange_metrics: Daily exchange volume metrics

    Bridges:
        - coin_category_bridge: Many-to-many coin-category relationship
"""

from typing import Dict, List, Any


# =============================================================================
# Dimension Definitions
# =============================================================================

DIMENSION_DEFINITIONS = {
    'dim_coin': {
        'natural_key': ['coin_id'],
        'attributes': [
            'coin_id',
            'symbol',
            'name',
            'image_url',
            'description',
            'genesis_date',
            'website',
            'github_url',
        ],
        'scd_type': 2,
        'description': 'Cryptocurrency master data with slowly changing attributes',
    },

    'dim_exchange': {
        'natural_key': ['exchange_id'],
        'attributes': [
            'exchange_id',
            'name',
            'year_established',
            'country',
            'url',
            'image_url',
            'has_trading_incentive',
        ],
        'scd_type': 1,
        'description': 'Cryptocurrency exchanges',
    },

    'dim_category': {
        'natural_key': ['category_name'],
        'attributes': [
            'category_name',
        ],
        'scd_type': 1,
        'description': 'Crypto categories (DeFi, Layer1, NFT, etc.)',
    },
}


# =============================================================================
# Fact Definitions
# =============================================================================

FACT_DEFINITIONS = {
    'fact_coin_metrics': {
        'grain': 'One row per coin per extraction date',
        'dimension_keys': {
            'dim_coin': ('coin_key', ['coin_id']),
            'dim_date': ('date_key', ['extracted_date']),
        },
        'measures': [
            # Price metrics
            'current_price',
            'market_cap',
            'fully_diluted_valuation',
            'total_volume',
            'market_cap_rank',

            # Supply metrics
            'circulating_supply',
            'total_supply',
            'max_supply',

            # Price changes
            'price_change_24h',
            'price_change_pct_24h',
            'price_change_pct_1h',
            'price_change_pct_7d',
            'price_change_pct_30d',

            # ATH/ATL
            'ath',
            'ath_change_pct',
            'atl',
            'atl_change_pct',

            # Community metrics
            'twitter_followers',
            'reddit_subscribers',
            'telegram_users',

            # Developer metrics
            'github_forks',
            'github_stars',
            'github_commit_count_4_weeks',

            # Sentiment
            'sentiment_up_pct',
            'sentiment_down_pct',
        ],
        'degenerate_dims': [
            'extraction_id',
        ],
        'description': 'Daily snapshot of cryptocurrency market metrics',
    },

    'fact_exchange_metrics': {
        'grain': 'One row per exchange per extraction date',
        'dimension_keys': {
            'dim_exchange': ('exchange_key', ['exchange_id']),
            'dim_date': ('date_key', ['extracted_date']),
        },
        'measures': [
            'trust_score',
            'trust_score_rank',
            'trade_volume_24h_btc',
            'trade_volume_24h_btc_normalized',
        ],
        'degenerate_dims': [
            'extraction_id',
        ],
        'description': 'Daily snapshot of exchange trading metrics',
    },
}


# =============================================================================
# Bridge Definitions
# =============================================================================

BRIDGE_DEFINITIONS = {
    'coin_category_bridge': {
        'left_dimension': 'dim_coin',
        'left_key': ('coin_key', ['coin_id']),
        'right_dimension': 'dim_category',
        'right_key': ('category_key', ['category_name']),
        'description': 'Many-to-many relationship between coins and categories',
    },
}


# =============================================================================
# Column Mappings
# =============================================================================

COLUMN_MAPPINGS = {
    'dim_coin': {
        'coin_id': 'coin_id',
        'symbol': 'symbol',
        'name': 'name',
        'image_url': 'image_url',
        'description': 'description',
        'genesis_date': 'genesis_date',
        'website': 'website',
        'github_url': 'github_url',
    },

    'dim_exchange': {
        'exchange_id': 'exchange_id',
        'name': 'name',
        'year_established': 'year_established',
        'country': 'country',
        'url': 'url',
        'image_url': 'image_url',
        'has_trading_incentive': 'has_trading_incentive',
    },

    'fact_coin_metrics': {
        'coin_id': 'coin_id',
        'current_price': 'current_price',
        'market_cap': 'market_cap',
        'total_volume': 'total_volume',
        'market_cap_rank': 'market_cap_rank',
        'circulating_supply': 'circulating_supply',
        'total_supply': 'total_supply',
        'max_supply': 'max_supply',
        'price_change_24h': 'price_change_24h',
        'price_change_pct_24h': 'price_change_pct_24h',
        'ath': 'ath',
        'atl': 'atl',
    },
}


# =============================================================================
# Schema Metadata
# =============================================================================

SCHEMA_METADATA = {
    'name': 'crypto',
    'version': '1.0.0',
    'description': 'Star schema for cryptocurrency analytics',
    'data_sources': ['CoinGecko API'],
    'dimensions': list(DIMENSION_DEFINITIONS.keys()),
    'facts': list(FACT_DEFINITIONS.keys()),
    'bridges': list(BRIDGE_DEFINITIONS.keys()),
    'target_rows': {
        'dim_coin': '10,000+',
        'fact_coin_metrics': '3,650,000+',  # 10K coins x 365 days
    },
}


def get_schema_info() -> Dict[str, Any]:
    """Return complete schema information."""
    return {
        'metadata': SCHEMA_METADATA,
        'dimensions': DIMENSION_DEFINITIONS,
        'facts': FACT_DEFINITIONS,
        'bridges': BRIDGE_DEFINITIONS,
        'column_mappings': COLUMN_MAPPINGS,
    }
