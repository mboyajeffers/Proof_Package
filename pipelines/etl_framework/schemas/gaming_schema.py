"""
Enterprise ETL Framework - Gaming Star Schema
Dimensional model definitions for gaming analytics (Steam, etc.)

Schema:
    Dimensions:
        - dim_game: Game master data (SCD Type 2)
        - dim_developer: Developer/publisher information
        - dim_genre: Game genres
        - dim_platform: Gaming platforms
        - dim_date: Standard date dimension

    Facts:
        - fact_game_metrics: Daily game performance metrics

    Bridges:
        - game_genre_bridge: Many-to-many game-genre relationship
        - game_tag_bridge: Many-to-many game-tag relationship
"""

from typing import Dict, List, Any


# =============================================================================
# Dimension Definitions
# =============================================================================

DIMENSION_DEFINITIONS = {
    'dim_game': {
        'natural_key': ['app_id'],
        'attributes': [
            'name',
            'type',
            'short_description',
            'is_free',
            'developer',
            'publisher',
            'release_date',
            'coming_soon',
            'required_age',
            'header_image',
            'website',
            'metacritic_score',
            'achievement_count',
            'platform_windows',
            'platform_mac',
            'platform_linux',
        ],
        'scd_type': 2,
        'description': 'Game master data with slowly changing attributes',
    },

    'dim_developer': {
        'natural_key': ['developer_name'],
        'attributes': [
            'developer_name',
            'game_count',
            'first_game_date',
            'latest_game_date',
        ],
        'scd_type': 1,
        'description': 'Game developers and studios',
    },

    'dim_publisher': {
        'natural_key': ['publisher_name'],
        'attributes': [
            'publisher_name',
            'game_count',
            'first_game_date',
            'latest_game_date',
        ],
        'scd_type': 1,
        'description': 'Game publishers',
    },

    'dim_genre': {
        'natural_key': ['genre_id'],
        'attributes': [
            'genre_name',
            'genre_id',
        ],
        'scd_type': 1,
        'description': 'Game genres (Action, RPG, Strategy, etc.)',
    },

    'dim_tag': {
        'natural_key': ['tag_name'],
        'attributes': [
            'tag_name',
        ],
        'scd_type': 1,
        'description': 'Community-defined game tags',
    },

    'dim_platform': {
        'natural_key': ['platform_name'],
        'attributes': [
            'platform_name',
            'platform_type',  # PC, Console, Mobile
        ],
        'scd_type': 1,
        'description': 'Gaming platforms',
    },
}


# =============================================================================
# Fact Definitions
# =============================================================================

FACT_DEFINITIONS = {
    'fact_game_metrics': {
        'grain': 'One row per game per extraction date',
        'dimension_keys': {
            'dim_game': ('game_key', ['app_id']),
            'dim_date': ('date_key', ['extracted_date']),
        },
        'measures': [
            # Player metrics
            'current_players',
            'players_forever',
            'players_2weeks',
            'ccu',  # Concurrent users peak

            # Ownership metrics
            'owners_min',
            'owners_max',
            'owners_estimate',

            # Playtime metrics (minutes)
            'average_playtime_forever',
            'average_playtime_2weeks',
            'median_playtime_forever',
            'median_playtime_2weeks',

            # Review metrics
            'positive_reviews',
            'negative_reviews',
            'total_reviews',
            'review_score',

            # Pricing
            'price_cents',
            'price_final_cents',
            'discount_percent',
        ],
        'degenerate_dims': [
            'extraction_id',
        ],
        'description': 'Daily snapshot of game performance metrics',
    },
}


# =============================================================================
# Bridge Definitions
# =============================================================================

BRIDGE_DEFINITIONS = {
    'game_genre_bridge': {
        'left_dimension': 'dim_game',
        'left_key': ('game_key', ['app_id']),
        'right_dimension': 'dim_genre',
        'right_key': ('genre_key', ['genre_id']),
        'description': 'Many-to-many relationship between games and genres',
    },

    'game_tag_bridge': {
        'left_dimension': 'dim_game',
        'left_key': ('game_key', ['app_id']),
        'right_dimension': 'dim_tag',
        'right_key': ('tag_key', ['tag_name']),
        'description': 'Many-to-many relationship between games and tags',
    },

    'game_developer_bridge': {
        'left_dimension': 'dim_game',
        'left_key': ('game_key', ['app_id']),
        'right_dimension': 'dim_developer',
        'right_key': ('developer_key', ['developer_name']),
        'description': 'Many-to-many relationship between games and developers',
    },
}


# =============================================================================
# Column Mappings
# =============================================================================

# Maps raw extraction columns to dimension columns
COLUMN_MAPPINGS = {
    'dim_game': {
        'app_id': 'app_id',
        'name': 'name',
        'type': 'type',
        'short_description': 'short_description',
        'is_free': 'is_free',
        'developer': 'developer',
        'publisher': 'publisher',
        'release_date': 'release_date',
        'coming_soon': 'coming_soon',
        'required_age': 'required_age',
        'header_image': 'header_image',
        'website': 'website',
        'metacritic_score': 'metacritic_score',
        'achievement_count': 'achievement_count',
        'platform_windows': 'platform_windows',
        'platform_mac': 'platform_mac',
        'platform_linux': 'platform_linux',
    },

    'fact_game_metrics': {
        'app_id': 'app_id',
        'current_players': 'current_players',
        'players_forever': 'players_forever',
        'players_2weeks': 'players_2weeks',
        'ccu': 'ccu',
        'owners_min': 'owners_min',
        'owners_max': 'owners_max',
        'owners_estimate': 'owners_estimate',
        'average_playtime_forever': 'average_playtime_forever',
        'average_playtime_2weeks': 'average_playtime_2weeks',
        'median_playtime_forever': 'median_playtime_forever',
        'median_playtime_2weeks': 'median_playtime_2weeks',
        'positive_reviews': 'positive_reviews',
        'negative_reviews': 'negative_reviews',
        'total_reviews': 'total_reviews',
        'review_score': 'review_score',
        'price_cents': 'price_cents',
        'price_final_cents': 'price_final_cents',
        'discount_percent': 'discount_percent',
    },
}


# =============================================================================
# Schema Metadata
# =============================================================================

SCHEMA_METADATA = {
    'name': 'gaming',
    'version': '1.0.0',
    'description': 'Star schema for gaming analytics (Steam, SteamSpy)',
    'data_sources': ['Steam Web API', 'SteamSpy API'],
    'dimensions': list(DIMENSION_DEFINITIONS.keys()),
    'facts': list(FACT_DEFINITIONS.keys()),
    'bridges': list(BRIDGE_DEFINITIONS.keys()),
    'target_rows': {
        'dim_game': '50,000+',
        'fact_game_metrics': '8,000,000+',
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
