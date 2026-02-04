"""
Enterprise ETL Framework - Betting Star Schema
Dimensional model definitions for sports betting analytics.

Schema:
    Dimensions:
        - dim_team: Team master data
        - dim_league: League/sport information
        - dim_venue: Stadium/arena data
        - dim_date: Standard date dimension

    Facts:
        - fact_team_standings: Team performance metrics
        - fact_game_results: Game outcomes and scores

    Bridges:
        - team_league_bridge: Team-league relationships (for multi-league teams)
"""

from typing import Dict, List, Any


# =============================================================================
# Dimension Definitions
# =============================================================================

DIMENSION_DEFINITIONS = {
    'dim_team': {
        'natural_key': ['team_id', 'league_code'],
        'attributes': [
            'team_id',
            'league_code',
            'abbreviation',
            'display_name',
            'short_name',
            'nickname',
            'location',
            'is_active',
            'logo_url',
            'color_primary',
            'color_alternate',
            'espn_url',
        ],
        'scd_type': 2,
        'description': 'Team master data with slowly changing attributes',
    },

    'dim_league': {
        'natural_key': ['league_code'],
        'attributes': [
            'league_code',
            'league_name',
            'sport',
        ],
        'scd_type': 1,
        'description': 'Sports leagues (NFL, NBA, etc.)',
    },

    'dim_venue': {
        'natural_key': ['venue_id'],
        'attributes': [
            'venue_id',
            'venue_name',
            'venue_city',
            'venue_state',
            'venue_capacity',
            'venue_indoor',
        ],
        'scd_type': 2,
        'description': 'Stadiums and arenas',
    },
}


# =============================================================================
# Fact Definitions
# =============================================================================

FACT_DEFINITIONS = {
    'fact_team_standings': {
        'grain': 'One row per team per extraction date',
        'dimension_keys': {
            'dim_team': ('team_key', ['team_id', 'league_code']),
            'dim_league': ('league_key', ['league_code']),
            'dim_date': ('date_key', ['extracted_date']),
        },
        'measures': [
            # Record
            'wins',
            'losses',
            'ties',
            'win_pct',

            # Scoring
            'points_for',
            'points_against',
            'point_differential',

            # Standings
            'playoff_seed',
            'games_back',

            # Streaks
            'streak_length',
            'streak_type',  # W or L
        ],
        'degenerate_dims': [
            'division',
            'division_record',
            'conference_record',
            'extraction_id',
        ],
        'description': 'Daily snapshot of team standings and performance',
    },

    'fact_game_results': {
        'grain': 'One row per game',
        'dimension_keys': {
            'dim_team': [
                ('home_team_key', ['home_team_id', 'league_code']),
                ('away_team_key', ['away_team_id', 'league_code']),
            ],
            'dim_venue': ('venue_key', ['venue_id']),
            'dim_date': ('date_key', ['game_date']),
        },
        'measures': [
            'home_score',
            'away_score',
            'total_score',
            'score_differential',
            'is_overtime',
        ],
        'degenerate_dims': [
            'game_id',
            'status',
            'status_detail',
        ],
        'description': 'Game results and scores',
    },
}


# =============================================================================
# Bridge Definitions
# =============================================================================

BRIDGE_DEFINITIONS = {
    'team_venue_bridge': {
        'left_dimension': 'dim_team',
        'left_key': ('team_key', ['team_id', 'league_code']),
        'right_dimension': 'dim_venue',
        'right_key': ('venue_key', ['venue_id']),
        'description': 'Team home venue relationship',
    },
}


# =============================================================================
# Column Mappings
# =============================================================================

COLUMN_MAPPINGS = {
    'dim_team': {
        'team_id': 'team_id',
        'league_code': 'league_code',
        'abbreviation': 'abbreviation',
        'display_name': 'display_name',
        'short_name': 'short_name',
        'nickname': 'nickname',
        'location': 'location',
        'is_active': 'is_active',
        'logo_url': 'logo_url',
        'color_primary': 'color_primary',
        'color_alternate': 'color_alternate',
        'espn_url': 'espn_url',
    },

    'dim_venue': {
        'venue_id': 'venue_id',
        'venue_name': 'venue_name',
        'venue_city': 'venue_city',
        'venue_state': 'venue_state',
        'venue_capacity': 'venue_capacity',
        'venue_indoor': 'venue_indoor',
    },

    'fact_team_standings': {
        'team_id': 'team_id',
        'league_code': 'league_code',
        'wins': 'wins',
        'losses': 'losses',
        'ties': 'ties',
        'win_pct': 'win_pct',
        'points_for': 'points_for',
        'points_against': 'points_against',
        'playoff_seed': 'playoff_seed',
        'games_back': 'games_back',
        'division': 'division',
    },
}


# =============================================================================
# League Reference Data
# =============================================================================

LEAGUE_REFERENCE = {
    'nfl': {'name': 'NFL', 'sport': 'Football', 'country': 'USA'},
    'nba': {'name': 'NBA', 'sport': 'Basketball', 'country': 'USA'},
    'mlb': {'name': 'MLB', 'sport': 'Baseball', 'country': 'USA'},
    'nhl': {'name': 'NHL', 'sport': 'Hockey', 'country': 'USA/Canada'},
    'ncaaf': {'name': 'NCAA Football', 'sport': 'Football', 'country': 'USA'},
    'ncaab': {'name': 'NCAA Basketball', 'sport': 'Basketball', 'country': 'USA'},
    'mls': {'name': 'MLS', 'sport': 'Soccer', 'country': 'USA'},
    'epl': {'name': 'Premier League', 'sport': 'Soccer', 'country': 'England'},
}


# =============================================================================
# Schema Metadata
# =============================================================================

SCHEMA_METADATA = {
    'name': 'betting',
    'version': '1.0.0',
    'description': 'Star schema for sports betting analytics',
    'data_sources': ['ESPN API'],
    'dimensions': list(DIMENSION_DEFINITIONS.keys()),
    'facts': list(FACT_DEFINITIONS.keys()),
    'bridges': list(BRIDGE_DEFINITIONS.keys()),
    'target_rows': {
        'dim_team': '500+',
        'fact_team_standings': '180,000+',  # 500 teams x 365 days
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
        'league_reference': LEAGUE_REFERENCE,
    }
