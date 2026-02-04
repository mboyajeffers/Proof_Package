"""
Enterprise ETL Framework - Media Star Schema
Dimensional model definitions for entertainment/media analytics.

Schema:
    Dimensions:
        - dim_title: Movie/TV show master data
        - dim_genre: Genre information
        - dim_language: Language dimension
        - dim_date: Standard date dimension

    Facts:
        - fact_title_metrics: Title popularity and rating metrics

    Bridges:
        - title_genre_bridge: Many-to-many title-genre relationship
"""

from typing import Dict, List, Any


# =============================================================================
# Dimension Definitions
# =============================================================================

DIMENSION_DEFINITIONS = {
    'dim_title': {
        'natural_key': ['tmdb_id'],
        'attributes': [
            'tmdb_id',
            'title',
            'original_title',
            'media_type',
            'original_language',
            'overview',
            'release_date',
            'adult',
            'poster_path',
            'backdrop_path',
        ],
        'scd_type': 2,
        'description': 'Movie and TV show master data',
    },

    'dim_genre': {
        'natural_key': ['genre_id'],
        'attributes': [
            'genre_id',
            'genre_name',
        ],
        'scd_type': 1,
        'description': 'Media genres (Action, Comedy, Drama, etc.)',
    },

    'dim_language': {
        'natural_key': ['language_code'],
        'attributes': [
            'language_code',
            'language_name',
        ],
        'scd_type': 1,
        'description': 'Content languages',
    },
}


# =============================================================================
# Fact Definitions
# =============================================================================

FACT_DEFINITIONS = {
    'fact_title_metrics': {
        'grain': 'One row per title per extraction date',
        'dimension_keys': {
            'dim_title': ('title_key', ['tmdb_id']),
            'dim_date': ('date_key', ['extracted_date']),
        },
        'measures': [
            # Popularity
            'popularity',
            'popularity_rank',

            # Ratings
            'vote_average',
            'vote_count',

            # Trending
            'is_trending',
            'trending_rank',
        ],
        'degenerate_dims': [
            'extraction_id',
        ],
        'description': 'Daily snapshot of title popularity and ratings',
    },
}


# =============================================================================
# Bridge Definitions
# =============================================================================

BRIDGE_DEFINITIONS = {
    'title_genre_bridge': {
        'left_dimension': 'dim_title',
        'left_key': ('title_key', ['tmdb_id']),
        'right_dimension': 'dim_genre',
        'right_key': ('genre_key', ['genre_id']),
        'description': 'Many-to-many relationship between titles and genres',
    },
}


# =============================================================================
# Column Mappings
# =============================================================================

COLUMN_MAPPINGS = {
    'dim_title': {
        'tmdb_id': 'tmdb_id',
        'title': 'title',
        'original_title': 'original_title',
        'media_type': 'media_type',
        'original_language': 'original_language',
        'overview': 'overview',
        'release_date': 'release_date',
        'adult': 'adult',
        'poster_path': 'poster_path',
        'backdrop_path': 'backdrop_path',
    },

    'fact_title_metrics': {
        'tmdb_id': 'tmdb_id',
        'popularity': 'popularity',
        'vote_average': 'vote_average',
        'vote_count': 'vote_count',
        'is_trending': 'is_trending',
    },
}


# =============================================================================
# Reference Data
# =============================================================================

LANGUAGE_REFERENCE = {
    'en': 'English',
    'es': 'Spanish',
    'fr': 'French',
    'de': 'German',
    'it': 'Italian',
    'pt': 'Portuguese',
    'ru': 'Russian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'zh': 'Chinese',
    'hi': 'Hindi',
    'ar': 'Arabic',
}

GENRE_REFERENCE = {
    # Movie genres
    28: 'Action',
    12: 'Adventure',
    16: 'Animation',
    35: 'Comedy',
    80: 'Crime',
    99: 'Documentary',
    18: 'Drama',
    10751: 'Family',
    14: 'Fantasy',
    36: 'History',
    27: 'Horror',
    10402: 'Music',
    9648: 'Mystery',
    10749: 'Romance',
    878: 'Science Fiction',
    10770: 'TV Movie',
    53: 'Thriller',
    10752: 'War',
    37: 'Western',
    # TV genres
    10759: 'Action & Adventure',
    10762: 'Kids',
    10763: 'News',
    10764: 'Reality',
    10765: 'Sci-Fi & Fantasy',
    10766: 'Soap',
    10767: 'Talk',
    10768: 'War & Politics',
}


# =============================================================================
# Schema Metadata
# =============================================================================

SCHEMA_METADATA = {
    'name': 'media',
    'version': '1.0.0',
    'description': 'Star schema for entertainment/media analytics',
    'data_sources': ['TMDb API'],
    'dimensions': list(DIMENSION_DEFINITIONS.keys()),
    'facts': list(FACT_DEFINITIONS.keys()),
    'bridges': list(BRIDGE_DEFINITIONS.keys()),
    'target_rows': {
        'dim_title': '100,000+',
        'fact_title_metrics': '36,500,000+',  # 100K titles x 365 days
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
        'language_reference': LANGUAGE_REFERENCE,
        'genre_reference': GENRE_REFERENCE,
    }
