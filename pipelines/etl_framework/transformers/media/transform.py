"""
Enterprise ETL Framework - Media Transformer
Transforms raw TMDb data into Kimball star schema.

Produces:
    - dim_title: Movie/TV title dimension
    - dim_genre: Genre dimension
    - dim_language: Language dimension
    - dim_date: Date dimension
    - fact_title_metrics: Title performance facts
    - title_genre_bridge: Title-genre relationships
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

import sys

    

from ...core.base_transformer import BaseTransformer, TransformationResult, TableType
from ...core.surrogate_keys import generate_surrogate_key, generate_date_key
from ...schemas.media_schema import (
    DIMENSION_DEFINITIONS, FACT_DEFINITIONS, BRIDGE_DEFINITIONS,
    COLUMN_MAPPINGS, SCHEMA_METADATA, LANGUAGE_REFERENCE, GENRE_REFERENCE
)


class MediaTransformer(BaseTransformer):
    """
    Transformer for media/entertainment data into star schema.

    Example:
        transformer = MediaTransformer()
        result = transformer.transform(raw_titles_data)
        transformer.save_all()
    """

    def __init__(self, output_dir: Optional[str] = None, **kwargs):
        super().__init__(output_dir=output_dir, **kwargs)

    def get_schema_name(self) -> str:
        return 'media'

    def get_dimension_definitions(self) -> Dict[str, Dict[str, Any]]:
        return DIMENSION_DEFINITIONS

    def get_fact_definitions(self) -> Dict[str, Dict[str, Any]]:
        return FACT_DEFINITIONS

    def transform(self, raw_data: List[Dict]) -> TransformationResult:
        """
        Transform raw TMDb data into star schema.

        Args:
            raw_data: List of title dictionaries from MediaExtractor

        Returns:
            TransformationResult with all tables
        """
        result = TransformationResult(success=False, source=self.get_schema_name())

        try:
            self.logger.info(f"Starting media transformation: {len(raw_data)} titles")

            # Convert to DataFrame
            df = pd.DataFrame(raw_data)

            if df.empty:
                result.error = "No data to transform"
                return result

            # Add extraction date for fact table
            df['extracted_date'] = pd.to_datetime(
                df['extracted_at'].str[:10] if 'extracted_at' in df.columns
                else datetime.utcnow().strftime('%Y-%m-%d')
            )

            # Create dimensions
            self._create_title_dimension(df)
            self._create_genre_dimension(df)
            self._create_language_dimension(df)

            # Create date dimension
            if self.generate_date_dim:
                self.create_date_dimension()

            # Create fact table
            self._create_title_metrics_fact(df)

            # Create bridge tables
            self._create_genre_bridge(df)

            # Validate referential integrity
            warnings = self.validate_referential_integrity()
            result.warnings = warnings

            # Save all tables
            paths = self.save_all()

            # Populate result
            result.success = True
            result.tables_created = list(self.get_all_tables().keys())
            result.rows_by_table = self.get_row_counts()
            result.total_rows = sum(result.rows_by_table.values())
            result.output_paths = paths
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.schema_info = SCHEMA_METADATA

            self.logger.info(f"Transformation complete: {result.total_rows:,} total rows across {len(result.tables_created)} tables")

        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            result.error = str(e)
            import traceback
            self.logger.error(traceback.format_exc())

        return result

    def _create_title_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_title from raw data."""
        self.logger.info("Creating dim_title")

        title_cols = [
            'tmdb_id', 'title', 'original_title', 'media_type',
            'original_language', 'overview', 'release_date',
            'adult', 'poster_path', 'backdrop_path'
        ]

        # Filter to existing columns
        existing_cols = [c for c in title_cols if c in df.columns]

        if 'tmdb_id' not in existing_cols:
            self.logger.error("No tmdb_id column found")
            return pd.DataFrame()

        dim_df = df[existing_cols].drop_duplicates(subset=['tmdb_id']).copy()

        # Generate surrogate key
        dim_df['title_key'] = dim_df['tmdb_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Clean up data
        if 'title' in dim_df.columns:
            dim_df['title'] = dim_df['title'].fillna('Unknown')
        else:
            dim_df['title'] = 'Unknown'

        if 'media_type' in dim_df.columns:
            dim_df['media_type'] = dim_df['media_type'].fillna('unknown')
        else:
            dim_df['media_type'] = 'unknown'

        if 'adult' in dim_df.columns:
            dim_df['adult'] = dim_df['adult'].fillna(False).astype(bool)
        else:
            dim_df['adult'] = False

        # Add metadata
        dim_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_df['_source'] = 'tmdb'

        # Reorder columns
        cols = ['title_key', 'tmdb_id'] + [c for c in existing_cols if c != 'tmdb_id']
        cols = cols + ['_loaded_at', '_source']
        dim_df = dim_df[[c for c in cols if c in dim_df.columns]]

        self._dimensions['dim_title'] = dim_df
        self.logger.info(f"Created dim_title: {len(dim_df):,} rows")

        return dim_df

    def _create_genre_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_genre from raw data and reference."""
        self.logger.info("Creating dim_genre")

        if 'genre_ids' not in df.columns and 'genres' not in df.columns:
            self.logger.warning("No genre columns found")
            return pd.DataFrame()

        # Extract unique genre IDs from data
        all_genre_ids = set()
        for genre_ids_str in df['genre_ids'].dropna():
            for gid in str(genre_ids_str).split(','):
                if gid.strip() and gid.strip().isdigit():
                    all_genre_ids.add(int(gid.strip()))

        if not all_genre_ids:
            self.logger.warning("No genre IDs found in data")
            return pd.DataFrame()

        # Build genre dimension from reference
        genre_data = []
        for gid in all_genre_ids:
            genre_data.append({
                'genre_id': gid,
                'genre_name': GENRE_REFERENCE.get(gid, f'Genre {gid}'),
            })

        genre_df = pd.DataFrame(genre_data)

        # Generate surrogate key
        genre_df['genre_key'] = genre_df['genre_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Add metadata
        genre_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        genre_df['_source'] = 'tmdb'

        self._dimensions['dim_genre'] = genre_df
        self.logger.info(f"Created dim_genre: {len(genre_df):,} rows")

        return genre_df

    def _create_language_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_language from raw data and reference."""
        self.logger.info("Creating dim_language")

        if 'original_language' not in df.columns:
            self.logger.warning("No language column found")
            return pd.DataFrame()

        # Get unique languages from data
        languages = df['original_language'].dropna().unique().tolist()

        if not languages:
            self.logger.warning("No languages found in data")
            return pd.DataFrame()

        # Build language dimension
        lang_data = []
        for code in languages:
            if code:
                lang_data.append({
                    'language_code': code,
                    'language_name': LANGUAGE_REFERENCE.get(code, code.upper()),
                })

        lang_df = pd.DataFrame(lang_data).drop_duplicates(subset=['language_code'])

        # Generate surrogate key
        lang_df['language_key'] = lang_df['language_code'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Add metadata
        lang_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        lang_df['_source'] = 'tmdb'

        self._dimensions['dim_language'] = lang_df
        self.logger.info(f"Created dim_language: {len(lang_df):,} rows")

        return lang_df

    def _create_title_metrics_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create fact_title_metrics from raw data."""
        self.logger.info("Creating fact_title_metrics")

        measure_cols = [
            'popularity', 'vote_average', 'vote_count', 'is_trending'
        ]

        # Filter to existing columns
        existing_measures = [c for c in measure_cols if c in df.columns]

        fact_df = df[['tmdb_id', 'extracted_date'] + existing_measures].copy()

        # Generate keys
        fact_df['title_key'] = fact_df['tmdb_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        fact_df['date_key'] = fact_df['extracted_date'].apply(
            lambda x: generate_date_key(x)
        )

        # Calculate popularity rank
        if 'popularity' in fact_df.columns:
            fact_df['popularity_rank'] = fact_df['popularity'].rank(
                ascending=False, method='min'
            ).astype(int)

        # Calculate trending rank
        if 'is_trending' in fact_df.columns:
            trending_df = fact_df[fact_df['is_trending'] == True].copy()
            if not trending_df.empty:
                trending_df['trending_rank'] = trending_df['popularity'].rank(
                    ascending=False, method='min'
                ).astype(int)
                fact_df = fact_df.merge(
                    trending_df[['tmdb_id', 'trending_rank']],
                    on='tmdb_id',
                    how='left'
                )

        # Add extraction ID
        fact_df['extraction_id'] = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        fact_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        # Reorder columns
        key_cols = ['title_key', 'date_key']
        final_cols = key_cols + existing_measures
        if 'popularity_rank' in fact_df.columns:
            final_cols.append('popularity_rank')
        if 'trending_rank' in fact_df.columns:
            final_cols.append('trending_rank')
        final_cols.extend(['extraction_id', '_loaded_at'])

        fact_df = fact_df[[c for c in final_cols if c in fact_df.columns]]

        self._facts['fact_title_metrics'] = fact_df
        self.logger.info(f"Created fact_title_metrics: {len(fact_df):,} rows")

        return fact_df

    def _create_genre_bridge(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create title_genre_bridge from raw data."""
        self.logger.info("Creating title_genre_bridge")

        if 'genre_ids' not in df.columns:
            self.logger.warning("No genre_ids column found")
            return pd.DataFrame()

        # Explode title-genre relationships
        relationships = []
        for _, row in df[['tmdb_id', 'genre_ids']].iterrows():
            if pd.notna(row['genre_ids']):
                for gid in str(row['genre_ids']).split(','):
                    if gid.strip() and gid.strip().isdigit():
                        relationships.append({
                            'tmdb_id': row['tmdb_id'],
                            'genre_id': int(gid.strip()),
                        })

        if not relationships:
            self.logger.warning("No title-genre relationships found")
            return pd.DataFrame()

        bridge_df = pd.DataFrame(relationships).drop_duplicates()

        # Generate keys
        bridge_df['title_key'] = bridge_df['tmdb_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        bridge_df['genre_key'] = bridge_df['genre_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Select just the keys
        bridge_df = bridge_df[['title_key', 'genre_key']].drop_duplicates()
        bridge_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        self._bridges['title_genre_bridge'] = bridge_df
        self.logger.info(f"Created title_genre_bridge: {len(bridge_df):,} rows")

        return bridge_df
