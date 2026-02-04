"""
Enterprise ETL Framework - Gaming Transformer
Transforms raw Steam/SteamSpy data into Kimball star schema.

Produces:
    - dim_game: Game master dimension
    - dim_developer: Developer dimension
    - dim_genre: Genre dimension
    - dim_tag: Tag dimension
    - dim_date: Date dimension
    - fact_game_metrics: Game performance facts
    - game_genre_bridge: Game-genre relationships
    - game_tag_bridge: Game-tag relationships
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

import sys

    

from ...core.base_transformer import BaseTransformer, TransformationResult, TableType
from ...core.surrogate_keys import generate_surrogate_key, generate_date_key
from ...schemas.gaming_schema import (
    DIMENSION_DEFINITIONS, FACT_DEFINITIONS, BRIDGE_DEFINITIONS,
    COLUMN_MAPPINGS, SCHEMA_METADATA
)


class GamingTransformer(BaseTransformer):
    """
    Transformer for gaming data (Steam/SteamSpy) into star schema.

    Example:
        transformer = GamingTransformer()
        result = transformer.transform(raw_games_data)
        transformer.save_all()
    """

    def __init__(self, output_dir: Optional[str] = None, **kwargs):
        super().__init__(output_dir=output_dir, **kwargs)

    def get_schema_name(self) -> str:
        return 'gaming'

    def get_dimension_definitions(self) -> Dict[str, Dict[str, Any]]:
        return DIMENSION_DEFINITIONS

    def get_fact_definitions(self) -> Dict[str, Dict[str, Any]]:
        return FACT_DEFINITIONS

    def transform(self, raw_data: List[Dict]) -> TransformationResult:
        """
        Transform raw gaming data into star schema.

        Args:
            raw_data: List of game dictionaries from SteamExtractor

        Returns:
            TransformationResult with all tables
        """
        result = TransformationResult(success=False, source=self.get_schema_name())

        try:
            self.logger.info(f"Starting gaming transformation: {len(raw_data)} games")

            # Convert to DataFrame
            df = pd.DataFrame(raw_data)

            if df.empty:
                result.error = "No data to transform"
                return result

            # Add extraction date for fact table
            if 'extracted_at' in df.columns:
                df['extracted_date'] = pd.to_datetime(df['extracted_at'].str[:10])
            else:
                df['extracted_date'] = pd.to_datetime(datetime.utcnow().strftime('%Y-%m-%d'))

            # Create dimensions
            self._create_game_dimension(df)
            self._create_tag_dimension(df)

            # Create date dimension
            if self.generate_date_dim:
                self.create_date_dimension()

            # Create fact table
            self._create_game_metrics_fact(df)

            # Create bridge tables
            self._create_tag_bridge(df)

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

    def _create_game_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_game from raw data."""
        self.logger.info("Creating dim_game")

        # Select columns for game dimension - only use what exists
        game_cols = [
            'app_id', 'name', 'type', 'short_description', 'is_free',
            'developer', 'publisher', 'release_date', 'coming_soon',
            'required_age', 'header_image', 'website', 'metacritic_score',
            'achievement_count', 'platform_windows', 'platform_mac', 'platform_linux'
        ]

        # Filter to existing columns
        existing_cols = [c for c in game_cols if c in df.columns]
        
        # Must have at least app_id
        if 'app_id' not in existing_cols:
            self.logger.error("No app_id column found")
            return pd.DataFrame()
            
        dim_df = df[existing_cols].drop_duplicates(subset=['app_id']).copy()

        # Generate surrogate key
        dim_df['game_key'] = dim_df['app_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Clean up data with safe checks
        if 'name' in dim_df.columns:
            dim_df['name'] = dim_df['name'].fillna('Unknown')
        else:
            dim_df['name'] = 'Unknown'
            
        if 'type' in dim_df.columns:
            dim_df['type'] = dim_df['type'].fillna('game')
        
        if 'is_free' in dim_df.columns:
            dim_df['is_free'] = dim_df['is_free'].fillna(False).astype(bool)

        # Add metadata
        dim_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_df['_source'] = 'steam'

        # Reorder columns (game_key first, then app_id, then rest)
        cols = ['game_key', 'app_id'] + [c for c in existing_cols if c not in ['app_id']]
        cols = cols + ['_loaded_at', '_source']
        dim_df = dim_df[[c for c in cols if c in dim_df.columns]]

        self._dimensions['dim_game'] = dim_df
        self.logger.info(f"Created dim_game: {len(dim_df):,} rows")

        return dim_df

    def _create_tag_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_tag from raw data (SteamSpy tags)."""
        self.logger.info("Creating dim_tag")

        if 'tags' not in df.columns:
            self.logger.warning("No tags column found")
            return pd.DataFrame()

        # Explode tags (comma-separated)
        all_tags = set()
        for tags_str in df['tags'].dropna():
            for tag in str(tags_str).split(','):
                if tag.strip():
                    all_tags.add(tag.strip())

        if not all_tags:
            return pd.DataFrame()

        tag_df = pd.DataFrame({'tag_name': list(all_tags)})

        # Generate surrogate key
        tag_df['tag_key'] = tag_df['tag_name'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Add metadata
        tag_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        tag_df['_source'] = 'steamspy'

        self._dimensions['dim_tag'] = tag_df
        self.logger.info(f"Created dim_tag: {len(tag_df):,} rows")

        return tag_df

    def _create_game_metrics_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create fact_game_metrics from raw data."""
        self.logger.info("Creating fact_game_metrics")

        # Measure columns
        measure_cols = [
            'current_players', 'players_forever', 'players_2weeks', 'ccu',
            'owners_min', 'owners_max', 'owners_estimate',
            'average_playtime_forever', 'average_playtime_2weeks',
            'median_playtime_forever', 'median_playtime_2weeks',
            'positive_reviews', 'negative_reviews', 'review_score',
            'price_cents', 'price_final_cents', 'discount_percent',
            'price_steamspy'
        ]

        # Filter to existing columns
        existing_measures = [c for c in measure_cols if c in df.columns]
        
        # Build fact table with app_id and date
        fact_cols = ['app_id', 'extracted_date'] + existing_measures
        fact_df = df[[c for c in fact_cols if c in df.columns]].copy()

        # Generate keys
        fact_df['game_key'] = fact_df['app_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        fact_df['date_key'] = fact_df['extracted_date'].apply(
            lambda x: generate_date_key(x)
        )

        # Calculate total reviews if not present
        if 'total_reviews' not in fact_df.columns:
            if 'positive_reviews' in fact_df.columns and 'negative_reviews' in fact_df.columns:
                fact_df['total_reviews'] = fact_df['positive_reviews'].fillna(0) + fact_df['negative_reviews'].fillna(0)

        # Add extraction ID (degenerate dimension)
        fact_df['extraction_id'] = datetime.utcnow().strftime('%Y%m%d%H%M%S')

        # Add metadata
        fact_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        # Reorder columns - keys first, then measures
        key_cols = ['game_key', 'date_key']
        other_cols = [c for c in fact_df.columns if c not in key_cols and c not in ['app_id', 'extracted_date']]
        fact_df = fact_df[key_cols + other_cols]

        self._facts['fact_game_metrics'] = fact_df
        self.logger.info(f"Created fact_game_metrics: {len(fact_df):,} rows")

        return fact_df

    def _create_tag_bridge(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create game_tag_bridge from raw data."""
        self.logger.info("Creating game_tag_bridge")

        if 'tags' not in df.columns:
            return pd.DataFrame()

        # Explode game-tag relationships
        relationships = []
        for _, row in df[['app_id', 'tags']].iterrows():
            if pd.notna(row['tags']):
                for tag in str(row['tags']).split(','):
                    if tag.strip():
                        relationships.append({
                            'app_id': row['app_id'],
                            'tag_name': tag.strip(),
                        })

        if not relationships:
            return pd.DataFrame()

        bridge_df = pd.DataFrame(relationships).drop_duplicates()

        # Generate keys
        bridge_df['game_key'] = bridge_df['app_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        bridge_df['tag_key'] = bridge_df['tag_name'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Select just the keys
        bridge_df = bridge_df[['game_key', 'tag_key']].drop_duplicates()
        bridge_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        self._bridges['game_tag_bridge'] = bridge_df
        self.logger.info(f"Created game_tag_bridge: {len(bridge_df):,} rows")

        return bridge_df
