"""
Enterprise ETL Framework - Betting Transformer
Transforms raw ESPN data into Kimball star schema.

Produces:
    - dim_team: Team dimension
    - dim_league: League dimension
    - dim_venue: Venue dimension
    - dim_date: Date dimension
    - fact_team_standings: Team standings facts
    - team_venue_bridge: Team-venue relationships
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

import sys

    

from ...core.base_transformer import BaseTransformer, TransformationResult, TableType
from ...core.surrogate_keys import generate_surrogate_key, generate_date_key
from ...schemas.betting_schema import (
    DIMENSION_DEFINITIONS, FACT_DEFINITIONS, BRIDGE_DEFINITIONS,
    COLUMN_MAPPINGS, SCHEMA_METADATA, LEAGUE_REFERENCE
)


class BettingTransformer(BaseTransformer):
    """
    Transformer for sports betting data into star schema.

    Example:
        transformer = BettingTransformer()
        result = transformer.transform(raw_teams_data)
        transformer.save_all()
    """

    def __init__(self, output_dir: Optional[str] = None, **kwargs):
        super().__init__(output_dir=output_dir, **kwargs)

    def get_schema_name(self) -> str:
        return 'betting'

    def get_dimension_definitions(self) -> Dict[str, Dict[str, Any]]:
        return DIMENSION_DEFINITIONS

    def get_fact_definitions(self) -> Dict[str, Dict[str, Any]]:
        return FACT_DEFINITIONS

    def transform(self, raw_data: List[Dict]) -> TransformationResult:
        """
        Transform raw ESPN data into star schema.

        Args:
            raw_data: List of team dictionaries from ESPNExtractor

        Returns:
            TransformationResult with all tables
        """
        result = TransformationResult(success=False, source=self.get_schema_name())

        try:
            self.logger.info(f"Starting betting transformation: {len(raw_data)} teams")

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
            self._create_team_dimension(df)
            self._create_league_dimension(df)
            self._create_venue_dimension(df)

            # Create date dimension
            if self.generate_date_dim:
                self.create_date_dimension()

            # Create fact table
            self._create_standings_fact(df)

            # Create bridge tables
            self._create_venue_bridge(df)

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

    def _create_team_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_team from raw data."""
        self.logger.info("Creating dim_team")

        team_cols = [
            'team_id', 'league_code', 'abbreviation', 'display_name',
            'short_name', 'nickname', 'location', 'is_active',
            'logo_url', 'color_primary', 'color_alternate', 'espn_url'
        ]

        # Filter to existing columns
        existing_cols = [c for c in team_cols if c in df.columns]

        if 'team_id' not in existing_cols:
            self.logger.error("No team_id column found")
            return pd.DataFrame()

        dim_df = df[existing_cols].drop_duplicates(subset=['team_id', 'league_code']).copy()

        # Generate surrogate key (composite: team_id + league_code)
        dim_df['team_key'] = dim_df.apply(
            lambda x: generate_surrogate_key(str(x.get('team_id', '')), str(x.get('league_code', ''))),
            axis=1
        )

        # Clean up data
        if 'display_name' in dim_df.columns:
            dim_df['display_name'] = dim_df['display_name'].fillna('Unknown')
        else:
            dim_df['display_name'] = 'Unknown'

        if 'is_active' in dim_df.columns:
            dim_df['is_active'] = dim_df['is_active'].fillna(True).astype(bool)
        else:
            dim_df['is_active'] = True

        # Add metadata
        dim_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_df['_source'] = 'espn'

        # Reorder columns
        cols = ['team_key', 'team_id', 'league_code'] + [c for c in existing_cols if c not in ['team_id', 'league_code']]
        cols = cols + ['_loaded_at', '_source']
        dim_df = dim_df[[c for c in cols if c in dim_df.columns]]

        self._dimensions['dim_team'] = dim_df
        self.logger.info(f"Created dim_team: {len(dim_df):,} rows")

        return dim_df

    def _create_league_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_league from raw data and reference."""
        self.logger.info("Creating dim_league")

        # Get unique leagues from data
        leagues = df['league_code'].unique().tolist() if 'league_code' in df.columns else []

        league_data = []
        for code in leagues:
            ref = LEAGUE_REFERENCE.get(code, {})
            league_data.append({
                'league_code': code,
                'league_name': ref.get('name', code.upper()),
                'sport': ref.get('sport', 'Unknown'),
                'country': ref.get('country', 'Unknown'),
            })

        if not league_data:
            self.logger.warning("No league data found")
            return pd.DataFrame()

        league_df = pd.DataFrame(league_data)

        # Generate surrogate key
        league_df['league_key'] = league_df['league_code'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Add metadata
        league_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        league_df['_source'] = 'espn'

        self._dimensions['dim_league'] = league_df
        self.logger.info(f"Created dim_league: {len(league_df):,} rows")

        return league_df

    def _create_venue_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_venue from raw data."""
        self.logger.info("Creating dim_venue")

        venue_cols = [
            'venue_id', 'venue_name', 'venue_city', 'venue_state',
            'venue_capacity', 'venue_indoor'
        ]

        existing_cols = [c for c in venue_cols if c in df.columns]

        if 'venue_id' not in existing_cols or df['venue_id'].isna().all():
            self.logger.warning("No venue data found")
            return pd.DataFrame()

        # Filter out rows without venue_id
        venue_df = df[df['venue_id'].notna() & (df['venue_id'] != '')][existing_cols].copy()
        venue_df = venue_df.drop_duplicates(subset=['venue_id'])

        if venue_df.empty:
            self.logger.warning("No valid venues found")
            return pd.DataFrame()

        # Generate surrogate key
        venue_df['venue_key'] = venue_df['venue_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Add metadata
        venue_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        venue_df['_source'] = 'espn'

        self._dimensions['dim_venue'] = venue_df
        self.logger.info(f"Created dim_venue: {len(venue_df):,} rows")

        return venue_df

    def _create_standings_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create fact_team_standings from raw data."""
        self.logger.info("Creating fact_team_standings")

        measure_cols = [
            'wins', 'losses', 'ties', 'win_pct',
            'points_for', 'points_against',
            'playoff_seed', 'games_back',
        ]

        degenerate_cols = [
            'division', 'division_record', 'conference_record', 'streak'
        ]

        # Filter to existing columns
        existing_measures = [c for c in measure_cols if c in df.columns]
        existing_degen = [c for c in degenerate_cols if c in df.columns]

        fact_cols = ['team_id', 'league_code', 'extracted_date'] + existing_measures + existing_degen
        fact_df = df[[c for c in fact_cols if c in df.columns]].copy()

        # Generate keys
        fact_df['team_key'] = fact_df.apply(
            lambda x: generate_surrogate_key(str(x.get('team_id', '')), str(x.get('league_code', ''))),
            axis=1
        )
        fact_df['league_key'] = fact_df['league_code'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        fact_df['date_key'] = fact_df['extracted_date'].apply(
            lambda x: generate_date_key(x)
        )

        # Calculate point differential if we have the data
        if 'points_for' in fact_df.columns and 'points_against' in fact_df.columns:
            fact_df['point_differential'] = fact_df['points_for'].fillna(0) - fact_df['points_against'].fillna(0)

        # Parse streak into length and type
        if 'streak' in fact_df.columns:
            fact_df['streak_type'] = fact_df['streak'].apply(
                lambda x: x[0] if pd.notna(x) and len(str(x)) > 0 else None
            )
            fact_df['streak_length'] = fact_df['streak'].apply(
                lambda x: int(x[1:]) if pd.notna(x) and len(str(x)) > 1 and str(x)[1:].isdigit() else 0
            )

        # Add extraction ID
        fact_df['extraction_id'] = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        fact_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        # Reorder columns
        key_cols = ['team_key', 'league_key', 'date_key']
        final_cols = key_cols + existing_measures
        if 'point_differential' in fact_df.columns:
            final_cols.append('point_differential')
        if 'streak_type' in fact_df.columns:
            final_cols.extend(['streak_type', 'streak_length'])
        final_cols.extend(existing_degen)
        final_cols.extend(['extraction_id', '_loaded_at'])

        fact_df = fact_df[[c for c in final_cols if c in fact_df.columns]]

        self._facts['fact_team_standings'] = fact_df
        self.logger.info(f"Created fact_team_standings: {len(fact_df):,} rows")

        return fact_df

    def _create_venue_bridge(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create team_venue_bridge from raw data."""
        self.logger.info("Creating team_venue_bridge")

        if 'venue_id' not in df.columns:
            self.logger.warning("No venue_id column found")
            return pd.DataFrame()

        # Filter to rows with venue data
        bridge_df = df[df['venue_id'].notna() & (df['venue_id'] != '')][
            ['team_id', 'league_code', 'venue_id']
        ].drop_duplicates().copy()

        if bridge_df.empty:
            self.logger.warning("No team-venue relationships found")
            return pd.DataFrame()

        # Generate keys
        bridge_df['team_key'] = bridge_df.apply(
            lambda x: generate_surrogate_key(str(x['team_id']), str(x['league_code'])),
            axis=1
        )
        bridge_df['venue_key'] = bridge_df['venue_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Select just the keys
        bridge_df = bridge_df[['team_key', 'venue_key']].drop_duplicates()
        bridge_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        self._bridges['team_venue_bridge'] = bridge_df
        self.logger.info(f"Created team_venue_bridge: {len(bridge_df):,} rows")

        return bridge_df
