"""
Enterprise ETL Framework - Crypto Transformer
Transforms raw CoinGecko data into Kimball star schema.

Produces:
    - dim_coin: Cryptocurrency dimension
    - dim_category: Category dimension
    - dim_exchange: Exchange dimension
    - dim_date: Date dimension
    - fact_coin_metrics: Coin performance facts
    - coin_category_bridge: Coin-category relationships
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

import sys

    

from ...core.base_transformer import BaseTransformer, TransformationResult, TableType
from ...core.surrogate_keys import generate_surrogate_key, generate_date_key
from ...schemas.crypto_schema import (
    DIMENSION_DEFINITIONS, FACT_DEFINITIONS, BRIDGE_DEFINITIONS,
    COLUMN_MAPPINGS, SCHEMA_METADATA
)


class CryptoTransformer(BaseTransformer):
    """
    Transformer for cryptocurrency data into star schema.

    Example:
        transformer = CryptoTransformer()
        result = transformer.transform(raw_coins_data)
        transformer.save_all()
    """

    def __init__(self, output_dir: Optional[str] = None, **kwargs):
        super().__init__(output_dir=output_dir, **kwargs)

    def get_schema_name(self) -> str:
        return 'crypto'

    def get_dimension_definitions(self) -> Dict[str, Dict[str, Any]]:
        return DIMENSION_DEFINITIONS

    def get_fact_definitions(self) -> Dict[str, Dict[str, Any]]:
        return FACT_DEFINITIONS

    def transform(self, raw_data: List[Dict]) -> TransformationResult:
        """
        Transform raw crypto data into star schema.

        Args:
            raw_data: List of coin dictionaries from CoinGeckoExtractor

        Returns:
            TransformationResult with all tables
        """
        result = TransformationResult(success=False, source=self.get_schema_name())

        try:
            self.logger.info(f"Starting crypto transformation: {len(raw_data)} coins")

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
            self._create_coin_dimension(df)
            self._create_category_dimension(df)

            # Create date dimension
            if self.generate_date_dim:
                self.create_date_dimension()

            # Create fact table
            self._create_coin_metrics_fact(df)

            # Create bridge tables
            self._create_category_bridge(df)

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

    def _create_coin_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_coin from raw data."""
        self.logger.info("Creating dim_coin")

        # Select columns for coin dimension
        coin_cols = [
            'coin_id', 'symbol', 'name', 'image_url', 'description',
            'genesis_date', 'website', 'github_url'
        ]

        # Filter to existing columns
        existing_cols = [c for c in coin_cols if c in df.columns]
        
        if 'coin_id' not in existing_cols:
            self.logger.error("No coin_id column found")
            return pd.DataFrame()

        dim_df = df[existing_cols].drop_duplicates(subset=['coin_id']).copy()

        # Generate surrogate key
        dim_df['coin_key'] = dim_df['coin_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Clean up data with safe checks
        if 'name' in dim_df.columns:
            dim_df['name'] = dim_df['name'].fillna('Unknown')
        else:
            dim_df['name'] = 'Unknown'
            
        if 'symbol' in dim_df.columns:
            dim_df['symbol'] = dim_df['symbol'].fillna('').str.upper()
        else:
            dim_df['symbol'] = ''

        # Add metadata
        dim_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_df['_source'] = 'coingecko'

        # Reorder columns
        cols = ['coin_key', 'coin_id'] + [c for c in existing_cols if c != 'coin_id']
        cols = cols + ['_loaded_at', '_source']
        dim_df = dim_df[[c for c in cols if c in dim_df.columns]]

        self._dimensions['dim_coin'] = dim_df
        self.logger.info(f"Created dim_coin: {len(dim_df):,} rows")

        return dim_df

    def _create_category_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_category from raw data."""
        self.logger.info("Creating dim_category")

        if 'categories' not in df.columns:
            self.logger.warning("No categories column found")
            return pd.DataFrame()

        # Explode categories (comma-separated)
        all_categories = set()
        for cats_str in df['categories'].dropna():
            for cat in str(cats_str).split(','):
                if cat.strip():
                    all_categories.add(cat.strip())

        if not all_categories:
            self.logger.warning("No categories found in data")
            return pd.DataFrame()

        cat_df = pd.DataFrame({'category_name': list(all_categories)})

        # Generate surrogate key
        cat_df['category_key'] = cat_df['category_name'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Add metadata
        cat_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        cat_df['_source'] = 'coingecko'

        self._dimensions['dim_category'] = cat_df
        self.logger.info(f"Created dim_category: {len(cat_df):,} rows")

        return cat_df

    def _create_coin_metrics_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create fact_coin_metrics from raw data."""
        self.logger.info("Creating fact_coin_metrics")

        # Measure columns
        measure_cols = [
            'current_price', 'market_cap', 'fully_diluted_valuation', 'total_volume',
            'market_cap_rank', 'circulating_supply', 'total_supply', 'max_supply',
            'price_change_24h', 'price_change_pct_24h', 'price_change_pct_1h',
            'price_change_pct_7d', 'price_change_pct_30d',
            'ath', 'ath_change_pct', 'atl', 'atl_change_pct',
            'twitter_followers', 'reddit_subscribers', 'telegram_users',
            'github_forks', 'github_stars', 'github_commit_count_4_weeks',
            'sentiment_up_pct', 'sentiment_down_pct',
        ]

        # Filter to existing columns
        existing_measures = [c for c in measure_cols if c in df.columns]

        fact_df = df[['coin_id', 'extracted_date'] + existing_measures].copy()

        # Generate keys
        fact_df['coin_key'] = fact_df['coin_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        fact_df['date_key'] = fact_df['extracted_date'].apply(
            lambda x: generate_date_key(x)
        )

        # Add extraction ID (degenerate dimension)
        fact_df['extraction_id'] = datetime.utcnow().strftime('%Y%m%d%H%M%S')

        # Add metadata
        fact_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        # Reorder columns
        key_cols = ['coin_key', 'date_key']
        final_cols = key_cols + existing_measures + ['extraction_id', '_loaded_at']
        fact_df = fact_df[[c for c in final_cols if c in fact_df.columns]]

        self._facts['fact_coin_metrics'] = fact_df
        self.logger.info(f"Created fact_coin_metrics: {len(fact_df):,} rows")

        return fact_df

    def _create_category_bridge(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create coin_category_bridge from raw data."""
        self.logger.info("Creating coin_category_bridge")

        if 'categories' not in df.columns:
            self.logger.warning("No categories column found")
            return pd.DataFrame()

        # Explode coin-category relationships
        relationships = []
        for _, row in df[['coin_id', 'categories']].iterrows():
            if pd.notna(row['categories']):
                for cat in str(row['categories']).split(','):
                    if cat.strip():
                        relationships.append({
                            'coin_id': row['coin_id'],
                            'category_name': cat.strip(),
                        })

        if not relationships:
            self.logger.warning("No category relationships found")
            return pd.DataFrame()

        bridge_df = pd.DataFrame(relationships).drop_duplicates()

        # Generate keys
        bridge_df['coin_key'] = bridge_df['coin_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        bridge_df['category_key'] = bridge_df['category_name'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        # Select just the keys
        bridge_df = bridge_df[['coin_key', 'category_key']].drop_duplicates()
        bridge_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        self._bridges['coin_category_bridge'] = bridge_df
        self.logger.info(f"Created coin_category_bridge: {len(bridge_df):,} rows")

        return bridge_df

    def transform_exchanges(self, exchange_data: List[Dict]) -> TransformationResult:
        """
        Transform exchange data into star schema.

        Args:
            exchange_data: List of exchange dictionaries from CoinGeckoExtractor

        Returns:
            TransformationResult with exchange tables
        """
        result = TransformationResult(success=False, source=self.get_schema_name())

        try:
            self.logger.info(f"Transforming {len(exchange_data)} exchanges")

            df = pd.DataFrame(exchange_data)

            if df.empty:
                result.error = "No exchange data to transform"
                return result

            # Create exchange dimension
            self._create_exchange_dimension(df)

            # Create exchange fact
            df['extracted_date'] = pd.to_datetime(
                df['extracted_at'].str[:10] if 'extracted_at' in df.columns
                else datetime.utcnow().strftime('%Y-%m-%d')
            )
            self._create_exchange_metrics_fact(df)

            # Save and return
            paths = self.save_all()

            result.success = True
            result.tables_created = list(self.get_all_tables().keys())
            result.rows_by_table = self.get_row_counts()
            result.total_rows = sum(result.rows_by_table.values())
            result.output_paths = paths
            result.completed_at = datetime.utcnow().isoformat() + 'Z'

            self.logger.info(f"Exchange transformation complete: {result.total_rows:,} rows")

        except Exception as e:
            self.logger.error(f"Exchange transformation failed: {e}")
            result.error = str(e)

        return result

    def _create_exchange_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dim_exchange from raw data."""
        self.logger.info("Creating dim_exchange")

        exchange_cols = [
            'exchange_id', 'name', 'year_established', 'country',
            'url', 'image_url', 'has_trading_incentive'
        ]

        existing_cols = [c for c in exchange_cols if c in df.columns]
        
        if 'exchange_id' not in existing_cols:
            self.logger.error("No exchange_id column found")
            return pd.DataFrame()

        dim_df = df[existing_cols].drop_duplicates(subset=['exchange_id']).copy()

        dim_df['exchange_key'] = dim_df['exchange_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )

        dim_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_df['_source'] = 'coingecko'

        self._dimensions['dim_exchange'] = dim_df
        self.logger.info(f"Created dim_exchange: {len(dim_df):,} rows")

        return dim_df

    def _create_exchange_metrics_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create fact_exchange_metrics from raw data."""
        self.logger.info("Creating fact_exchange_metrics")

        measure_cols = [
            'trust_score', 'trust_score_rank',
            'trade_volume_24h_btc', 'trade_volume_24h_btc_normalized'
        ]

        existing_measures = [c for c in measure_cols if c in df.columns]
        fact_df = df[['exchange_id', 'extracted_date'] + existing_measures].copy()

        fact_df['exchange_key'] = fact_df['exchange_id'].apply(
            lambda x: generate_surrogate_key(str(x))
        )
        fact_df['date_key'] = fact_df['extracted_date'].apply(
            lambda x: generate_date_key(x)
        )

        fact_df['extraction_id'] = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        fact_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        key_cols = ['exchange_key', 'date_key']
        final_cols = key_cols + existing_measures + ['extraction_id', '_loaded_at']
        fact_df = fact_df[[c for c in final_cols if c in fact_df.columns]]

        self._facts['fact_exchange_metrics'] = fact_df
        self.logger.info(f"Created fact_exchange_metrics: {len(fact_df):,} rows")

        return fact_df
