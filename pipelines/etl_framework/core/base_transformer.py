"""
Enterprise ETL Framework - Base Transformer
Parent class for all star schema transformations (Kimball dimensional modeling).

This module provides the foundation for transforming raw extracted data into
dimensional models (fact tables, dimension tables, bridge tables).
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple
from pathlib import Path

import pandas as pd
import numpy as np

# Import existing infrastructure
import sys

    

from .core.surrogate_keys import generate_surrogate_key, generate_date_key


# =============================================================================
# Transformation Result
# =============================================================================

@dataclass
class TransformationResult:
    """Result of a transformation operation."""
    success: bool
    source: str
    tables_created: List[str] = field(default_factory=list)
    total_rows: int = 0
    rows_by_table: Dict[str, int] = field(default_factory=dict)
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    completed_at: Optional[str] = None
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    output_paths: Dict[str, str] = field(default_factory=dict)
    schema_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "source": self.source,
            "tables_created": self.tables_created,
            "total_rows": self.total_rows,
            "rows_by_table": self.rows_by_table,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "warnings": self.warnings,
            "output_paths": self.output_paths,
            "schema_info": self.schema_info,
        }

    def add_table(self, name: str, rows: int, path: str) -> None:
        """Record a created table."""
        self.tables_created.append(name)
        self.rows_by_table[name] = rows
        self.output_paths[name] = path
        self.total_rows += rows

    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self.warnings.append(warning)


# =============================================================================
# Table Types
# =============================================================================

class TableType:
    """Constants for dimensional table types."""
    DIMENSION = 'dimension'
    FACT = 'fact'
    BRIDGE = 'bridge'


# =============================================================================
# Base Transformer
# =============================================================================

class BaseTransformer(ABC):
    """
    Abstract base class for all star schema transformers.

    Provides common infrastructure for:
    - Dimension table creation (SCD Type 1 and 2)
    - Fact table creation
    - Bridge table creation (many-to-many relationships)
    - Surrogate key generation
    - Date dimension handling
    - Data quality validation

    Subclasses must implement:
    - get_schema_name(): Return the schema identifier
    - get_dimension_definitions(): Return dimension table specs
    - get_fact_definitions(): Return fact table specs
    - transform(): Main transformation logic
    """

    def __init__(
        self,
        output_dir: Optional[str] = None,
        validate_output: bool = True,
        generate_date_dim: bool = True,
    ):
        """
        Initialize the transformer.

        Args:
            output_dir: Directory for output files (default: ./data/data/etl)
            validate_output: Whether to validate output data quality
            generate_date_dim: Whether to auto-generate date dimension
        """
        self.output_dir = Path(output_dir or './data/data/etl')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.validate_output = validate_output
        self.generate_date_dim = generate_date_dim

        # Setup logging
        self.logger = self._setup_logger()

        # Storage for transformed tables
        self._dimensions: Dict[str, pd.DataFrame] = {}
        self._facts: Dict[str, pd.DataFrame] = {}
        self._bridges: Dict[str, pd.DataFrame] = {}

        self.logger.info(f"Initialized {self.get_schema_name()} transformer")

    def _setup_logger(self) -> logging.Logger:
        """Setup transformer-specific logger."""
        logger = logging.getLogger(f"etl.transformer.{self.get_schema_name()}")

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f"[%(asctime)s] [ETL:{self.get_schema_name()}] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

        return logger

    # =========================================================================
    # Abstract Methods (Must Implement)
    # =========================================================================

    @abstractmethod
    def get_schema_name(self) -> str:
        """Return the schema identifier (e.g., 'gaming', 'betting', 'media')."""
        pass

    @abstractmethod
    def get_dimension_definitions(self) -> Dict[str, Dict[str, Any]]:
        """
        Return dimension table definitions.

        Returns:
            Dict mapping dimension name to definition:
            {
                'dim_game': {
                    'natural_key': ['game_id'],
                    'attributes': ['name', 'developer', 'release_date'],
                    'scd_type': 2,  # 1 = overwrite, 2 = track history
                },
                ...
            }
        """
        pass

    @abstractmethod
    def get_fact_definitions(self) -> Dict[str, Dict[str, Any]]:
        """
        Return fact table definitions.

        Returns:
            Dict mapping fact name to definition:
            {
                'fact_game_metrics': {
                    'grain': 'one row per game per day',
                    'dimension_keys': ['game_key', 'date_key'],
                    'measures': ['player_count', 'revenue', 'hours_played'],
                    'degenerate_dims': ['snapshot_id'],
                },
                ...
            }
        """
        pass

    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> TransformationResult:
        """
        Main transformation method. Must be implemented by subclasses.

        Args:
            raw_data: Dictionary of raw extracted data

        Returns:
            TransformationResult with transformed tables and metadata
        """
        pass

    # =========================================================================
    # Dimension Table Helpers
    # =========================================================================

    def create_dimension(
        self,
        name: str,
        data: pd.DataFrame,
        natural_key: List[str],
        attributes: List[str],
        key_prefix: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Create a dimension table with surrogate keys.

        Args:
            name: Dimension table name (e.g., 'dim_game')
            data: Source data
            natural_key: Columns forming the natural key
            attributes: Columns to include as attributes
            key_prefix: Prefix for surrogate key (default: derived from name)

        Returns:
            Dimension table DataFrame with surrogate key
        """
        self.logger.info(f"Creating dimension: {name}")

        # Derive key column name
        if key_prefix is None:
            key_prefix = name.replace('dim_', '')
        key_col = f"{key_prefix}_key"

        # Select and deduplicate
        cols = natural_key + [c for c in attributes if c in data.columns]
        dim_df = data[cols].drop_duplicates(subset=natural_key).copy()

        # Generate surrogate keys
        dim_df[key_col] = dim_df.apply(
            lambda row: generate_surrogate_key(
                *[str(row[col]) for col in natural_key]
            ),
            axis=1
        )

        # Add metadata columns
        dim_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_df['_source'] = self.get_schema_name()

        # Reorder columns (key first)
        cols = [key_col] + natural_key + [c for c in attributes if c in dim_df.columns]
        cols = cols + ['_loaded_at', '_source']
        dim_df = dim_df[[c for c in cols if c in dim_df.columns]]

        self._dimensions[name] = dim_df
        self.logger.info(f"Created {name}: {len(dim_df)} rows")

        return dim_df

    def create_date_dimension(
        self,
        start_date: str = '2020-01-01',
        end_date: str = '2030-12-31',
    ) -> pd.DataFrame:
        """
        Create a standard date dimension table.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Date dimension DataFrame
        """
        self.logger.info("Creating date dimension")

        dates = pd.date_range(start=start_date, end=end_date, freq='D')

        dim_date = pd.DataFrame({
            'date_key': [generate_date_key(d) for d in dates],
            'full_date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.month_name(),
            'week': dates.isocalendar().week,
            'day_of_month': dates.day,
            'day_of_week': dates.dayofweek,
            'day_name': dates.day_name(),
            'is_weekend': dates.dayofweek >= 5,
            'is_month_start': dates.is_month_start,
            'is_month_end': dates.is_month_end,
            'is_quarter_start': dates.is_quarter_start,
            'is_quarter_end': dates.is_quarter_end,
            'is_year_start': dates.is_year_start,
            'is_year_end': dates.is_year_end,
        })

        dim_date['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
        dim_date['_source'] = 'system'

        self._dimensions['dim_date'] = dim_date
        self.logger.info(f"Created dim_date: {len(dim_date)} rows")

        return dim_date

    # =========================================================================
    # Fact Table Helpers
    # =========================================================================

    def create_fact(
        self,
        name: str,
        data: pd.DataFrame,
        dimension_keys: Dict[str, Tuple[str, List[str]]],
        measures: List[str],
        degenerate_dims: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Create a fact table with foreign keys to dimensions.

        Args:
            name: Fact table name (e.g., 'fact_game_metrics')
            data: Source data
            dimension_keys: Dict mapping dimension name to (key_col, lookup_cols)
                e.g., {'dim_game': ('game_key', ['game_id'])}
            measures: List of measure columns
            degenerate_dims: Columns to include as degenerate dimensions

        Returns:
            Fact table DataFrame with dimension keys
        """
        self.logger.info(f"Creating fact table: {name}")

        fact_df = data.copy()

        # Resolve dimension keys
        for dim_name, (key_col, lookup_cols) in dimension_keys.items():
            if dim_name not in self._dimensions:
                self.logger.warning(f"Dimension {dim_name} not found, skipping key resolution")
                continue

            dim_df = self._dimensions[dim_name]

            # Merge to get surrogate key
            merge_cols = [c for c in lookup_cols if c in fact_df.columns and c in dim_df.columns]
            if merge_cols:
                fact_df = fact_df.merge(
                    dim_df[[key_col] + merge_cols],
                    on=merge_cols,
                    how='left'
                )

        # Select columns
        key_cols = [v[0] for v in dimension_keys.values()]
        measure_cols = [c for c in measures if c in fact_df.columns]
        degen_cols = [c for c in (degenerate_dims or []) if c in fact_df.columns]

        select_cols = key_cols + measure_cols + degen_cols
        select_cols = [c for c in select_cols if c in fact_df.columns]
        fact_df = fact_df[select_cols].copy()

        # Add metadata
        fact_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        self._facts[name] = fact_df
        self.logger.info(f"Created {name}: {len(fact_df)} rows")

        return fact_df

    # =========================================================================
    # Bridge Table Helpers
    # =========================================================================

    def create_bridge(
        self,
        name: str,
        data: pd.DataFrame,
        left_key: Tuple[str, List[str]],
        right_key: Tuple[str, List[str]],
    ) -> pd.DataFrame:
        """
        Create a bridge table for many-to-many relationships.

        Args:
            name: Bridge table name (e.g., 'game_genre_bridge')
            data: Source data with the relationship
            left_key: (key_col, lookup_cols) for left dimension
            right_key: (key_col, lookup_cols) for right dimension

        Returns:
            Bridge table DataFrame
        """
        self.logger.info(f"Creating bridge table: {name}")

        left_key_col, left_lookup = left_key
        right_key_col, right_lookup = right_key

        bridge_df = data[left_lookup + right_lookup].drop_duplicates().copy()

        # Generate keys
        bridge_df[left_key_col] = bridge_df.apply(
            lambda row: generate_surrogate_key(*[str(row[c]) for c in left_lookup]),
            axis=1
        )
        bridge_df[right_key_col] = bridge_df.apply(
            lambda row: generate_surrogate_key(*[str(row[c]) for c in right_lookup]),
            axis=1
        )

        # Select just the keys
        bridge_df = bridge_df[[left_key_col, right_key_col]].drop_duplicates()
        bridge_df['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'

        self._bridges[name] = bridge_df
        self.logger.info(f"Created {name}: {len(bridge_df)} rows")

        return bridge_df

    # =========================================================================
    # Output Helpers
    # =========================================================================

    def save_all(self, subdir: Optional[str] = None) -> Dict[str, str]:
        """
        Save all tables to Parquet files.

        Args:
            subdir: Subdirectory under output_dir (default: schema name)

        Returns:
            Dict mapping table name to file path
        """
        from .core.parquet_writer import ParquetWriter

        subdir = subdir or self.get_schema_name()
        output_path = self.output_dir / subdir
        output_path.mkdir(parents=True, exist_ok=True)

        paths = {}
        writer = ParquetWriter(str(output_path))

        # Save dimensions
        for name, df in self._dimensions.items():
            path = writer.write(df, name)
            paths[name] = path

        # Save facts
        for name, df in self._facts.items():
            path = writer.write(df, name)
            paths[name] = path

        # Save bridges
        for name, df in self._bridges.items():
            path = writer.write(df, name)
            paths[name] = path

        self.logger.info(f"Saved {len(paths)} tables to {output_path}")
        return paths

    def get_all_tables(self) -> Dict[str, pd.DataFrame]:
        """Get all transformed tables."""
        return {
            **self._dimensions,
            **self._facts,
            **self._bridges,
        }

    def get_row_counts(self) -> Dict[str, int]:
        """Get row counts for all tables."""
        return {
            name: len(df)
            for name, df in self.get_all_tables().items()
        }

    # =========================================================================
    # Validation Helpers
    # =========================================================================

    def validate_referential_integrity(self) -> List[str]:
        """
        Validate referential integrity between facts and dimensions.

        Returns:
            List of validation warnings/errors
        """
        warnings = []

        for fact_name, fact_df in self._facts.items():
            for col in fact_df.columns:
                if col.endswith('_key') and col != '_loaded_at':
                    # Find corresponding dimension
                    dim_name = 'dim_' + col.replace('_key', '')
                    if dim_name in self._dimensions:
                        dim_keys = set(self._dimensions[dim_name][col])
                        fact_keys = set(fact_df[col].dropna())
                        orphans = fact_keys - dim_keys
                        if orphans:
                            warnings.append(
                                f"{fact_name}.{col}: {len(orphans)} orphan keys not in {dim_name}"
                            )

        return warnings
