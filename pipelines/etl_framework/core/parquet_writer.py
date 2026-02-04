"""
Enterprise ETL Framework - Parquet Writer
Utilities for writing DataFrames to Parquet format with compression and metadata.

Parquet is the standard columnar format for big data analytics, providing:
- Efficient compression (typically 80-90% smaller than CSV)
- Column pruning (read only needed columns)
- Predicate pushdown (filter at storage level)
- Schema preservation (no type inference issues)
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
import hashlib

import pandas as pd

logger = logging.getLogger('etl.parquet_writer')


class ParquetWriter:
    """
    Writer for Parquet files with metadata and checksums.

    Features:
    - Snappy compression (fast, good ratio)
    - Row group optimization
    - Automatic schema detection
    - Checksum generation for verification
    - Metadata embedding
    """

    def __init__(
        self,
        output_dir: Union[str, Path],
        compression: str = 'snappy',
        row_group_size: int = 100_000,
    ):
        """
        Initialize the Parquet writer.

        Args:
            output_dir: Base directory for output files
            compression: Compression codec ('snappy', 'gzip', 'zstd', 'none')
            row_group_size: Number of rows per row group
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.compression = compression if compression != 'none' else None
        self.row_group_size = row_group_size

        logger.info(f"ParquetWriter initialized: {self.output_dir} ({compression})")

    def write(
        self,
        df: pd.DataFrame,
        name: str,
        partition_cols: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Write a DataFrame to Parquet.

        Args:
            df: DataFrame to write
            name: Base name for the file (without extension)
            partition_cols: Columns to partition by (creates directory structure)
            metadata: Additional metadata to embed

        Returns:
            Path to the written file or directory
        """
        output_path = self.output_dir / f"{name}.parquet"

        # Build metadata
        file_metadata = {
            'framework_version': '1.0',
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'table_name': name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
        }

        if metadata:
            file_metadata.update(metadata)

        try:
            if partition_cols:
                # Partitioned write (creates directory structure)
                df.to_parquet(
                    str(output_path),
                    engine='pyarrow',
                    compression=self.compression,
                    partition_cols=partition_cols,
                    index=False,
                )
            else:
                # Single file write
                df.to_parquet(
                    str(output_path),
                    engine='pyarrow',
                    compression=self.compression,
                    index=False,
                    row_group_size=self.row_group_size,
                )

            # Write metadata sidecar
            self._write_metadata(output_path, file_metadata, df)

            logger.info(f"Wrote {name}: {len(df):,} rows to {output_path}")
            return str(output_path)

        except ImportError:
            # Fallback to fastparquet if pyarrow not available
            logger.warning("pyarrow not available, trying fastparquet")
            try:
                df.to_parquet(
                    str(output_path),
                    engine='fastparquet',
                    compression=self.compression or 'snappy',
                    index=False,
                )
                self._write_metadata(output_path, file_metadata, df)
                return str(output_path)
            except ImportError:
                logger.error("Neither pyarrow nor fastparquet available")
                raise ImportError(
                    "Parquet support requires pyarrow or fastparquet. "
                    "Install with: pip install pyarrow"
                )

    def _write_metadata(
        self,
        parquet_path: Path,
        metadata: Dict[str, Any],
        df: pd.DataFrame,
    ) -> None:
        """Write metadata sidecar JSON file."""
        # Generate checksum
        metadata['checksum'] = self._generate_checksum(df)
        metadata['file_path'] = str(parquet_path)

        # Add statistics
        metadata['statistics'] = self._generate_statistics(df)

        meta_path = parquet_path.with_suffix('.parquet.meta.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

    def _generate_checksum(self, df: pd.DataFrame) -> str:
        """Generate a checksum for data verification."""
        # Hash the shape and first/last rows
        check_data = f"{df.shape}|{df.head(1).to_json()}|{df.tail(1).to_json()}"
        return hashlib.md5(check_data.encode()).hexdigest()

    def _generate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate basic statistics for numeric columns."""
        stats = {}

        for col in df.columns:
            col_stats = {'null_count': int(df[col].isna().sum())}

            if pd.api.types.is_numeric_dtype(df[col]):
                col_stats.update({
                    'min': float(df[col].min()) if not df[col].isna().all() else None,
                    'max': float(df[col].max()) if not df[col].isna().all() else None,
                    'mean': float(df[col].mean()) if not df[col].isna().all() else None,
                })
            elif pd.api.types.is_string_dtype(df[col]):
                col_stats['unique_count'] = int(df[col].nunique())

            stats[col] = col_stats

        return stats

    def write_batch(
        self,
        tables: Dict[str, pd.DataFrame],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """
        Write multiple tables to Parquet.

        Args:
            tables: Dict mapping table name to DataFrame
            metadata: Shared metadata for all tables

        Returns:
            Dict mapping table name to file path
        """
        paths = {}
        for name, df in tables.items():
            table_meta = dict(metadata or {})
            table_meta['table_name'] = name
            paths[name] = self.write(df, name, metadata=table_meta)

        return paths


def read_parquet(path: Union[str, Path], columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Read a Parquet file with optional column selection.

    Args:
        path: Path to Parquet file
        columns: List of columns to read (None = all)

    Returns:
        DataFrame
    """
    try:
        return pd.read_parquet(path, columns=columns, engine='pyarrow')
    except ImportError:
        return pd.read_parquet(path, columns=columns, engine='fastparquet')


def get_parquet_metadata(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Read metadata from a Parquet file's sidecar JSON.

    Args:
        path: Path to Parquet file

    Returns:
        Metadata dictionary
    """
    meta_path = Path(path).with_suffix('.parquet.meta.json')
    if meta_path.exists():
        with open(meta_path) as f:
            return json.load(f)
    return {}


def verify_parquet_checksum(path: Union[str, Path]) -> bool:
    """
    Verify a Parquet file's checksum against its metadata.

    Args:
        path: Path to Parquet file

    Returns:
        True if checksum matches, False otherwise
    """
    metadata = get_parquet_metadata(path)
    if 'checksum' not in metadata:
        logger.warning(f"No checksum in metadata for {path}")
        return False

    df = read_parquet(path)
    check_data = f"{df.shape}|{df.head(1).to_json()}|{df.tail(1).to_json()}"
    actual_checksum = hashlib.md5(check_data.encode()).hexdigest()

    return actual_checksum == metadata['checksum']
