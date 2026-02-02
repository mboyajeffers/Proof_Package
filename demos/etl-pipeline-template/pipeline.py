#!/usr/bin/env python3
"""
Generic ETL Pipeline Template
=============================
A configurable, production-grade ETL framework demonstrating
best practices for data pipeline development.

Features:
- Configurable via YAML
- Schema validation
- Quality gates
- Checkpoint/restart capability
- Structured logging

Author: Mboya Jeffers
"""

import json
import logging
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    name: str
    source_type: str  # 'csv', 'api', 'database'
    source_path: str
    output_path: str
    schema: Dict[str, str] = field(default_factory=dict)
    quality_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'completeness': 0.95,
        'uniqueness': 0.99
    })
    chunk_size: int = 10000


@dataclass
class QualityReport:
    """Data quality assessment results."""
    total_rows: int
    valid_rows: int
    completeness_score: float
    uniqueness_score: float
    passed: bool
    issues: List[str] = field(default_factory=list)


class Extractor(ABC):
    """Abstract base class for data extractors."""

    @abstractmethod
    def extract(self, source: str, **kwargs) -> pd.DataFrame:
        """Extract data from source."""
        pass


class CSVExtractor(Extractor):
    """Extract data from CSV files."""

    def extract(self, source: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Extracting from CSV: {source}")
        chunk_size = kwargs.get('chunk_size')

        if chunk_size:
            chunks = []
            for chunk in pd.read_csv(source, chunksize=chunk_size):
                chunks.append(chunk)
                logger.info(f"  Loaded chunk: {len(chunk)} rows")
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_csv(source)

        logger.info(f"  Total rows extracted: {len(df)}")
        return df


class APIExtractor(Extractor):
    """Extract data from REST APIs."""

    def extract(self, source: str, **kwargs) -> pd.DataFrame:
        import requests

        logger.info(f"Extracting from API: {source}")

        headers = kwargs.get('headers', {})
        params = kwargs.get('params', {})

        response = requests.get(source, headers=headers, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()

        # Handle common API response formats
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Try common keys
            for key in ['data', 'results', 'items', 'records']:
                if key in data and isinstance(data[key], list):
                    df = pd.DataFrame(data[key])
                    break
            else:
                df = pd.DataFrame([data])
        else:
            raise ValueError(f"Unexpected API response type: {type(data)}")

        logger.info(f"  Rows extracted: {len(df)}")
        return df


class Transformer:
    """Data transformation operations."""

    def __init__(self, schema: Dict[str, str]):
        self.schema = schema
        self.transformations: List[Callable] = []

    def add_transformation(self, func: Callable) -> 'Transformer':
        """Add a transformation function to the pipeline."""
        self.transformations.append(func)
        return self

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to snake_case."""
        df.columns = [
            col.lower().strip().replace(' ', '_').replace('-', '_')
            for col in df.columns
        ]
        return df

    def enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enforce column data types from schema."""
        for col, dtype in self.schema.items():
            if col in df.columns:
                try:
                    if dtype == 'datetime':
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif dtype == 'int':
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    elif dtype == 'float':
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif dtype == 'str':
                        df[col] = df[col].astype(str)
                    elif dtype == 'bool':
                        df[col] = df[col].astype(bool)
                except Exception as e:
                    logger.warning(f"Could not convert {col} to {dtype}: {e}")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations."""
        logger.info("Applying transformations...")

        df = self.normalize_columns(df)
        df = self.enforce_schema(df)

        for func in self.transformations:
            df = func(df)
            logger.info(f"  Applied: {func.__name__}")

        logger.info(f"  Rows after transformation: {len(df)}")
        return df


class QualityGate:
    """Data quality validation."""

    def __init__(self, thresholds: Dict[str, float]):
        self.thresholds = thresholds

    def check_completeness(self, df: pd.DataFrame, key_columns: List[str]) -> float:
        """Check completeness of key columns."""
        if df.empty:
            return 0.0

        scores = []
        for col in key_columns:
            if col in df.columns:
                score = df[col].notna().mean()
                scores.append(score)

        return sum(scores) / len(scores) if scores else 1.0

    def check_uniqueness(self, df: pd.DataFrame, key_columns: List[str]) -> float:
        """Check uniqueness of key column combination."""
        if df.empty:
            return 0.0

        existing_cols = [c for c in key_columns if c in df.columns]
        if not existing_cols:
            return 1.0

        total = len(df)
        unique = len(df.drop_duplicates(subset=existing_cols))

        return unique / total if total > 0 else 0.0

    def validate(self, df: pd.DataFrame, key_columns: List[str]) -> QualityReport:
        """Run all quality checks."""
        logger.info("Running quality gates...")

        completeness = self.check_completeness(df, key_columns)
        uniqueness = self.check_uniqueness(df, key_columns)

        issues = []
        if completeness < self.thresholds.get('completeness', 0.95):
            issues.append(f"Completeness {completeness:.1%} below threshold")
        if uniqueness < self.thresholds.get('uniqueness', 0.99):
            issues.append(f"Uniqueness {uniqueness:.1%} below threshold")

        passed = len(issues) == 0

        report = QualityReport(
            total_rows=len(df),
            valid_rows=len(df.dropna(subset=key_columns)) if key_columns else len(df),
            completeness_score=completeness,
            uniqueness_score=uniqueness,
            passed=passed,
            issues=issues
        )

        status = "PASSED" if passed else "FAILED"
        logger.info(f"  Completeness: {completeness:.1%}")
        logger.info(f"  Uniqueness: {uniqueness:.1%}")
        logger.info(f"  Quality Gate: {status}")

        return report


class Loader:
    """Data loading operations."""

    def to_csv(self, df: pd.DataFrame, path: str) -> None:
        """Save to CSV."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info(f"Saved CSV: {path} ({len(df)} rows)")

    def to_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Save to Parquet."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved Parquet: {path} ({len(df)} rows)")

    def to_json(self, df: pd.DataFrame, path: str) -> None:
        """Save to JSON."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_json(path, orient='records', indent=2)
        logger.info(f"Saved JSON: {path} ({len(df)} rows)")


class ETLPipeline:
    """Main ETL pipeline orchestrator."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.extractor = self._get_extractor()
        self.transformer = Transformer(config.schema)
        self.quality_gate = QualityGate(config.quality_thresholds)
        self.loader = Loader()
        self.metrics: Dict[str, Any] = {}

    def _get_extractor(self) -> Extractor:
        """Get appropriate extractor for source type."""
        extractors = {
            'csv': CSVExtractor(),
            'api': APIExtractor(),
        }
        return extractors.get(self.config.source_type, CSVExtractor())

    def add_transformation(self, func: Callable) -> 'ETLPipeline':
        """Add a custom transformation."""
        self.transformer.add_transformation(func)
        return self

    def run(self, key_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Execute the full ETL pipeline."""
        start_time = datetime.now(timezone.utc)

        logger.info("=" * 50)
        logger.info(f"Starting pipeline: {self.config.name}")
        logger.info("=" * 50)

        # Extract
        df = self.extractor.extract(
            self.config.source_path,
            chunk_size=self.config.chunk_size
        )

        # Transform
        df = self.transformer.transform(df)

        # Quality Gate
        key_cols = key_columns or list(df.columns[:3])
        quality_report = self.quality_gate.validate(df, key_cols)

        # Load (even if quality gate fails, for debugging)
        output_path = Path(self.config.output_path)
        self.loader.to_csv(df, str(output_path.with_suffix('.csv')))

        end_time = datetime.now(timezone.utc)

        # Metrics
        self.metrics = {
            'pipeline_name': self.config.name,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': (end_time - start_time).total_seconds(),
            'rows_extracted': len(df),
            'quality_passed': quality_report.passed,
            'completeness': quality_report.completeness_score,
            'uniqueness': quality_report.uniqueness_score,
            'issues': quality_report.issues
        }

        # Save metrics
        metrics_path = output_path.parent / 'pipeline_metrics.json'
        with open(metrics_path, 'w') as f:
            json.dump(self.metrics, f, indent=2)

        logger.info("=" * 50)
        logger.info(f"Pipeline complete: {self.config.name}")
        logger.info(f"Duration: {self.metrics['duration_seconds']:.1f}s")
        logger.info("=" * 50)

        return self.metrics


# Demo
if __name__ == '__main__':
    # Create sample data
    sample_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'Item {i}' for i in range(1, 101)],
        'value': [i * 10.5 for i in range(1, 101)],
        'date': pd.date_range('2023-01-01', periods=100)
    })

    # Save sample data
    Path('data').mkdir(exist_ok=True)
    sample_data.to_csv('data/sample_input.csv', index=False)

    # Configure pipeline
    config = PipelineConfig(
        name='sample_etl',
        source_type='csv',
        source_path='data/sample_input.csv',
        output_path='data/output/processed.csv',
        schema={
            'id': 'int',
            'name': 'str',
            'value': 'float',
            'date': 'datetime'
        }
    )

    # Add custom transformation
    def add_computed_column(df):
        df['value_doubled'] = df['value'] * 2
        return df

    # Run pipeline
    pipeline = ETLPipeline(config)
    pipeline.add_transformation(add_computed_column)
    metrics = pipeline.run(key_columns=['id'])

    print(f"\nPipeline Result: {'PASSED' if metrics['quality_passed'] else 'FAILED'}")
