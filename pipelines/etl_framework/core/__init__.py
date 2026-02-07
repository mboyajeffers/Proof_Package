"""Core ETL infrastructure components."""
from .surrogate_keys import generate_surrogate_key, generate_date_key

try:
    from .base_extractor import BaseExtractor, ExtractionResult
    from .base_transformer import BaseTransformer, TransformationResult
    from .parquet_writer import ParquetWriter
    from .etl_registry import ETLRegistry, get_registry
    from .etl_orchestrator import ETLOrchestrator, get_orchestrator
except ImportError:
    pass
