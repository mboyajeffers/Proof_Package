"""
Enterprise ETL Framework - ETL Orchestrator
Coordinates ETL pipeline execution with job tracking.

Features:
- Run single pipeline or batch jobs
- Track job status and history
- Handle errors and retries
- Generate execution reports
"""

import os
import json
import logging
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum
import traceback

import sys

    

from .core.etl_registry import get_registry, PipelineStatus


class JobStatus(Enum):
    """ETL job status."""
    PENDING = 'pending'
    RUNNING = 'running'
    EXTRACTING = 'extracting'
    TRANSFORMING = 'transforming'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


@dataclass
class ETLJobResult:
    """Result of an ETL job execution."""
    job_id: str
    pipeline: str
    status: JobStatus
    started_at: str
    completed_at: Optional[str] = None
    
    # Extraction results
    records_extracted: int = 0
    extraction_error: Optional[str] = None
    
    # Transformation results
    tables_created: List[str] = field(default_factory=list)
    total_rows: int = 0
    output_paths: Dict[str, str] = field(default_factory=dict)
    transformation_error: Optional[str] = None
    
    # Timing
    extract_duration_sec: float = 0
    transform_duration_sec: float = 0
    total_duration_sec: float = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = asdict(self)
        result['status'] = self.status.value
        return result


class ETLOrchestrator:
    """
    Orchestrates ETL pipeline execution.
    
    Example:
        orchestrator = ETLOrchestrator()
        result = orchestrator.run_pipeline('gaming', limit=100)
        print(result.total_rows)
    """
    
    def __init__(self, output_base_dir: str = './data/data/etl'):
        self.output_base_dir = output_base_dir
        self.logger = logging.getLogger('ETL:Orchestrator')
        self.registry = get_registry()
        self._jobs: Dict[str, ETLJobResult] = {}
        self._lock = threading.Lock()
        self._job_counter = 0
        
        # Ensure output directory exists
        os.makedirs(output_base_dir, exist_ok=True)
    
    def _generate_job_id(self) -> str:
        """Generate unique job ID."""
        with self._lock:
            self._job_counter += 1
            timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            return f"ETL-{timestamp}-{self._job_counter:04d}"
    
    def run_pipeline(
        self,
        pipeline_name: str,
        output_dir: Optional[str] = None,
        extractor_params: Optional[Dict[str, Any]] = None,
        transformer_params: Optional[Dict[str, Any]] = None,
    ) -> ETLJobResult:
        """
        Run a complete ETL pipeline (extract + transform).
        
        Args:
            pipeline_name: Name of the pipeline to run
            output_dir: Override output directory
            extractor_params: Parameters for extractor
            transformer_params: Parameters for transformer
            
        Returns:
            ETLJobResult with execution details
        """
        job_id = self._generate_job_id()
        start_time = datetime.utcnow()
        
        result = ETLJobResult(
            job_id=job_id,
            pipeline=pipeline_name,
            status=JobStatus.PENDING,
            started_at=start_time.isoformat() + 'Z',
        )
        
        self._jobs[job_id] = result
        
        try:
            # Check pipeline status
            info = self.registry.get_pipeline_info(pipeline_name)
            if not info:
                raise ValueError(f"Unknown pipeline: {pipeline_name}")
            
            status = self.registry.check_pipeline_status(pipeline_name)
            if status == PipelineStatus.REQUIRES_KEY:
                raise ValueError(f"Pipeline '{pipeline_name}' requires API key: {info.api_key_name}")
            
            result.status = JobStatus.RUNNING
            self.logger.info(f"[{job_id}] Starting pipeline: {pipeline_name}")
            
            # Merge default params with provided params
            ext_params = self.registry.get_default_params(pipeline_name)
            if extractor_params:
                ext_params.update(extractor_params)
            
            # EXTRACTION PHASE
            result.status = JobStatus.EXTRACTING
            extract_start = datetime.utcnow()
            
            self.logger.info(f"[{job_id}] Extraction phase starting")
            extractor = self.registry.get_extractor(pipeline_name)
            
            # Call appropriate extract method based on pipeline
            if pipeline_name == 'gaming':
                extract_result = extractor.extract_top_games(
                    count=ext_params.get('limit', 100),
                    sort_by=ext_params.get('sort_by', 'owners'),
                )
            elif pipeline_name == 'crypto':
                extract_result = extractor.extract_top_coins(
                    count=ext_params.get('limit', 100),
                    vs_currency=ext_params.get('vs_currency', 'usd'),
                )
            elif pipeline_name == 'betting':
                extract_result = extractor.extract(
                    leagues=ext_params.get('leagues', ['nfl', 'nba', 'mlb', 'nhl']),
                    include_standings=ext_params.get('include_standings', True),
                )
            elif pipeline_name == 'media':
                extract_result = extractor.extract(
                    movies_limit=ext_params.get('movies_limit', 100),
                    tv_limit=ext_params.get('tv_limit', 100),
                )
            else:
                extract_result = extractor.extract(**ext_params)
            
            extract_end = datetime.utcnow()
            result.extract_duration_sec = (extract_end - extract_start).total_seconds()
            
            if not extract_result.success:
                result.extraction_error = extract_result.error
                raise Exception(f"Extraction failed: {extract_result.error}")
            
            result.records_extracted = extract_result.records_extracted
            self.logger.info(f"[{job_id}] Extracted {result.records_extracted} records")
            
            # TRANSFORMATION PHASE
            result.status = JobStatus.TRANSFORMING
            transform_start = datetime.utcnow()
            
            self.logger.info(f"[{job_id}] Transformation phase starting")
            
            # Determine output directory
            if not output_dir:
                output_dir = os.path.join(self.output_base_dir, pipeline_name, job_id)
            
            trans_params = transformer_params or {}
            trans_params['output_dir'] = output_dir
            
            transformer = self.registry.get_transformer(pipeline_name, **trans_params)
            transform_result = transformer.transform(extract_result.data)
            
            transform_end = datetime.utcnow()
            result.transform_duration_sec = (transform_end - transform_start).total_seconds()
            
            if not transform_result.success:
                result.transformation_error = transform_result.error
                raise Exception(f"Transformation failed: {transform_result.error}")
            
            result.tables_created = transform_result.tables_created
            result.total_rows = transform_result.total_rows
            result.output_paths = transform_result.output_paths
            
            self.logger.info(f"[{job_id}] Transformed into {len(result.tables_created)} tables, {result.total_rows:,} rows")
            
            # SUCCESS
            result.status = JobStatus.COMPLETED
            
        except Exception as e:
            result.status = JobStatus.FAILED
            error_msg = str(e)
            if not result.extraction_error and not result.transformation_error:
                result.extraction_error = error_msg
            self.logger.error(f"[{job_id}] Pipeline failed: {error_msg}")
            self.logger.error(traceback.format_exc())
        
        finally:
            end_time = datetime.utcnow()
            result.completed_at = end_time.isoformat() + 'Z'
            result.total_duration_sec = (end_time - start_time).total_seconds()
            
            # Save job result
            self._save_job_result(result)
        
        return result
    
    def run_all_pipelines(
        self,
        pipelines: Optional[List[str]] = None,
        skip_api_key_required: bool = True,
    ) -> List[ETLJobResult]:
        """
        Run multiple pipelines sequentially.
        
        Args:
            pipelines: List of pipeline names (None = all available)
            skip_api_key_required: Skip pipelines that require API keys
            
        Returns:
            List of ETLJobResult for each pipeline
        """
        results = []
        
        if pipelines is None:
            pipelines = [p['name'] for p in self.registry.list_pipelines()]
        
        for pipeline_name in pipelines:
            # Check if we should skip
            if skip_api_key_required:
                status = self.registry.check_pipeline_status(pipeline_name)
                if status == PipelineStatus.REQUIRES_KEY:
                    self.logger.info(f"Skipping {pipeline_name} (requires API key)")
                    continue
            
            result = self.run_pipeline(pipeline_name)
            results.append(result)
        
        return results
    
    def get_job(self, job_id: str) -> Optional[ETLJobResult]:
        """Get job result by ID."""
        return self._jobs.get(job_id)
    
    def list_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List recent jobs."""
        jobs = sorted(
            self._jobs.values(),
            key=lambda j: j.started_at,
            reverse=True
        )[:limit]
        return [j.to_dict() for j in jobs]
    
    def _save_job_result(self, result: ETLJobResult):
        """Save job result to file."""
        try:
            jobs_dir = os.path.join(self.output_base_dir, '_jobs')
            os.makedirs(jobs_dir, exist_ok=True)
            
            filepath = os.path.join(jobs_dir, f"{result.job_id}.json")
            with open(filepath, 'w') as f:
                json.dump(result.to_dict(), f, indent=2)
            
            self.logger.debug(f"Saved job result: {filepath}")
        except Exception as e:
            self.logger.error(f"Failed to save job result: {e}")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get orchestrator summary."""
        pipelines = self.registry.list_pipelines()
        jobs = list(self._jobs.values())
        
        completed = [j for j in jobs if j.status == JobStatus.COMPLETED]
        failed = [j for j in jobs if j.status == JobStatus.FAILED]
        
        return {
            'pipelines_registered': len(pipelines),
            'pipelines_available': sum(1 for p in pipelines if p['status'] == 'available'),
            'pipelines_require_key': sum(1 for p in pipelines if p['status'] == 'requires_api_key'),
            'total_jobs': len(jobs),
            'completed_jobs': len(completed),
            'failed_jobs': len(failed),
            'total_rows_processed': sum(j.total_rows for j in completed),
            'total_records_extracted': sum(j.records_extracted for j in completed),
        }


# Global orchestrator instance
_orchestrator = None

def get_orchestrator() -> ETLOrchestrator:
    """Get the global ETL orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = ETLOrchestrator()
    return _orchestrator
