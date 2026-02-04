"""
P07 Media & Streaming Analytics - Pipeline Orchestrator
Author: Mboya Jeffers

Enterprise-scale ETL pipeline for media and entertainment analytics.
Coordinates extraction, transformation, analytics, and evidence generation.
"""

import argparse
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Local imports
from extract import IMDBDataExtractor
from transform import MediaStarSchemaTransformer
from analytics import MediaAnalyticsEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MediaAnalyticsPipeline:
    """
    End-to-end pipeline orchestrator for media analytics.

    Stages:
    1. EXTRACT: Download IMDB datasets
    2. TRANSFORM: Build Kimball star schema
    3. ANALYZE: Calculate media KPIs
    4. EVIDENCE: Generate audit trail and quality metrics
    """

    def __init__(self, output_dir: str = "data"):
        """Initialize pipeline with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.evidence_dir = self.output_dir.parent / "evidence"
        self.evidence_dir.mkdir(parents=True, exist_ok=True)

        self.pipeline_log = {
            'pipeline_id': self._generate_pipeline_id(),
            'start_time': None,
            'end_time': None,
            'stages': {},
            'total_rows': 0,
            'status': 'initialized'
        }

    def _generate_pipeline_id(self) -> str:
        """Generate unique pipeline execution ID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"P07_MEDIA_{timestamp}"

    def _calculate_checksum(self, data: Dict) -> str:
        """Calculate MD5 checksum for data verification."""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(json_str.encode()).hexdigest()

    def run_extraction(self, mode: str = 'test', limit: int = 1000) -> Dict:
        """
        Stage 1: Run data extraction from IMDB.

        Args:
            mode: 'test' for limited extraction, 'full' for complete
            limit: Record limit per dataset for test mode

        Returns:
            Extracted data dictionary
        """
        logger.info("=" * 60)
        logger.info("STAGE 1: EXTRACTION")
        logger.info("=" * 60)

        stage_start = datetime.now()
        extractor = IMDBDataExtractor(output_dir=str(self.output_dir))

        if mode == 'test':
            data = extractor.run_test_extraction(limit=limit)
        else:
            data = extractor.run_full_extraction()

        self.pipeline_log['stages']['extraction'] = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'mode': mode,
            'titles_extracted': len(data.get('titles', [])),
            'ratings_extracted': len(data.get('ratings', [])),
            'names_extracted': len(data.get('names', [])),
            'principals_extracted': len(data.get('principals', [])),
            'errors': len(extractor.extraction_log.get('errors', []))
        }

        return data

    def run_transformation(self, extracted_data: Dict) -> Dict:
        """
        Stage 2: Run star schema transformation.

        Args:
            extracted_data: Raw data from extraction stage

        Returns:
            Dictionary of transformed DataFrames
        """
        logger.info("=" * 60)
        logger.info("STAGE 2: TRANSFORMATION")
        logger.info("=" * 60)

        stage_start = datetime.now()
        transformer = MediaStarSchemaTransformer(output_dir=str(self.output_dir))
        tables = transformer.run_transformation(extracted_data)

        self.pipeline_log['stages']['transformation'] = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'tables_created': list(tables.keys()),
            'row_counts': {name: len(df) for name, df in tables.items()},
            'total_rows': sum(len(df) for df in tables.values())
        }

        self.pipeline_log['total_rows'] = self.pipeline_log['stages']['transformation']['total_rows']

        return tables

    def run_analytics(self, tables: Dict) -> Dict:
        """
        Stage 3: Run media analytics.

        Args:
            tables: Transformed dimensional tables

        Returns:
            Analytics results dictionary
        """
        logger.info("=" * 60)
        logger.info("STAGE 3: ANALYTICS")
        logger.info("=" * 60)

        stage_start = datetime.now()
        engine = MediaAnalyticsEngine(data_dir=str(self.output_dir))
        results = engine.run_analytics(tables)

        self.pipeline_log['stages']['analytics'] = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'kpis_calculated': results.get('kpi_summary', {}).get('kpi_count', 0),
            'analyses_completed': len(results) - 1
        }

        return results

    def generate_evidence(self, extracted_data: Dict, tables: Dict,
                         analytics_results: Dict) -> Dict:
        """
        Stage 4: Generate evidence and audit trail.

        Args:
            extracted_data: Raw extraction data
            tables: Transformed tables
            analytics_results: Analytics output

        Returns:
            Evidence dictionary
        """
        logger.info("=" * 60)
        logger.info("STAGE 4: EVIDENCE GENERATION")
        logger.info("=" * 60)

        stage_start = datetime.now()

        evidence = {
            'project': 'P07_Media_Streaming',
            'version': '1.0.0',
            'author': 'Mboya Jeffers',
            'generated_at': datetime.now().isoformat(),
            'pipeline_id': self.pipeline_log['pipeline_id'],

            'data_sources': {
                'primary': [
                    {
                        'name': 'IMDB Datasets',
                        'url': 'https://datasets.imdbws.com',
                        'type': 'TSV.GZ (compressed)',
                        'auth': 'None (public)',
                        'data_available': 'Titles, ratings, cast/crew'
                    }
                ],
                'datasets': ['title.basics', 'title.ratings', 'name.basics', 'title.principals']
            },

            'extraction_summary': {
                'titles_extracted': len(extracted_data.get('titles', [])),
                'ratings_extracted': len(extracted_data.get('ratings', [])),
                'names_extracted': len(extracted_data.get('names', [])),
                'principals_extracted': len(extracted_data.get('principals', []))
            },

            'transformation_summary': {
                'schema_pattern': 'Kimball Star Schema',
                'tables_created': {name: len(df) for name, df in tables.items()},
                'total_rows': sum(len(df) for df in tables.values()),
                'dimensions': [k for k in tables.keys() if k.startswith('dim_')],
                'facts': [k for k in tables.keys() if k.startswith('fact_')],
                'bridges': [k for k in tables.keys() if 'bridge' in k]
            },

            'analytics_summary': {
                'kpis_calculated': analytics_results.get('kpi_summary', {}).get('kpi_count', 0),
                'kpi_list': list(analytics_results.get('kpi_summary', {}).get('kpis', {}).keys())
            },

            'quality_metrics': {
                'extraction_errors': self.pipeline_log['stages'].get('extraction', {}).get('errors', 0),
                'completeness': 'PASS' if self.pipeline_log['total_rows'] > 0 else 'FAIL',
                'data_checksums': {}
            },

            'pipeline_execution': self.pipeline_log
        }

        # Calculate checksums for key data
        for table_name, df in tables.items():
            if len(df) > 0:
                checksum = hashlib.md5(df.to_json().encode()).hexdigest()[:16]
                evidence['quality_metrics']['data_checksums'][table_name] = checksum

        self.pipeline_log['stages']['evidence'] = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'evidence_generated': True
        }

        # Save evidence
        evidence_path = self.evidence_dir / "P07_evidence.json"
        with open(evidence_path, 'w') as f:
            json.dump(evidence, f, indent=2)
        logger.info(f"Evidence saved: {evidence_path}")

        return evidence

    def run(self, mode: str = 'test', limit: int = 1000):
        """
        Execute complete pipeline.

        Args:
            mode: 'test' for limited run, 'full' for production
            limit: Record limit per dataset for test mode
        """
        self.pipeline_log['start_time'] = datetime.now().isoformat()
        self.pipeline_log['status'] = 'running'

        logger.info("=" * 60)
        logger.info("P07 MEDIA & STREAMING ANALYTICS PIPELINE")
        logger.info(f"Mode: {mode.upper()}")
        logger.info("=" * 60)

        try:
            # Stage 1: Extraction
            extracted_data = self.run_extraction(mode=mode, limit=limit)

            # Stage 2: Transformation
            tables = self.run_transformation(extracted_data)

            # Stage 3: Analytics
            analytics_results = self.run_analytics(tables)

            # Stage 4: Evidence
            evidence = self.generate_evidence(extracted_data, tables, analytics_results)

            self.pipeline_log['end_time'] = datetime.now().isoformat()
            self.pipeline_log['status'] = 'completed'

            # Final summary
            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Total rows: {self.pipeline_log['total_rows']}")
            logger.info(f"Tables: {len(tables)}")
            logger.info(f"KPIs: {analytics_results.get('kpi_summary', {}).get('kpi_count', 0)}")
            logger.info(f"Evidence: {self.evidence_dir / 'P07_evidence.json'}")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.pipeline_log['status'] = 'failed'
            self.pipeline_log['error'] = str(e)
            raise

        finally:
            # Save pipeline log
            log_path = self.output_dir / "pipeline_log.json"
            with open(log_path, 'w') as f:
                json.dump(self.pipeline_log, f, indent=2)
            logger.info(f"Pipeline log saved: {log_path}")


def main():
    """Main entry point for command line execution."""
    parser = argparse.ArgumentParser(
        description='P07 Media & Streaming Analytics Pipeline'
    )
    parser.add_argument(
        '--mode',
        choices=['test', 'full'],
        default='test',
        help='Execution mode: test (limited) or full (production)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Record limit per dataset for test mode (default: 1000)'
    )
    parser.add_argument(
        '--output-dir',
        default='data',
        help='Output directory for data files'
    )

    args = parser.parse_args()

    pipeline = MediaAnalyticsPipeline(output_dir=args.output_dir)
    pipeline.run(mode=args.mode, limit=args.limit)


if __name__ == "__main__":
    main()
