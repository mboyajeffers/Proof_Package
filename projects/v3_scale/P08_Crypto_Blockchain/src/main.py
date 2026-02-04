"""
P08 Crypto & Blockchain Analytics - Pipeline Orchestrator
Author: Mboya Jeffers

Enterprise-scale ETL pipeline for cryptocurrency market analytics.
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
from extract import CryptoDataExtractor
from transform import CryptoStarSchemaTransformer
from analytics import CryptoAnalyticsEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CryptoAnalyticsPipeline:
    """
    End-to-end pipeline orchestrator for cryptocurrency analytics.

    Stages:
    1. EXTRACT: Fetch data from CoinGecko API
    2. TRANSFORM: Build Kimball star schema
    3. ANALYZE: Calculate crypto KPIs
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
        return f"P08_CRYPTO_{timestamp}"

    def _calculate_checksum(self, data: Dict) -> str:
        """Calculate MD5 checksum for data verification."""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(json_str.encode()).hexdigest()

    def run_extraction(self, mode: str = 'test', limit: int = 50) -> Dict:
        """
        Stage 1: Run data extraction from CoinGecko.

        Args:
            mode: 'test' for limited extraction, 'full' for complete
            limit: Coin limit for test mode

        Returns:
            Extracted data dictionary
        """
        logger.info("=" * 60)
        logger.info("STAGE 1: EXTRACTION")
        logger.info("=" * 60)

        stage_start = datetime.now()
        extractor = CryptoDataExtractor(output_dir=str(self.output_dir))

        if mode == 'test':
            data = extractor.run_test_extraction(limit=limit)
        else:
            data = extractor.run_full_extraction(pages=20)

        self.pipeline_log['stages']['extraction'] = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'mode': mode,
            'coins_extracted': len(data.get('market_data', [])),
            'ohlcv_records': len(data.get('ohlcv', [])),
            'exchanges_extracted': len(data.get('exchanges', [])),
            'api_calls': extractor.extraction_log.get('api_calls', 0),
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
        transformer = CryptoStarSchemaTransformer(output_dir=str(self.output_dir))
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
        Stage 3: Run crypto analytics.

        Args:
            tables: Transformed dimensional tables

        Returns:
            Analytics results dictionary
        """
        logger.info("=" * 60)
        logger.info("STAGE 3: ANALYTICS")
        logger.info("=" * 60)

        stage_start = datetime.now()
        engine = CryptoAnalyticsEngine(data_dir=str(self.output_dir))
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
            'project': 'P08_Crypto_Blockchain',
            'version': '1.0.0',
            'author': 'Mboya Jeffers',
            'generated_at': datetime.now().isoformat(),
            'pipeline_id': self.pipeline_log['pipeline_id'],

            'data_sources': {
                'primary': [
                    {
                        'name': 'CoinGecko API',
                        'url': 'https://api.coingecko.com/api/v3',
                        'type': 'REST API',
                        'auth': 'None (free tier)',
                        'data_available': 'Market data, OHLCV, exchanges'
                    }
                ],
                'rate_limits': '10-30 requests/minute (free tier)'
            },

            'extraction_summary': {
                'coins_extracted': len(extracted_data.get('market_data', [])),
                'ohlcv_records': len(extracted_data.get('ohlcv', [])),
                'exchanges_extracted': len(extracted_data.get('exchanges', [])),
                'global_metrics': extracted_data.get('global_metrics') is not None
            },

            'transformation_summary': {
                'schema_pattern': 'Kimball Star Schema',
                'tables_created': {name: len(df) for name, df in tables.items()},
                'total_rows': sum(len(df) for df in tables.values()),
                'dimensions': [k for k in tables.keys() if k.startswith('dim_')],
                'facts': [k for k in tables.keys() if k.startswith('fact_')]
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
        evidence_path = self.evidence_dir / "P08_evidence.json"
        with open(evidence_path, 'w') as f:
            json.dump(evidence, f, indent=2)
        logger.info(f"Evidence saved: {evidence_path}")

        return evidence

    def run(self, mode: str = 'test', limit: int = 50):
        """
        Execute complete pipeline.

        Args:
            mode: 'test' for limited run, 'full' for production
            limit: Coin limit for test mode
        """
        self.pipeline_log['start_time'] = datetime.now().isoformat()
        self.pipeline_log['status'] = 'running'

        logger.info("=" * 60)
        logger.info("P08 CRYPTO & BLOCKCHAIN ANALYTICS PIPELINE")
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
            logger.info(f"Evidence: {self.evidence_dir / 'P08_evidence.json'}")

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
        description='P08 Crypto & Blockchain Analytics Pipeline'
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
        default=50,
        help='Coin limit for test mode (default: 50)'
    )
    parser.add_argument(
        '--output-dir',
        default='data',
        help='Output directory for data files'
    )

    args = parser.parse_args()

    pipeline = CryptoAnalyticsPipeline(output_dir=args.output_dir)
    pipeline.run(mode=args.mode, limit=args.limit)


if __name__ == "__main__":
    main()
