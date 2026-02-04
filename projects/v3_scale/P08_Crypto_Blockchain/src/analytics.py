"""
P08 Crypto & Blockchain Analytics - Analytics Module
Author: Mboya Jeffers

Calculates key performance indicators for cryptocurrency market analytics.
Maps to CMS crypto engine KPIs for report generation.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CryptoAnalyticsEngine:
    """
    Calculates cryptocurrency market analytics KPIs.

    KPI Categories:
    - Market Overview: Total cap, volume, dominance
    - Price Performance: Top gainers/losers, volatility
    - Asset Analysis: By category, by market cap tier
    - Exchange Analysis: Volume distribution, trust scores

    Maps to CMS Crypto Engine (30 KPIs).
    """

    def __init__(self, data_dir: str = "data"):
        """Initialize analytics engine with data directory."""
        self.data_dir = Path(data_dir)
        self.analytics_results = {}

    def _load_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """Load parquet table from data directory."""
        path = self.data_dir / f"{table_name}.parquet"
        if path.exists():
            return pd.read_parquet(path)
        logger.warning(f"Table not found: {path}")
        return None

    def calculate_market_overview(self, fact_prices: pd.DataFrame,
                                   fact_global: pd.DataFrame) -> Dict:
        """
        Calculate overall market metrics.

        Metrics:
        - Total market cap
        - 24h volume
        - BTC/ETH dominance
        - Active assets

        Args:
            fact_prices: Price fact table
            fact_global: Global metrics fact table

        Returns:
            Dictionary of market overview metrics
        """
        logger.info("Calculating market overview metrics...")

        results = {
            'total_market_cap': None,
            'total_volume_24h': None,
            'btc_dominance': None,
            'eth_dominance': None,
            'active_assets': 0,
            'assets_with_positive_24h': 0
        }

        if fact_global is not None and len(fact_global) > 0:
            latest = fact_global.iloc[0]
            results['total_market_cap'] = latest.get('total_market_cap')
            results['total_volume_24h'] = latest.get('total_volume_24h')
            results['btc_dominance'] = latest.get('btc_dominance')
            results['eth_dominance'] = latest.get('eth_dominance')

        if fact_prices is not None:
            results['active_assets'] = len(fact_prices)
            positive_change = fact_prices[fact_prices['price_change_pct_24h'] > 0]
            results['assets_with_positive_24h'] = len(positive_change)
            results['positive_24h_pct'] = round(len(positive_change) / len(fact_prices) * 100, 1) if len(fact_prices) > 0 else 0

        logger.info(f"  Active assets: {results['active_assets']}")
        return results

    def calculate_price_performance(self, fact_prices: pd.DataFrame,
                                     dim_asset: pd.DataFrame) -> Dict:
        """
        Calculate price performance metrics.

        Metrics:
        - Top gainers (24h, 7d, 30d)
        - Top losers (24h, 7d, 30d)
        - Volatility leaders
        - New ATH/ATL

        Args:
            fact_prices: Price fact table
            dim_asset: Asset dimension table

        Returns:
            Dictionary of price performance metrics
        """
        logger.info("Calculating price performance metrics...")

        results = {
            'top_gainers_24h': [],
            'top_losers_24h': [],
            'highest_volume': [],
            'price_change_distribution': {}
        }

        if fact_prices is None or dim_asset is None:
            return results

        # Merge with asset info
        merged = fact_prices.merge(
            dim_asset[['asset_key', 'symbol', 'name', 'category']],
            on='asset_key',
            how='left'
        )

        # Filter to meaningful assets (min $1M market cap)
        significant = merged[merged['market_cap'] > 1000000].copy()

        if len(significant) == 0:
            significant = merged.copy()

        # Top gainers 24h
        gainers = significant.nlargest(10, 'price_change_pct_24h')
        results['top_gainers_24h'] = gainers[['symbol', 'name', 'price_usd',
            'price_change_pct_24h', 'market_cap']].to_dict('records')

        # Top losers 24h
        losers = significant.nsmallest(10, 'price_change_pct_24h')
        results['top_losers_24h'] = losers[['symbol', 'name', 'price_usd',
            'price_change_pct_24h', 'market_cap']].to_dict('records')

        # Highest volume
        by_volume = significant.nlargest(10, 'volume_24h')
        results['highest_volume'] = by_volume[['symbol', 'name', 'volume_24h',
            'market_cap']].to_dict('records')

        # Price change distribution
        bins = [-100, -10, -5, 0, 5, 10, 100]
        labels = ['<-10%', '-10% to -5%', '-5% to 0%', '0% to 5%', '5% to 10%', '>10%']

        if 'price_change_pct_24h' in merged.columns:
            merged['change_bucket'] = pd.cut(
                merged['price_change_pct_24h'].fillna(0),
                bins=bins, labels=labels
            )
            distribution = merged['change_bucket'].value_counts().sort_index().to_dict()
            results['price_change_distribution'] = {str(k): int(v) for k, v in distribution.items()}

        logger.info(f"  Analyzed {len(merged)} assets")
        return results

    def calculate_category_analysis(self, fact_prices: pd.DataFrame,
                                     dim_asset: pd.DataFrame) -> Dict:
        """
        Calculate metrics by asset category.

        Metrics:
        - Market cap by category
        - Volume by category
        - Performance by category

        Args:
            fact_prices: Price fact table
            dim_asset: Asset dimension table

        Returns:
            Dictionary of category analysis metrics
        """
        logger.info("Calculating category analysis metrics...")

        results = {
            'by_category': [],
            'total_categories': 0
        }

        if fact_prices is None or dim_asset is None:
            return results

        # Merge tables
        merged = fact_prices.merge(
            dim_asset[['asset_key', 'symbol', 'name', 'category']],
            on='asset_key',
            how='left'
        )

        # Group by category
        category_stats = merged.groupby('category').agg({
            'market_cap': ['sum', 'count'],
            'volume_24h': 'sum',
            'price_change_pct_24h': 'mean'
        }).reset_index()

        category_stats.columns = ['category', 'total_market_cap', 'asset_count',
                                  'total_volume', 'avg_change_24h']

        category_stats = category_stats.sort_values('total_market_cap', ascending=False)

        results['by_category'] = category_stats.round(2).to_dict('records')
        results['total_categories'] = len(category_stats)

        logger.info(f"  Analyzed {len(category_stats)} categories")
        return results

    def calculate_market_cap_tiers(self, fact_prices: pd.DataFrame,
                                    dim_asset: pd.DataFrame) -> Dict:
        """
        Calculate metrics by market cap tier.

        Tiers:
        - Large cap: >$10B
        - Mid cap: $1B-$10B
        - Small cap: $100M-$1B
        - Micro cap: <$100M

        Args:
            fact_prices: Price fact table
            dim_asset: Asset dimension table

        Returns:
            Dictionary of tier analysis
        """
        logger.info("Calculating market cap tier analysis...")

        results = {
            'by_tier': [],
            'tier_distribution': {}
        }

        if fact_prices is None:
            return results

        # Define tiers
        def get_tier(market_cap):
            if market_cap is None:
                return 'Unknown'
            if market_cap >= 10_000_000_000:
                return 'Large Cap (>$10B)'
            elif market_cap >= 1_000_000_000:
                return 'Mid Cap ($1B-$10B)'
            elif market_cap >= 100_000_000:
                return 'Small Cap ($100M-$1B)'
            else:
                return 'Micro Cap (<$100M)'

        fact_prices = fact_prices.copy()
        fact_prices['tier'] = fact_prices['market_cap'].apply(get_tier)

        # Group by tier
        tier_stats = fact_prices.groupby('tier').agg({
            'market_cap': ['sum', 'count'],
            'volume_24h': 'sum',
            'price_change_pct_24h': 'mean'
        }).reset_index()

        tier_stats.columns = ['tier', 'total_market_cap', 'asset_count',
                             'total_volume', 'avg_change_24h']

        results['by_tier'] = tier_stats.round(2).to_dict('records')

        # Distribution
        results['tier_distribution'] = fact_prices['tier'].value_counts().to_dict()

        logger.info(f"  Analyzed {len(tier_stats)} market cap tiers")
        return results

    def calculate_exchange_analysis(self, dim_exchange: pd.DataFrame) -> Dict:
        """
        Calculate exchange metrics.

        Metrics:
        - Volume distribution
        - Trust score distribution
        - Geographic distribution

        Args:
            dim_exchange: Exchange dimension table

        Returns:
            Dictionary of exchange metrics
        """
        logger.info("Calculating exchange analysis metrics...")

        results = {
            'total_exchanges': 0,
            'top_by_volume': [],
            'by_country': [],
            'trust_score_distribution': {}
        }

        if dim_exchange is None or len(dim_exchange) == 0:
            return results

        results['total_exchanges'] = len(dim_exchange)

        # Top by volume
        if 'trade_volume_24h_btc' in dim_exchange.columns:
            by_volume = dim_exchange.nlargest(10, 'trade_volume_24h_btc')
            results['top_by_volume'] = by_volume[['exchange_name', 'trade_volume_24h_btc',
                'trust_score', 'country']].to_dict('records')

        # By country
        if 'country' in dim_exchange.columns:
            by_country = dim_exchange.groupby('country').size().sort_values(ascending=False)
            results['by_country'] = by_country.head(10).to_dict()

        # Trust score distribution
        if 'trust_score' in dim_exchange.columns:
            trust_dist = dim_exchange['trust_score'].value_counts().sort_index().to_dict()
            results['trust_score_distribution'] = {str(k): int(v) for k, v in trust_dist.items()}

        logger.info(f"  Analyzed {len(dim_exchange)} exchanges")
        return results

    def generate_kpi_summary(self) -> Dict:
        """
        Generate summary of all calculated KPIs.

        Maps to CMS Crypto Engine KPIs.

        Returns:
            Dictionary of KPI summaries
        """
        logger.info("Generating KPI summary...")

        kpi_summary = {
            'generated_at': datetime.now().isoformat(),
            'kpi_count': 0,
            'kpis': {}
        }

        # Map analytics results to crypto KPIs
        if 'market_overview' in self.analytics_results:
            overview = self.analytics_results['market_overview']
            kpi_summary['kpis']['total_market_cap'] = {
                'value': overview.get('total_market_cap'),
                'description': 'Total cryptocurrency market capitalization'
            }
            kpi_summary['kpis']['btc_dominance'] = {
                'value': overview.get('btc_dominance'),
                'description': 'Bitcoin market dominance percentage'
            }
            kpi_summary['kpis']['active_assets'] = {
                'value': overview.get('active_assets'),
                'description': 'Number of active cryptocurrencies'
            }

        if 'price_performance' in self.analytics_results:
            perf = self.analytics_results['price_performance']
            if perf.get('top_gainers_24h'):
                top_gainer = perf['top_gainers_24h'][0]
                kpi_summary['kpis']['top_gainer_24h'] = {
                    'value': f"{top_gainer.get('symbol')}: {top_gainer.get('price_change_pct_24h')}%",
                    'description': 'Top performing asset (24h)'
                }

        if 'category_analysis' in self.analytics_results:
            cat = self.analytics_results['category_analysis']
            kpi_summary['kpis']['total_categories'] = {
                'value': cat.get('total_categories'),
                'description': 'Number of asset categories'
            }

        if 'exchange_analysis' in self.analytics_results:
            exch = self.analytics_results['exchange_analysis']
            kpi_summary['kpis']['total_exchanges'] = {
                'value': exch.get('total_exchanges'),
                'description': 'Number of tracked exchanges'
            }

        kpi_summary['kpi_count'] = len(kpi_summary['kpis'])
        logger.info(f"  Generated {kpi_summary['kpi_count']} KPIs")

        return kpi_summary

    def run_analytics(self, tables: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
        """
        Execute full analytics pipeline.

        Args:
            tables: Optional dictionary of DataFrames

        Returns:
            Dictionary of all analytics results
        """
        logger.info("=" * 60)
        logger.info("STARTING CRYPTO ANALYTICS")
        logger.info("=" * 60)

        # Load tables if not provided
        if tables is None:
            tables = {
                'fact_prices': self._load_table('fact_prices'),
                'fact_ohlcv': self._load_table('fact_ohlcv'),
                'fact_global_metrics': self._load_table('fact_global_metrics'),
                'dim_asset': self._load_table('dim_asset'),
                'dim_exchange': self._load_table('dim_exchange')
            }

        # Run analytics
        self.analytics_results['market_overview'] = self.calculate_market_overview(
            tables.get('fact_prices'),
            tables.get('fact_global_metrics')
        )

        self.analytics_results['price_performance'] = self.calculate_price_performance(
            tables.get('fact_prices'),
            tables.get('dim_asset')
        )

        self.analytics_results['category_analysis'] = self.calculate_category_analysis(
            tables.get('fact_prices'),
            tables.get('dim_asset')
        )

        self.analytics_results['market_cap_tiers'] = self.calculate_market_cap_tiers(
            tables.get('fact_prices'),
            tables.get('dim_asset')
        )

        self.analytics_results['exchange_analysis'] = self.calculate_exchange_analysis(
            tables.get('dim_exchange')
        )

        # Generate KPI summary
        self.analytics_results['kpi_summary'] = self.generate_kpi_summary()

        # Save analytics results
        output_path = self.data_dir / "analytics_results.json"
        with open(output_path, 'w') as f:
            json.dump(self.analytics_results, f, indent=2, default=str)
        logger.info(f"Analytics results saved: {output_path}")

        logger.info("=" * 60)
        logger.info("ANALYTICS COMPLETE")
        logger.info("=" * 60)

        return self.analytics_results


if __name__ == "__main__":
    engine = CryptoAnalyticsEngine()
    results = engine.run_analytics()

    print("\nAnalytics Summary:")
    if 'kpi_summary' in results:
        print(f"  KPIs calculated: {results['kpi_summary']['kpi_count']}")
        for kpi_name, kpi_data in results['kpi_summary']['kpis'].items():
            print(f"    {kpi_name}: {kpi_data['value']}")
