"""
P08 Crypto & Blockchain Analytics - Transformation Module
Author: Mboya Jeffers

Transforms raw cryptocurrency data into Kimball star schema dimensional model.
Implements surrogate key generation, category classification, and quality validation.
"""

import hashlib
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


class CryptoStarSchemaTransformer:
    """
    Transforms raw cryptocurrency data into Kimball dimensional model.

    Dimensions:
    - dim_asset: Cryptocurrency master data
    - dim_exchange: Exchange information
    - dim_chain: Blockchain platforms
    - dim_date: Date dimension

    Facts:
    - fact_prices: Daily price and market data
    - fact_ohlcv: Historical OHLCV time series
    - fact_global_metrics: Global market metrics
    """

    # Asset categories based on characteristics
    STABLECOINS = ['usdt', 'usdc', 'busd', 'dai', 'tusd', 'usdp', 'frax', 'lusd']
    DEFI_TOKENS = ['uni', 'aave', 'mkr', 'comp', 'crv', 'snx', 'yfi', 'sushi', '1inch']

    def __init__(self, output_dir: str = "data"):
        """Initialize transformer with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Surrogate key caches
        self._asset_keys = {}
        self._exchange_keys = {}
        self._chain_keys = {}
        self._date_keys = {}

        self.transformation_log = {
            'start_time': None,
            'end_time': None,
            'tables_created': {},
            'total_rows': 0,
            'errors': []
        }

    def _generate_surrogate_key(self, *values) -> str:
        """
        Generate MD5-based surrogate key from input values.

        Args:
            values: Variable values to hash

        Returns:
            16-character hex key
        """
        combined = '|'.join(str(v) for v in values if v is not None)
        return hashlib.md5(combined.encode()).hexdigest()[:16]

    def _get_asset_key(self, asset_id: str) -> str:
        """Get or create surrogate key for asset."""
        if asset_id not in self._asset_keys:
            self._asset_keys[asset_id] = self._generate_surrogate_key('asset', asset_id)
        return self._asset_keys[asset_id]

    def _get_exchange_key(self, exchange_id: str) -> str:
        """Get or create surrogate key for exchange."""
        if exchange_id not in self._exchange_keys:
            self._exchange_keys[exchange_id] = self._generate_surrogate_key('exchange', exchange_id)
        return self._exchange_keys[exchange_id]

    def _get_date_key(self, date_str: str) -> str:
        """Get or create surrogate key for date."""
        if date_str not in self._date_keys:
            self._date_keys[date_str] = self._generate_surrogate_key('date', date_str)
        return self._date_keys[date_str]

    def _classify_asset_category(self, asset: Dict) -> str:
        """
        Classify asset into category based on symbol and characteristics.

        Args:
            asset: Asset dictionary

        Returns:
            Category string
        """
        symbol = asset.get('symbol', '').lower()

        if symbol in self.STABLECOINS:
            return 'stablecoin'
        elif symbol in self.DEFI_TOKENS:
            return 'defi'
        elif symbol in ['btc', 'bitcoin']:
            return 'cryptocurrency'
        elif symbol in ['eth', 'ethereum']:
            return 'smart_contract_platform'
        elif asset.get('platform'):
            return 'token'
        else:
            return 'cryptocurrency'

    def build_dim_asset(self, market_data: List[Dict]) -> pd.DataFrame:
        """
        Build asset dimension table.

        Args:
            market_data: List of market data dictionaries from CoinGecko

        Returns:
            DataFrame with dim_asset schema
        """
        logger.info("Building dim_asset...")

        records = []
        seen_assets = set()

        for asset in market_data:
            asset_id = asset.get('id')
            if not asset_id or asset_id in seen_assets:
                continue
            seen_assets.add(asset_id)

            asset_key = self._get_asset_key(asset_id)
            category = self._classify_asset_category(asset)

            records.append({
                'asset_key': asset_key,
                'asset_id': asset_id,
                'symbol': asset.get('symbol', '').upper(),
                'name': asset.get('name'),
                'category': category,
                'market_cap_rank': asset.get('market_cap_rank'),
                'is_active': True,
                'effective_date': datetime.now().date(),
                'is_current': True
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_asset: {len(df)} records")
        return df

    def build_dim_exchange(self, exchanges: List[Dict]) -> pd.DataFrame:
        """
        Build exchange dimension table.

        Args:
            exchanges: List of exchange dictionaries

        Returns:
            DataFrame with dim_exchange schema
        """
        logger.info("Building dim_exchange...")

        records = []
        seen_exchanges = set()

        for exchange in exchanges:
            exchange_id = exchange.get('id')
            if not exchange_id or exchange_id in seen_exchanges:
                continue
            seen_exchanges.add(exchange_id)

            exchange_key = self._get_exchange_key(exchange_id)

            records.append({
                'exchange_key': exchange_key,
                'exchange_id': exchange_id,
                'exchange_name': exchange.get('name'),
                'country': exchange.get('country'),
                'year_established': exchange.get('year_established'),
                'trust_score': exchange.get('trust_score'),
                'trust_score_rank': exchange.get('trust_score_rank'),
                'trade_volume_24h_btc': exchange.get('trade_volume_24h_btc'),
                'url': exchange.get('url'),
                'is_centralized': exchange.get('centralized', True)
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_exchange: {len(df)} records")
        return df

    def build_dim_date(self, ohlcv: List[Dict], market_data: List[Dict]) -> pd.DataFrame:
        """
        Build date dimension from timestamps.

        Args:
            ohlcv: List of OHLCV records with timestamps
            market_data: Market data with last_updated

        Returns:
            DataFrame with dim_date schema
        """
        logger.info("Building dim_date...")

        # Collect all unique dates
        dates = set()

        for record in ohlcv:
            ts = record.get('timestamp')
            if ts:
                dt = datetime.fromtimestamp(ts / 1000)
                dates.add(dt.date())

        # Add today
        dates.add(datetime.now().date())

        records = []
        for date in sorted(dates):
            date_key = self._get_date_key(date.isoformat())

            records.append({
                'date_key': date_key,
                'full_date': date,
                'year': date.year,
                'month': date.month,
                'day': date.day,
                'quarter': (date.month - 1) // 3 + 1,
                'week_of_year': date.isocalendar()[1],
                'day_of_week': date.weekday(),
                'day_name': date.strftime('%A'),
                'is_weekend': date.weekday() >= 5
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_date: {len(df)} records")
        return df

    def build_fact_prices(self, market_data: List[Dict]) -> pd.DataFrame:
        """
        Build price fact table from market data.

        Args:
            market_data: List of market data dictionaries

        Returns:
            DataFrame with fact_prices schema
        """
        logger.info("Building fact_prices...")

        records = []
        today = datetime.now().date()
        date_key = self._get_date_key(today.isoformat())

        for asset in market_data:
            asset_id = asset.get('id')
            if not asset_id:
                continue

            asset_key = self._get_asset_key(asset_id)
            price_key = self._generate_surrogate_key('price', asset_id, str(today))

            records.append({
                'price_key': price_key,
                'asset_key': asset_key,
                'date_key': date_key,
                'asset_id': asset_id,

                # Price metrics
                'price_usd': asset.get('current_price'),
                'price_btc': asset.get('price_btc'),

                # Volume
                'volume_24h': asset.get('total_volume'),

                # Market metrics
                'market_cap': asset.get('market_cap'),
                'market_cap_rank': asset.get('market_cap_rank'),
                'fully_diluted_valuation': asset.get('fully_diluted_valuation'),

                # Supply
                'circulating_supply': asset.get('circulating_supply'),
                'total_supply': asset.get('total_supply'),
                'max_supply': asset.get('max_supply'),

                # Price changes
                'price_change_24h': asset.get('price_change_24h'),
                'price_change_pct_24h': asset.get('price_change_percentage_24h'),
                'price_change_pct_7d': asset.get('price_change_percentage_7d_in_currency'),
                'price_change_pct_30d': asset.get('price_change_percentage_30d_in_currency'),

                # ATH/ATL
                'ath_price': asset.get('ath'),
                'ath_change_pct': asset.get('ath_change_percentage'),
                'atl_price': asset.get('atl'),
                'atl_change_pct': asset.get('atl_change_percentage')
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_prices: {len(df)} records")
        return df

    def build_fact_ohlcv(self, ohlcv: List[Dict]) -> pd.DataFrame:
        """
        Build OHLCV fact table from historical data.

        Args:
            ohlcv: List of OHLCV records

        Returns:
            DataFrame with fact_ohlcv schema
        """
        logger.info("Building fact_ohlcv...")

        records = []

        for record in ohlcv:
            coin_id = record.get('coin_id')
            timestamp = record.get('timestamp')

            if not coin_id or not timestamp:
                continue

            dt = datetime.fromtimestamp(timestamp / 1000)
            date = dt.date()

            asset_key = self._get_asset_key(coin_id)
            date_key = self._get_date_key(date.isoformat())
            ohlcv_key = self._generate_surrogate_key('ohlcv', coin_id, str(timestamp))

            open_price = record.get('open', 0) or 0
            high_price = record.get('high', 0) or 0
            low_price = record.get('low', 0) or 0
            close_price = record.get('close', 0) or 0

            # Calculate metrics
            price_range_pct = None
            if open_price > 0:
                price_range_pct = round((high_price - low_price) / open_price * 100, 4)

            records.append({
                'ohlcv_key': ohlcv_key,
                'asset_key': asset_key,
                'date_key': date_key,
                'open_price': open_price,
                'high_price': high_price,
                'low_price': low_price,
                'close_price': close_price,
                'volume': 0,  # CoinGecko OHLC doesn't include volume
                'price_range_pct': price_range_pct
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_ohlcv: {len(df)} records")
        return df

    def build_fact_global_metrics(self, global_data: Optional[Dict]) -> pd.DataFrame:
        """
        Build global market metrics fact table.

        Args:
            global_data: Global market data from CoinGecko

        Returns:
            DataFrame with fact_global_metrics schema
        """
        logger.info("Building fact_global_metrics...")

        records = []

        if global_data:
            today = datetime.now().date()
            date_key = self._get_date_key(today.isoformat())
            metric_key = self._generate_surrogate_key('global', str(today))

            records.append({
                'metric_key': metric_key,
                'date_key': date_key,
                'snapshot_date': today,
                'total_market_cap': global_data.get('total_market_cap', {}).get('usd'),
                'total_volume_24h': global_data.get('total_volume', {}).get('usd'),
                'active_cryptocurrencies': global_data.get('active_cryptocurrencies'),
                'btc_dominance': global_data.get('market_cap_percentage', {}).get('btc'),
                'eth_dominance': global_data.get('market_cap_percentage', {}).get('eth'),
                'market_cap_change_24h': global_data.get('market_cap_change_percentage_24h_usd')
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_global_metrics: {len(df)} records")
        return df

    def run_transformation(self, extracted_data: Dict) -> Dict[str, pd.DataFrame]:
        """
        Execute full transformation pipeline.

        Args:
            extracted_data: Raw data from extraction

        Returns:
            Dictionary of transformed DataFrames
        """
        self.transformation_log['start_time'] = datetime.now().isoformat()

        logger.info("=" * 60)
        logger.info("STARTING STAR SCHEMA TRANSFORMATION")
        logger.info(f"Input market data: {len(extracted_data.get('market_data', []))}")
        logger.info(f"Input OHLCV: {len(extracted_data.get('ohlcv', []))}")
        logger.info(f"Input exchanges: {len(extracted_data.get('exchanges', []))}")
        logger.info("=" * 60)

        market_data = extracted_data.get('market_data', [])
        ohlcv = extracted_data.get('ohlcv', [])
        exchanges = extracted_data.get('exchanges', [])
        global_data = extracted_data.get('global_metrics')

        tables = {}

        # Build dimensions
        tables['dim_asset'] = self.build_dim_asset(market_data)
        tables['dim_exchange'] = self.build_dim_exchange(exchanges)
        tables['dim_date'] = self.build_dim_date(ohlcv, market_data)

        # Build facts
        tables['fact_prices'] = self.build_fact_prices(market_data)
        tables['fact_ohlcv'] = self.build_fact_ohlcv(ohlcv)
        tables['fact_global_metrics'] = self.build_fact_global_metrics(global_data)

        # Calculate totals
        total_rows = sum(len(df) for df in tables.values())
        self.transformation_log['total_rows'] = total_rows

        for table_name, df in tables.items():
            self.transformation_log['tables_created'][table_name] = len(df)

            # Save to parquet
            output_path = self.output_dir / f"{table_name}.parquet"
            df.to_parquet(output_path, index=False)
            logger.info(f"  Saved: {output_path}")

        self.transformation_log['end_time'] = datetime.now().isoformat()

        # Save transformation log
        log_path = self.output_dir / "transformation_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.transformation_log, f, indent=2)
        logger.info(f"Transformation log saved: {log_path}")

        logger.info("=" * 60)
        logger.info(f"TRANSFORMATION COMPLETE: {total_rows} total rows")
        logger.info("=" * 60)

        return tables


if __name__ == "__main__":
    # Test with sample data
    sample_data = {
        'market_data': [
            {
                'id': 'bitcoin',
                'symbol': 'btc',
                'name': 'Bitcoin',
                'current_price': 50000,
                'market_cap': 1000000000000,
                'market_cap_rank': 1,
                'total_volume': 50000000000
            }
        ],
        'ohlcv': [
            {'coin_id': 'bitcoin', 'timestamp': 1704067200000, 'open': 49000, 'high': 51000, 'low': 48500, 'close': 50000}
        ],
        'exchanges': [
            {'id': 'binance', 'name': 'Binance', 'country': 'Cayman Islands', 'trust_score': 10}
        ],
        'global_metrics': {
            'total_market_cap': {'usd': 2000000000000},
            'market_cap_percentage': {'btc': 50, 'eth': 15}
        }
    }

    transformer = CryptoStarSchemaTransformer(output_dir="test_output")
    tables = transformer.run_transformation(sample_data)

    print("\nTransformation Summary:")
    for name, df in tables.items():
        print(f"  {name}: {len(df)} rows")
