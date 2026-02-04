"""
Enterprise ETL Framework - CoinGecko Extractor
Extracts cryptocurrency data from CoinGecko API.

Data Sources:
- CoinGecko API: Market data, coin details, historical prices

Rate Limits:
- Free tier: 10-30 requests/min (conservative: 10/min)
- Pro tier: 500 requests/min
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import sys

    

from ..core.base_extractor import BaseExtractor, ExtractionResult
from ..config import ENDPOINTS, RATE_LIMITS


class CoinGeckoExtractor(BaseExtractor):
    """
    Extractor for CoinGecko cryptocurrency data.

    Extracts:
    - Coin market data (price, volume, market cap)
    - Coin details (description, links, categories)
    - Historical price data
    - Exchange information

    Example:
        extractor = CoinGeckoExtractor()
        result = extractor.extract_markets(limit=100)
    """

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize CoinGecko extractor.

        Args:
            api_key: Optional CoinGecko Pro API key
            **kwargs: Additional arguments passed to BaseExtractor
        """
        super().__init__(api_key=api_key, **kwargs)

    def get_source_name(self) -> str:
        return 'COINGECKO'

    def get_endpoints(self) -> Dict[str, str]:
        return {
            'markets': ENDPOINTS.get('COINGECKO_MARKETS', 'https://api.coingecko.com/api/v3/coins/markets'),
            'coin_detail': ENDPOINTS.get('COINGECKO_COIN', 'https://api.coingecko.com/api/v3/coins'),
            'market_chart': ENDPOINTS.get('COINGECKO_CHART', 'https://api.coingecko.com/api/v3/coins/{id}/market_chart'),
            'exchanges': ENDPOINTS.get('COINGECKO_EXCHANGES', 'https://api.coingecko.com/api/v3/exchanges'),
            'categories': ENDPOINTS.get('COINGECKO_CATEGORIES', 'https://api.coingecko.com/api/v3/coins/categories'),
        }

    def get_rate_limit(self) -> int:
        return RATE_LIMITS.get('COINGECKO', 10)

    def extract(
        self,
        limit: Optional[int] = 250,
        include_details: bool = False,
        include_history: bool = False,
        history_days: int = 30,
        vs_currency: str = 'usd',
    ) -> ExtractionResult:
        """
        Extract cryptocurrency market data from CoinGecko.

        Args:
            limit: Maximum number of coins to extract
            include_details: Fetch detailed coin information
            include_history: Fetch historical price data
            history_days: Days of history to fetch (if include_history)
            vs_currency: Quote currency (usd, eur, btc, etc.)

        Returns:
            ExtractionResult with coins data
        """
        result = ExtractionResult(success=False, source=self.get_source_name())
        coins_data = []

        try:
            self.logger.info(f"Starting CoinGecko extraction (limit={limit}, details={include_details})")

            # Step 1: Get market data (paginated, 250 max per page)
            coins = self._get_markets(limit=limit, vs_currency=vs_currency)

            if not coins:
                result.error = "Failed to fetch market data"
                return result

            self.logger.info(f"Fetched {len(coins)} coins from markets endpoint")

            # Step 2: Enrich each coin
            for i, coin in enumerate(coins):
                if i > 0 and i % 50 == 0:
                    self.logger.info(f"Progress: {i}/{len(coins)} coins processed")

                coin_data = self._parse_market_data(coin)
                coin_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'

                # Get detailed info (slower, more rate-limited)
                if include_details:
                    details = self._get_coin_details(coin['id'])
                    if details:
                        coin_data.update(self._parse_coin_details(details))
                    time.sleep(0.5)  # Respect rate limits

                # Get historical data
                if include_history:
                    history = self._get_market_chart(coin['id'], days=history_days, vs_currency=vs_currency)
                    if history:
                        coin_data['price_history'] = history.get('prices', [])
                        coin_data['volume_history'] = history.get('total_volumes', [])
                    time.sleep(0.5)

                coins_data.append(coin_data)
                result.records_extracted += 1

            result.success = True
            result.data = coins_data
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.metadata = {
                'coins_extracted': len(coins_data),
                'vs_currency': vs_currency,
                'include_details': include_details,
                'include_history': include_history,
                'api_calls': self._api_calls,
            }

            self.logger.info(f"Extraction complete: {len(coins_data)} coins")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result

    def _get_markets(
        self,
        limit: int = 250,
        vs_currency: str = 'usd',
    ) -> List[Dict]:
        """Fetch coin market data with pagination."""
        all_coins = []
        page = 1
        per_page = min(250, limit)

        while len(all_coins) < limit:
            response = self._get(
                self.get_endpoints()['markets'],
                params={
                    'vs_currency': vs_currency,
                    'order': 'market_cap_desc',
                    'per_page': per_page,
                    'page': page,
                    'sparkline': 'false',
                    'price_change_percentage': '1h,24h,7d,30d',
                }
            )

            if not response:
                break

            all_coins.extend(response)
            
            if len(response) < per_page:
                break  # No more data
                
            page += 1
            time.sleep(0.3)  # Rate limit between pages

        return all_coins[:limit]

    def _parse_market_data(self, coin: Dict) -> Dict:
        """Parse market data into flat structure."""
        return {
            'coin_id': coin.get('id', ''),
            'symbol': coin.get('symbol', '').upper(),
            'name': coin.get('name', ''),
            'image_url': coin.get('image', ''),
            'market_cap_rank': coin.get('market_cap_rank'),
            
            # Price metrics
            'current_price': coin.get('current_price'),
            'market_cap': coin.get('market_cap'),
            'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
            'total_volume': coin.get('total_volume'),
            
            # Supply metrics
            'circulating_supply': coin.get('circulating_supply'),
            'total_supply': coin.get('total_supply'),
            'max_supply': coin.get('max_supply'),
            
            # Price changes
            'price_change_24h': coin.get('price_change_24h'),
            'price_change_pct_24h': coin.get('price_change_percentage_24h'),
            'price_change_pct_1h': coin.get('price_change_percentage_1h_in_currency'),
            'price_change_pct_7d': coin.get('price_change_percentage_7d_in_currency'),
            'price_change_pct_30d': coin.get('price_change_percentage_30d_in_currency'),
            
            # ATH/ATL
            'ath': coin.get('ath'),
            'ath_change_pct': coin.get('ath_change_percentage'),
            'ath_date': coin.get('ath_date'),
            'atl': coin.get('atl'),
            'atl_change_pct': coin.get('atl_change_percentage'),
            'atl_date': coin.get('atl_date'),
            
            # Timestamps
            'last_updated': coin.get('last_updated'),
        }

    def _get_coin_details(self, coin_id: str) -> Optional[Dict]:
        """Fetch detailed information for a single coin."""
        response = self._get(
            f"{self.get_endpoints()['coin_detail']}/{coin_id}",
            params={
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'true',
                'community_data': 'true',
                'developer_data': 'true',
            }
        )
        return response

    def _parse_coin_details(self, details: Dict) -> Dict:
        """Parse detailed coin info into flat structure."""
        result = {}
        
        # Categories
        categories = details.get('categories', [])
        result['categories'] = ','.join([c for c in categories if c])
        
        # Description
        description = details.get('description', {})
        result['description'] = (description.get('en', '') or '')[:1000]
        
        # Genesis date
        result['genesis_date'] = details.get('genesis_date')
        
        # Sentiment
        result['sentiment_up_pct'] = details.get('sentiment_votes_up_percentage')
        result['sentiment_down_pct'] = details.get('sentiment_votes_down_percentage')
        
        # Community data
        community = details.get('community_data', {})
        result['twitter_followers'] = community.get('twitter_followers')
        result['reddit_subscribers'] = community.get('reddit_subscribers')
        result['telegram_users'] = community.get('telegram_channel_user_count')
        
        # Developer data
        developer = details.get('developer_data', {})
        result['github_forks'] = developer.get('forks')
        result['github_stars'] = developer.get('stars')
        result['github_subscribers'] = developer.get('subscribers')
        result['github_total_issues'] = developer.get('total_issues')
        result['github_closed_issues'] = developer.get('closed_issues')
        result['github_pull_requests_merged'] = developer.get('pull_requests_merged')
        result['github_commit_count_4_weeks'] = developer.get('commit_count_4_weeks')
        
        # Links
        links = details.get('links', {})
        homepage = links.get('homepage', [])
        result['website'] = homepage[0] if homepage else ''
        
        repos = links.get('repos_url', {})
        github = repos.get('github', [])
        result['github_url'] = github[0] if github else ''
        
        return result

    def _get_market_chart(
        self,
        coin_id: str,
        days: int = 30,
        vs_currency: str = 'usd',
    ) -> Optional[Dict]:
        """Fetch historical price data for a coin."""
        response = self._get(
            self.get_endpoints()['market_chart'].format(id=coin_id),
            params={
                'vs_currency': vs_currency,
                'days': days,
            }
        )
        return response

    def extract_top_coins(
        self,
        count: int = 100,
        vs_currency: str = 'usd',
    ) -> ExtractionResult:
        """
        Extract top coins by market cap (optimized path).

        Args:
            count: Number of top coins
            vs_currency: Quote currency

        Returns:
            ExtractionResult with top coins
        """
        result = ExtractionResult(success=False, source=self.get_source_name())

        try:
            self.logger.info(f"Extracting top {count} coins by market cap")

            coins = self._get_markets(limit=count, vs_currency=vs_currency)

            if not coins:
                result.error = "Failed to fetch market data"
                return result

            coins_data = []
            for coin in coins:
                coin_data = self._parse_market_data(coin)
                coin_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                coins_data.append(coin_data)

            result.success = True
            result.data = coins_data
            result.records_extracted = len(coins_data)
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.metadata = {
                'vs_currency': vs_currency,
                'total_fetched': len(coins_data),
            }

            self.logger.info(f"Extracted {len(coins_data)} top coins")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result

    def extract_exchanges(self, limit: int = 100) -> ExtractionResult:
        """
        Extract exchange data from CoinGecko.

        Args:
            limit: Maximum exchanges to fetch

        Returns:
            ExtractionResult with exchanges data
        """
        result = ExtractionResult(success=False, source=self.get_source_name())

        try:
            self.logger.info(f"Extracting top {limit} exchanges")

            response = self._get(
                self.get_endpoints()['exchanges'],
                params={'per_page': min(250, limit), 'page': 1}
            )

            if not response:
                result.error = "Failed to fetch exchanges"
                return result

            exchanges_data = []
            for ex in response[:limit]:
                exchange = {
                    'exchange_id': ex.get('id', ''),
                    'name': ex.get('name', ''),
                    'year_established': ex.get('year_established'),
                    'country': ex.get('country'),
                    'trust_score': ex.get('trust_score'),
                    'trust_score_rank': ex.get('trust_score_rank'),
                    'trade_volume_24h_btc': ex.get('trade_volume_24h_btc'),
                    'trade_volume_24h_btc_normalized': ex.get('trade_volume_24h_btc_normalized'),
                    'url': ex.get('url', ''),
                    'image_url': ex.get('image', ''),
                    'has_trading_incentive': ex.get('has_trading_incentive', False),
                    'extracted_at': datetime.utcnow().isoformat() + 'Z',
                }
                exchanges_data.append(exchange)

            result.success = True
            result.data = exchanges_data
            result.records_extracted = len(exchanges_data)
            result.completed_at = datetime.utcnow().isoformat() + 'Z'

            self.logger.info(f"Extracted {len(exchanges_data)} exchanges")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result
