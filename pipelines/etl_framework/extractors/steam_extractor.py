"""
Enterprise ETL Framework - Steam & SteamSpy Extractor
Extracts gaming data from Steam Web API and SteamSpy API.

Data Sources:
- Steam Web API: App list, player counts, app details
- SteamSpy API: Sales estimates, player statistics, ownership data

Rate Limits:
- Steam: ~200 requests/5min (conservative)
- SteamSpy: 4 requests/min (1 per 15 sec)
"""

import time
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any, Generator

import sys

    

from ..core.base_extractor import BaseExtractor, ExtractionResult
from ..config import ENDPOINTS, RATE_LIMITS


class SteamExtractor(BaseExtractor):
    """
    Extractor for Steam and SteamSpy gaming data.

    Extracts:
    - Game catalog (app list)
    - Game details (name, developer, publisher, genres, tags)
    - Player counts (current players)
    - SteamSpy metrics (owners, players, playtime, reviews)

    Example:
        extractor = SteamExtractor()
        result = extractor.extract(limit=1000, include_steamspy=True)
    """

    # SteamSpy requires slower rate limiting
    STEAMSPY_DELAY = 1.5  # seconds between SteamSpy calls

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Steam extractor.

        Args:
            api_key: Optional Steam Web API key (most endpoints don't require it)
            **kwargs: Additional arguments passed to BaseExtractor
        """
        super().__init__(api_key=api_key, **kwargs)
        self._steamspy_last_call = 0

    def get_source_name(self) -> str:
        return 'STEAM'

    def get_endpoints(self) -> Dict[str, str]:
        return {
            'app_list': ENDPOINTS.get('STEAM_APP_LIST', 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'),
            'app_details': ENDPOINTS.get('STEAM_APP_DETAILS', 'https://store.steampowered.com/api/appdetails'),
            'player_count': ENDPOINTS.get('STEAM_PLAYER_COUNT', 'https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/'),
            'steamspy_all': ENDPOINTS.get('STEAMSPY_ALL', 'https://steamspy.com/api.php?request=all'),
            'steamspy_app': ENDPOINTS.get('STEAMSPY_APP', 'https://steamspy.com/api.php?request=appdetails'),
        }

    def get_rate_limit(self) -> int:
        return RATE_LIMITS.get('STEAM', 200)

    def extract(
        self,
        limit: Optional[int] = None,
        app_ids: Optional[List[int]] = None,
        include_details: bool = True,
        include_player_count: bool = True,
        include_steamspy: bool = True,
        min_owners: int = 10000,
    ) -> ExtractionResult:
        """
        Extract gaming data from Steam and SteamSpy.

        Args:
            limit: Maximum number of games to extract (None = all)
            app_ids: Specific app IDs to extract (overrides limit)
            include_details: Fetch detailed app information
            include_player_count: Fetch current player counts
            include_steamspy: Fetch SteamSpy statistics
            min_owners: Minimum owners filter for SteamSpy data

        Returns:
            ExtractionResult with games data
        """
        result = ExtractionResult(success=False, source=self.get_source_name())
        games_data = []

        try:
            self.logger.info(f"Starting Steam extraction (limit={limit}, steamspy={include_steamspy})")

            # Step 1: Get app list or use provided IDs
            if app_ids:
                target_apps = [{'appid': aid, 'name': ''} for aid in app_ids]
            else:
                target_apps = self._get_app_list(limit)

            if not target_apps:
                result.error = "Failed to fetch app list"
                return result

            self.logger.info(f"Processing {len(target_apps)} apps")

            # Step 2: Get SteamSpy data (bulk endpoint is more efficient)
            steamspy_data = {}
            if include_steamspy:
                steamspy_data = self._get_steamspy_bulk(min_owners=min_owners)
                self.logger.info(f"Fetched SteamSpy data for {len(steamspy_data)} games")

            # Step 3: Enrich each app
            for i, app in enumerate(target_apps):
                app_id = app['appid']

                if i > 0 and i % 100 == 0:
                    self.logger.info(f"Progress: {i}/{len(target_apps)} apps processed")

                game = {
                    'app_id': app_id,
                    'name': app.get('name', ''),
                    'extracted_at': datetime.utcnow().isoformat() + 'Z',
                }

                # Get Steam store details
                if include_details:
                    details = self._get_app_details(app_id)
                    if details:
                        game.update(self._parse_app_details(details))

                # Get current player count
                if include_player_count:
                    player_count = self._get_player_count(app_id)
                    game['current_players'] = player_count

                # Merge SteamSpy data
                if include_steamspy and str(app_id) in steamspy_data:
                    spy_data = steamspy_data[str(app_id)]
                    game.update(self._parse_steamspy_data(spy_data))

                # Only include games with meaningful data
                if game.get('name') or game.get('owners'):
                    games_data.append(game)
                    result.records_extracted += 1
                else:
                    result.records_failed += 1

                # Respect rate limits
                if include_details or include_player_count:
                    time.sleep(0.3)  # 300ms between Steam API calls

            result.success = True
            result.data = games_data
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.metadata = {
                'games_extracted': len(games_data),
                'include_details': include_details,
                'include_player_count': include_player_count,
                'include_steamspy': include_steamspy,
                'api_calls': self._api_calls,
            }

            self.logger.info(f"Extraction complete: {len(games_data)} games")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result

    def _get_app_list(self, limit: Optional[int] = None) -> List[Dict]:
        """Fetch the complete Steam app list."""
        self.logger.info("Fetching Steam app list")

        response = self._get(self.get_endpoints()['app_list'])
        if not response:
            return []

        apps = response.get('applist', {}).get('apps', [])

        # Filter out non-games (DLC, soundtracks, etc. often have empty names)
        apps = [a for a in apps if a.get('name')]

        if limit:
            apps = apps[:limit]

        return apps

    def _get_app_details(self, app_id: int) -> Optional[Dict]:
        """Fetch detailed information for a single app."""
        response = self._get(
            self.get_endpoints()['app_details'],
            params={'appids': app_id, 'cc': 'us', 'l': 'english'}
        )

        if not response:
            return None

        app_data = response.get(str(app_id), {})
        if not app_data.get('success'):
            return None

        return app_data.get('data', {})

    def _parse_app_details(self, details: Dict) -> Dict:
        """Parse Steam store details into flat structure."""
        result = {
            'name': details.get('name', ''),
            'type': details.get('type', ''),
            'is_free': details.get('is_free', False),
            'short_description': details.get('short_description', '')[:500],
            'header_image': details.get('header_image', ''),
            'website': details.get('website', ''),
            'required_age': details.get('required_age', 0),
        }

        # Developers and publishers
        developers = details.get('developers', [])
        publishers = details.get('publishers', [])
        result['developer'] = developers[0] if developers else ''
        result['publisher'] = publishers[0] if publishers else ''
        result['developers'] = ','.join(developers)
        result['publishers'] = ','.join(publishers)

        # Genres
        genres = details.get('genres', [])
        result['genres'] = ','.join([g.get('description', '') for g in genres])
        result['genre_ids'] = ','.join([str(g.get('id', '')) for g in genres])

        # Categories (features like multiplayer, controller support)
        categories = details.get('categories', [])
        result['categories'] = ','.join([c.get('description', '') for c in categories])

        # Platforms
        platforms = details.get('platforms', {})
        result['platform_windows'] = platforms.get('windows', False)
        result['platform_mac'] = platforms.get('mac', False)
        result['platform_linux'] = platforms.get('linux', False)

        # Pricing
        price_overview = details.get('price_overview', {})
        result['price_cents'] = price_overview.get('initial', 0)
        result['price_final_cents'] = price_overview.get('final', 0)
        result['discount_percent'] = price_overview.get('discount_percent', 0)
        result['currency'] = price_overview.get('currency', 'USD')

        # Release date
        release_date = details.get('release_date', {})
        result['release_date'] = release_date.get('date', '')
        result['coming_soon'] = release_date.get('coming_soon', False)

        # Metacritic
        metacritic = details.get('metacritic', {})
        result['metacritic_score'] = metacritic.get('score')

        # Recommendations (reviews)
        recommendations = details.get('recommendations', {})
        result['total_reviews'] = recommendations.get('total', 0)

        # Achievements
        achievements = details.get('achievements', {})
        result['achievement_count'] = achievements.get('total', 0)

        return result

    def _get_player_count(self, app_id: int) -> Optional[int]:
        """Fetch current player count for an app."""
        response = self._get(
            self.get_endpoints()['player_count'],
            params={'appid': app_id}
        )

        if not response:
            return None

        return response.get('response', {}).get('player_count')

    def _get_steamspy_bulk(self, min_owners: int = 10000) -> Dict[str, Dict]:
        """Fetch bulk SteamSpy data for all games."""
        self._respect_steamspy_limit()

        response = self._get(
            self.get_endpoints()['steamspy_all'],
            cache_key=f'steamspy_all_{min_owners}'
        )

        if not response:
            return {}

        # Filter by minimum owners
        filtered = {}
        for app_id, data in response.items():
            owners_str = data.get('owners', '0 .. 0')
            try:
                # Parse "X .. Y" format
                min_own = int(owners_str.split('..')[0].strip().replace(',', ''))
                if min_own >= min_owners:
                    filtered[app_id] = data
            except (ValueError, IndexError):
                pass

        return filtered

    def _get_steamspy_app(self, app_id: int) -> Optional[Dict]:
        """Fetch SteamSpy data for a single app."""
        self._respect_steamspy_limit()

        response = self._get(
            self.get_endpoints()['steamspy_app'],
            params={'appid': app_id}
        )

        return response

    def _parse_steamspy_data(self, spy_data: Dict) -> Dict:
        """Parse SteamSpy data into flat structure."""
        result = {}

        # Owners range (e.g., "1,000,000 .. 2,000,000")
        owners_str = spy_data.get('owners', '0 .. 0')
        try:
            parts = owners_str.split('..')
            result['owners_min'] = int(parts[0].strip().replace(',', ''))
            result['owners_max'] = int(parts[1].strip().replace(',', ''))
            result['owners_estimate'] = (result['owners_min'] + result['owners_max']) // 2
        except (ValueError, IndexError):
            result['owners_min'] = 0
            result['owners_max'] = 0
            result['owners_estimate'] = 0

        # Player counts
        result['players_forever'] = spy_data.get('players_forever', 0)
        result['players_2weeks'] = spy_data.get('players_2weeks', 0)

        # Playtime (in minutes)
        result['average_playtime_forever'] = spy_data.get('average_forever', 0)
        result['average_playtime_2weeks'] = spy_data.get('average_2weeks', 0)
        result['median_playtime_forever'] = spy_data.get('median_forever', 0)
        result['median_playtime_2weeks'] = spy_data.get('median_2weeks', 0)

        # Reviews
        result['positive_reviews'] = spy_data.get('positive', 0)
        result['negative_reviews'] = spy_data.get('negative', 0)
        total_reviews = result['positive_reviews'] + result['negative_reviews']
        if total_reviews > 0:
            result['review_score'] = round(result['positive_reviews'] / total_reviews * 100, 2)
        else:
            result['review_score'] = None

        # CCU (concurrent users) peak
        result['ccu'] = spy_data.get('ccu', 0)

        # Price (in cents)
        result['price_steamspy'] = spy_data.get('price', 0)

        # Tags
        tags = spy_data.get('tags', {})
        if isinstance(tags, dict):
            result['tags'] = ','.join(list(tags.keys())[:20])  # Top 20 tags
        else:
            result['tags'] = ''

        return result

    def _respect_steamspy_limit(self):
        """Ensure we don't exceed SteamSpy rate limit."""
        elapsed = time.time() - self._steamspy_last_call
        if elapsed < self.STEAMSPY_DELAY:
            time.sleep(self.STEAMSPY_DELAY - elapsed)
        self._steamspy_last_call = time.time()

    def extract_top_games(
        self,
        count: int = 1000,
        sort_by: str = 'owners',
    ) -> ExtractionResult:
        """
        Extract top games by a metric (optimized path using SteamSpy bulk).

        Args:
            count: Number of top games to extract
            sort_by: Metric to sort by ('owners', 'players_2weeks', 'ccu')

        Returns:
            ExtractionResult with top games
        """
        result = ExtractionResult(success=False, source=self.get_source_name())

        try:
            self.logger.info(f"Extracting top {count} games by {sort_by}")

            # Get bulk SteamSpy data
            steamspy_data = self._get_steamspy_bulk(min_owners=0)

            if not steamspy_data:
                result.error = "Failed to fetch SteamSpy data"
                return result

            # Parse and sort
            games = []
            for app_id, data in steamspy_data.items():
                game = {
                    'app_id': int(app_id),
                    'name': data.get('name', ''),
                    **self._parse_steamspy_data(data),
                    'extracted_at': datetime.utcnow().isoformat() + 'Z',
                }
                games.append(game)

            # Sort by metric
            sort_key = {
                'owners': 'owners_estimate',
                'players_2weeks': 'players_2weeks',
                'ccu': 'ccu',
                'playtime': 'average_playtime_forever',
            }.get(sort_by, 'owners_estimate')

            games.sort(key=lambda x: x.get(sort_key, 0) or 0, reverse=True)
            games = games[:count]

            result.success = True
            result.data = games
            result.records_extracted = len(games)
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.metadata = {
                'sort_by': sort_by,
                'total_in_steamspy': len(steamspy_data),
            }

            self.logger.info(f"Extracted {len(games)} top games")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result
