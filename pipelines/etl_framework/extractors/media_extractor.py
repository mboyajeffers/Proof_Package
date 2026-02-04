"""
Enterprise ETL Framework - Media Extractor
Extracts movie and TV data from TMDb (The Movie Database) API.

Data Sources:
- TMDb API: Movies, TV shows, trending content, genres

Rate Limits:
- TMDb: ~40 requests/10sec (conservative: 10/min without key)

Note: TMDb API key required for full functionality.
Get free key at: https://www.themoviedb.org/settings/api
"""

import time
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any

import sys

    

from ..core.base_extractor import BaseExtractor, ExtractionResult
from ..config import ENDPOINTS, RATE_LIMITS


class MediaExtractor(BaseExtractor):
    """
    Extractor for TMDb movie and TV data.

    Extracts:
    - Popular movies and TV shows
    - Trending content
    - Movie/TV details (cast, crew, ratings)
    - Genre information

    Example:
        extractor = MediaExtractor(api_key='your_tmdb_key')
        result = extractor.extract_popular_movies(limit=100)
    """

    BASE_URL = 'https://api.themoviedb.org/3'

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Media extractor.

        Args:
            api_key: TMDb API key (get free at themoviedb.org)
            **kwargs: Additional arguments passed to BaseExtractor
        """
        super().__init__(api_key=api_key, **kwargs)
        # Try to get API key from config if not provided
        if not self.api_key:
            from ..config import API_KEYS
            self.api_key = API_KEYS.get('TMDB', '')

    def get_source_name(self) -> str:
        return 'TMDB'

    def get_endpoints(self) -> Dict[str, str]:
        return {
            'popular_movies': f'{self.BASE_URL}/movie/popular',
            'popular_tv': f'{self.BASE_URL}/tv/popular',
            'top_rated_movies': f'{self.BASE_URL}/movie/top_rated',
            'top_rated_tv': f'{self.BASE_URL}/tv/top_rated',
            'trending': f'{self.BASE_URL}/trending/{{media_type}}/{{time_window}}',
            'movie_details': f'{self.BASE_URL}/movie/{{id}}',
            'tv_details': f'{self.BASE_URL}/tv/{{id}}',
            'genres_movie': f'{self.BASE_URL}/genre/movie/list',
            'genres_tv': f'{self.BASE_URL}/genre/tv/list',
            'discover_movies': f'{self.BASE_URL}/discover/movie',
            'discover_tv': f'{self.BASE_URL}/discover/tv',
        }

    def get_rate_limit(self) -> int:
        return RATE_LIMITS.get('TMDB', 10)

    def _get_with_key(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make request with API key."""
        params = params or {}
        if self.api_key:
            params['api_key'] = self.api_key
        return self._get(url, params=params)

    def extract(
        self,
        include_movies: bool = True,
        include_tv: bool = True,
        include_trending: bool = True,
        movies_limit: int = 100,
        tv_limit: int = 100,
    ) -> ExtractionResult:
        """
        Extract media data from TMDb.

        Args:
            include_movies: Fetch popular movies
            include_tv: Fetch popular TV shows
            include_trending: Fetch trending content
            movies_limit: Max movies to fetch
            tv_limit: Max TV shows to fetch

        Returns:
            ExtractionResult with media data
        """
        result = ExtractionResult(success=False, source=self.get_source_name())
        all_data = []

        try:
            self.logger.info(f"Starting TMDb extraction (movies={include_movies}, tv={include_tv})")

            # Get genres first for reference
            genres = self._get_all_genres()

            # Extract movies
            if include_movies:
                movies = self._get_popular_movies(limit=movies_limit)
                for movie in movies:
                    movie_data = self._parse_movie(movie, genres)
                    movie_data['media_type'] = 'movie'
                    movie_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                    all_data.append(movie_data)
                    result.records_extracted += 1

            # Extract TV shows
            if include_tv:
                tv_shows = self._get_popular_tv(limit=tv_limit)
                for show in tv_shows:
                    show_data = self._parse_tv_show(show, genres)
                    show_data['media_type'] = 'tv'
                    show_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                    all_data.append(show_data)
                    result.records_extracted += 1

            # Extract trending
            if include_trending:
                trending = self._get_trending(media_type='all', time_window='week')
                for item in trending[:50]:  # Top 50 trending
                    if item.get('media_type') == 'movie':
                        item_data = self._parse_movie(item, genres)
                    else:
                        item_data = self._parse_tv_show(item, genres)
                    item_data['media_type'] = item.get('media_type', 'unknown')
                    item_data['is_trending'] = True
                    item_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                    # Avoid duplicates
                    if not any(d.get('tmdb_id') == item_data.get('tmdb_id') for d in all_data):
                        all_data.append(item_data)
                        result.records_extracted += 1

            result.success = True
            result.data = all_data
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.metadata = {
                'total_items': len(all_data),
                'movies': sum(1 for d in all_data if d.get('media_type') == 'movie'),
                'tv_shows': sum(1 for d in all_data if d.get('media_type') == 'tv'),
                'api_calls': self._api_calls,
            }

            self.logger.info(f"Extraction complete: {len(all_data)} items")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result

    def _get_all_genres(self) -> Dict[int, str]:
        """Get all genres for movies and TV."""
        genres = {}

        # Movie genres
        response = self._get_with_key(self.get_endpoints()['genres_movie'])
        if response:
            for g in response.get('genres', []):
                genres[g['id']] = g['name']

        # TV genres
        response = self._get_with_key(self.get_endpoints()['genres_tv'])
        if response:
            for g in response.get('genres', []):
                genres[g['id']] = g['name']

        return genres

    def _get_popular_movies(self, limit: int = 100) -> List[Dict]:
        """Fetch popular movies with pagination."""
        all_movies = []
        page = 1
        per_page = 20  # TMDb returns 20 per page

        while len(all_movies) < limit:
            response = self._get_with_key(
                self.get_endpoints()['popular_movies'],
                params={'page': page, 'language': 'en-US'}
            )

            if not response:
                break

            movies = response.get('results', [])
            all_movies.extend(movies)

            if page >= response.get('total_pages', 1):
                break

            page += 1
            time.sleep(0.3)

        return all_movies[:limit]

    def _get_popular_tv(self, limit: int = 100) -> List[Dict]:
        """Fetch popular TV shows with pagination."""
        all_shows = []
        page = 1

        while len(all_shows) < limit:
            response = self._get_with_key(
                self.get_endpoints()['popular_tv'],
                params={'page': page, 'language': 'en-US'}
            )

            if not response:
                break

            shows = response.get('results', [])
            all_shows.extend(shows)

            if page >= response.get('total_pages', 1):
                break

            page += 1
            time.sleep(0.3)

        return all_shows[:limit]

    def _get_trending(self, media_type: str = 'all', time_window: str = 'week') -> List[Dict]:
        """Fetch trending content."""
        url = self.get_endpoints()['trending'].format(
            media_type=media_type,
            time_window=time_window
        )
        response = self._get_with_key(url)
        return response.get('results', []) if response else []

    def _parse_movie(self, movie: Dict, genres: Dict[int, str]) -> Dict:
        """Parse movie data into flat structure."""
        genre_ids = movie.get('genre_ids', [])
        genre_names = [genres.get(gid, '') for gid in genre_ids if gid in genres]

        return {
            'tmdb_id': movie.get('id'),
            'title': movie.get('title', ''),
            'original_title': movie.get('original_title', ''),
            'original_language': movie.get('original_language', ''),
            'overview': (movie.get('overview', '') or '')[:1000],
            'release_date': movie.get('release_date', ''),
            'popularity': movie.get('popularity', 0),
            'vote_average': movie.get('vote_average', 0),
            'vote_count': movie.get('vote_count', 0),
            'adult': movie.get('adult', False),
            'video': movie.get('video', False),
            'poster_path': movie.get('poster_path', ''),
            'backdrop_path': movie.get('backdrop_path', ''),
            'genre_ids': ','.join(str(g) for g in genre_ids),
            'genres': ','.join(genre_names),
            'is_trending': False,
        }

    def _parse_tv_show(self, show: Dict, genres: Dict[int, str]) -> Dict:
        """Parse TV show data into flat structure."""
        genre_ids = show.get('genre_ids', [])
        genre_names = [genres.get(gid, '') for gid in genre_ids if gid in genres]

        return {
            'tmdb_id': show.get('id'),
            'title': show.get('name', ''),
            'original_title': show.get('original_name', ''),
            'original_language': show.get('original_language', ''),
            'overview': (show.get('overview', '') or '')[:1000],
            'release_date': show.get('first_air_date', ''),
            'popularity': show.get('popularity', 0),
            'vote_average': show.get('vote_average', 0),
            'vote_count': show.get('vote_count', 0),
            'adult': show.get('adult', False),
            'video': False,
            'poster_path': show.get('poster_path', ''),
            'backdrop_path': show.get('backdrop_path', ''),
            'genre_ids': ','.join(str(g) for g in genre_ids),
            'genres': ','.join(genre_names),
            'origin_country': ','.join(show.get('origin_country', [])),
            'is_trending': False,
        }

    def extract_top_rated(self, media_type: str = 'movie', limit: int = 100) -> ExtractionResult:
        """
        Extract top-rated content.

        Args:
            media_type: 'movie' or 'tv'
            limit: Max items to fetch

        Returns:
            ExtractionResult with top-rated content
        """
        result = ExtractionResult(success=False, source=self.get_source_name())

        try:
            self.logger.info(f"Extracting top {limit} {media_type}s")

            genres = self._get_all_genres()
            all_items = []
            page = 1

            endpoint = 'top_rated_movies' if media_type == 'movie' else 'top_rated_tv'

            while len(all_items) < limit:
                response = self._get_with_key(
                    self.get_endpoints()[endpoint],
                    params={'page': page, 'language': 'en-US'}
                )

                if not response:
                    break

                items = response.get('results', [])
                for item in items:
                    if media_type == 'movie':
                        item_data = self._parse_movie(item, genres)
                    else:
                        item_data = self._parse_tv_show(item, genres)
                    item_data['media_type'] = media_type
                    item_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                    all_items.append(item_data)

                if page >= response.get('total_pages', 1):
                    break

                page += 1
                time.sleep(0.3)

            all_items = all_items[:limit]

            result.success = True
            result.data = all_items
            result.records_extracted = len(all_items)
            result.completed_at = datetime.utcnow().isoformat() + 'Z'

            self.logger.info(f"Extracted {len(all_items)} top-rated {media_type}s")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result

    def extract_discover(
        self,
        media_type: str = 'movie',
        year: Optional[int] = None,
        min_vote_average: float = 7.0,
        limit: int = 100,
    ) -> ExtractionResult:
        """
        Discover content with filters.

        Args:
            media_type: 'movie' or 'tv'
            year: Filter by release year
            min_vote_average: Minimum rating
            limit: Max items to fetch

        Returns:
            ExtractionResult with discovered content
        """
        result = ExtractionResult(success=False, source=self.get_source_name())

        try:
            self.logger.info(f"Discovering {media_type}s (year={year}, min_rating={min_vote_average})")

            genres = self._get_all_genres()
            all_items = []
            page = 1

            endpoint = 'discover_movies' if media_type == 'movie' else 'discover_tv'

            params = {
                'language': 'en-US',
                'sort_by': 'popularity.desc',
                'vote_average.gte': min_vote_average,
                'vote_count.gte': 100,
            }

            if year:
                if media_type == 'movie':
                    params['primary_release_year'] = year
                else:
                    params['first_air_date_year'] = year

            while len(all_items) < limit:
                params['page'] = page
                response = self._get_with_key(self.get_endpoints()[endpoint], params=params)

                if not response:
                    break

                items = response.get('results', [])
                for item in items:
                    if media_type == 'movie':
                        item_data = self._parse_movie(item, genres)
                    else:
                        item_data = self._parse_tv_show(item, genres)
                    item_data['media_type'] = media_type
                    item_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                    all_items.append(item_data)

                if page >= response.get('total_pages', 1):
                    break

                page += 1
                time.sleep(0.3)

            all_items = all_items[:limit]

            result.success = True
            result.data = all_items
            result.records_extracted = len(all_items)
            result.completed_at = datetime.utcnow().isoformat() + 'Z'

            self.logger.info(f"Discovered {len(all_items)} {media_type}s")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result
