"""
Enterprise ETL Framework - Base Extractor
Parent class for all ETL data extractors (Steam, ESPN, IMDB, CoinGecko, etc.)

This module provides the foundation for extracting data from public APIs
and bulk data sources, with built-in rate limiting, caching, and error handling.
"""

import logging
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Any, Generator
import json

import requests

# Import existing infrastructure
import sys

    

from integrations.rate_limiter import RateLimiter
from integrations.cache import DataCache


# =============================================================================
# Extraction Result
# =============================================================================

@dataclass
class ExtractionResult:
    """Result of an extraction operation."""
    success: bool
    source: str
    records_extracted: int = 0
    records_failed: int = 0
    api_calls_made: int = 0
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    completed_at: Optional[str] = None
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    data: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "source": self.source,
            "records_extracted": self.records_extracted,
            "records_failed": self.records_failed,
            "api_calls_made": self.api_calls_made,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "warnings": self.warnings,
            "metadata": self.metadata,
        }

    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self.warnings.append(warning)


# =============================================================================
# Base Extractor
# =============================================================================

class BaseExtractor(ABC):
    """
    Abstract base class for all ETL extractors.

    Provides common infrastructure for:
    - Rate limiting (token bucket)
    - Response caching
    - Retry logic with exponential backoff
    - Pagination handling
    - Progress tracking
    - Error handling

    Subclasses must implement:
    - extract(): Main extraction logic
    - get_source_name(): Return the data source identifier
    - get_endpoints(): Return API endpoints used
    """

    # Class-level shared resources (thread-safe singletons)
    _rate_limiter: Optional[RateLimiter] = None
    _cache: Optional[DataCache] = None
    _lock = threading.Lock()

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        use_cache: bool = True,
        cache_ttl: int = 3600,
    ):
        """
        Initialize the extractor.

        Args:
            api_key: API key for authenticated endpoints (if required)
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for failed requests
            use_cache: Whether to cache API responses
            cache_ttl: Cache time-to-live in seconds
        """
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.use_cache = use_cache
        self.cache_ttl = cache_ttl

        # Initialize shared resources
        self._init_shared_resources()

        # Configure rate limiter for this source
        self._configure_rate_limit()

        # HTTP session with connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataEngineering-Portfolio/1.0',
            'Accept': 'application/json',
        })

        # Setup logging
        self.logger = self._setup_logger()

        # Tracking
        self._api_calls = 0
        self._cache_hits = 0

        self.logger.info(f"Initialized {self.get_source_name()} extractor")

    @classmethod
    def _init_shared_resources(cls):
        """Initialize shared rate limiter and cache (thread-safe)."""
        with cls._lock:
            if cls._rate_limiter is None:
                cls._rate_limiter = RateLimiter()
            if cls._cache is None:
                cls._cache = DataCache()

    def _setup_logger(self) -> logging.Logger:
        """Setup extractor-specific logger."""
        logger = logging.getLogger(f"etl.extractor.{self.get_source_name()}")

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f"[%(asctime)s] [ETL:{self.get_source_name()}] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

        return logger

    # =========================================================================
    # Abstract Methods (Must Implement)
    # =========================================================================

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the data source identifier (e.g., 'STEAM', 'ESPN', 'COINGECKO')."""
        pass

    @abstractmethod
    def get_endpoints(self) -> Dict[str, str]:
        """Return dictionary of endpoint names to URLs."""
        pass

    @abstractmethod
    def get_rate_limit(self) -> int:
        """Return rate limit in requests per minute."""
        pass

    @abstractmethod
    def extract(self, **kwargs) -> ExtractionResult:
        """
        Main extraction method. Must be implemented by subclasses.

        Returns:
            ExtractionResult with extracted data and metadata
        """
        pass

    # =========================================================================
    # Rate Limiting
    # =========================================================================

    def _configure_rate_limit(self):
        """Configure rate limit for this extractor's source."""
        self._rate_limiter.configure(self.get_source_name(), self.get_rate_limit())

    def _acquire_rate_limit(self, timeout: float = 30.0) -> bool:
        """
        Acquire a rate limit token.

        Args:
            timeout: Maximum time to wait for a token

        Returns:
            True if token acquired, False if timeout
        """
        return self._rate_limiter.acquire(self.get_source_name(), timeout)

    # =========================================================================
    # HTTP Request Helpers
    # =========================================================================

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        cache_key: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Make an HTTP request with rate limiting, caching, and retry logic.

        Args:
            method: HTTP method (GET, POST)
            url: Request URL
            params: Query parameters
            headers: Additional headers
            json_data: JSON body for POST requests
            cache_key: Optional cache key (auto-generated if None)

        Returns:
            Response data as dict, or None if failed
        """
        # Check cache first
        if self.use_cache and method.upper() == 'GET':
            cache_key = cache_key or self._generate_cache_key(url, params)
            cached = self._cache.get(self.get_source_name(), cache_key, self.cache_ttl)
            if cached is not None:
                self._cache_hits += 1
                self.logger.debug(f"Cache hit for {cache_key}")
                return cached

        # Acquire rate limit token
        if not self._acquire_rate_limit():
            self.logger.warning("Rate limit timeout - request skipped")
            return None

        # Retry loop
        last_error = None
        for attempt in range(self.max_retries):
            try:
                self._api_calls += 1

                # Merge headers
                req_headers = dict(self.session.headers)
                if headers:
                    req_headers.update(headers)
                if self.api_key:
                    req_headers['Authorization'] = f'Bearer {self.api_key}'

                # Make request
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    headers=req_headers,
                    json=json_data,
                    timeout=self.timeout,
                )

                # Check for rate limit response
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                data = response.json()

                # Cache successful GET responses
                if self.use_cache and method.upper() == 'GET':
                    self._cache.set(self.get_source_name(), cache_key, data)

                return data

            except requests.exceptions.Timeout:
                last_error = f"Request timeout after {self.timeout}s"
                self.logger.warning(f"Attempt {attempt + 1}/{self.max_retries}: {last_error}")

            except requests.exceptions.HTTPError as e:
                last_error = f"HTTP error: {e.response.status_code}"
                self.logger.warning(f"Attempt {attempt + 1}/{self.max_retries}: {last_error}")
                if e.response.status_code in (400, 401, 403, 404):
                    break  # Don't retry client errors

            except requests.exceptions.RequestException as e:
                last_error = f"Request failed: {str(e)}"
                self.logger.warning(f"Attempt {attempt + 1}/{self.max_retries}: {last_error}")

            # Exponential backoff
            if attempt < self.max_retries - 1:
                wait = (2 ** attempt) + (time.time() % 1)
                self.logger.debug(f"Waiting {wait:.1f}s before retry")
                time.sleep(wait)

        self.logger.error(f"All retries failed: {last_error}")
        return None

    def _get(self, url: str, params: Optional[Dict] = None, **kwargs) -> Optional[Dict]:
        """Convenience method for GET requests."""
        return self._request('GET', url, params=params, **kwargs)

    def _post(self, url: str, json_data: Optional[Dict] = None, **kwargs) -> Optional[Dict]:
        """Convenience method for POST requests."""
        return self._request('POST', url, json_data=json_data, **kwargs)

    def _generate_cache_key(self, url: str, params: Optional[Dict] = None) -> str:
        """Generate a cache key from URL and parameters."""
        import hashlib
        key_data = url + json.dumps(params or {}, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()

    # =========================================================================
    # Pagination Helpers
    # =========================================================================

    def _paginate(
        self,
        url: str,
        params: Optional[Dict] = None,
        page_param: str = 'page',
        per_page_param: str = 'per_page',
        per_page: int = 100,
        max_pages: Optional[int] = None,
        results_key: Optional[str] = None,
    ) -> Generator[List[Dict], None, None]:
        """
        Paginate through API results.

        Args:
            url: Base URL
            params: Base query parameters
            page_param: Name of page parameter
            per_page_param: Name of per-page parameter
            per_page: Results per page
            max_pages: Maximum pages to fetch (None = all)
            results_key: Key in response containing results (None = root is list)

        Yields:
            List of results for each page
        """
        params = dict(params or {})
        params[per_page_param] = per_page
        page = 1

        while True:
            params[page_param] = page
            self.logger.debug(f"Fetching page {page}")

            response = self._get(url, params)
            if response is None:
                break

            # Extract results
            if results_key:
                results = response.get(results_key, [])
            else:
                results = response if isinstance(response, list) else []

            if not results:
                break

            yield results

            # Check if more pages
            if len(results) < per_page:
                break
            if max_pages and page >= max_pages:
                break

            page += 1

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics."""
        return {
            'source': self.get_source_name(),
            'api_calls': self._api_calls,
            'cache_hits': self._cache_hits,
            'rate_limit': self._rate_limiter.status(self.get_source_name()),
        }

    def reset_stats(self):
        """Reset extraction statistics."""
        self._api_calls = 0
        self._cache_hits = 0
