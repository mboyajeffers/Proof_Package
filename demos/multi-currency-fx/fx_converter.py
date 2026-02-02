#!/usr/bin/env python3
"""
Multi-Currency FX Converter
===========================
Currency conversion with multiple data sources and fallback logic.

Features:
- ECB (European Central Bank) as primary source
- Fallback to exchangerate-api.com
- Rate caching with TTL
- Batch conversion support
- Historical rate lookup

Author: Mboya Jeffers
"""

import json
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CachedRate:
    """Cached exchange rate with timestamp."""
    rate: float
    timestamp: datetime
    source: str


@dataclass
class ConversionResult:
    """Result of a currency conversion."""
    from_currency: str
    to_currency: str
    amount: float
    converted_amount: float
    rate: float
    rate_source: str
    timestamp: str


class RateCache:
    """In-memory cache for exchange rates with TTL."""

    def __init__(self, ttl_minutes: int = 60):
        self.cache: Dict[str, CachedRate] = {}
        self.ttl = timedelta(minutes=ttl_minutes)

    def get(self, key: str) -> Optional[CachedRate]:
        """Get cached rate if not expired."""
        if key not in self.cache:
            return None

        cached = self.cache[key]
        if datetime.now(timezone.utc) - cached.timestamp > self.ttl:
            del self.cache[key]
            return None

        return cached

    def set(self, key: str, rate: float, source: str) -> None:
        """Cache a rate."""
        self.cache[key] = CachedRate(
            rate=rate,
            timestamp=datetime.now(timezone.utc),
            source=source
        )

    def clear(self) -> None:
        """Clear all cached rates."""
        self.cache.clear()


class ECBRateProvider:
    """European Central Bank exchange rate provider."""

    BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json'
        })

    def get_rate(self, from_currency: str, to_currency: str) -> Optional[float]:
        """
        Get exchange rate from ECB.

        ECB provides rates relative to EUR.
        """
        try:
            # ECB uses EUR as base
            if from_currency == 'EUR':
                rate_to = self._get_eur_rate(to_currency)
                return rate_to
            elif to_currency == 'EUR':
                rate_from = self._get_eur_rate(from_currency)
                return 1 / rate_from if rate_from else None
            else:
                # Cross rate via EUR
                rate_from = self._get_eur_rate(from_currency)
                rate_to = self._get_eur_rate(to_currency)
                if rate_from and rate_to:
                    return rate_to / rate_from
                return None

        except Exception as e:
            logger.warning(f"ECB API error: {e}")
            return None

    def _get_eur_rate(self, currency: str) -> Optional[float]:
        """Get rate for currency vs EUR."""
        if currency == 'EUR':
            return 1.0

        # ECB API format: D.{currency}.EUR.SP00.A
        url = f"{self.BASE_URL}/D.{currency}.EUR.SP00.A"
        params = {'lastNObservations': 1}

        try:
            response = self.session.get(url, params=params, timeout=30)

            if response.status_code == 404:
                return None

            response.raise_for_status()
            data = response.json()

            # Navigate ECB response structure
            series = data.get('dataSets', [{}])[0].get('series', {})
            if series:
                first_series = list(series.values())[0]
                observations = first_series.get('observations', {})
                if observations:
                    last_obs = list(observations.values())[-1]
                    return float(last_obs[0])

            return None

        except Exception as e:
            logger.debug(f"ECB rate lookup failed for {currency}: {e}")
            return None


class ExchangeRateAPIProvider:
    """Fallback provider using exchangerate-api.com (free tier)."""

    BASE_URL = "https://open.er-api.com/v6/latest"

    def __init__(self):
        self.session = requests.Session()
        self._rates_cache: Dict[str, Dict] = {}
        self._cache_time: Optional[datetime] = None

    def get_rate(self, from_currency: str, to_currency: str) -> Optional[float]:
        """Get exchange rate from exchangerate-api."""
        try:
            rates = self._get_rates(from_currency)
            if rates and to_currency in rates:
                return rates[to_currency]
            return None

        except Exception as e:
            logger.warning(f"ExchangeRate API error: {e}")
            return None

    def _get_rates(self, base: str) -> Optional[Dict[str, float]]:
        """Get all rates for a base currency."""
        # Use cached rates if recent
        if (self._cache_time and
            base in self._rates_cache and
            datetime.now(timezone.utc) - self._cache_time < timedelta(minutes=30)):
            return self._rates_cache[base]

        url = f"{self.BASE_URL}/{base}"

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('result') == 'success':
                rates = data.get('rates', {})
                self._rates_cache[base] = rates
                self._cache_time = datetime.now(timezone.utc)
                return rates

            return None

        except Exception:
            return None


class FXConverter:
    """
    Multi-source currency converter with caching and fallback.

    Usage:
        converter = FXConverter()
        result = converter.convert(100, 'USD', 'EUR')
        print(f"{result.amount} {result.from_currency} = {result.converted_amount:.2f} {result.to_currency}")
    """

    # Common currency codes
    SUPPORTED_CURRENCIES = {
        'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD',
        'CNY', 'HKD', 'SGD', 'SEK', 'NOK', 'DKK', 'MXN', 'BRL',
        'INR', 'KRW', 'ZAR', 'RUB', 'TRY', 'PLN', 'THB', 'IDR',
        'MYR', 'PHP', 'CZK', 'HUF', 'ILS', 'CLP', 'AED', 'SAR'
    }

    def __init__(self, cache_ttl_minutes: int = 60):
        self.cache = RateCache(cache_ttl_minutes)
        self.ecb = ECBRateProvider()
        self.fallback = ExchangeRateAPIProvider()

    def get_rate(self, from_currency: str, to_currency: str) -> Tuple[float, str]:
        """
        Get exchange rate with fallback logic.

        Returns:
            Tuple of (rate, source)
        """
        from_currency = from_currency.upper()
        to_currency = to_currency.upper()

        if from_currency == to_currency:
            return 1.0, 'identity'

        # Check cache
        cache_key = f"{from_currency}_{to_currency}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached.rate, f"cache ({cached.source})"

        # Try ECB first
        rate = self.ecb.get_rate(from_currency, to_currency)
        if rate:
            self.cache.set(cache_key, rate, 'ECB')
            return rate, 'ECB'

        # Fallback to exchangerate-api
        rate = self.fallback.get_rate(from_currency, to_currency)
        if rate:
            self.cache.set(cache_key, rate, 'ExchangeRate-API')
            return rate, 'ExchangeRate-API'

        raise ValueError(f"Could not get rate for {from_currency}/{to_currency}")

    def convert(self, amount: float, from_currency: str,
                to_currency: str) -> ConversionResult:
        """
        Convert amount from one currency to another.

        Args:
            amount: Amount to convert
            from_currency: Source currency code (e.g., 'USD')
            to_currency: Target currency code (e.g., 'EUR')

        Returns:
            ConversionResult with conversion details
        """
        rate, source = self.get_rate(from_currency, to_currency)
        converted = amount * rate

        return ConversionResult(
            from_currency=from_currency.upper(),
            to_currency=to_currency.upper(),
            amount=amount,
            converted_amount=round(converted, 4),
            rate=round(rate, 6),
            rate_source=source,
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    def convert_batch(self, amounts: List[Tuple[float, str, str]]) -> List[ConversionResult]:
        """
        Convert multiple amounts.

        Args:
            amounts: List of (amount, from_currency, to_currency) tuples

        Returns:
            List of ConversionResult objects
        """
        results = []
        for amount, from_curr, to_curr in amounts:
            try:
                result = self.convert(amount, from_curr, to_curr)
                results.append(result)
            except ValueError as e:
                logger.error(f"Conversion failed: {e}")

        return results

    def get_all_rates(self, base_currency: str = 'USD') -> Dict[str, float]:
        """Get rates for all supported currencies from a base."""
        rates = {}
        for currency in self.SUPPORTED_CURRENCIES:
            if currency != base_currency.upper():
                try:
                    rate, _ = self.get_rate(base_currency, currency)
                    rates[currency] = rate
                except ValueError:
                    pass
        return rates


# Demo
if __name__ == '__main__':
    print("=" * 50)
    print("Multi-Currency FX Converter Demo")
    print("=" * 50)

    converter = FXConverter()

    # Single conversion
    print("\nSingle Conversion:")
    result = converter.convert(1000, 'USD', 'EUR')
    print(f"  {result.amount} {result.from_currency} = {result.converted_amount:.2f} {result.to_currency}")
    print(f"  Rate: {result.rate} (Source: {result.rate_source})")

    # Batch conversion
    print("\nBatch Conversion (1000 USD to various):")
    conversions = [
        (1000, 'USD', 'EUR'),
        (1000, 'USD', 'GBP'),
        (1000, 'USD', 'JPY'),
        (1000, 'USD', 'CAD'),
    ]

    results = converter.convert_batch(conversions)
    for r in results:
        print(f"  {r.amount} {r.from_currency} = {r.converted_amount:,.2f} {r.to_currency}")

    # Cross rates
    print("\nCross Rates (EUR to Asian currencies):")
    for curr in ['JPY', 'CNY', 'KRW', 'SGD']:
        try:
            result = converter.convert(1000, 'EUR', curr)
            print(f"  1000 EUR = {result.converted_amount:,.2f} {curr}")
        except ValueError as e:
            print(f"  EUR/{curr}: {e}")
