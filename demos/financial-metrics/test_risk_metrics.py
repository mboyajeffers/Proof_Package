#!/usr/bin/env python3
"""
Tests for Financial Risk Metrics
"""

import pytest
import numpy as np
import pandas as pd
from risk_metrics import (
    calculate_returns,
    parametric_var,
    historical_var,
    sharpe_ratio,
    sortino_ratio,
    maximum_drawdown,
    beta,
    calculate_all_metrics
)


@pytest.fixture
def sample_prices():
    """Generate sample price series for testing."""
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=100, freq='B')
    returns = np.random.normal(0.001, 0.02, 100)
    prices = 100 * np.cumprod(1 + returns)
    return pd.Series(prices, index=dates)


@pytest.fixture
def sample_returns():
    """Generate sample returns series for testing."""
    np.random.seed(42)
    return pd.Series(np.random.normal(0.001, 0.02, 100))


class TestCalculateReturns:
    def test_simple_returns(self, sample_prices):
        returns = calculate_returns(sample_prices, method='simple')
        assert len(returns) == len(sample_prices) - 1
        assert not returns.isna().any()

    def test_log_returns(self, sample_prices):
        returns = calculate_returns(sample_prices, method='log')
        assert len(returns) == len(sample_prices) - 1
        assert not returns.isna().any()


class TestParametricVaR:
    def test_var_95(self, sample_returns):
        var = parametric_var(sample_returns, confidence=0.95)
        assert isinstance(var, float)
        assert var > 0  # VaR should be positive (represents loss)

    def test_var_99_greater_than_95(self, sample_returns):
        var_95 = parametric_var(sample_returns, confidence=0.95)
        var_99 = parametric_var(sample_returns, confidence=0.99)
        assert var_99 > var_95  # Higher confidence = higher VaR


class TestHistoricalVaR:
    def test_var_95(self, sample_returns):
        var = historical_var(sample_returns, confidence=0.95)
        assert isinstance(var, float)
        assert var > 0

    def test_var_within_range(self, sample_returns):
        var = historical_var(sample_returns, confidence=0.95)
        # VaR should be within reasonable bounds
        assert var < 0.5  # Less than 50% loss for normal market data


class TestSharpeRatio:
    def test_sharpe_calculation(self, sample_returns):
        sharpe = sharpe_ratio(sample_returns, risk_free_rate=0.02)
        assert isinstance(sharpe, float)

    def test_zero_risk_free(self, sample_returns):
        sharpe = sharpe_ratio(sample_returns, risk_free_rate=0.0)
        assert isinstance(sharpe, float)

    def test_constant_returns_zero_sharpe(self):
        constant_returns = pd.Series([0.01] * 100)
        sharpe = sharpe_ratio(constant_returns)
        # Constant returns = zero volatility = undefined/zero Sharpe
        assert sharpe == 0.0


class TestSortinoRatio:
    def test_sortino_calculation(self, sample_returns):
        sortino = sortino_ratio(sample_returns, risk_free_rate=0.02)
        assert isinstance(sortino, float)

    def test_sortino_higher_than_sharpe_for_positive_skew(self):
        # For positive returns with low downside, Sortino > Sharpe
        positive_returns = pd.Series(np.abs(np.random.normal(0.01, 0.02, 100)))
        sharpe = sharpe_ratio(positive_returns)
        sortino = sortino_ratio(positive_returns)
        # Sortino should generally be higher when downside is limited
        assert sortino >= sharpe or np.isinf(sortino)


class TestMaximumDrawdown:
    def test_drawdown_calculation(self, sample_prices):
        max_dd, peak, trough = maximum_drawdown(sample_prices)
        assert isinstance(max_dd, float)
        assert 0 <= max_dd <= 1  # Drawdown is a percentage
        assert peak < trough  # Peak comes before trough

    def test_no_drawdown_for_increasing_prices(self):
        prices = pd.Series([100, 101, 102, 103, 104],
                          index=pd.date_range('2023-01-01', periods=5))
        max_dd, _, _ = maximum_drawdown(prices)
        assert max_dd == 0.0


class TestBeta:
    def test_beta_calculation(self):
        np.random.seed(42)
        market = pd.Series(np.random.normal(0.001, 0.02, 100))
        # Asset with higher volatility correlated with market
        asset = market * 1.5 + np.random.normal(0, 0.005, 100)

        b = beta(asset, market)
        assert isinstance(b, float)
        assert b > 1.0  # Higher volatility asset should have beta > 1

    def test_beta_of_benchmark_is_one(self):
        market = pd.Series(np.random.normal(0.001, 0.02, 100))
        b = beta(market, market)
        assert abs(b - 1.0) < 0.01  # Beta of market vs itself = 1


class TestCalculateAllMetrics:
    def test_all_metrics_returned(self, sample_prices):
        metrics = calculate_all_metrics(sample_prices)

        required_keys = [
            'total_return', 'annualized_return', 'annualized_volatility',
            'var_95_parametric', 'var_99_parametric', 'var_95_historical',
            'sharpe_ratio', 'sortino_ratio', 'max_drawdown'
        ]

        for key in required_keys:
            assert key in metrics, f"Missing metric: {key}"

    def test_with_benchmark(self, sample_prices):
        np.random.seed(123)
        benchmark = pd.Series(
            100 * np.cumprod(1 + np.random.normal(0.0005, 0.015, 100)),
            index=sample_prices.index
        )

        metrics = calculate_all_metrics(sample_prices, benchmark)
        assert 'beta' in metrics


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
