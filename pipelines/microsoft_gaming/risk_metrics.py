"""Risk metrics calculations following CFA/Basel standards."""

import numpy as np
import pandas as pd
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class RiskMetricsConfig:
    """Configuration for risk calculations."""
    trading_days_per_year: int = 252
    risk_free_rate: float = 0.0
    var_confidence_levels: tuple = (0.95, 0.99)


class RiskMetricsCalculator:
    """Calculate portfolio risk metrics."""

    def __init__(self, config: RiskMetricsConfig = None):
        self.config = config or RiskMetricsConfig()

    def calculate_all_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate all risk metrics from price data.

        Args:
            df: DataFrame with 'daily_return' and 'cumulative_return' columns

        Returns:
            Dict with all calculated metrics
        """
        returns = df["daily_return"].dropna()

        metrics = {
            "total_return": self.total_return(df),
            "annualized_return": self.annualized_return(returns),
            "volatility": self.volatility(returns),
            "sharpe_ratio": self.sharpe_ratio(returns),
            "sortino_ratio": self.sortino_ratio(returns),
            "max_drawdown": self.max_drawdown(df),
            "var_95": self.value_at_risk(returns, 0.95),
            "var_99": self.value_at_risk(returns, 0.99),
            "positive_days_pct": self.positive_days_percentage(returns),
            "best_day": returns.max(),
            "worst_day": returns.min(),
            "trading_days": len(returns),
            "years_analyzed": len(returns) / self.config.trading_days_per_year
        }

        return metrics

    def total_return(self, df: pd.DataFrame) -> float:
        """
        Calculate total cumulative return.

        Formula: (End Price / Start Price) - 1
        """
        prices = df["adj_close"].dropna()
        return (prices.iloc[-1] / prices.iloc[0]) - 1

    def annualized_return(self, returns: pd.Series) -> float:
        """
        Calculate annualized geometric mean return.

        Formula: (1 + total_return)^(252/n) - 1
        """
        total = (1 + returns).prod()
        years = len(returns) / self.config.trading_days_per_year
        return total ** (1 / years) - 1

    def volatility(self, returns: pd.Series) -> float:
        """
        Calculate annualized volatility.

        Formula: StdDev(daily returns) × sqrt(252)
        """
        return returns.std() * np.sqrt(self.config.trading_days_per_year)

    def sharpe_ratio(self, returns: pd.Series) -> float:
        """
        Calculate Sharpe Ratio.

        Formula: (Annualized Return - Risk Free Rate) / Volatility

        Interpretation:
        - < 1.0: Suboptimal
        - 1.0-2.0: Good
        - 2.0-3.0: Very good
        - > 3.0: Excellent
        """
        ann_return = self.annualized_return(returns)
        vol = self.volatility(returns)

        if vol == 0:
            return 0.0

        return (ann_return - self.config.risk_free_rate) / vol

    def sortino_ratio(self, returns: pd.Series) -> float:
        """
        Calculate Sortino Ratio.

        Formula: (Annualized Return - Risk Free Rate) / Downside Deviation

        Like Sharpe but only penalizes downside volatility.
        """
        ann_return = self.annualized_return(returns)

        # Downside deviation: std of negative returns only
        negative_returns = returns[returns < 0]
        if len(negative_returns) == 0:
            return float("inf")

        downside_dev = negative_returns.std() * np.sqrt(self.config.trading_days_per_year)

        if downside_dev == 0:
            return float("inf")

        return (ann_return - self.config.risk_free_rate) / downside_dev

    def max_drawdown(self, df: pd.DataFrame) -> float:
        """
        Calculate maximum drawdown.

        Formula: (Trough - Peak) / Peak

        Represents the largest peak-to-trough decline.
        """
        prices = df["adj_close"].dropna()
        cumulative_max = prices.cummax()
        drawdown = (prices - cumulative_max) / cumulative_max
        return drawdown.min()

    def value_at_risk(self, returns: pd.Series, confidence: float = 0.95) -> float:
        """
        Calculate Value at Risk (historical method).

        Formula: Percentile of returns at (1 - confidence)

        VaR(95%) answers: "What's the worst daily loss I can expect
        95% of the time?"
        """
        percentile = (1 - confidence) * 100
        return np.percentile(returns, percentile)

    def positive_days_percentage(self, returns: pd.Series) -> float:
        """Calculate percentage of days with positive returns."""
        return (returns > 0).sum() / len(returns)

    def format_metrics_summary(self, metrics: Dict[str, Any]) -> str:
        """Format metrics as readable summary."""
        lines = [
            "Risk Metrics Summary",
            "=" * 40,
            f"Total Return (10Y):    {metrics['total_return']:+.1%}",
            f"Annualized Return:     {metrics['annualized_return']:+.1%}",
            f"Volatility:            {metrics['volatility']:.1%}",
            f"Sharpe Ratio:          {metrics['sharpe_ratio']:.2f}",
            f"Sortino Ratio:         {metrics['sortino_ratio']:.2f}",
            f"Max Drawdown:          {metrics['max_drawdown']:.1%}",
            f"VaR (95%):             {metrics['var_95']:.2%}",
            f"VaR (99%):             {metrics['var_99']:.2%}",
            "",
            f"Trading Days:          {metrics['trading_days']:,}",
            f"Positive Days:         {metrics['positive_days_pct']:.1%}",
            f"Best Day:              {metrics['best_day']:+.2%}",
            f"Worst Day:             {metrics['worst_day']:+.2%}",
        ]
        return "\n".join(lines)


def main():
    """Example usage with sample data."""
    # Generate sample returns for demonstration
    np.random.seed(42)
    n_days = 2514  # ~10 years

    # Simulate daily returns (mean 0.1% daily, 1.5% std)
    daily_returns = np.random.normal(0.001, 0.015, n_days)

    df = pd.DataFrame({
        "daily_return": daily_returns,
        "adj_close": 100 * (1 + pd.Series(daily_returns)).cumprod()
    })

    calculator = RiskMetricsCalculator()
    metrics = calculator.calculate_all_metrics(df)

    print(calculator.format_metrics_summary(metrics))


if __name__ == "__main__":
    main()
