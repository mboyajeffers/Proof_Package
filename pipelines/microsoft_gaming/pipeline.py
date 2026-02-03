"""Main pipeline orchestration for Microsoft Gaming Analytics."""

import argparse
import json
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from .sec_client import SECClient
from .yahoo_client import YahooClient
from .risk_metrics import RiskMetricsCalculator


class MicrosoftGamingPipeline:
    """End-to-end pipeline for gaming sector analysis."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.sec_client = SECClient()
        self.yahoo_client = YahooClient()
        self.risk_calculator = RiskMetricsCalculator()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file) as f:
                return yaml.safe_load(f)

        # Default configuration
        return {
            "company": {
                "ticker": "MSFT",
                "cik": "0000789019",
                "name": "Microsoft Corporation"
            },
            "analysis": {
                "years": 10,
                "risk_free_rate": 0.0
            },
            "output": {
                "data_dir": "data/",
                "reports_dir": "reports/"
            }
        }

    def run(self) -> Dict[str, Any]:
        """Execute the full pipeline."""
        print("=" * 60)
        print("Microsoft Gaming Analytics Pipeline")
        print("=" * 60)

        ticker = self.config["company"]["ticker"]
        cik = self.config["company"]["cik"]
        years = self.config["analysis"]["years"]

        # Step 1: Fetch SEC EDGAR data
        print(f"\n[1/4] Fetching SEC EDGAR data for CIK {cik}...")
        sec_facts = self.sec_client.get_company_facts(cik)
        financials = self.sec_client.extract_financial_metrics(sec_facts)
        print(f"      Company: {sec_facts.get('entityName')}")
        print(f"      XBRL concepts: {len(sec_facts.get('facts', {}).get('us-gaap', {})):,}")

        # Step 2: Fetch Yahoo Finance data
        print(f"\n[2/4] Fetching {years} years of stock data for {ticker}...")
        price_df = self.yahoo_client.get_historical_prices(ticker, years=years)
        price_df = self.yahoo_client.calculate_returns(price_df)
        print(f"      Date range: {price_df['date'].min()} to {price_df['date'].max()}")
        print(f"      Trading days: {len(price_df):,}")

        # Step 3: Calculate risk metrics
        print("\n[3/4] Calculating risk metrics...")
        risk_metrics = self.risk_calculator.calculate_all_metrics(price_df)
        print(f"      Total return: {risk_metrics['total_return']:+.1%}")
        print(f"      Sharpe ratio: {risk_metrics['sharpe_ratio']:.2f}")

        # Step 4: Compile results
        print("\n[4/4] Compiling results...")
        results = self._compile_results(sec_facts, financials, price_df, risk_metrics)

        # Save outputs
        self._save_outputs(results, price_df)

        print("\n" + "=" * 60)
        print("Pipeline complete!")
        print("=" * 60)

        return results

    def _compile_results(
        self,
        sec_facts: Dict,
        financials: Dict,
        price_df,
        risk_metrics: Dict
    ) -> Dict[str, Any]:
        """Compile all results into a single structure."""
        return {
            "metadata": {
                "company": sec_facts.get("entityName"),
                "ticker": self.config["company"]["ticker"],
                "cik": self.config["company"]["cik"],
                "generated_at": datetime.now().isoformat(),
                "analysis_years": self.config["analysis"]["years"]
            },
            "financials": {
                "revenue": self._format_financial(financials.get("revenue")),
                "net_income": self._format_financial(financials.get("net_income")),
                "total_assets": self._format_financial(financials.get("total_assets")),
                "stockholders_equity": self._format_financial(financials.get("stockholders_equity")),
                "eps_basic": self._format_financial(financials.get("eps_basic"))
            },
            "stock_performance": {
                "start_date": str(price_df["date"].min()),
                "end_date": str(price_df["date"].max()),
                "trading_days": int(risk_metrics["trading_days"]),
                "latest_close": float(price_df["adj_close"].iloc[-1])
            },
            "risk_metrics": {
                "total_return": float(risk_metrics["total_return"]),
                "annualized_return": float(risk_metrics["annualized_return"]),
                "volatility": float(risk_metrics["volatility"]),
                "sharpe_ratio": float(risk_metrics["sharpe_ratio"]),
                "sortino_ratio": float(risk_metrics["sortino_ratio"]),
                "max_drawdown": float(risk_metrics["max_drawdown"]),
                "var_95": float(risk_metrics["var_95"]),
                "var_99": float(risk_metrics["var_99"]),
                "positive_days_pct": float(risk_metrics["positive_days_pct"]),
                "best_day": float(risk_metrics["best_day"]),
                "worst_day": float(risk_metrics["worst_day"])
            },
            "data_sources": {
                "financials": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{self.config['company']['cik']}.json",
                "stock_prices": f"Yahoo Finance ({self.config['company']['ticker']})"
            }
        }

    def _format_financial(self, data: Dict) -> Dict:
        """Format financial metric for output."""
        if not data:
            return None
        return {
            "value": data.get("value"),
            "period_end": data.get("end_date"),
            "form": data.get("form"),
            "sec_accession": data.get("accession")
        }

    def _save_outputs(self, results: Dict, price_df) -> None:
        """Save pipeline outputs to files."""
        data_dir = Path(self.config["output"]["data_dir"])
        data_dir.mkdir(exist_ok=True)

        # Save JSON results
        json_path = data_dir / "analysis_results.json"
        with open(json_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"      Saved: {json_path}")

        # Save price data as CSV
        csv_path = data_dir / "price_history.csv"
        price_df.to_csv(csv_path, index=False)
        print(f"      Saved: {csv_path}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Microsoft Gaming Analytics Pipeline"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--ticker",
        default=None,
        help="Override ticker symbol"
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Override analysis period (years)"
    )

    args = parser.parse_args()

    pipeline = MicrosoftGamingPipeline(config_path=args.config)

    # Apply CLI overrides
    if args.ticker:
        pipeline.config["company"]["ticker"] = args.ticker
    if args.years:
        pipeline.config["analysis"]["years"] = args.years

    results = pipeline.run()

    # Print summary
    print("\n" + pipeline.risk_calculator.format_metrics_summary(results["risk_metrics"]))


if __name__ == "__main__":
    main()
