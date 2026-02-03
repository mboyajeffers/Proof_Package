"""SEC EDGAR API client for XBRL financial data extraction."""

import time
import requests
from typing import Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class SECConfig:
    """Configuration for SEC EDGAR API."""
    base_url: str = "https://data.sec.gov"
    user_agent: str = "MboyaJeffers MboyaJeffers9@gmail.com"
    rate_limit: float = 0.1  # 10 requests per second max


class SECClient:
    """Client for SEC EDGAR XBRL API with rate limiting."""

    def __init__(self, config: Optional[SECConfig] = None):
        self.config = config or SECConfig()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config.user_agent,
            "Accept": "application/json"
        })
        self._last_request_time = 0

    def _rate_limit(self) -> None:
        """Enforce SEC rate limiting (10 req/sec)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.config.rate_limit:
            time.sleep(self.config.rate_limit - elapsed)
        self._last_request_time = time.time()

    def get_company_facts(self, cik: str) -> Dict[str, Any]:
        """
        Fetch all XBRL facts for a company.

        Args:
            cik: Central Index Key (e.g., "0000789019" for Microsoft)

        Returns:
            Dict containing all XBRL facts from SEC filings
        """
        self._rate_limit()

        # Normalize CIK to 10 digits with leading zeros
        cik_normalized = cik.zfill(10)
        url = f"{self.config.base_url}/api/xbrl/companyfacts/CIK{cik_normalized}.json"

        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        return response.json()

    def extract_financial_metrics(self, facts: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key financial metrics from XBRL facts.

        Args:
            facts: Raw XBRL facts from SEC EDGAR

        Returns:
            Dict with normalized financial metrics
        """
        metrics = {}
        us_gaap = facts.get("facts", {}).get("us-gaap", {})

        # Revenue - try multiple concept names
        revenue_concepts = [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerIncludingAssessedTax"
        ]
        metrics["revenue"] = self._get_latest_annual(us_gaap, revenue_concepts)

        # Net Income
        net_income_concepts = [
            "NetIncomeLoss",
            "ProfitLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic"
        ]
        metrics["net_income"] = self._get_latest_annual(us_gaap, net_income_concepts)

        # Total Assets
        metrics["total_assets"] = self._get_latest_annual(
            us_gaap, ["Assets"]
        )

        # Stockholders Equity
        equity_concepts = [
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
        ]
        metrics["stockholders_equity"] = self._get_latest_annual(us_gaap, equity_concepts)

        # EPS
        eps_concepts = [
            "EarningsPerShareBasic",
            "EarningsPerShareDiluted"
        ]
        metrics["eps_basic"] = self._get_latest_annual(us_gaap, eps_concepts)

        return metrics

    def _get_latest_annual(
        self,
        us_gaap: Dict[str, Any],
        concept_names: list
    ) -> Optional[Dict[str, Any]]:
        """Get most recent 10-K value for a metric."""
        for concept in concept_names:
            if concept not in us_gaap:
                continue

            units = us_gaap[concept].get("units", {})

            # Try USD first, then shares for EPS
            for unit_type in ["USD", "USD/shares"]:
                if unit_type not in units:
                    continue

                # Filter for 10-K filings only
                annual_values = [
                    v for v in units[unit_type]
                    if v.get("form") == "10-K"
                ]

                if annual_values:
                    # Sort by end date, get most recent
                    latest = sorted(annual_values, key=lambda x: x.get("end", ""))[-1]
                    return {
                        "value": latest.get("val"),
                        "end_date": latest.get("end"),
                        "form": latest.get("form"),
                        "accession": latest.get("accn"),
                        "concept": concept
                    }

        return None


def main():
    """Example usage."""
    client = SECClient()

    # Microsoft CIK
    cik = "0000789019"

    print(f"Fetching XBRL facts for CIK {cik}...")
    facts = client.get_company_facts(cik)

    print(f"Company: {facts.get('entityName')}")
    print(f"Total concepts: {len(facts.get('facts', {}).get('us-gaap', {}))}")

    metrics = client.extract_financial_metrics(facts)

    print("\nKey Financials:")
    for key, data in metrics.items():
        if data:
            value = data["value"]
            if isinstance(value, (int, float)) and abs(value) > 1_000_000:
                print(f"  {key}: ${value/1e9:.1f}B")
            else:
                print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
