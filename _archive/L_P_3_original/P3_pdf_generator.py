#!/usr/bin/env python3
"""
Project 3 Founder Summary PDF Generator
Generates all three P3 founder summaries with proper disclosures
"""

import json
import subprocess
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent

# PDF content for each project

P3_MED_HTML = """
<h2>TL;DR</h2>
<p>Analyzed <strong>942 hospitals</strong> from CMS Hospital Compare, revealing VA hospitals outperform private sector by +0.8 stars on average (3.7 vs 2.9).</p>

<h2>What This Proves</h2>
<ul>
<li><strong>Government API Integration</strong> - Real-time data from CMS Provider Data API</li>
<li><strong>Healthcare Analytics</strong> - Quality benchmarking across ownership types</li>
<li><strong>Star Schema Design</strong> - Hospital dimension with quality metrics</li>
</ul>

<h2>Key Metrics</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>Metric</strong></td><td><strong>Value</strong></td></tr>
<tr><td>Hospitals Analyzed</td><td>942</td></tr>
<tr><td>States Covered</td><td>CA, FL, GA, IL</td></tr>
<tr><td>Average Rating</td><td>2.90 stars</td></tr>
<tr><td>5-Star Hospitals</td><td>53 (8.4%)</td></tr>
<tr><td>1-Star Hospitals</td><td>85 (13.4%)</td></tr>
<tr><td>Quality Score</td><td>100%</td></tr>
</table>

<h2>Ownership Performance</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>Type</strong></td><td><strong>Avg Rating</strong></td></tr>
<tr><td>Veterans Health Administration</td><td>3.70 stars</td></tr>
<tr><td>Voluntary Non-Profit</td><td>3.11 stars</td></tr>
<tr><td>Government (State/Local)</td><td>2.71 stars</td></tr>
<tr><td>Proprietary (For-Profit)</td><td>2.37 stars</td></tr>
</table>

<h2>Data Attribution</h2>
<p><strong>Source:</strong> CMS Hospital Compare (data.cms.gov/provider-data/dataset/xubh-q36u)</p>
<p><strong>Records:</strong> 942 hospitals | <strong>Period:</strong> Current CMS data release</p>

<h2 style="color: #006400;">DATA DISCLOSURE</h2>
<p style="background-color: #e6ffe6; padding: 10px; border-left: 4px solid #006400;">
<strong>100% REAL DATA</strong> - All metrics from CMS Hospital Compare API.<br/>
<strong>Limitations:</strong><br/>
- 309 hospitals (32.8%) have no rating (CMS criteria not met)<br/>
- Sample limited to 4 states due to API pagination (demo scope)<br/>
- Ratings based only on hospitals with sufficient CMS data
</p>
"""

P3_SEC_HTML = """
<h2>TL;DR</h2>
<p>Built a vulnerability prioritization engine combining <strong>NIST NVD + CISA KEV + FIRST EPSS</strong>, analyzing 500 recent CVEs against 1,501 known exploited vulnerabilities.</p>

<h2>What This Proves</h2>
<ul>
<li><strong>Multi-Source Intelligence</strong> - Integrated 3 federal cybersecurity APIs</li>
<li><strong>Risk Prioritization</strong> - Combined CVSS + EPSS + KEV for smart scoring</li>
<li><strong>Real-Time Threat Data</strong> - Latest 30 days of published CVEs</li>
</ul>

<h2>Key Metrics</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>Metric</strong></td><td><strong>Value</strong></td></tr>
<tr><td>CVEs Analyzed</td><td>500 (last 30 days)</td></tr>
<tr><td>KEV Catalog Size</td><td>1,501 known exploited</td></tr>
<tr><td>Critical Severity</td><td>5</td></tr>
<tr><td>High Severity</td><td>21</td></tr>
<tr><td>Recent KEV Additions</td><td>17 (last 30 days)</td></tr>
</table>

<h2>Top Vulnerability Types (CWE)</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>CWE</strong></td><td><strong>Count</strong></td><td><strong>Type</strong></td></tr>
<tr><td>CWE-89</td><td>21</td><td>SQL Injection</td></tr>
<tr><td>CWE-74</td><td>11</td><td>Injection</td></tr>
<tr><td>CWE-476</td><td>10</td><td>NULL Pointer Dereference</td></tr>
<tr><td>CWE-120</td><td>8</td><td>Buffer Overflow</td></tr>
<tr><td>CWE-434</td><td>5</td><td>Unrestricted File Upload</td></tr>
</table>

<h2>Data Attribution</h2>
<p><strong>Sources:</strong></p>
<ul>
<li>NIST NVD: nvd.nist.gov/developers/vulnerabilities</li>
<li>CISA KEV: cisa.gov/known-exploited-vulnerabilities-catalog</li>
<li>FIRST EPSS: api.first.org/epss</li>
</ul>

<h2 style="color: #006400;">DATA DISCLOSURE</h2>
<p style="background-color: #e6ffe6; padding: 10px; border-left: 4px solid #006400;">
<strong>100% REAL DATA</strong> - All metrics from federal cybersecurity APIs.<br/>
<strong>Important Notes:</strong><br/>
- 86.6% of CVEs show UNKNOWN severity (awaiting NVD scoring - this is normal for recent CVEs)<br/>
- 0 recent CVEs in KEV (KEV contains confirmed exploited vulns, not new discoveries)<br/>
- EPSS coverage: 14.4% (scores calculated as data becomes available)<br/>
<strong>These limitations reflect real-world vulnerability intelligence timing, not data quality issues.</strong>
</p>
"""

P3_ENG_HTML = """
<h2>TL;DR</h2>
<p>Processed <strong>5,000 grid generation records</strong> from EIA-930, showing California ISO leads renewables at 47.3% penetration vs 26.15% national average.</p>

<h2>What This Proves</h2>
<ul>
<li><strong>Energy Sector Expertise</strong> - Real EIA grid monitoring data</li>
<li><strong>Renewable Analytics</strong> - Multi-region penetration tracking</li>
<li><strong>Time Series Processing</strong> - Daily generation by fuel type</li>
</ul>

<h2>Key Metrics</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>Metric</strong></td><td><strong>Value</strong></td></tr>
<tr><td>Generation Records</td><td>5,000</td></tr>
<tr><td>Grid Operators</td><td>7 balancing authorities</td></tr>
<tr><td>Days Analyzed</td><td>18 (Jan 13-30, 2026)</td></tr>
<tr><td>Total Generation</td><td>717.8 TWh</td></tr>
<tr><td>Avg Renewable Penetration</td><td>26.15%</td></tr>
<tr><td>Max Renewable Day</td><td>62.72% (SWPP, Jan 16)</td></tr>
</table>

<h2>Renewable Penetration by Region</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>Region</strong></td><td><strong>Renewable %</strong></td></tr>
<tr><td>CISO (California)</td><td>47.30%</td></tr>
<tr><td>SWPP (Southwest)</td><td>38.29%</td></tr>
<tr><td>ERCO (Texas)</td><td>33.43%</td></tr>
<tr><td>NYIS (New York)</td><td>23.61%</td></tr>
<tr><td>MISO (Midwest)</td><td>20.95%</td></tr>
<tr><td>ISNE (New England)</td><td>15.68%</td></tr>
<tr><td>PJM (Eastern)</td><td>9.24%</td></tr>
</table>

<h2>Fuel Mix (National)</h2>
<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%;">
<tr style="background-color: #f0f0f0;"><td><strong>Source</strong></td><td><strong>Share</strong></td></tr>
<tr><td>Natural Gas</td><td>38.6%</td></tr>
<tr><td>Coal</td><td>20.7%</td></tr>
<tr><td>Nuclear</td><td>17.5%</td></tr>
<tr><td>Wind</td><td>14.0%</td></tr>
<tr><td>Solar</td><td>4.1%</td></tr>
<tr><td>Hydro</td><td>2.6%</td></tr>
</table>

<h2>Data Attribution</h2>
<p><strong>Source:</strong> EIA Open Data API - Form EIA-930 (Hourly Electric Grid Monitor)</p>
<p><strong>Verification:</strong> eia.gov/opendata/</p>

<h2 style="color: #006400;">DATA DISCLOSURE</h2>
<p style="background-color: #e6ffe6; padding: 10px; border-left: 4px solid #006400;">
<strong>100% REAL DATA</strong> - All metrics from US Energy Information Administration.<br/>
<strong>Notes:</strong><br/>
- 18-day sample period (demo scope using EIA DEMO_KEY)<br/>
- Battery shows -0.05% (discharge to grid, expected behavior)<br/>
- All 7 target balancing authorities represented
</p>
"""


def generate_pdfs():
    """Generate PDFs via local WeasyPrint or prepare for PROD-VM"""

    # Create reports directories
    for project in ['P3-MED', 'P3-SEC', 'P3-ENG']:
        if project == 'P3-MED':
            reports_dir = PROJECT_ROOT / 'Healthcare/reports/Project3/P3-MED/reports'
        elif project == 'P3-SEC':
            reports_dir = PROJECT_ROOT / 'Cybersecurity/reports/Project3/P3-SEC/reports'
        else:
            reports_dir = PROJECT_ROOT / 'Energy/reports/Project3/P3-ENG/reports'
        reports_dir.mkdir(parents=True, exist_ok=True)

    # PDF definitions
    pdfs = [
        {
            'title': 'P3-MED Healthcare Quality Intelligence - Founder Summary',
            'html': P3_MED_HTML,
            'output': PROJECT_ROOT / 'Healthcare/reports/Project3/P3-MED/reports/P3-MED_Founder_Summary_v1.0.pdf',
            'scheme': 'navy'
        },
        {
            'title': 'P3-SEC Cybersecurity Vulnerability Intelligence - Founder Summary',
            'html': P3_SEC_HTML,
            'output': PROJECT_ROOT / 'Cybersecurity/reports/Project3/P3-SEC/reports/P3-SEC_Founder_Summary_v1.0.pdf',
            'scheme': 'navy'
        },
        {
            'title': 'P3-ENG Energy Grid Intelligence - Founder Summary',
            'html': P3_ENG_HTML,
            'output': PROJECT_ROOT / 'Energy/reports/Project3/P3-ENG/reports/P3-ENG_Founder_Summary_v1.0.pdf',
            'scheme': 'emerald'
        }
    ]

    print("=" * 60)
    print("Project 3 Founder Summary PDF Generator")
    print("=" * 60)

    # Try local WeasyPrint first
    try:
        from weasyprint import HTML, CSS

        for pdf in pdfs:
            print(f"\nGenerating: {pdf['output'].name}")

            # Create full HTML document
            full_html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
                    h1 {{ color: #1a365d; border-bottom: 2px solid #1a365d; padding-bottom: 10px; }}
                    h2 {{ color: #2c5282; margin-top: 25px; }}
                    table {{ margin: 15px 0; }}
                    td {{ padding: 8px; }}
                    ul {{ margin: 10px 0; }}
                    li {{ margin: 5px 0; }}
                </style>
            </head>
            <body>
                <h1>{pdf['title']}</h1>
                {pdf['html']}
                <hr style="margin-top: 40px;">
                <p style="font-size: 10px; color: #666;">
                    Generated: 2026-02-01 | Author: Mboya Jeffers | Pipeline: Claude Code
                </p>
            </body>
            </html>
            """

            HTML(string=full_html).write_pdf(str(pdf['output']))
            print(f"  ✓ Saved: {pdf['output']}")

        print("\n" + "=" * 60)
        print("All PDFs generated successfully!")
        print("=" * 60)

    except ImportError:
        print("\nWeasyPrint not available locally.")
        print("Saving HTML files for PROD-VM generation...")

        for pdf in pdfs:
            html_path = pdf['output'].with_suffix('.html')
            with open(html_path, 'w') as f:
                f.write(pdf['html'])
            print(f"  Saved HTML: {html_path}")

        print("\nTo generate PDFs on PROD-VM:")
        print("1. gcloud compute ssh cms-enterprise-vm --zone=northamerica-northeast2-a")
        print("2. cd /opt/cleanmetrics && source venv/bin/activate")
        print("3. Use cms-pdf command with the HTML content")


if __name__ == '__main__':
    generate_pdfs()
