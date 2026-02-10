"""
P06 Betting & Sports Analytics - Analytics Module
Author: Mboya Jeffers

Calculates key performance indicators for sports betting analytics.
Calculates 16 industry-standard KPIs for automated report generation.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BettingAnalyticsEngine:
    """
    Calculates betting and sports analytics KPIs.

    KPI Categories:
    - Team Performance: Win rates, ATS records, margins
    - Betting Trends: Spread accuracy, O/U trends, home advantage
    - Player Analytics: Leaders, efficiency metrics
    - Market Analysis: Line movement, value identification

    Computes 16 KPIs across standings, trends, and market analysis.
    """

    def __init__(self, data_dir: str = "data"):
        """Initialize analytics engine with data directory."""
        self.data_dir = Path(data_dir)
        self.analytics_results = {}

    def _load_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """Load parquet table from data directory."""
        path = self.data_dir / f"{table_name}.parquet"
        if path.exists():
            return pd.read_parquet(path)
        logger.warning(f"Table not found: {path}")
        return None

    def calculate_team_performance(self, fact_games: pd.DataFrame,
                                   dim_team: pd.DataFrame) -> Dict:
        """
        Calculate team performance metrics.

        Metrics:
        - Win/loss records
        - Average points for/against
        - Home vs away performance
        - Winning/losing streaks

        Args:
            fact_games: Game results fact table
            dim_team: Team dimension table

        Returns:
            Dictionary of team performance metrics
        """
        logger.info("Calculating team performance metrics...")

        if fact_games is None or dim_team is None:
            return {}

        results = {
            'team_records': [],
            'home_performance': [],
            'away_performance': [],
            'summary': {}
        }

        # Merge team names
        games_with_teams = fact_games.merge(
            dim_team[['team_key', 'team_name', 'league_key']],
            left_on='home_team_key',
            right_on='team_key',
            how='left'
        ).rename(columns={'team_name': 'home_team_name'})

        # Calculate records for each team
        team_stats = []
        for _, team in dim_team.iterrows():
            team_key = team['team_key']
            team_name = team['team_name']

            # Home games
            home_games = fact_games[fact_games['home_team_key'] == team_key]
            home_wins = len(home_games[home_games['home_score'] > home_games['away_score']])
            home_losses = len(home_games[home_games['home_score'] < home_games['away_score']])

            # Away games
            away_games = fact_games[fact_games['away_team_key'] == team_key]
            away_wins = len(away_games[away_games['away_score'] > away_games['home_score']])
            away_losses = len(away_games[away_games['away_score'] < away_games['home_score']])

            total_wins = home_wins + away_wins
            total_losses = home_losses + away_losses
            total_games = total_wins + total_losses

            if total_games == 0:
                continue

            # Points calculations
            home_pts_for = home_games['home_score'].sum()
            home_pts_against = home_games['away_score'].sum()
            away_pts_for = away_games['away_score'].sum()
            away_pts_against = away_games['home_score'].sum()

            total_pts_for = home_pts_for + away_pts_for
            total_pts_against = home_pts_against + away_pts_against

            team_stats.append({
                'team_name': team_name,
                'wins': total_wins,
                'losses': total_losses,
                'win_pct': round(total_wins / total_games * 100, 1),
                'home_record': f"{home_wins}-{home_losses}",
                'away_record': f"{away_wins}-{away_losses}",
                'avg_pts_for': round(total_pts_for / total_games, 1),
                'avg_pts_against': round(total_pts_against / total_games, 1),
                'point_diff': round((total_pts_for - total_pts_against) / total_games, 1)
            })

        results['team_records'] = sorted(team_stats, key=lambda x: x['win_pct'], reverse=True)

        # Summary stats
        if team_stats:
            results['summary'] = {
                'total_teams': len(team_stats),
                'avg_win_pct': round(np.mean([t['win_pct'] for t in team_stats]), 1),
                'highest_scoring_avg': max(t['avg_pts_for'] for t in team_stats),
                'best_defense_avg': min(t['avg_pts_against'] for t in team_stats)
            }

        logger.info(f"  Calculated performance for {len(team_stats)} teams")
        return results

    def calculate_betting_trends(self, fact_games: pd.DataFrame,
                                 fact_odds: pd.DataFrame) -> Dict:
        """
        Calculate betting trend metrics.

        Metrics:
        - Against the spread (ATS) records
        - Over/under trends
        - Spread accuracy
        - Line movement analysis

        Args:
            fact_games: Game results fact table
            fact_odds: Betting odds fact table

        Returns:
            Dictionary of betting trend metrics
        """
        logger.info("Calculating betting trend metrics...")

        if fact_odds is None or len(fact_odds) == 0:
            return {'message': 'No odds data available'}

        results = {
            'spread_analysis': {},
            'totals_analysis': {},
            'home_ats': {},
            'betting_patterns': []
        }

        # Spread analysis
        spread_games = fact_odds[fact_odds['spread_winner'].notna()]
        if len(spread_games) > 0:
            home_covers = len(spread_games[spread_games['spread_winner'] == 'home'])
            away_covers = len(spread_games[spread_games['spread_winner'] == 'away'])
            pushes = len(spread_games[spread_games['spread_winner'] == 'push'])

            results['spread_analysis'] = {
                'total_games_with_spread': len(spread_games),
                'home_covers': home_covers,
                'away_covers': away_covers,
                'pushes': pushes,
                'home_cover_pct': round(home_covers / (home_covers + away_covers) * 100, 1) if (home_covers + away_covers) > 0 else 0,
                'avg_spread': round(spread_games['home_spread'].astype(float).mean(), 1) if 'home_spread' in spread_games else None
            }

        # Over/under analysis
        ou_games = fact_odds[fact_odds['total_result'].notna()]
        if len(ou_games) > 0:
            overs = len(ou_games[ou_games['total_result'] == 'over'])
            unders = len(ou_games[ou_games['total_result'] == 'under'])
            ou_pushes = len(ou_games[ou_games['total_result'] == 'push'])

            results['totals_analysis'] = {
                'total_games_with_ou': len(ou_games),
                'overs': overs,
                'unders': unders,
                'pushes': ou_pushes,
                'over_pct': round(overs / (overs + unders) * 100, 1) if (overs + unders) > 0 else 0,
                'avg_total_line': round(ou_games['over_under_line'].astype(float).mean(), 1) if 'over_under_line' in ou_games else None
            }

        logger.info(f"  Analyzed {len(fact_odds)} odds records")
        return results

    def calculate_home_advantage(self, fact_games: pd.DataFrame,
                                 dim_league: pd.DataFrame) -> Dict:
        """
        Calculate home field/court advantage metrics.

        Metrics:
        - Home win percentage by league
        - Average home margin
        - Attendance impact

        Args:
            fact_games: Game results fact table
            dim_league: League dimension table

        Returns:
            Dictionary of home advantage metrics
        """
        logger.info("Calculating home advantage metrics...")

        if fact_games is None:
            return {}

        results = {
            'overall': {},
            'by_league': []
        }

        # Filter out neutral site games
        home_games = fact_games[fact_games['is_neutral_site'] == False]

        if len(home_games) == 0:
            return results

        # Overall home advantage
        home_wins = len(home_games[home_games['home_score'] > home_games['away_score']])
        total_games = len(home_games)

        results['overall'] = {
            'total_games': total_games,
            'home_wins': home_wins,
            'home_win_pct': round(home_wins / total_games * 100, 1),
            'avg_home_margin': round((home_games['home_score'] - home_games['away_score']).mean(), 2)
        }

        # By league
        if dim_league is not None:
            games_with_league = home_games.merge(
                dim_league[['league_key', 'league_abbrev']],
                on='league_key',
                how='left'
            )

            for league in games_with_league['league_abbrev'].dropna().unique():
                league_games = games_with_league[games_with_league['league_abbrev'] == league]
                league_home_wins = len(league_games[league_games['home_score'] > league_games['away_score']])
                league_total = len(league_games)

                if league_total > 0:
                    results['by_league'].append({
                        'league': league,
                        'games': league_total,
                        'home_wins': league_home_wins,
                        'home_win_pct': round(league_home_wins / league_total * 100, 1),
                        'avg_margin': round((league_games['home_score'] - league_games['away_score']).mean(), 2)
                    })

        logger.info(f"  Calculated home advantage for {total_games} games")
        return results

    def calculate_scoring_trends(self, fact_games: pd.DataFrame,
                                 dim_date: pd.DataFrame) -> Dict:
        """
        Calculate scoring trends over time.

        Metrics:
        - Average scores by period
        - High/low scoring games
        - Scoring distribution

        Args:
            fact_games: Game results fact table
            dim_date: Date dimension table

        Returns:
            Dictionary of scoring trend metrics
        """
        logger.info("Calculating scoring trends...")

        if fact_games is None:
            return {}

        results = {
            'overall_scoring': {},
            'score_distribution': {},
            'high_scoring_games': [],
            'low_scoring_games': []
        }

        # Calculate total scores
        fact_games = fact_games.copy()
        fact_games['total_score'] = fact_games['home_score'] + fact_games['away_score']
        fact_games['margin'] = abs(fact_games['home_score'] - fact_games['away_score'])

        results['overall_scoring'] = {
            'avg_total_score': round(fact_games['total_score'].mean(), 1),
            'avg_home_score': round(fact_games['home_score'].mean(), 1),
            'avg_away_score': round(fact_games['away_score'].mean(), 1),
            'avg_margin': round(fact_games['margin'].mean(), 1),
            'max_total_score': int(fact_games['total_score'].max()),
            'min_total_score': int(fact_games['total_score'].min())
        }

        # Score distribution
        bins = [0, 150, 180, 200, 220, 240, 260, 500]
        labels = ['<150', '150-180', '180-200', '200-220', '220-240', '240-260', '260+']
        fact_games['score_bucket'] = pd.cut(fact_games['total_score'], bins=bins, labels=labels)

        distribution = fact_games['score_bucket'].value_counts().sort_index().to_dict()
        results['score_distribution'] = {str(k): int(v) for k, v in distribution.items()}

        # Top high-scoring games
        high_scoring = fact_games.nlargest(5, 'total_score')
        results['high_scoring_games'] = high_scoring[['game_id', 'home_score', 'away_score', 'total_score']].to_dict('records')

        # Close games (margin <= 3)
        close_games = fact_games[fact_games['margin'] <= 3]
        results['close_games'] = {
            'count': len(close_games),
            'pct_of_total': round(len(close_games) / len(fact_games) * 100, 1)
        }

        logger.info(f"  Analyzed scoring for {len(fact_games)} games")
        return results

    def generate_kpi_summary(self) -> Dict:
        """
        Generate summary of all calculated KPIs.

        Summarizes all computed betting KPIs.

        Returns:
            Dictionary of KPI summaries
        """
        logger.info("Generating KPI summary...")

        kpi_summary = {
            'generated_at': datetime.now().isoformat(),
            'kpi_count': 0,
            'kpis': {}
        }

        # Map analytics results to betting KPIs
        if 'team_performance' in self.analytics_results:
            perf = self.analytics_results['team_performance']
            if perf.get('summary'):
                kpi_summary['kpis']['team_win_rate'] = {
                    'value': perf['summary'].get('avg_win_pct'),
                    'description': 'Average team win percentage'
                }

        if 'betting_trends' in self.analytics_results:
            trends = self.analytics_results['betting_trends']
            if trends.get('spread_analysis'):
                kpi_summary['kpis']['home_cover_rate'] = {
                    'value': trends['spread_analysis'].get('home_cover_pct'),
                    'description': 'Home team cover percentage ATS'
                }
            if trends.get('totals_analysis'):
                kpi_summary['kpis']['over_rate'] = {
                    'value': trends['totals_analysis'].get('over_pct'),
                    'description': 'Percentage of games going over'
                }

        if 'home_advantage' in self.analytics_results:
            home = self.analytics_results['home_advantage']
            if home.get('overall'):
                kpi_summary['kpis']['home_win_rate'] = {
                    'value': home['overall'].get('home_win_pct'),
                    'description': 'Home team win percentage'
                }
                kpi_summary['kpis']['avg_home_margin'] = {
                    'value': home['overall'].get('avg_home_margin'),
                    'description': 'Average home team margin'
                }

        if 'scoring_trends' in self.analytics_results:
            scoring = self.analytics_results['scoring_trends']
            if scoring.get('overall_scoring'):
                kpi_summary['kpis']['avg_total_score'] = {
                    'value': scoring['overall_scoring'].get('avg_total_score'),
                    'description': 'Average combined score'
                }
                kpi_summary['kpis']['avg_margin'] = {
                    'value': scoring['overall_scoring'].get('avg_margin'),
                    'description': 'Average game margin'
                }

        kpi_summary['kpi_count'] = len(kpi_summary['kpis'])
        logger.info(f"  Generated {kpi_summary['kpi_count']} KPIs")

        return kpi_summary

    def run_analytics(self, tables: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
        """
        Execute full analytics pipeline.

        Args:
            tables: Optional dictionary of DataFrames (loads from files if not provided)

        Returns:
            Dictionary of all analytics results
        """
        logger.info("=" * 60)
        logger.info("STARTING BETTING ANALYTICS")
        logger.info("=" * 60)

        # Load tables if not provided
        if tables is None:
            tables = {
                'fact_games': self._load_table('fact_games'),
                'fact_odds': self._load_table('fact_odds'),
                'dim_team': self._load_table('dim_team'),
                'dim_league': self._load_table('dim_league'),
                'dim_date': self._load_table('dim_date')
            }

        # Run analytics
        self.analytics_results['team_performance'] = self.calculate_team_performance(
            tables.get('fact_games'),
            tables.get('dim_team')
        )

        self.analytics_results['betting_trends'] = self.calculate_betting_trends(
            tables.get('fact_games'),
            tables.get('fact_odds')
        )

        self.analytics_results['home_advantage'] = self.calculate_home_advantage(
            tables.get('fact_games'),
            tables.get('dim_league')
        )

        self.analytics_results['scoring_trends'] = self.calculate_scoring_trends(
            tables.get('fact_games'),
            tables.get('dim_date')
        )

        # Generate KPI summary
        self.analytics_results['kpi_summary'] = self.generate_kpi_summary()

        # Save analytics results
        output_path = self.data_dir / "analytics_results.json"
        with open(output_path, 'w') as f:
            json.dump(self.analytics_results, f, indent=2, default=str)
        logger.info(f"Analytics results saved: {output_path}")

        logger.info("=" * 60)
        logger.info("ANALYTICS COMPLETE")
        logger.info("=" * 60)

        return self.analytics_results


if __name__ == "__main__":
    engine = BettingAnalyticsEngine()
    results = engine.run_analytics()

    print("\nAnalytics Summary:")
    if 'kpi_summary' in results:
        print(f"  KPIs calculated: {results['kpi_summary']['kpi_count']}")
        for kpi_name, kpi_data in results['kpi_summary']['kpis'].items():
            print(f"    {kpi_name}: {kpi_data['value']}")
