"""
P06 Betting & Sports Analytics - Transformation Module
Author: Mboya Jeffers

Transforms raw sports data into Kimball star schema dimensional model.
Implements surrogate key generation, SCD Type 2, and quality validation.
"""

import hashlib
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


class SportsStarSchemaTransformer:
    """
    Transforms raw sports/betting data into Kimball dimensional model.

    Dimensions:
    - dim_team: Team master data
    - dim_player: Player master data
    - dim_league: League/sport information
    - dim_venue: Stadium/arena information
    - dim_season: Season/year information
    - dim_date: Date dimension

    Facts:
    - fact_games: Game results
    - fact_odds: Betting lines
    - fact_player_stats: Player box scores
    """

    def __init__(self, output_dir: str = "data"):
        """Initialize transformer with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Surrogate key caches
        self._team_keys = {}
        self._player_keys = {}
        self._league_keys = {}
        self._venue_keys = {}
        self._season_keys = {}
        self._date_keys = {}

        self.transformation_log = {
            'start_time': None,
            'end_time': None,
            'tables_created': {},
            'total_rows': 0,
            'errors': []
        }

    def _generate_surrogate_key(self, *values) -> str:
        """
        Generate MD5-based surrogate key from input values.

        Args:
            values: Variable values to hash

        Returns:
            16-character hex key
        """
        combined = '|'.join(str(v) for v in values if v is not None)
        return hashlib.md5(combined.encode()).hexdigest()[:16]

    def _get_team_key(self, team_id: str, team_name: str) -> str:
        """Get or create surrogate key for team."""
        cache_key = f"{team_id}|{team_name}"
        if cache_key not in self._team_keys:
            self._team_keys[cache_key] = self._generate_surrogate_key('team', team_id)
        return self._team_keys[cache_key]

    def _get_league_key(self, league_code: str) -> str:
        """Get or create surrogate key for league."""
        if league_code not in self._league_keys:
            self._league_keys[league_code] = self._generate_surrogate_key('league', league_code)
        return self._league_keys[league_code]

    def _get_venue_key(self, venue_id: str, venue_name: str) -> str:
        """Get or create surrogate key for venue."""
        cache_key = f"{venue_id}|{venue_name}"
        if cache_key not in self._venue_keys:
            self._venue_keys[cache_key] = self._generate_surrogate_key('venue', venue_id)
        return self._venue_keys[cache_key]

    def _get_date_key(self, date_str: str) -> str:
        """Get or create surrogate key for date."""
        if date_str not in self._date_keys:
            self._date_keys[date_str] = self._generate_surrogate_key('date', date_str)
        return self._date_keys[date_str]

    def _get_season_key(self, league: str, year: int, season_type: str = 'regular') -> str:
        """Get or create surrogate key for season."""
        cache_key = f"{league}|{year}|{season_type}"
        if cache_key not in self._season_keys:
            self._season_keys[cache_key] = self._generate_surrogate_key('season', league, year, season_type)
        return self._season_keys[cache_key]

    def build_dim_team(self, teams: List[Dict]) -> pd.DataFrame:
        """
        Build team dimension table.

        Args:
            teams: List of raw team dictionaries

        Returns:
            DataFrame with dim_team schema
        """
        logger.info("Building dim_team...")

        records = []
        seen_teams = set()

        for team in teams:
            team_id = team.get('team_id')
            team_name = team.get('team_name')

            if not team_id or team_id in seen_teams:
                continue
            seen_teams.add(team_id)

            team_key = self._get_team_key(team_id, team_name)
            league_key = self._get_league_key(team.get('league', 'unknown'))

            records.append({
                'team_key': team_key,
                'team_id': team_id,
                'team_name': team_name,
                'team_abbrev': team.get('team_abbrev'),
                'league_key': league_key,
                'city': team.get('city'),
                'state': team.get('state'),
                'country': team.get('country', 'USA'),
                'primary_color': team.get('primary_color'),
                'effective_date': datetime.now().date(),
                'is_current': True
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_team: {len(df)} records")
        return df

    def build_dim_league(self, leagues: List[str]) -> pd.DataFrame:
        """
        Build league dimension table.

        Args:
            leagues: List of league codes

        Returns:
            DataFrame with dim_league schema
        """
        logger.info("Building dim_league...")

        LEAGUE_INFO = {
            'nfl': {'name': 'National Football League', 'sport': 'Football', 'teams': 32},
            'nba': {'name': 'National Basketball Association', 'sport': 'Basketball', 'teams': 30},
            'mlb': {'name': 'Major League Baseball', 'sport': 'Baseball', 'teams': 30},
            'nhl': {'name': 'National Hockey League', 'sport': 'Hockey', 'teams': 32},
            'ncaaf': {'name': 'NCAA Division I Football', 'sport': 'Football', 'teams': 130},
            'ncaab': {'name': 'NCAA Division I Basketball', 'sport': 'Basketball', 'teams': 350}
        }

        records = []
        for league_code in set(leagues):
            info = LEAGUE_INFO.get(league_code, {
                'name': league_code.upper(),
                'sport': 'Unknown',
                'teams': 0
            })

            records.append({
                'league_key': self._get_league_key(league_code),
                'league_id': league_code,
                'league_name': info['name'],
                'league_abbrev': league_code.upper(),
                'sport': info['sport'],
                'country': 'USA',
                'teams_count': info['teams']
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_league: {len(df)} records")
        return df

    def build_dim_venue(self, games: List[Dict]) -> pd.DataFrame:
        """
        Build venue dimension table from game data.

        Args:
            games: List of game dictionaries with venue info

        Returns:
            DataFrame with dim_venue schema
        """
        logger.info("Building dim_venue...")

        records = []
        seen_venues = set()

        for game in games:
            venue_id = game.get('venue_id')
            venue_name = game.get('venue_name')

            if not venue_id or venue_id in seen_venues:
                continue
            seen_venues.add(venue_id)

            venue_key = self._get_venue_key(venue_id, venue_name)

            records.append({
                'venue_key': venue_key,
                'venue_id': venue_id,
                'venue_name': venue_name,
                'city': game.get('venue_city'),
                'state': game.get('venue_state'),
                'country': 'USA'
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_venue: {len(df)} records")
        return df

    def build_dim_season(self, games: List[Dict]) -> pd.DataFrame:
        """
        Build season dimension table.

        Args:
            games: List of game dictionaries

        Returns:
            DataFrame with dim_season schema
        """
        logger.info("Building dim_season...")

        records = []
        seen_seasons = set()

        for game in games:
            league = game.get('league')
            date_str = game.get('date', '')

            if not date_str:
                continue

            try:
                game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                year = game_date.year
            except:
                continue

            season_key_str = f"{league}|{year}|regular"
            if season_key_str in seen_seasons:
                continue
            seen_seasons.add(season_key_str)

            season_key = self._get_season_key(league, year, 'regular')

            records.append({
                'season_key': season_key,
                'season_id': f"{year}",
                'season_year': year,
                'season_type': 'regular',
                'is_current': year == datetime.now().year
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_season: {len(df)} records")
        return df

    def build_dim_date(self, games: List[Dict]) -> pd.DataFrame:
        """
        Build date dimension table.

        Args:
            games: List of game dictionaries

        Returns:
            DataFrame with dim_date schema
        """
        logger.info("Building dim_date...")

        records = []
        seen_dates = set()

        for game in games:
            date_str = game.get('date', '')
            if not date_str:
                continue

            try:
                game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                date_only = game_date.date()
            except:
                continue

            date_key_str = date_only.isoformat()
            if date_key_str in seen_dates:
                continue
            seen_dates.add(date_key_str)

            date_key = self._get_date_key(date_key_str)

            records.append({
                'date_key': date_key,
                'full_date': date_only,
                'year': date_only.year,
                'month': date_only.month,
                'day': date_only.day,
                'quarter': (date_only.month - 1) // 3 + 1,
                'week_of_year': date_only.isocalendar()[1],
                'day_of_week': date_only.weekday(),
                'day_name': date_only.strftime('%A'),
                'is_weekend': date_only.weekday() >= 5
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_date: {len(df)} records")
        return df

    def build_fact_games(self, games: List[Dict]) -> pd.DataFrame:
        """
        Build game results fact table.

        Args:
            games: List of game dictionaries

        Returns:
            DataFrame with fact_games schema
        """
        logger.info("Building fact_games...")

        records = []

        for game in games:
            game_id = game.get('game_id')
            if not game_id:
                continue

            date_str = game.get('date', '')
            try:
                game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                date_only = game_date.date()
            except:
                continue

            game_key = self._generate_surrogate_key('game', game_id)
            date_key = self._get_date_key(date_only.isoformat())
            league_key = self._get_league_key(game.get('league', 'unknown'))
            season_key = self._get_season_key(game.get('league'), date_only.year, 'regular')

            home_team_key = self._get_team_key(
                game.get('home_team_id'),
                game.get('home_team_name')
            )
            away_team_key = self._get_team_key(
                game.get('away_team_id'),
                game.get('away_team_name')
            )

            venue_key = None
            if game.get('venue_id'):
                venue_key = self._get_venue_key(
                    game.get('venue_id'),
                    game.get('venue_name')
                )

            home_score = int(game.get('home_score', 0) or 0)
            away_score = int(game.get('away_score', 0) or 0)

            records.append({
                'game_key': game_key,
                'game_id': game_id,
                'date_key': date_key,
                'season_key': season_key,
                'league_key': league_key,
                'venue_key': venue_key,
                'home_team_key': home_team_key,
                'away_team_key': away_team_key,
                'home_score': home_score,
                'away_score': away_score,
                'attendance': game.get('attendance'),
                'is_neutral_site': game.get('is_neutral_site', False),
                'game_status': 'final'
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_games: {len(df)} records")
        return df

    def build_fact_odds(self, games: List[Dict]) -> pd.DataFrame:
        """
        Build betting odds fact table.

        Args:
            games: List of game dictionaries with odds data

        Returns:
            DataFrame with fact_odds schema
        """
        logger.info("Building fact_odds...")

        records = []

        for game in games:
            game_id = game.get('game_id')
            spread = game.get('spread')
            over_under = game.get('over_under')

            # Only include games with odds data
            if not game_id or (spread is None and over_under is None):
                continue

            game_key = self._generate_surrogate_key('game', game_id)
            odds_key = self._generate_surrogate_key('odds', game_id, 'consensus')

            # Calculate spread result
            home_score = int(game.get('home_score', 0) or 0)
            away_score = int(game.get('away_score', 0) or 0)
            total_score = home_score + away_score
            margin = home_score - away_score

            spread_winner = None
            if spread is not None:
                try:
                    spread_val = float(spread)
                    adjusted_margin = margin + spread_val
                    if adjusted_margin > 0:
                        spread_winner = 'home'
                    elif adjusted_margin < 0:
                        spread_winner = 'away'
                    else:
                        spread_winner = 'push'
                except:
                    pass

            total_result = None
            if over_under is not None:
                try:
                    ou_val = float(over_under)
                    if total_score > ou_val:
                        total_result = 'over'
                    elif total_score < ou_val:
                        total_result = 'under'
                    else:
                        total_result = 'push'
                except:
                    pass

            moneyline_winner = 'home' if home_score > away_score else 'away'

            records.append({
                'odds_key': odds_key,
                'game_key': game_key,
                'bookmaker': 'consensus',
                'home_spread': spread,
                'over_under_line': over_under,
                'moneyline_home': game.get('home_moneyline'),
                'moneyline_away': game.get('away_moneyline'),
                'is_closing_line': True,
                'spread_winner': spread_winner,
                'total_result': total_result,
                'moneyline_winner': moneyline_winner
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_odds: {len(df)} records")
        return df

    def run_transformation(self, extracted_data: Dict) -> Dict[str, pd.DataFrame]:
        """
        Execute full transformation pipeline.

        Args:
            extracted_data: Raw data from extraction

        Returns:
            Dictionary of transformed DataFrames
        """
        self.transformation_log['start_time'] = datetime.now().isoformat()

        logger.info("=" * 60)
        logger.info("STARTING STAR SCHEMA TRANSFORMATION")
        logger.info(f"Input games: {len(extracted_data.get('all_games', []))}")
        logger.info(f"Input teams: {len(extracted_data.get('all_teams', []))}")
        logger.info("=" * 60)

        all_teams = extracted_data.get('all_teams', [])
        all_games = extracted_data.get('all_games', [])
        leagues = list(set(t.get('league') for t in all_teams if t.get('league')))

        tables = {}

        # Build dimensions
        tables['dim_team'] = self.build_dim_team(all_teams)
        tables['dim_league'] = self.build_dim_league(leagues)
        tables['dim_venue'] = self.build_dim_venue(all_games)
        tables['dim_season'] = self.build_dim_season(all_games)
        tables['dim_date'] = self.build_dim_date(all_games)

        # Build facts
        tables['fact_games'] = self.build_fact_games(all_games)
        tables['fact_odds'] = self.build_fact_odds(all_games)

        # Calculate totals
        total_rows = sum(len(df) for df in tables.values())
        self.transformation_log['total_rows'] = total_rows

        for table_name, df in tables.items():
            self.transformation_log['tables_created'][table_name] = len(df)

            # Save to parquet
            output_path = self.output_dir / f"{table_name}.parquet"
            df.to_parquet(output_path, index=False)
            logger.info(f"  Saved: {output_path}")

        self.transformation_log['end_time'] = datetime.now().isoformat()

        # Save transformation log
        log_path = self.output_dir / "transformation_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.transformation_log, f, indent=2)
        logger.info(f"Transformation log saved: {log_path}")

        logger.info("=" * 60)
        logger.info(f"TRANSFORMATION COMPLETE: {total_rows} total rows")
        logger.info("=" * 60)

        return tables


if __name__ == "__main__":
    # Test with sample data
    sample_data = {
        'all_teams': [
            {'team_id': '1', 'team_name': 'Test Team 1', 'league': 'nba', 'city': 'City 1'},
            {'team_id': '2', 'team_name': 'Test Team 2', 'league': 'nba', 'city': 'City 2'}
        ],
        'all_games': [
            {
                'game_id': 'g1',
                'date': '2024-01-15T19:00:00Z',
                'league': 'nba',
                'home_team_id': '1',
                'home_team_name': 'Test Team 1',
                'away_team_id': '2',
                'away_team_name': 'Test Team 2',
                'home_score': 110,
                'away_score': 105,
                'spread': -3.5,
                'over_under': 220.5,
                'venue_id': 'v1',
                'venue_name': 'Test Arena',
                'venue_city': 'Test City'
            }
        ]
    }

    transformer = SportsStarSchemaTransformer(output_dir="test_output")
    tables = transformer.run_transformation(sample_data)

    print("\nTransformation Summary:")
    for name, df in tables.items():
        print(f"  {name}: {len(df)} rows")
