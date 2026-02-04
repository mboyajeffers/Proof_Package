"""
Enterprise ETL Framework - ESPN Extractor
Extracts sports data from ESPN public APIs.

Data Sources:
- ESPN Site API: Scores, teams, standings, athletes

Supported Leagues:
- NFL, NBA, MLB, NHL
- College Football, College Basketball
- Soccer (MLS, Premier League, etc.)

Rate Limits:
- Public API: ~60 requests/min (conservative)
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import sys

    

from ..core.base_extractor import BaseExtractor, ExtractionResult
from ..config import ENDPOINTS, RATE_LIMITS


class ESPNExtractor(BaseExtractor):
    """
    Extractor for ESPN sports data.

    Extracts:
    - Team information (name, location, colors, venue)
    - League standings
    - Game scores and schedules
    - Athlete data

    Example:
        extractor = ESPNExtractor()
        result = extractor.extract_league('nfl')
    """

    # Supported sports and leagues
    LEAGUES = {
        'nfl': {'sport': 'football', 'league': 'nfl', 'name': 'NFL'},
        'nba': {'sport': 'basketball', 'league': 'nba', 'name': 'NBA'},
        'mlb': {'sport': 'baseball', 'league': 'mlb', 'name': 'MLB'},
        'nhl': {'sport': 'hockey', 'league': 'nhl', 'name': 'NHL'},
        'ncaaf': {'sport': 'football', 'league': 'college-football', 'name': 'NCAA Football'},
        'ncaab': {'sport': 'basketball', 'league': 'mens-college-basketball', 'name': 'NCAA Basketball'},
        'mls': {'sport': 'soccer', 'league': 'usa.1', 'name': 'MLS'},
        'epl': {'sport': 'soccer', 'league': 'eng.1', 'name': 'Premier League'},
    }

    BASE_URL = 'https://site.api.espn.com/apis/site/v2/sports'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_source_name(self) -> str:
        return 'ESPN'

    def get_endpoints(self) -> Dict[str, str]:
        return {
            'teams': f'{self.BASE_URL}/{{sport}}/{{league}}/teams',
            'standings': f'{self.BASE_URL}/{{sport}}/{{league}}/standings',
            'scoreboard': f'{self.BASE_URL}/{{sport}}/{{league}}/scoreboard',
            'athletes': f'{self.BASE_URL}/{{sport}}/{{league}}/athletes',
        }

    def get_rate_limit(self) -> int:
        return RATE_LIMITS.get('ESPN', 60)

    def extract(
        self,
        leagues: Optional[List[str]] = None,
        include_standings: bool = True,
        include_scores: bool = True,
        include_athletes: bool = False,
    ) -> ExtractionResult:
        """
        Extract sports data from ESPN for multiple leagues.

        Args:
            leagues: List of league codes (nfl, nba, etc.). Default: major US leagues
            include_standings: Fetch current standings
            include_scores: Fetch recent/upcoming scores
            include_athletes: Fetch athlete data (slower)

        Returns:
            ExtractionResult with combined sports data
        """
        result = ExtractionResult(success=False, source=self.get_source_name())
        all_data = []

        if leagues is None:
            leagues = ['nfl', 'nba', 'mlb', 'nhl']

        try:
            self.logger.info(f"Starting ESPN extraction for leagues: {leagues}")

            for league_code in leagues:
                if league_code not in self.LEAGUES:
                    self.logger.warning(f"Unknown league: {league_code}")
                    continue

                league_result = self.extract_league(
                    league_code,
                    include_standings=include_standings,
                    include_scores=include_scores,
                    include_athletes=include_athletes,
                )

                if league_result.success and league_result.data:
                    all_data.extend(league_result.data)
                    result.records_extracted += league_result.records_extracted

                time.sleep(0.5)  # Rate limit between leagues

            result.success = True
            result.data = all_data
            result.completed_at = datetime.utcnow().isoformat() + 'Z'
            result.metadata = {
                'leagues': leagues,
                'total_records': len(all_data),
                'api_calls': self._api_calls,
            }

            self.logger.info(f"Extraction complete: {len(all_data)} records from {len(leagues)} leagues")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            result.error = str(e)

        return result

    def extract_league(
        self,
        league_code: str,
        include_standings: bool = True,
        include_scores: bool = True,
        include_athletes: bool = False,
    ) -> ExtractionResult:
        """
        Extract data for a single league.

        Args:
            league_code: League identifier (nfl, nba, etc.)
            include_standings: Fetch standings
            include_scores: Fetch scores
            include_athletes: Fetch athletes

        Returns:
            ExtractionResult with league data
        """
        result = ExtractionResult(success=False, source=self.get_source_name())

        if league_code not in self.LEAGUES:
            result.error = f"Unknown league: {league_code}"
            return result

        league_info = self.LEAGUES[league_code]
        sport = league_info['sport']
        league = league_info['league']
        league_name = league_info['name']

        try:
            self.logger.info(f"Extracting {league_name} data")
            league_data = []

            # Get teams
            teams = self._get_teams(sport, league)
            if teams:
                for team in teams:
                    team_data = self._parse_team(team, league_code, league_name)
                    team_data['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                    league_data.append(team_data)
                    result.records_extracted += 1

            # Get standings
            if include_standings:
                standings = self._get_standings(sport, league)
                if standings:
                    # Merge standings into team data
                    standings_map = {s['team_id']: s for s in standings}
                    for team in league_data:
                        if team['team_id'] in standings_map:
                            team.update(standings_map[team['team_id']])

            # Get recent scores
            if include_scores:
                scores = self._get_scoreboard(sport, league)
                # Store scores separately in metadata for now
                result.metadata = result.metadata or {}
                result.metadata['recent_games'] = scores

            result.success = True
            result.data = league_data
            result.completed_at = datetime.utcnow().isoformat() + 'Z'

            self.logger.info(f"Extracted {len(league_data)} teams from {league_name}")

        except Exception as e:
            self.logger.error(f"League extraction failed: {e}")
            result.error = str(e)

        return result

    def _get_teams(self, sport: str, league: str) -> List[Dict]:
        """Fetch all teams for a league."""
        url = self.get_endpoints()['teams'].format(sport=sport, league=league)
        response = self._get(url, params={'limit': 100})

        if not response:
            return []

        teams = response.get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', [])
        return [t.get('team', t) for t in teams]

    def _parse_team(self, team: Dict, league_code: str, league_name: str) -> Dict:
        """Parse team data into flat structure."""
        result = {
            'team_id': team.get('id', ''),
            'league_code': league_code,
            'league_name': league_name,
            'abbreviation': team.get('abbreviation', ''),
            'display_name': team.get('displayName', ''),
            'short_name': team.get('shortDisplayName', ''),
            'nickname': team.get('nickname', ''),
            'location': team.get('location', ''),
            'is_active': team.get('isActive', True),
        }

        # Logos
        logos = team.get('logos', [])
        result['logo_url'] = logos[0].get('href', '') if logos else ''

        # Colors
        result['color_primary'] = team.get('color', '')
        result['color_alternate'] = team.get('alternateColor', '')

        # Venue
        venue = team.get('venue', {}) or {}
        result['venue_id'] = venue.get('id', '')
        result['venue_name'] = venue.get('fullName', '')
        result['venue_city'] = venue.get('address', {}).get('city', '') if venue.get('address') else ''
        result['venue_state'] = venue.get('address', {}).get('state', '') if venue.get('address') else ''
        result['venue_capacity'] = venue.get('capacity')
        result['venue_indoor'] = venue.get('indoor', False)

        # Links
        links = team.get('links', [])
        for link in links:
            if link.get('rel', [None])[0] == 'clubhouse':
                result['espn_url'] = link.get('href', '')
                break

        return result

    def _get_standings(self, sport: str, league: str) -> List[Dict]:
        """Fetch league standings."""
        url = self.get_endpoints()['standings'].format(sport=sport, league=league)
        response = self._get(url)

        if not response:
            return []

        standings = []
        children = response.get('children', [])

        for group in children:
            group_name = group.get('name', '')
            for entry_group in group.get('standings', {}).get('entries', []):
                team_data = self._parse_standings_entry(entry_group, group_name)
                if team_data:
                    standings.append(team_data)

        return standings

    def _parse_standings_entry(self, entry: Dict, group_name: str) -> Dict:
        """Parse a standings entry."""
        team = entry.get('team', {})
        stats = entry.get('stats', [])

        result = {
            'team_id': team.get('id', ''),
            'division': group_name,
        }

        # Parse stats (wins, losses, etc.)
        for stat in stats:
            name = stat.get('name', '').lower()
            value = stat.get('value')
            display = stat.get('displayValue', '')

            if name == 'wins':
                result['wins'] = int(value) if value is not None else 0
            elif name == 'losses':
                result['losses'] = int(value) if value is not None else 0
            elif name == 'ties':
                result['ties'] = int(value) if value is not None else 0
            elif name == 'winpercent' or name == 'winningpercent':
                result['win_pct'] = float(value) if value is not None else 0.0
            elif name == 'pointsfor' or name == 'runsfor':
                result['points_for'] = int(value) if value is not None else 0
            elif name == 'pointsagainst' or name == 'runsagainst':
                result['points_against'] = int(value) if value is not None else 0
            elif name == 'streak':
                result['streak'] = display
            elif name == 'playoffseed':
                result['playoff_seed'] = int(value) if value is not None else None
            elif name == 'gamesback':
                result['games_back'] = float(value) if value is not None else 0.0
            elif name == 'divisionrecord':
                result['division_record'] = display
            elif name == 'conferencerecord':
                result['conference_record'] = display

        return result

    def _get_scoreboard(self, sport: str, league: str) -> List[Dict]:
        """Fetch recent/current scores."""
        url = self.get_endpoints()['scoreboard'].format(sport=sport, league=league)
        response = self._get(url)

        if not response:
            return []

        games = []
        events = response.get('events', [])

        for event in events:
            game = self._parse_game(event)
            if game:
                games.append(game)

        return games

    def _parse_game(self, event: Dict) -> Dict:
        """Parse a game/event."""
        result = {
            'game_id': event.get('id', ''),
            'name': event.get('name', ''),
            'short_name': event.get('shortName', ''),
            'date': event.get('date', ''),
            'status': event.get('status', {}).get('type', {}).get('name', ''),
            'status_detail': event.get('status', {}).get('type', {}).get('detail', ''),
        }

        # Venue
        venue = event.get('competitions', [{}])[0].get('venue', {})
        result['venue_name'] = venue.get('fullName', '')
        result['venue_city'] = venue.get('address', {}).get('city', '') if venue.get('address') else ''

        # Teams and scores
        competitors = event.get('competitions', [{}])[0].get('competitors', [])
        for comp in competitors:
            team = comp.get('team', {})
            home_away = comp.get('homeAway', '')
            prefix = 'home' if home_away == 'home' else 'away'

            result[f'{prefix}_team_id'] = team.get('id', '')
            result[f'{prefix}_team'] = team.get('displayName', '')
            result[f'{prefix}_score'] = comp.get('score', '')
            result[f'{prefix}_winner'] = comp.get('winner', False)

        return result

    def extract_teams(self, league_code: str) -> ExtractionResult:
        """
        Extract just team data for a league.

        Args:
            league_code: League identifier

        Returns:
            ExtractionResult with teams
        """
        return self.extract_league(
            league_code,
            include_standings=True,
            include_scores=False,
            include_athletes=False,
        )

    def extract_all_leagues(self) -> ExtractionResult:
        """
        Extract data from all supported leagues.

        Returns:
            ExtractionResult with all leagues data
        """
        return self.extract(
            leagues=list(self.LEAGUES.keys()),
            include_standings=True,
            include_scores=False,
            include_athletes=False,
        )
