-- P06 Betting & Sports Analytics - Star Schema DDL
-- Author: Mboya Jeffers
-- Database: PostgreSQL
-- Pattern: Kimball Dimensional Model

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Team Dimension
CREATE TABLE IF NOT EXISTS dim_team (
    team_key        VARCHAR(16) PRIMARY KEY,
    team_id         VARCHAR(20) NOT NULL,
    team_name       VARCHAR(100) NOT NULL,
    team_abbrev     VARCHAR(10),
    league_key      VARCHAR(16),
    city            VARCHAR(100),
    state           VARCHAR(50),
    country         VARCHAR(50) DEFAULT 'USA',
    founded_year    INTEGER,
    venue_key       VARCHAR(16),
    primary_color   VARCHAR(7),
    effective_date  DATE NOT NULL,
    is_current      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_team_id ON dim_team(team_id);
CREATE INDEX idx_dim_team_name ON dim_team(team_name);
CREATE INDEX idx_dim_team_league ON dim_team(league_key);

-- Player Dimension
CREATE TABLE IF NOT EXISTS dim_player (
    player_key      VARCHAR(16) PRIMARY KEY,
    player_id       VARCHAR(20) NOT NULL,
    player_name     VARCHAR(150) NOT NULL,
    position        VARCHAR(30),
    jersey_number   INTEGER,
    height_inches   INTEGER,
    weight_lbs      INTEGER,
    birth_date      DATE,
    birth_city      VARCHAR(100),
    college         VARCHAR(100),
    draft_year      INTEGER,
    draft_round     INTEGER,
    draft_pick      INTEGER,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_player_id ON dim_player(player_id);
CREATE INDEX idx_dim_player_name ON dim_player(player_name);
CREATE INDEX idx_dim_player_position ON dim_player(position);

-- League/Sport Dimension
CREATE TABLE IF NOT EXISTS dim_league (
    league_key      VARCHAR(16) PRIMARY KEY,
    league_id       VARCHAR(20) NOT NULL,
    league_name     VARCHAR(100) NOT NULL,
    league_abbrev   VARCHAR(10),
    sport           VARCHAR(50) NOT NULL,
    country         VARCHAR(50) DEFAULT 'USA',
    season_type     VARCHAR(20),  -- regular, playoffs, etc.
    teams_count     INTEGER,
    founded_year    INTEGER,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_league_id ON dim_league(league_id);
CREATE INDEX idx_dim_league_sport ON dim_league(sport);

-- Venue Dimension
CREATE TABLE IF NOT EXISTS dim_venue (
    venue_key       VARCHAR(16) PRIMARY KEY,
    venue_id        VARCHAR(20) NOT NULL,
    venue_name      VARCHAR(150) NOT NULL,
    city            VARCHAR(100),
    state           VARCHAR(50),
    country         VARCHAR(50) DEFAULT 'USA',
    capacity        INTEGER,
    surface         VARCHAR(50),  -- grass, turf, court, ice
    is_dome         BOOLEAN DEFAULT FALSE,
    opened_year     INTEGER,
    latitude        DECIMAL(10, 7),
    longitude       DECIMAL(10, 7),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_venue_id ON dim_venue(venue_id);
CREATE INDEX idx_dim_venue_city ON dim_venue(city);

-- Season Dimension
CREATE TABLE IF NOT EXISTS dim_season (
    season_key      VARCHAR(16) PRIMARY KEY,
    season_id       VARCHAR(10) NOT NULL,
    season_year     INTEGER NOT NULL,
    season_type     VARCHAR(20) NOT NULL,  -- preseason, regular, postseason
    start_date      DATE,
    end_date        DATE,
    games_per_team  INTEGER,
    is_current      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_season_year ON dim_season(season_year);
CREATE INDEX idx_dim_season_type ON dim_season(season_type);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        VARCHAR(16) PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    week_of_year    INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,  -- 0=Monday, 6=Sunday
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_dow ON dim_date(day_of_week);

-- ============================================
-- FACT TABLES
-- ============================================

-- Game Results Fact Table
CREATE TABLE IF NOT EXISTS fact_games (
    game_key            VARCHAR(32) PRIMARY KEY,
    game_id             VARCHAR(20) NOT NULL,
    date_key            VARCHAR(16) NOT NULL REFERENCES dim_date(date_key),
    season_key          VARCHAR(16) NOT NULL REFERENCES dim_season(season_key),
    league_key          VARCHAR(16) NOT NULL REFERENCES dim_league(league_key),
    venue_key           VARCHAR(16) REFERENCES dim_venue(venue_key),
    home_team_key       VARCHAR(16) NOT NULL REFERENCES dim_team(team_key),
    away_team_key       VARCHAR(16) NOT NULL REFERENCES dim_team(team_key),

    -- Score metrics
    home_score          INTEGER NOT NULL,
    away_score          INTEGER NOT NULL,
    total_score         INTEGER GENERATED ALWAYS AS (home_score + away_score) STORED,
    score_margin        INTEGER GENERATED ALWAYS AS (home_score - away_score) STORED,

    -- Game details
    attendance          INTEGER,
    game_duration_mins  INTEGER,
    is_overtime         BOOLEAN DEFAULT FALSE,
    overtime_periods    INTEGER DEFAULT 0,
    is_neutral_site     BOOLEAN DEFAULT FALSE,

    -- Status
    game_status         VARCHAR(20) DEFAULT 'final',  -- scheduled, in_progress, final

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_games_date ON fact_games(date_key);
CREATE INDEX idx_fact_games_home ON fact_games(home_team_key);
CREATE INDEX idx_fact_games_away ON fact_games(away_team_key);
CREATE INDEX idx_fact_games_league ON fact_games(league_key);
CREATE INDEX idx_fact_games_season ON fact_games(season_key);

-- Betting Lines/Odds Fact Table
CREATE TABLE IF NOT EXISTS fact_odds (
    odds_key            VARCHAR(32) PRIMARY KEY,
    game_key            VARCHAR(32) NOT NULL REFERENCES fact_games(game_key),
    bookmaker           VARCHAR(50) NOT NULL,

    -- Spread (point spread)
    home_spread         DECIMAL(5,1),
    away_spread         DECIMAL(5,1),
    spread_odds_home    INTEGER,  -- American odds format (-110, +100, etc)
    spread_odds_away    INTEGER,

    -- Moneyline
    moneyline_home      INTEGER,
    moneyline_away      INTEGER,

    -- Totals (over/under)
    over_under_line     DECIMAL(5,1),
    over_odds           INTEGER,
    under_odds          INTEGER,

    -- Line movement
    is_opening_line     BOOLEAN DEFAULT FALSE,
    is_closing_line     BOOLEAN DEFAULT FALSE,
    line_timestamp      TIMESTAMP,

    -- Outcome tracking
    spread_winner       VARCHAR(10),  -- home, away, push
    moneyline_winner    VARCHAR(10),  -- home, away
    total_result        VARCHAR(10),  -- over, under, push

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_odds_game ON fact_odds(game_key);
CREATE INDEX idx_fact_odds_bookmaker ON fact_odds(bookmaker);
CREATE INDEX idx_fact_odds_line ON fact_odds(over_under_line);

-- Player Game Stats Fact Table
CREATE TABLE IF NOT EXISTS fact_player_stats (
    stat_key            VARCHAR(32) PRIMARY KEY,
    game_key            VARCHAR(32) NOT NULL REFERENCES fact_games(game_key),
    player_key          VARCHAR(16) NOT NULL REFERENCES dim_player(player_key),
    team_key            VARCHAR(16) NOT NULL REFERENCES dim_team(team_key),

    -- Playing time
    minutes_played      DECIMAL(5,1),
    is_starter          BOOLEAN DEFAULT FALSE,

    -- Universal stats
    points              INTEGER DEFAULT 0,
    assists             INTEGER DEFAULT 0,
    rebounds            INTEGER DEFAULT 0,
    steals              INTEGER DEFAULT 0,
    blocks              INTEGER DEFAULT 0,
    turnovers           INTEGER DEFAULT 0,

    -- Shooting (basketball/hockey)
    fg_made             INTEGER DEFAULT 0,
    fg_attempted        INTEGER DEFAULT 0,
    three_pt_made       INTEGER DEFAULT 0,
    three_pt_attempted  INTEGER DEFAULT 0,
    ft_made             INTEGER DEFAULT 0,
    ft_attempted        INTEGER DEFAULT 0,

    -- Football specific
    passing_yards       INTEGER DEFAULT 0,
    passing_tds         INTEGER DEFAULT 0,
    interceptions       INTEGER DEFAULT 0,
    rushing_yards       INTEGER DEFAULT 0,
    rushing_tds         INTEGER DEFAULT 0,
    receiving_yards     INTEGER DEFAULT 0,
    receiving_tds       INTEGER DEFAULT 0,
    receptions          INTEGER DEFAULT 0,
    tackles             INTEGER DEFAULT 0,
    sacks               DECIMAL(3,1) DEFAULT 0,

    -- Baseball specific
    at_bats             INTEGER DEFAULT 0,
    hits                INTEGER DEFAULT 0,
    home_runs           INTEGER DEFAULT 0,
    rbi                 INTEGER DEFAULT 0,
    walks               INTEGER DEFAULT 0,
    strikeouts          INTEGER DEFAULT 0,
    innings_pitched     DECIMAL(4,1) DEFAULT 0,
    earned_runs         INTEGER DEFAULT 0,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_player_stats_game ON fact_player_stats(game_key);
CREATE INDEX idx_fact_player_stats_player ON fact_player_stats(player_key);
CREATE INDEX idx_fact_player_stats_team ON fact_player_stats(team_key);
CREATE INDEX idx_fact_player_stats_points ON fact_player_stats(points DESC);

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- Team Performance Summary
CREATE OR REPLACE VIEW v_team_performance AS
SELECT
    t.team_name,
    l.league_abbrev,
    s.season_year,
    COUNT(*) AS games_played,
    SUM(CASE WHEN (g.home_team_key = t.team_key AND g.home_score > g.away_score)
             OR (g.away_team_key = t.team_key AND g.away_score > g.home_score)
         THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (g.home_team_key = t.team_key AND g.home_score < g.away_score)
             OR (g.away_team_key = t.team_key AND g.away_score < g.home_score)
         THEN 1 ELSE 0 END) AS losses,
    ROUND(AVG(CASE WHEN g.home_team_key = t.team_key THEN g.home_score ELSE g.away_score END), 1) AS avg_points_for,
    ROUND(AVG(CASE WHEN g.home_team_key = t.team_key THEN g.away_score ELSE g.home_score END), 1) AS avg_points_against
FROM dim_team t
JOIN fact_games g ON t.team_key = g.home_team_key OR t.team_key = g.away_team_key
JOIN dim_league l ON g.league_key = l.league_key
JOIN dim_season s ON g.season_key = s.season_key
GROUP BY t.team_name, l.league_abbrev, s.season_year
ORDER BY wins DESC;

-- Betting Trends by League
CREATE OR REPLACE VIEW v_betting_trends AS
SELECT
    l.league_abbrev,
    s.season_year,
    COUNT(DISTINCT g.game_key) AS total_games,
    -- Home team ATS
    SUM(CASE WHEN o.spread_winner = 'home' THEN 1 ELSE 0 END) AS home_covers,
    -- Over/Under trends
    SUM(CASE WHEN o.total_result = 'over' THEN 1 ELSE 0 END) AS overs_hit,
    SUM(CASE WHEN o.total_result = 'under' THEN 1 ELSE 0 END) AS unders_hit,
    -- Average line
    ROUND(AVG(o.over_under_line), 1) AS avg_total_line,
    ROUND(AVG(ABS(o.home_spread)), 1) AS avg_spread
FROM fact_odds o
JOIN fact_games g ON o.game_key = g.game_key
JOIN dim_league l ON g.league_key = l.league_key
JOIN dim_season s ON g.season_key = s.season_key
WHERE o.is_closing_line = TRUE
GROUP BY l.league_abbrev, s.season_year
ORDER BY s.season_year DESC, l.league_abbrev;

-- Player Performance Leaders
CREATE OR REPLACE VIEW v_player_leaders AS
SELECT
    p.player_name,
    t.team_name,
    l.league_abbrev,
    COUNT(*) AS games_played,
    ROUND(AVG(ps.points), 1) AS ppg,
    ROUND(AVG(ps.rebounds), 1) AS rpg,
    ROUND(AVG(ps.assists), 1) AS apg,
    SUM(ps.points) AS total_points
FROM fact_player_stats ps
JOIN dim_player p ON ps.player_key = p.player_key
JOIN dim_team t ON ps.team_key = t.team_key
JOIN fact_games g ON ps.game_key = g.game_key
JOIN dim_league l ON g.league_key = l.league_key
GROUP BY p.player_name, t.team_name, l.league_abbrev
HAVING COUNT(*) >= 10
ORDER BY ppg DESC;

-- Home Field Advantage Analysis
CREATE OR REPLACE VIEW v_home_advantage AS
SELECT
    l.league_abbrev,
    s.season_year,
    COUNT(*) AS total_games,
    SUM(CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END) AS home_wins,
    ROUND(100.0 * SUM(CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END) / COUNT(*), 1) AS home_win_pct,
    ROUND(AVG(g.home_score - g.away_score), 2) AS avg_home_margin,
    ROUND(AVG(g.attendance), 0) AS avg_attendance
FROM fact_games g
JOIN dim_league l ON g.league_key = l.league_key
JOIN dim_season s ON g.season_key = s.season_key
WHERE g.is_neutral_site = FALSE
GROUP BY l.league_abbrev, s.season_year
ORDER BY s.season_year DESC, home_win_pct DESC;

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE dim_team IS 'Team master data dimension - SCD Type 2 enabled';
COMMENT ON TABLE fact_games IS 'Game results fact - grain is one row per game';
COMMENT ON TABLE fact_odds IS 'Betting lines fact - one row per game per bookmaker';
COMMENT ON TABLE fact_player_stats IS 'Player box score stats - one row per player per game';
COMMENT ON COLUMN fact_odds.spread_odds_home IS 'American odds format: -110 means bet $110 to win $100';
COMMENT ON COLUMN fact_odds.moneyline_home IS 'American moneyline: +150 means bet $100 to win $150';
