-- P07 Media & Streaming Analytics - Star Schema DDL
-- Author: Mboya Jeffers
-- Database: PostgreSQL
-- Pattern: Kimball Dimensional Model

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Title Dimension (Movies, TV Shows, Episodes)
CREATE TABLE IF NOT EXISTS dim_title (
    title_key           VARCHAR(16) PRIMARY KEY,
    title_id            VARCHAR(20) NOT NULL UNIQUE,  -- IMDB tconst
    title_type          VARCHAR(50) NOT NULL,  -- movie, tvSeries, tvEpisode, etc.
    primary_title       VARCHAR(500) NOT NULL,
    original_title      VARCHAR(500),
    is_adult            BOOLEAN DEFAULT FALSE,
    start_year          INTEGER,
    end_year            INTEGER,  -- For TV series
    runtime_minutes     INTEGER,
    parent_title_id     VARCHAR(20),  -- For episodes
    season_number       INTEGER,
    episode_number      INTEGER,
    effective_date      DATE NOT NULL,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_title_id ON dim_title(title_id);
CREATE INDEX idx_dim_title_type ON dim_title(title_type);
CREATE INDEX idx_dim_title_year ON dim_title(start_year);
CREATE INDEX idx_dim_title_parent ON dim_title(parent_title_id);

-- Person Dimension (Actors, Directors, Writers)
CREATE TABLE IF NOT EXISTS dim_person (
    person_key          VARCHAR(16) PRIMARY KEY,
    person_id           VARCHAR(20) NOT NULL UNIQUE,  -- IMDB nconst
    primary_name        VARCHAR(255) NOT NULL,
    birth_year          INTEGER,
    death_year          INTEGER,
    primary_profession  VARCHAR(255),  -- Comma-separated
    known_for_titles    VARCHAR(255),  -- Comma-separated tconsts
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_person_id ON dim_person(person_id);
CREATE INDEX idx_dim_person_name ON dim_person(primary_name);
CREATE INDEX idx_dim_person_profession ON dim_person(primary_profession);

-- Genre Dimension
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key           VARCHAR(16) PRIMARY KEY,
    genre_id            SERIAL,
    genre_name          VARCHAR(50) NOT NULL UNIQUE,
    is_primary          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_genre_name ON dim_genre(genre_name);

-- Platform Dimension (Streaming services)
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_key        VARCHAR(16) PRIMARY KEY,
    platform_id         INTEGER NOT NULL,
    platform_name       VARCHAR(100) NOT NULL,
    platform_type       VARCHAR(50),  -- streaming, theatrical, etc.
    launch_year         INTEGER,
    parent_company      VARCHAR(100),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_platform_name ON dim_platform(platform_name);

-- Region Dimension
CREATE TABLE IF NOT EXISTS dim_region (
    region_key          VARCHAR(16) PRIMARY KEY,
    region_code         VARCHAR(10) NOT NULL,
    region_name         VARCHAR(100) NOT NULL,
    language_code       VARCHAR(10),
    language_name       VARCHAR(100),
    is_original         BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_region_code ON dim_region(region_code);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            VARCHAR(16) PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    year                INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    day                 INTEGER NOT NULL,
    quarter             INTEGER NOT NULL,
    week_of_year        INTEGER NOT NULL,
    day_of_week         INTEGER NOT NULL,
    day_name            VARCHAR(10) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dim_date(year);

-- ============================================
-- FACT TABLES
-- ============================================

-- Ratings Fact Table
CREATE TABLE IF NOT EXISTS fact_ratings (
    rating_key          VARCHAR(32) PRIMARY KEY,
    title_key           VARCHAR(16) NOT NULL REFERENCES dim_title(title_key),
    title_id            VARCHAR(20) NOT NULL,
    snapshot_date       DATE NOT NULL,

    -- IMDB Ratings
    average_rating      DECIMAL(3,1) NOT NULL,
    num_votes           INTEGER NOT NULL,
    weighted_rating     DECIMAL(5,2),  -- Bayesian average

    -- Rating distribution
    votes_10            INTEGER DEFAULT 0,
    votes_9             INTEGER DEFAULT 0,
    votes_8             INTEGER DEFAULT 0,
    votes_7             INTEGER DEFAULT 0,
    votes_6             INTEGER DEFAULT 0,
    votes_5             INTEGER DEFAULT 0,
    votes_4             INTEGER DEFAULT 0,
    votes_3             INTEGER DEFAULT 0,
    votes_2             INTEGER DEFAULT 0,
    votes_1             INTEGER DEFAULT 0,

    -- Demographics (if available)
    male_avg            DECIMAL(3,1),
    female_avg          DECIMAL(3,1),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_ratings_title ON fact_ratings(title_key);
CREATE INDEX idx_fact_ratings_avg ON fact_ratings(average_rating DESC);
CREATE INDEX idx_fact_ratings_votes ON fact_ratings(num_votes DESC);

-- Cast/Crew Fact Table (Title-Person relationship)
CREATE TABLE IF NOT EXISTS fact_cast_crew (
    credit_key          VARCHAR(32) PRIMARY KEY,
    title_key           VARCHAR(16) NOT NULL REFERENCES dim_title(title_key),
    person_key          VARCHAR(16) NOT NULL REFERENCES dim_person(person_key),

    -- Role information
    category            VARCHAR(50) NOT NULL,  -- actor, director, writer, etc.
    job                 VARCHAR(255),  -- Specific job title
    characters          TEXT,  -- JSON array for actors

    -- Ordering
    ordering            INTEGER,  -- Billing order
    is_lead             BOOLEAN DEFAULT FALSE,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_cast_title ON fact_cast_crew(title_key);
CREATE INDEX idx_fact_cast_person ON fact_cast_crew(person_key);
CREATE INDEX idx_fact_cast_category ON fact_cast_crew(category);

-- Title-Genre Bridge Table
CREATE TABLE IF NOT EXISTS title_genre_bridge (
    title_key           VARCHAR(16) NOT NULL REFERENCES dim_title(title_key),
    genre_key           VARCHAR(16) NOT NULL REFERENCES dim_genre(genre_key),
    is_primary          BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (title_key, genre_key)
);

CREATE INDEX idx_bridge_title ON title_genre_bridge(title_key);
CREATE INDEX idx_bridge_genre ON title_genre_bridge(genre_key);

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- Top Rated Movies
CREATE OR REPLACE VIEW v_top_rated_movies AS
SELECT
    t.primary_title,
    t.start_year,
    r.average_rating,
    r.num_votes,
    r.weighted_rating,
    t.runtime_minutes
FROM fact_ratings r
JOIN dim_title t ON r.title_key = t.title_key
WHERE t.title_type = 'movie'
  AND r.num_votes >= 10000
ORDER BY r.weighted_rating DESC
LIMIT 250;

-- Genre Performance Summary
CREATE OR REPLACE VIEW v_genre_performance AS
SELECT
    g.genre_name,
    COUNT(DISTINCT r.title_key) AS title_count,
    ROUND(AVG(r.average_rating), 2) AS avg_rating,
    SUM(r.num_votes) AS total_votes,
    ROUND(AVG(t.runtime_minutes), 0) AS avg_runtime
FROM fact_ratings r
JOIN title_genre_bridge tg ON r.title_key = tg.title_key
JOIN dim_genre g ON tg.genre_key = g.genre_key
JOIN dim_title t ON r.title_key = t.title_key
WHERE t.title_type = 'movie'
GROUP BY g.genre_name
ORDER BY avg_rating DESC;

-- Most Prolific Actors
CREATE OR REPLACE VIEW v_prolific_actors AS
SELECT
    p.primary_name,
    p.birth_year,
    COUNT(DISTINCT c.title_key) AS total_credits,
    ROUND(AVG(r.average_rating), 2) AS avg_film_rating,
    SUM(r.num_votes) AS total_votes
FROM fact_cast_crew c
JOIN dim_person p ON c.person_key = p.person_key
JOIN fact_ratings r ON c.title_key = r.title_key
WHERE c.category = 'actor' OR c.category = 'actress'
GROUP BY p.primary_name, p.birth_year
HAVING COUNT(DISTINCT c.title_key) >= 10
ORDER BY total_credits DESC
LIMIT 100;

-- Movies by Decade
CREATE OR REPLACE VIEW v_movies_by_decade AS
SELECT
    (t.start_year / 10) * 10 AS decade,
    COUNT(*) AS movie_count,
    ROUND(AVG(r.average_rating), 2) AS avg_rating,
    ROUND(AVG(r.num_votes), 0) AS avg_votes,
    ROUND(AVG(t.runtime_minutes), 0) AS avg_runtime
FROM fact_ratings r
JOIN dim_title t ON r.title_key = t.title_key
WHERE t.title_type = 'movie'
  AND t.start_year >= 1920
GROUP BY (t.start_year / 10) * 10
ORDER BY decade;

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE dim_title IS 'Title master data from IMDB - SCD Type 2 enabled';
COMMENT ON TABLE dim_person IS 'Person/talent data from IMDB names dataset';
COMMENT ON TABLE fact_ratings IS 'Rating aggregates - snapshot grain';
COMMENT ON TABLE fact_cast_crew IS 'Title-person relationships with role details';
COMMENT ON COLUMN fact_ratings.weighted_rating IS 'Bayesian weighted average: (v/(v+m)) * R + (m/(v+m)) * C';
