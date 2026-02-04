"""
P07 Media & Streaming Analytics - Transformation Module
Author: Mboya Jeffers

Transforms raw IMDB data into Kimball star schema dimensional model.
Implements surrogate key generation, genre normalization, and quality validation.
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


class MediaStarSchemaTransformer:
    """
    Transforms raw IMDB data into Kimball dimensional model.

    Dimensions:
    - dim_title: Movies and TV shows
    - dim_person: Actors, directors, writers
    - dim_genre: Genre categories
    - dim_date: Date dimension

    Facts:
    - fact_ratings: Rating aggregates
    - fact_cast_crew: Title-person relationships

    Bridge:
    - title_genre_bridge: Many-to-many title-genre
    """

    # Standard genres in IMDB
    STANDARD_GENRES = [
        'Action', 'Adventure', 'Animation', 'Biography', 'Comedy',
        'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy',
        'Film-Noir', 'Game-Show', 'History', 'Horror', 'Music',
        'Musical', 'Mystery', 'News', 'Reality-TV', 'Romance',
        'Sci-Fi', 'Short', 'Sport', 'Talk-Show', 'Thriller',
        'War', 'Western'
    ]

    def __init__(self, output_dir: str = "data"):
        """Initialize transformer with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Surrogate key caches
        self._title_keys = {}
        self._person_keys = {}
        self._genre_keys = {}
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

    def _get_title_key(self, title_id: str) -> str:
        """Get or create surrogate key for title."""
        if title_id not in self._title_keys:
            self._title_keys[title_id] = self._generate_surrogate_key('title', title_id)
        return self._title_keys[title_id]

    def _get_person_key(self, person_id: str) -> str:
        """Get or create surrogate key for person."""
        if person_id not in self._person_keys:
            self._person_keys[person_id] = self._generate_surrogate_key('person', person_id)
        return self._person_keys[person_id]

    def _get_genre_key(self, genre_name: str) -> str:
        """Get or create surrogate key for genre."""
        if genre_name not in self._genre_keys:
            self._genre_keys[genre_name] = self._generate_surrogate_key('genre', genre_name)
        return self._genre_keys[genre_name]

    def build_dim_title(self, titles: List[Dict]) -> pd.DataFrame:
        """
        Build title dimension table.

        Args:
            titles: List of raw title dictionaries from IMDB

        Returns:
            DataFrame with dim_title schema
        """
        logger.info("Building dim_title...")

        records = []
        seen_titles = set()

        for title in titles:
            title_id = title.get('tconst')
            if not title_id or title_id in seen_titles:
                continue
            seen_titles.add(title_id)

            title_key = self._get_title_key(title_id)

            records.append({
                'title_key': title_key,
                'title_id': title_id,
                'title_type': title.get('titleType'),
                'primary_title': title.get('primaryTitle'),
                'original_title': title.get('originalTitle'),
                'is_adult': title.get('isAdult', False),
                'start_year': title.get('startYear'),
                'end_year': title.get('endYear'),
                'runtime_minutes': title.get('runtimeMinutes'),
                'effective_date': datetime.now().date(),
                'is_current': True
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_title: {len(df)} records")
        return df

    def build_dim_person(self, names: List[Dict]) -> pd.DataFrame:
        """
        Build person dimension table.

        Args:
            names: List of raw name dictionaries from IMDB

        Returns:
            DataFrame with dim_person schema
        """
        logger.info("Building dim_person...")

        records = []
        seen_persons = set()

        for person in names:
            person_id = person.get('nconst')
            if not person_id or person_id in seen_persons:
                continue
            seen_persons.add(person_id)

            person_key = self._get_person_key(person_id)

            records.append({
                'person_key': person_key,
                'person_id': person_id,
                'primary_name': person.get('primaryName'),
                'birth_year': person.get('birthYear'),
                'death_year': person.get('deathYear'),
                'primary_profession': person.get('primaryProfession'),
                'known_for_titles': person.get('knownForTitles')
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_person: {len(df)} records")
        return df

    def build_dim_genre(self, titles: List[Dict]) -> pd.DataFrame:
        """
        Build genre dimension table from title genres.

        Args:
            titles: List of title dictionaries with genres

        Returns:
            DataFrame with dim_genre schema
        """
        logger.info("Building dim_genre...")

        # Collect all unique genres
        all_genres = set()
        for title in titles:
            genres_str = title.get('genres')
            if genres_str:
                genres = genres_str.split(',')
                all_genres.update(g.strip() for g in genres if g.strip())

        records = []
        for genre_name in sorted(all_genres):
            genre_key = self._get_genre_key(genre_name)
            records.append({
                'genre_key': genre_key,
                'genre_id': len(records) + 1,
                'genre_name': genre_name,
                'is_primary': genre_name in self.STANDARD_GENRES
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_genre: {len(df)} records")
        return df

    def build_dim_date(self, titles: List[Dict]) -> pd.DataFrame:
        """
        Build date dimension for title years.

        Args:
            titles: List of title dictionaries

        Returns:
            DataFrame with dim_date schema
        """
        logger.info("Building dim_date...")

        # Get unique years from titles
        years = set()
        for title in titles:
            start_year = title.get('startYear')
            if start_year and isinstance(start_year, int) and 1800 <= start_year <= 2030:
                years.add(start_year)

        records = []
        for year in sorted(years):
            # Create January 1 for each year
            date = datetime(year, 1, 1).date()
            date_key = self._generate_surrogate_key('date', str(year))

            records.append({
                'date_key': date_key,
                'full_date': date,
                'year': year,
                'month': 1,
                'day': 1,
                'quarter': 1,
                'week_of_year': 1,
                'day_of_week': date.weekday(),
                'day_name': date.strftime('%A'),
                'is_weekend': date.weekday() >= 5
            })

        df = pd.DataFrame(records)
        logger.info(f"dim_date: {len(df)} records")
        return df

    def build_fact_ratings(self, ratings: List[Dict], titles: List[Dict]) -> pd.DataFrame:
        """
        Build ratings fact table.

        Args:
            ratings: List of rating dictionaries
            titles: List of title dictionaries (for title key lookup)

        Returns:
            DataFrame with fact_ratings schema
        """
        logger.info("Building fact_ratings...")

        # Build title_id -> title lookup
        title_lookup = {t['tconst']: t for t in titles if t.get('tconst')}

        records = []
        today = datetime.now().date()

        for rating in ratings:
            title_id = rating.get('tconst')
            if not title_id or title_id not in title_lookup:
                continue

            title_key = self._get_title_key(title_id)
            rating_key = self._generate_surrogate_key('rating', title_id, str(today))

            avg_rating = rating.get('averageRating')
            num_votes = rating.get('numVotes', 0) or 0

            # Calculate weighted rating (Bayesian average)
            # Formula: (v/(v+m)) * R + (m/(v+m)) * C
            # m = minimum votes required (25000)
            # C = mean rating across all titles (~7.0)
            m = 25000
            C = 7.0
            if num_votes > 0 and avg_rating:
                weighted = (num_votes / (num_votes + m)) * avg_rating + (m / (num_votes + m)) * C
            else:
                weighted = None

            records.append({
                'rating_key': rating_key,
                'title_key': title_key,
                'title_id': title_id,
                'snapshot_date': today,
                'average_rating': avg_rating,
                'num_votes': num_votes,
                'weighted_rating': round(weighted, 2) if weighted else None
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_ratings: {len(df)} records")
        return df

    def build_fact_cast_crew(self, principals: List[Dict]) -> pd.DataFrame:
        """
        Build cast/crew fact table.

        Args:
            principals: List of principal dictionaries from IMDB

        Returns:
            DataFrame with fact_cast_crew schema
        """
        logger.info("Building fact_cast_crew...")

        records = []
        seen_credits = set()

        for principal in principals:
            title_id = principal.get('tconst')
            person_id = principal.get('nconst')

            if not title_id or not person_id:
                continue

            # Create unique credit key
            credit_key_str = f"{title_id}|{person_id}|{principal.get('ordering', 0)}"
            if credit_key_str in seen_credits:
                continue
            seen_credits.add(credit_key_str)

            title_key = self._get_title_key(title_id)
            person_key = self._get_person_key(person_id)
            credit_key = self._generate_surrogate_key('credit', title_id, person_id, str(principal.get('ordering')))

            ordering = principal.get('ordering')

            records.append({
                'credit_key': credit_key,
                'title_key': title_key,
                'person_key': person_key,
                'category': principal.get('category'),
                'job': principal.get('job'),
                'characters': principal.get('characters'),
                'ordering': ordering,
                'is_lead': ordering is not None and ordering <= 3
            })

        df = pd.DataFrame(records)
        logger.info(f"fact_cast_crew: {len(df)} records")
        return df

    def build_title_genre_bridge(self, titles: List[Dict]) -> pd.DataFrame:
        """
        Build title-genre bridge table.

        Args:
            titles: List of title dictionaries

        Returns:
            DataFrame with title_genre_bridge schema
        """
        logger.info("Building title_genre_bridge...")

        records = []
        seen_pairs = set()

        for title in titles:
            title_id = title.get('tconst')
            genres_str = title.get('genres')

            if not title_id or not genres_str:
                continue

            title_key = self._get_title_key(title_id)
            genres = [g.strip() for g in genres_str.split(',') if g.strip()]

            for i, genre_name in enumerate(genres):
                pair_key = f"{title_id}|{genre_name}"
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                genre_key = self._get_genre_key(genre_name)
                records.append({
                    'title_key': title_key,
                    'genre_key': genre_key,
                    'is_primary': i == 0  # First genre is primary
                })

        df = pd.DataFrame(records)
        logger.info(f"title_genre_bridge: {len(df)} records")
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
        logger.info(f"Input titles: {len(extracted_data.get('titles', []))}")
        logger.info(f"Input ratings: {len(extracted_data.get('ratings', []))}")
        logger.info(f"Input names: {len(extracted_data.get('names', []))}")
        logger.info(f"Input principals: {len(extracted_data.get('principals', []))}")
        logger.info("=" * 60)

        titles = extracted_data.get('titles', [])
        ratings = extracted_data.get('ratings', [])
        names = extracted_data.get('names', [])
        principals = extracted_data.get('principals', [])

        tables = {}

        # Build dimensions
        tables['dim_title'] = self.build_dim_title(titles)
        tables['dim_person'] = self.build_dim_person(names)
        tables['dim_genre'] = self.build_dim_genre(titles)
        tables['dim_date'] = self.build_dim_date(titles)

        # Build facts
        tables['fact_ratings'] = self.build_fact_ratings(ratings, titles)
        tables['fact_cast_crew'] = self.build_fact_cast_crew(principals)

        # Build bridges
        tables['title_genre_bridge'] = self.build_title_genre_bridge(titles)

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
        'titles': [
            {'tconst': 'tt0001', 'titleType': 'movie', 'primaryTitle': 'Test Movie',
             'startYear': 2020, 'runtimeMinutes': 120, 'genres': 'Action,Drama'}
        ],
        'ratings': [
            {'tconst': 'tt0001', 'averageRating': 7.5, 'numVotes': 50000}
        ],
        'names': [
            {'nconst': 'nm0001', 'primaryName': 'Test Actor', 'birthYear': 1980,
             'primaryProfession': 'actor'}
        ],
        'principals': [
            {'tconst': 'tt0001', 'nconst': 'nm0001', 'ordering': 1, 'category': 'actor'}
        ]
    }

    transformer = MediaStarSchemaTransformer(output_dir="test_output")
    tables = transformer.run_transformation(sample_data)

    print("\nTransformation Summary:")
    for name, df in tables.items():
        print(f"  {name}: {len(df)} rows")
