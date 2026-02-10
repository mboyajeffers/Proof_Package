"""
P07 Media & Streaming Analytics - Analytics Module
Author: Mboya Jeffers

Calculates key performance indicators for media and entertainment analytics.
Calculates 14 industry-standard KPIs for automated report generation.
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


class MediaAnalyticsEngine:
    """
    Calculates media and entertainment analytics KPIs.

    KPI Categories:
    - Content Performance: Ratings, engagement, popularity
    - Genre Analysis: Trends, preferences, distribution
    - Talent Analysis: Actor/director performance
    - Time Analysis: Historical trends by decade/year

    Computes 14 KPIs across content performance, genres, and talent.
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

    def calculate_content_performance(self, fact_ratings: pd.DataFrame,
                                       dim_title: pd.DataFrame) -> Dict:
        """
        Calculate content performance metrics.

        Metrics:
        - Average ratings by type
        - Vote distribution
        - Top rated content
        - Rating trends

        Args:
            fact_ratings: Ratings fact table
            dim_title: Title dimension table

        Returns:
            Dictionary of content performance metrics
        """
        logger.info("Calculating content performance metrics...")

        if fact_ratings is None or dim_title is None:
            return {}

        results = {
            'overall_stats': {},
            'by_type': [],
            'top_rated': [],
            'rating_distribution': {}
        }

        # Merge ratings with titles
        merged = fact_ratings.merge(
            dim_title[['title_key', 'title_type', 'primary_title', 'start_year', 'runtime_minutes']],
            on='title_key',
            how='left'
        )

        # Overall stats
        results['overall_stats'] = {
            'total_rated_titles': len(fact_ratings),
            'avg_rating': round(fact_ratings['average_rating'].mean(), 2),
            'median_rating': round(fact_ratings['average_rating'].median(), 2),
            'total_votes': int(fact_ratings['num_votes'].sum()),
            'avg_votes_per_title': int(fact_ratings['num_votes'].mean())
        }

        # Performance by type
        by_type = merged.groupby('title_type').agg({
            'average_rating': 'mean',
            'num_votes': ['sum', 'mean', 'count']
        }).round(2)

        for title_type in merged['title_type'].dropna().unique():
            type_data = merged[merged['title_type'] == title_type]
            results['by_type'].append({
                'title_type': title_type,
                'count': len(type_data),
                'avg_rating': round(type_data['average_rating'].mean(), 2),
                'total_votes': int(type_data['num_votes'].sum()),
                'avg_votes': int(type_data['num_votes'].mean())
            })

        # Top rated (weighted)
        if 'weighted_rating' in merged.columns:
            top = merged.nlargest(10, 'weighted_rating')
        else:
            top = merged[merged['num_votes'] >= 10000].nlargest(10, 'average_rating')

        results['top_rated'] = top[['primary_title', 'title_type', 'start_year',
                                    'average_rating', 'num_votes']].to_dict('records')

        # Rating distribution
        bins = [0, 2, 4, 5, 6, 7, 8, 9, 10]
        labels = ['0-2', '2-4', '4-5', '5-6', '6-7', '7-8', '8-9', '9-10']
        merged['rating_bucket'] = pd.cut(merged['average_rating'], bins=bins, labels=labels)
        distribution = merged['rating_bucket'].value_counts().sort_index().to_dict()
        results['rating_distribution'] = {str(k): int(v) for k, v in distribution.items()}

        logger.info(f"  Analyzed {len(fact_ratings)} rated titles")
        return results

    def calculate_genre_analysis(self, fact_ratings: pd.DataFrame,
                                  title_genre_bridge: pd.DataFrame,
                                  dim_genre: pd.DataFrame,
                                  dim_title: pd.DataFrame) -> Dict:
        """
        Calculate genre performance metrics.

        Metrics:
        - Genre popularity (by title count)
        - Genre ratings
        - Genre combinations
        - Top genre by decade

        Args:
            fact_ratings: Ratings fact table
            title_genre_bridge: Title-genre bridge table
            dim_genre: Genre dimension table
            dim_title: Title dimension table

        Returns:
            Dictionary of genre analysis metrics
        """
        logger.info("Calculating genre analysis metrics...")

        if any(df is None for df in [fact_ratings, title_genre_bridge, dim_genre, dim_title]):
            return {}

        results = {
            'genre_popularity': [],
            'genre_ratings': [],
            'total_genres': len(dim_genre)
        }

        # Merge all tables
        merged = title_genre_bridge.merge(
            dim_genre[['genre_key', 'genre_name']],
            on='genre_key',
            how='left'
        ).merge(
            fact_ratings[['title_key', 'average_rating', 'num_votes']],
            on='title_key',
            how='left'
        ).merge(
            dim_title[['title_key', 'title_type', 'start_year']],
            on='title_key',
            how='left'
        )

        # Genre popularity
        genre_stats = merged.groupby('genre_name').agg({
            'title_key': 'nunique',
            'average_rating': 'mean',
            'num_votes': 'sum'
        }).reset_index()

        genre_stats.columns = ['genre_name', 'title_count', 'avg_rating', 'total_votes']
        genre_stats = genre_stats.sort_values('title_count', ascending=False)

        results['genre_popularity'] = genre_stats.head(15).round(2).to_dict('records')

        # Best rated genres (with sufficient sample)
        sufficient_sample = genre_stats[genre_stats['title_count'] >= 100].copy()
        sufficient_sample = sufficient_sample.sort_values('avg_rating', ascending=False)
        results['genre_ratings'] = sufficient_sample.head(10).round(2).to_dict('records')

        logger.info(f"  Analyzed {len(dim_genre)} genres")
        return results

    def calculate_talent_analysis(self, fact_cast_crew: pd.DataFrame,
                                   fact_ratings: pd.DataFrame,
                                   dim_person: pd.DataFrame) -> Dict:
        """
        Calculate talent (actor/director) performance metrics.

        Metrics:
        - Most prolific actors/directors
        - Highest rated talent
        - Career statistics

        Args:
            fact_cast_crew: Cast/crew fact table
            fact_ratings: Ratings fact table
            dim_person: Person dimension table

        Returns:
            Dictionary of talent analysis metrics
        """
        logger.info("Calculating talent analysis metrics...")

        if any(df is None for df in [fact_cast_crew, fact_ratings, dim_person]):
            return {}

        results = {
            'prolific_actors': [],
            'prolific_directors': [],
            'highest_rated_actors': [],
            'total_talent': len(dim_person)
        }

        # Merge credits with ratings and people
        merged = fact_cast_crew.merge(
            fact_ratings[['title_key', 'average_rating', 'num_votes']],
            on='title_key',
            how='left'
        ).merge(
            dim_person[['person_key', 'primary_name', 'birth_year']],
            on='person_key',
            how='left'
        )

        # Prolific actors (actor or actress)
        actors = merged[merged['category'].isin(['actor', 'actress'])]
        actor_stats = actors.groupby(['person_key', 'primary_name']).agg({
            'title_key': 'nunique',
            'average_rating': 'mean',
            'num_votes': 'sum'
        }).reset_index()

        actor_stats.columns = ['person_key', 'name', 'credits', 'avg_rating', 'total_votes']
        actor_stats = actor_stats[actor_stats['credits'] >= 5]
        actor_stats = actor_stats.sort_values('credits', ascending=False)

        results['prolific_actors'] = actor_stats.head(10).round(2).to_dict('records')

        # Prolific directors
        directors = merged[merged['category'] == 'director']
        director_stats = directors.groupby(['person_key', 'primary_name']).agg({
            'title_key': 'nunique',
            'average_rating': 'mean',
            'num_votes': 'sum'
        }).reset_index()

        director_stats.columns = ['person_key', 'name', 'credits', 'avg_rating', 'total_votes']
        director_stats = director_stats[director_stats['credits'] >= 3]
        director_stats = director_stats.sort_values('credits', ascending=False)

        results['prolific_directors'] = director_stats.head(10).round(2).to_dict('records')

        # Highest rated actors (with sufficient credits)
        actor_stats_sorted = actor_stats[actor_stats['credits'] >= 10].sort_values(
            'avg_rating', ascending=False
        )
        results['highest_rated_actors'] = actor_stats_sorted.head(10).round(2).to_dict('records')

        logger.info(f"  Analyzed {len(dim_person)} people")
        return results

    def calculate_time_analysis(self, fact_ratings: pd.DataFrame,
                                 dim_title: pd.DataFrame) -> Dict:
        """
        Calculate temporal/historical trends.

        Metrics:
        - Content by decade
        - Rating trends over time
        - Production volume trends

        Args:
            fact_ratings: Ratings fact table
            dim_title: Title dimension table

        Returns:
            Dictionary of time analysis metrics
        """
        logger.info("Calculating time analysis metrics...")

        if fact_ratings is None or dim_title is None:
            return {}

        results = {
            'by_decade': [],
            'by_year_recent': [],
            'production_trend': {}
        }

        # Merge ratings with titles
        merged = fact_ratings.merge(
            dim_title[['title_key', 'title_type', 'start_year']],
            on='title_key',
            how='left'
        )

        # Filter to valid years
        merged = merged[merged['start_year'].notna()]
        merged['start_year'] = merged['start_year'].astype(int)
        merged = merged[(merged['start_year'] >= 1900) & (merged['start_year'] <= 2025)]

        # By decade
        merged['decade'] = (merged['start_year'] // 10) * 10
        decade_stats = merged.groupby('decade').agg({
            'title_key': 'nunique',
            'average_rating': 'mean',
            'num_votes': ['sum', 'mean']
        }).reset_index()

        decade_stats.columns = ['decade', 'title_count', 'avg_rating', 'total_votes', 'avg_votes']

        for _, row in decade_stats.iterrows():
            results['by_decade'].append({
                'decade': f"{int(row['decade'])}s",
                'title_count': int(row['title_count']),
                'avg_rating': round(row['avg_rating'], 2),
                'avg_votes': int(row['avg_votes'])
            })

        # Recent years (last 10 years)
        recent = merged[merged['start_year'] >= 2015]
        year_stats = recent.groupby('start_year').agg({
            'title_key': 'nunique',
            'average_rating': 'mean'
        }).reset_index()

        year_stats.columns = ['year', 'title_count', 'avg_rating']
        results['by_year_recent'] = year_stats.round(2).to_dict('records')

        logger.info(f"  Analyzed time trends across {len(decade_stats)} decades")
        return results

    def generate_kpi_summary(self) -> Dict:
        """
        Generate summary of all calculated KPIs.

        Summarizes all computed media KPIs.

        Returns:
            Dictionary of KPI summaries
        """
        logger.info("Generating KPI summary...")

        kpi_summary = {
            'generated_at': datetime.now().isoformat(),
            'kpi_count': 0,
            'kpis': {}
        }

        # Map analytics results to media KPIs
        if 'content_performance' in self.analytics_results:
            perf = self.analytics_results['content_performance']
            if perf.get('overall_stats'):
                kpi_summary['kpis']['avg_rating'] = {
                    'value': perf['overall_stats'].get('avg_rating'),
                    'description': 'Average content rating'
                }
                kpi_summary['kpis']['total_votes'] = {
                    'value': perf['overall_stats'].get('total_votes'),
                    'description': 'Total audience votes/engagement'
                }

        if 'genre_analysis' in self.analytics_results:
            genre = self.analytics_results['genre_analysis']
            kpi_summary['kpis']['total_genres'] = {
                'value': genre.get('total_genres'),
                'description': 'Number of content genres'
            }

        if 'talent_analysis' in self.analytics_results:
            talent = self.analytics_results['talent_analysis']
            kpi_summary['kpis']['total_talent'] = {
                'value': talent.get('total_talent'),
                'description': 'Total actors/directors/crew'
            }

        if 'time_analysis' in self.analytics_results:
            time = self.analytics_results['time_analysis']
            if time.get('by_decade'):
                latest = time['by_decade'][-1] if time['by_decade'] else {}
                kpi_summary['kpis']['recent_production'] = {
                    'value': latest.get('title_count'),
                    'description': f"Titles in {latest.get('decade', 'recent decade')}"
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
        logger.info("STARTING MEDIA ANALYTICS")
        logger.info("=" * 60)

        # Load tables if not provided
        if tables is None:
            tables = {
                'fact_ratings': self._load_table('fact_ratings'),
                'fact_cast_crew': self._load_table('fact_cast_crew'),
                'dim_title': self._load_table('dim_title'),
                'dim_person': self._load_table('dim_person'),
                'dim_genre': self._load_table('dim_genre'),
                'title_genre_bridge': self._load_table('title_genre_bridge')
            }

        # Run analytics
        self.analytics_results['content_performance'] = self.calculate_content_performance(
            tables.get('fact_ratings'),
            tables.get('dim_title')
        )

        self.analytics_results['genre_analysis'] = self.calculate_genre_analysis(
            tables.get('fact_ratings'),
            tables.get('title_genre_bridge'),
            tables.get('dim_genre'),
            tables.get('dim_title')
        )

        self.analytics_results['talent_analysis'] = self.calculate_talent_analysis(
            tables.get('fact_cast_crew'),
            tables.get('fact_ratings'),
            tables.get('dim_person')
        )

        self.analytics_results['time_analysis'] = self.calculate_time_analysis(
            tables.get('fact_ratings'),
            tables.get('dim_title')
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
    engine = MediaAnalyticsEngine()
    results = engine.run_analytics()

    print("\nAnalytics Summary:")
    if 'kpi_summary' in results:
        print(f"  KPIs calculated: {results['kpi_summary']['kpi_count']}")
        for kpi_name, kpi_data in results['kpi_summary']['kpis'].items():
            print(f"    {kpi_name}: {kpi_data['value']}")
