"""
Enterprise ETL Framework - ETL Registry
Central registry for all ETL extractors and transformers.

Provides:
- Discovery of available pipelines
- Factory methods for creating extractors/transformers
- Pipeline metadata and configuration
"""

import logging
from typing import Dict, List, Any, Optional, Type
from dataclasses import dataclass, field
from enum import Enum

import sys

    


class PipelineStatus(Enum):
    """Pipeline availability status."""
    AVAILABLE = 'available'
    REQUIRES_KEY = 'requires_api_key'
    DISABLED = 'disabled'
    ERROR = 'error'


@dataclass
class PipelineInfo:
    """Information about a registered ETL pipeline."""
    name: str
    vertical: str
    description: str
    extractor_class: str
    transformer_class: str
    data_sources: List[str]
    requires_api_key: bool = False
    api_key_name: Optional[str] = None
    default_params: Dict[str, Any] = field(default_factory=dict)
    schema_tables: List[str] = field(default_factory=list)
    status: PipelineStatus = PipelineStatus.AVAILABLE


class ETLRegistry:
    """
    Central registry for ETL pipelines.
    
    Example:
        registry = ETLRegistry()
        pipelines = registry.list_pipelines()
        extractor = registry.get_extractor('gaming')
        transformer = registry.get_transformer('gaming')
    """
    
    _instance = None
    _pipelines: Dict[str, PipelineInfo] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.logger = logging.getLogger('ETL:Registry')
        self._register_default_pipelines()
    
    def _register_default_pipelines(self):
        """Register all built-in ETL pipelines."""
        
        # Gaming Pipeline (Steam/SteamSpy)
        self.register_pipeline(PipelineInfo(
            name='gaming',
            vertical='Gaming',
            description='Steam and SteamSpy gaming data - players, ownership, reviews',
            extractor_class='engines.extractors.steam_extractor.SteamExtractor',
            transformer_class='engines.industry.gaming.transform.GamingTransformer',
            data_sources=['Steam Web API', 'SteamSpy API'],
            requires_api_key=False,
            default_params={'limit': 100, 'include_steamspy': True},
            schema_tables=['dim_game', 'dim_date', 'fact_game_metrics'],
        ))
        
        # Crypto Pipeline (CoinGecko)
        self.register_pipeline(PipelineInfo(
            name='crypto',
            vertical='Crypto',
            description='Cryptocurrency market data - prices, market cap, volume',
            extractor_class='engines.extractors.coingecko_extractor.CoinGeckoExtractor',
            transformer_class='engines.industry.crypto.transform.CryptoTransformer',
            data_sources=['CoinGecko API'],
            requires_api_key=False,
            default_params={'limit': 100, 'vs_currency': 'usd'},
            schema_tables=['dim_coin', 'dim_date', 'fact_coin_metrics'],
        ))
        
        # Betting Pipeline (ESPN)
        self.register_pipeline(PipelineInfo(
            name='betting',
            vertical='Betting',
            description='Sports data - teams, standings, scores from major leagues',
            extractor_class='engines.extractors.espn_extractor.ESPNExtractor',
            transformer_class='engines.industry.betting.transform.BettingTransformer',
            data_sources=['ESPN API'],
            requires_api_key=False,
            default_params={'leagues': ['nfl', 'nba', 'mlb', 'nhl']},
            schema_tables=['dim_team', 'dim_league', 'dim_date', 'fact_team_standings'],
        ))
        
        # Media Pipeline (TMDb)
        self.register_pipeline(PipelineInfo(
            name='media',
            vertical='Media',
            description='Movies and TV shows - ratings, popularity, genres',
            extractor_class='engines.extractors.media_extractor.MediaExtractor',
            transformer_class='engines.industry.media.transform.MediaTransformer',
            data_sources=['TMDb API'],
            requires_api_key=True,
            api_key_name='TMDB',
            default_params={'movies_limit': 100, 'tv_limit': 100},
            schema_tables=['dim_title', 'dim_genre', 'dim_language', 'dim_date', 'fact_title_metrics', 'title_genre_bridge'],
            status=PipelineStatus.REQUIRES_KEY,
        ))
        
        self.logger.info(f"Registered {len(self._pipelines)} ETL pipelines")
    
    def register_pipeline(self, info: PipelineInfo):
        """Register a new pipeline."""
        self._pipelines[info.name] = info
        self.logger.debug(f"Registered pipeline: {info.name}")
    
    def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all registered pipelines with metadata."""
        return [
            {
                'name': p.name,
                'vertical': p.vertical,
                'description': p.description,
                'data_sources': p.data_sources,
                'requires_api_key': p.requires_api_key,
                'api_key_name': p.api_key_name,
                'status': p.status.value,
                'schema_tables': p.schema_tables,
            }
            for p in self._pipelines.values()
        ]
    
    def get_pipeline_info(self, name: str) -> Optional[PipelineInfo]:
        """Get pipeline info by name."""
        return self._pipelines.get(name)
    
    def get_extractor(self, name: str, **kwargs):
        """
        Get an extractor instance for a pipeline.
        
        Args:
            name: Pipeline name
            **kwargs: Arguments to pass to extractor constructor
            
        Returns:
            Extractor instance
        """
        info = self._pipelines.get(name)
        if not info:
            raise ValueError(f"Unknown pipeline: {name}")
        
        # Import and instantiate
        module_path, class_name = info.extractor_class.rsplit('.', 1)
        module = __import__(module_path, fromlist=[class_name])
        extractor_class = getattr(module, class_name)
        
        return extractor_class(**kwargs)
    
    def get_transformer(self, name: str, **kwargs):
        """
        Get a transformer instance for a pipeline.
        
        Args:
            name: Pipeline name
            **kwargs: Arguments to pass to transformer constructor
            
        Returns:
            Transformer instance
        """
        info = self._pipelines.get(name)
        if not info:
            raise ValueError(f"Unknown pipeline: {name}")
        
        # Import and instantiate
        module_path, class_name = info.transformer_class.rsplit('.', 1)
        module = __import__(module_path, fromlist=[class_name])
        transformer_class = getattr(module, class_name)
        
        return transformer_class(**kwargs)
    
    def get_default_params(self, name: str) -> Dict[str, Any]:
        """Get default parameters for a pipeline."""
        info = self._pipelines.get(name)
        return info.default_params.copy() if info else {}
    
    def check_pipeline_status(self, name: str) -> PipelineStatus:
        """Check if a pipeline is ready to run."""
        info = self._pipelines.get(name)
        if not info:
            return PipelineStatus.ERROR
        
        if info.requires_api_key:
            # Check if API key is configured
            try:
                from .config import API_KEYS
                key = API_KEYS.get(info.api_key_name, '')
                if not key:
                    return PipelineStatus.REQUIRES_KEY
            except:
                return PipelineStatus.REQUIRES_KEY
        
        return PipelineStatus.AVAILABLE


# Global registry instance
_registry = None

def get_registry() -> ETLRegistry:
    """Get the global ETL registry instance."""
    global _registry
    if _registry is None:
        _registry = ETLRegistry()
    return _registry
