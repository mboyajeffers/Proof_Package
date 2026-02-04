"""
ETL Framework Configuration
API endpoints and rate limits for data extraction.

Author: Mboya Jeffers
"""

# API Endpoints
ENDPOINTS = {
    # Steam
    'STEAM_APP_LIST': 'https://api.steampowered.com/ISteamApps/GetAppList/v2/',
    'STEAM_APP_DETAILS': 'https://store.steampowered.com/api/appdetails',
    'STEAM_PLAYER_COUNT': 'https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/',
    'STEAMSPY_ALL': 'https://steamspy.com/api.php?request=all',
    'STEAMSPY_APP': 'https://steamspy.com/api.php?request=appdetails',
    
    # CoinGecko
    'COINGECKO_MARKETS': 'https://api.coingecko.com/api/v3/coins/markets',
    'COINGECKO_COIN': 'https://api.coingecko.com/api/v3/coins',
    
    # ESPN
    'ESPN_BASE': 'https://site.api.espn.com/apis/site/v2/sports',
    
    # TMDb
    'TMDB_BASE': 'https://api.themoviedb.org/3',
}

# Rate Limits (requests per minute)
RATE_LIMITS = {
    'STEAM': 200,
    'STEAMSPY': 4,
    'COINGECKO': 10,
    'ESPN': 60,
    'TMDB': 10,
}

# API Keys (set via environment variables in production)
API_KEYS = {
    'TMDB': '',  # Get free key at themoviedb.org
}

# Cache TTL (seconds)
CACHE_TTL = {
    'gaming': 3600,
    'crypto': 300,
    'betting': 300,
    'media': 86400,
}
