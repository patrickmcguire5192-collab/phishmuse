"""
Phish.net API Client with Local Caching

Handles all API interactions with automatic caching to avoid
repeated API calls during analysis iterations.
"""

import os
import json
import hashlib
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

load_dotenv()

class PhishNetAPI:
    """Client for Phish.net API v5 with local file caching."""

    BASE_URL = "https://api.phish.net/v5"

    def __init__(self, api_key: Optional[str] = None, cache_dir: str = "data/cache"):
        self.api_key = api_key or os.getenv("PHISHNET_API_KEY")
        if not self.api_key:
            raise ValueError("API key required. Set PHISHNET_API_KEY env var or pass api_key parameter.")

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_expiry_hours = 24  # Re-fetch data older than this

    def _cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate a unique cache key for this request."""
        param_str = json.dumps(params, sort_keys=True)
        key_str = f"{endpoint}:{param_str}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def _get_cached(self, cache_key: str) -> Optional[Dict]:
        """Get cached response if valid."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if not cache_file.exists():
            return None

        with open(cache_file, 'r') as f:
            cached = json.load(f)

        # Check expiry
        cached_time = datetime.fromisoformat(cached['_cached_at'])
        if datetime.now() - cached_time > timedelta(hours=self.cache_expiry_hours):
            return None

        return cached['data']

    def _save_cache(self, cache_key: str, data: Dict):
        """Save response to cache."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        cached = {
            '_cached_at': datetime.now().isoformat(),
            'data': data
        }
        with open(cache_file, 'w') as f:
            json.dump(cached, f)

    def _request(self, endpoint: str, params: Optional[Dict] = None, use_cache: bool = True) -> Dict:
        """Make API request with optional caching."""
        params = params or {}
        params['apikey'] = self.api_key

        cache_key = self._cache_key(endpoint, params)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if use_cache:
            self._save_cache(cache_key, data)

        return data

    # ==================== Show Endpoints ====================

    def get_shows_by_year(self, year: int) -> List[Dict]:
        """Get all shows for a specific year."""
        result = self._request(f"shows/showyear/{year}.json", {'order_by': 'showdate'})
        return result.get('data', [])

    def get_show_by_date(self, date: str) -> Dict:
        """Get show details for a specific date (YYYY-MM-DD)."""
        result = self._request(f"shows/showdate/{date}.json")
        return result.get('data', [])

    def get_all_shows(self, start_year: int = 1983, end_year: int = None) -> List[Dict]:
        """Get all shows in a year range."""
        end_year = end_year or datetime.now().year
        all_shows = []
        for year in range(start_year, end_year + 1):
            shows = self.get_shows_by_year(year)
            all_shows.extend(shows)
            print(f"Fetched {len(shows)} shows from {year}")
        return all_shows

    # ==================== Setlist Endpoints ====================

    def get_setlists_by_year(self, year: int) -> List[Dict]:
        """Get all setlist entries for a specific year."""
        result = self._request(
            f"setlists/showyear/{year}.json",
            {'order_by': 'showdate', 'direction': 'asc'}
        )
        return result.get('data', [])

    def get_setlist_by_date(self, date: str) -> List[Dict]:
        """Get setlist for a specific show date."""
        result = self._request(f"setlists/showdate/{date}.json")
        return result.get('data', [])

    def get_setlists_by_song(self, song_slug: str) -> List[Dict]:
        """Get all setlist entries for a specific song."""
        result = self._request(f"setlists/slug/{song_slug}.json")
        return result.get('data', [])

    def get_all_setlists(self, start_year: int = 1983, end_year: int = None) -> List[Dict]:
        """Get all setlist entries in a year range."""
        end_year = end_year or datetime.now().year
        all_setlists = []
        for year in range(start_year, end_year + 1):
            setlists = self.get_setlists_by_year(year)
            all_setlists.extend(setlists)
            print(f"Fetched {len(setlists)} setlist entries from {year}")
        return all_setlists

    # ==================== Song Endpoints ====================

    def get_all_songs(self) -> List[Dict]:
        """Get list of all songs in the database."""
        result = self._request("songs.json")
        return result.get('data', [])

    def get_song_by_slug(self, slug: str) -> Dict:
        """Get song details by slug."""
        result = self._request(f"songs/slug/{slug}.json")
        data = result.get('data', [])
        return data[0] if data else {}

    # ==================== Venue Endpoints ====================

    def get_all_venues(self) -> List[Dict]:
        """Get list of all venues."""
        result = self._request("venues.json")
        return result.get('data', [])

    # ==================== Jamcharts ====================

    def get_jamcharts(self) -> List[Dict]:
        """Get all jamchart entries."""
        result = self._request("jamcharts.json")
        return result.get('data', [])

    def get_jamcharts_by_song(self, song_slug: str) -> List[Dict]:
        """Get jamchart entries for a specific song."""
        result = self._request(f"jamcharts/slug/{song_slug}.json")
        return result.get('data', [])


def clear_cache(cache_dir: str = "data/cache"):
    """Clear all cached data."""
    cache_path = Path(cache_dir)
    if cache_path.exists():
        for f in cache_path.glob("*.json"):
            f.unlink()
        print(f"Cleared cache at {cache_path}")


if __name__ == "__main__":
    # Quick test
    api = PhishNetAPI()
    shows = api.get_shows_by_year(2024)
    print(f"Found {len(shows)} shows in 2024")
    if shows:
        print(f"First show: {shows[0].get('showdate')} at {shows[0].get('venue')}")
