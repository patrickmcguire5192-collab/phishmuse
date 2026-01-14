"""
Phish.in API Client

Phish.in is an archive of live Phish audience recordings with DURATION DATA.
This is the key missing piece from Phish.net - actual song lengths in milliseconds.

API Base: https://phish.in/api/v2
Authentication: NOT required for public read endpoints (shows, tracks, songs, venues)
Rate Limits: Be respectful - cache aggressively

Key data available:
- Track duration in milliseconds
- Show total duration
- Audio URLs (MP3)
- Jam chart tags with timestamps
- Waveform images
"""

import os
import json
import hashlib
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any


class PhishInAPI:
    """Client for Phish.in API v2 - the source for song duration data."""

    BASE_URL = "https://phish.in/api/v2"

    def __init__(self, cache_dir: str = "data/cache/phishin"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_expiry_hours = 24 * 7  # Cache for a week (data doesn't change often)

    def _cache_key(self, endpoint: str) -> str:
        """Generate a unique cache key for this request."""
        return hashlib.md5(endpoint.encode()).hexdigest()

    def _get_cached(self, cache_key: str) -> Optional[Dict]:
        """Get cached response if valid."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if not cache_file.exists():
            return None

        with open(cache_file, 'r') as f:
            cached = json.load(f)

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

    def _request(self, endpoint: str, use_cache: bool = True) -> Dict:
        """Make API request with optional caching."""
        cache_key = self._cache_key(endpoint)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if use_cache:
            self._save_cache(cache_key, data)

        return data

    # ==================== Show Endpoints ====================

    def get_show(self, date: str) -> Dict:
        """
        Get show details including all tracks with durations.

        Args:
            date: Show date in YYYY-MM-DD format

        Returns:
            Show dict with 'tracks' array, each containing 'duration' in milliseconds
        """
        return self._request(f"shows/{date}")

    def get_shows_by_year(self, year: int) -> List[Dict]:
        """Get all shows for a year."""
        # The API uses 'years' endpoint
        data = self._request(f"years/{year}")
        return data.get('shows', [])

    def get_random_show(self) -> Dict:
        """Get a random show."""
        return self._request("shows/random")

    # ==================== Track Endpoints ====================

    def get_track(self, track_id: int) -> Dict:
        """Get details for a specific track."""
        return self._request(f"tracks/{track_id}")

    # ==================== Song Endpoints ====================

    def get_songs(self) -> List[Dict]:
        """Get all songs in the database."""
        return self._request("songs")

    def get_song(self, slug: str) -> Dict:
        """
        Get song metadata (not including tracks).
        Use get_song_tracks() to get all performances with durations.
        """
        return self._request(f"songs/{slug}")

    def get_song_tracks(self, song_slug: str, per_page: int = 1000) -> List[Dict]:
        """
        Get all track performances for a song with duration data.

        Args:
            song_slug: URL-friendly song name (e.g., 'tweezer', 'down-with-disease')
            per_page: Number of results per page (max 1000)

        Returns:
            List of track dicts, each with 'duration' in milliseconds
        """
        data = self._request(f"tracks?song_slug={song_slug}&per_page={per_page}")
        return data.get('tracks', [])

    # ==================== Venue Endpoints ====================

    def get_venues(self) -> List[Dict]:
        """Get all venues."""
        return self._request("venues")

    def get_venue(self, slug: str) -> Dict:
        """Get venue details including shows."""
        return self._request(f"venues/{slug}")

    # ==================== Tour Endpoints ====================

    def get_tours(self) -> List[Dict]:
        """Get all tours."""
        return self._request("tours")

    def get_tour(self, slug: str) -> Dict:
        """Get tour details including shows."""
        return self._request(f"tours/{slug}")

    # ==================== Search ====================

    def search(self, term: str) -> Dict:
        """Search across shows, songs, venues, etc."""
        return self._request(f"search/{term}")

    # ==================== Utility Methods ====================

    def get_track_duration_minutes(self, track: Dict) -> float:
        """Convert track duration from milliseconds to minutes."""
        return track.get('duration', 0) / 1000 / 60

    def get_show_duration_minutes(self, show: Dict) -> float:
        """Convert show duration from milliseconds to minutes."""
        return show.get('duration', 0) / 1000 / 60

    def get_longest_track(self, show: Dict) -> Optional[Dict]:
        """Get the longest track from a show."""
        tracks = show.get('tracks', [])
        if not tracks:
            return None
        return max(tracks, key=lambda t: t.get('duration', 0))


def get_song_duration_stats(api: PhishInAPI, song_slug: str) -> Dict:
    """
    Get duration statistics for a song across all performances.

    Returns:
        Dict with min, max, mean, median durations and performance count
    """
    import numpy as np

    # Get song metadata
    song_data = api.get_song(song_slug)

    # Get all track performances
    tracks = api.get_song_tracks(song_slug)

    if not tracks:
        return {'error': 'No tracks found', 'slug': song_slug}

    durations_ms = [t.get('duration', 0) for t in tracks if t.get('duration')]
    durations_min = [d / 1000 / 60 for d in durations_ms]

    return {
        'song': song_data.get('title', song_slug),
        'slug': song_slug,
        'total_performances': len(tracks),
        'performances_with_duration': len(durations_min),
        'min_duration_min': min(durations_min) if durations_min else 0,
        'max_duration_min': max(durations_min) if durations_min else 0,
        'mean_duration_min': np.mean(durations_min) if durations_min else 0,
        'median_duration_min': np.median(durations_min) if durations_min else 0,
        'std_duration_min': np.std(durations_min) if durations_min else 0,
    }


if __name__ == "__main__":
    api = PhishInAPI()

    # Test: Get NYE 2024 show
    print("Fetching 2024-12-31 show...")
    show = api.get_show("2024-12-31")

    print(f"\nShow: {show['date']} @ {show['venue_name']}")
    print(f"Total duration: {api.get_show_duration_minutes(show):.0f} minutes")

    print(f"\nLongest song of the night:")
    longest = api.get_longest_track(show)
    if longest:
        print(f"  {longest['title']}: {api.get_track_duration_minutes(longest):.1f} minutes")

    print("\nAll tracks:")
    for track in show.get('tracks', [])[:10]:
        mins = api.get_track_duration_minutes(track)
        print(f"  {track['position']:2}. {track['title']:<35} {mins:5.1f} min")

    # Test: Get Tweezer stats
    print("\n" + "="*50)
    print("Tweezer duration statistics:")
    stats = get_song_duration_stats(api, "tweezer")
    print(f"  Performances: {stats['total_performances']}")
    print(f"  Mean: {stats['mean_duration_min']:.1f} min")
    print(f"  Median: {stats['median_duration_min']:.1f} min")
    print(f"  Min: {stats['min_duration_min']:.1f} min")
    print(f"  Max: {stats['max_duration_min']:.1f} min")
