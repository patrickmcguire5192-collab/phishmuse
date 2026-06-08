#!/usr/bin/env python3
"""
Phantasy Tour Engine for JamMuse
=================================

Query engine for bands available through the Phantasy Tour API.
Supports String Cheese Incident, Disco Biscuits, and Lotus.

Note: Phantasy Tour doesn't have duration data, so we can answer
play counts, gaps, setlists, debuts - but not "longest" or "best" queries.

PT API:
- Shows: GET /api/bands/{id}/shows?pageSize=100&page={n}
- Songs: GET /api/songs/?bandId={id}
- Setlist: GET /api/shows/{showId}/songstats
- No API key needed
"""

import json
import re
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import random as _random

from scripts.date_utils import (
    extract_date_from_query, is_setlist_query,
    is_random_show_query, extract_year_filter,
    is_shows_on_date_query, extract_month_day_from_query,
    detect_holiday_name, HOLIDAY_DISPLAY,
)

# Cache directory
CACHE_DIR = Path(__file__).parent.parent / "data" / "pt_cache"
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    pass


@dataclass
class QueryResult:
    """Structured result from a query."""
    success: bool
    answer: str
    band: str = None
    highlight: Optional[str] = None
    card_data: Optional[Dict[str, Any]] = None
    related_queries: Optional[List[str]] = None
    raw_data: Optional[Any] = None


# =============================================================================
# BAND CONFIGURATIONS - Phantasy Tour bands
# =============================================================================

PT_BANDS = {
    "sci": {
        "name": "String Cheese Incident",
        "pt_id": 3,
        "aliases": ["string cheese", "sci", "cheese incident", "string cheese incident",
                     "the string cheese incident"],

        "song_aliases": {
            "black clouds": "Black Clouds",
            "rivertrance": "Rivertrance",
            "texas": "Texas",
            "sometimes a river": "Sometimes a River",
            "round the wheel": "Round the Wheel",
            "rtw": "Round the Wheel",
            "rosie": "Rosie",
            "lands end": "Land's End",
            "land's end": "Land's End",
            "joyful sound": "Joyful Sound",
            "search": "Search",
            "desert dawn": "Desert Dawn",
            "jellyfish": "Jellyfish",
            "close your eyes": "Close Your Eyes",
            "got what he wanted": "Got What He Wanted",
            "born on the wrong planet": "Born on the Wrong Planet",
            "wrong planet": "Born on the Wrong Planet",
            "bumpin reel": "Bumpin' Reel",
            "outside inside": "Outside Inside",
            "sirens": "Sirens",
            "rhythm of the road": "Rhythm of the Road",
            "restless wind": "Restless Wind",
            "colorado bluebird sky": "Colorado Bluebird Sky",
            "bluebird sky": "Colorado Bluebird Sky",
            "cedar laurels": "Cedar Laurels",
            "missin the ski boat": "Missin' the Ski Boat",
            "ski boat": "Missin' the Ski Boat",
            "one step closer": "One Step Closer",
            "45th of november": "45th of November",
            "cant stop now": "Can't Stop Now",
            "can't stop now": "Can't Stop Now",
            "little hands": "Little Hands",
            "bam": "BAM!",
            "until the music's over": "Until the Music's Over",
            "way that it goes": "Way That It Goes",
            "howard": "Howard",
            "best feeling": "Best Feeling",
            "lester": "Lester Had a Coconut",
            "lester had a coconut": "Lester Had a Coconut",
            "on the road": "On the Road",
            "just passing through": "Just Passing Through",
        },

        "jam_vehicles": [
            "Black Clouds", "Rivertrance", "Texas", "Sometimes a River",
            "Round the Wheel", "Rosie", "Land's End", "Joyful Sound",
            "Search", "Desert Dawn"
        ],
    },

    "biscuits": {
        "name": "The Disco Biscuits",
        "pt_id": 4,
        "aliases": ["disco biscuits", "biscuits", "bisco", "the disco biscuits", "tdb"],

        "song_aliases": {
            "basis for a day": "Basis For a Day",
            "basis": "Basis For a Day",
            "bfad": "Basis For a Day",
            "helicopters": "Helicopters",
            "helis": "Helicopters",
            "magellan": "Magellan",
            "morph dusseldorf": "Morph Dusseldorf",
            "morph": "Morph Dusseldorf",
            "memphis": "M.E.M.P.H.I.S.",
            "m.e.m.p.h.i.s.": "M.E.M.P.H.I.S.",
            "spaga": "Spaga",
            "voices insane": "Voices Insane",
            "voices": "Voices Insane",
            "i-man": "I-Man",
            "iman": "I-Man",
            "cyclone": "Cyclone",
            "reactor": "Reactor",
            "above the waves": "Above the Waves",
            "atw": "Above the Waves",
            "astronaut": "Astronaut",
            "vassillios": "Vassillios",
            "story of the world": "Story of the World",
            "sotw": "Story of the World",
            "hope": "Hope",
            "hot air balloon": "Hot Air Balloon",
            "hab": "Hot Air Balloon",
            "palace": "Palace",
            "crickets": "Crickets",
            "shelby rose": "Shelby Rose",
            "shelby": "Shelby Rose",
            "caterpillar": "Caterpillar",
            "munchkin invasion": "Munchkin Invasion",
            "munchkin": "Munchkin Invasion",
            "home again": "Home Again",
            "orch theme": "Orch Theme",
            "seven seas": "Seven Seas",
            "shimmy": "Shimmy",
            "mindless dribble": "Mindless Dribble",
            "floes": "Floes",
            "aceetobee": "Aceetobee",
            "jigsaw earth": "Jigsaw Earth",
            "svenghali": "Svenghali",
            "little betty boop": "Little Betty Boop",
            "betty boop": "Little Betty Boop",
            "boop": "Little Betty Boop",
            "portal to an empty head": "Portal to an Empty Head",
            "portal": "Portal to an Empty Head",
            "spraypaint": "Spraypaint",
            "bazaar escape": "Bazaar Escape",
            "escape": "Bazaar Escape",
            "the very moon": "The Very Moon",
            "very moon": "The Very Moon",
            "invincible": "Invincible",
            "ms bees schoolyard": "Ms. Bees Schoolyard",
            "ms bees": "Ms. Bees Schoolyard",
            "tractorbeam": "Tractorbeam",
            "rock candy": "Rock Candy",
            "abraxas": "Abraxas",
            "kamaole sands": "Kamaole Sands",
        },

        "jam_vehicles": [
            "Basis For a Day", "Helicopters", "Magellan", "Morph Dusseldorf",
            "M.E.M.P.H.I.S.", "Spaga", "Voices Insane", "I-Man",
            "Cyclone", "Reactor"
        ],
    },

    "lotus": {
        "name": "Lotus",
        "pt_id": 12,
        "aliases": ["lotus"],

        "song_aliases": {
            "greet the mind": "Greet the Mind",
            "nematode": "Nematode",
            "shimmer and out": "Shimmer and Out",
            "spiritualize": "Spiritualize",
            "umbilical moonrise": "Umbilical Moonrise",
            "moonrise": "Umbilical Moonrise",
            "wax": "Wax",
            "tip of the tongue": "Tip of the Tongue",
            "flower sermon": "Flower Sermon",
            "suitcases": "Suitcases",
            "age of inexperience": "Age of Inexperience",
            "age": "Age of Inexperience",
            "bush pilot": "Bush Pilot",
            "colorado": "Colorado",
            "did fatt": "Did Fatt",
            "disappear in a blood red sky": "Disappear in a Blood Red Sky",
            "disappear": "Disappear in a Blood Red Sky",
            "gilded age": "Gilded Age",
            "hammerstrike": "Hammerstrike",
            "in an outline": "In an Outline",
            "intro to a cell": "Intro to a Cell",
            "jump off": "Jump Off",
            "juggernaut": "Juggernaut",
            "lucid awakening": "Lucid Awakening",
            "lucid": "Lucid Awakening",
            "mikesnack": "Mikesnack",
            "kodiak": "Kodiak",
            "113": "113",
            "128": "128",
            "slow cookin": "Slow Cookin'",
            "slow cookin'": "Slow Cookin'",
            "travel": "Travel",
            "what did i do wrong": "What Did I Do Wrong",
            "behind midwest lines": "Behind Midwest Lines",
            "midwest lines": "Behind Midwest Lines",
            "constellations": "Constellations",
            "bubonic tonic": "Bubonic Tonic",
        },

        "jam_vehicles": [
            "Greet the Mind", "Nematode", "Shimmer and Out", "Spiritualize",
            "Umbilical Moonrise", "Wax", "Tip of the Tongue", "Flower Sermon",
            "Suitcases"
        ],
    },
}


PT_API_BASE = "https://www.phantasytour.com/api"

# Live-fetch throttling (cache misses only; no effect on cache hits / production serving).
PT_REQUEST_DELAY_SEC = 0.3
PT_MAX_RETRIES = 4


class PhantasyTourEngine:
    """Query engine for Phantasy Tour bands."""

    def __init__(self, band_key: str):
        """Initialize engine for a specific band."""
        if band_key not in PT_BANDS:
            raise ValueError(f"Unknown band: {band_key}. Available: {list(PT_BANDS.keys())}")

        self.band_key = band_key
        self.config = PT_BANDS[band_key]
        self.band_name = self.config["name"]
        self.pt_id = self.config["pt_id"]

        # Caches
        self._shows_cache = None
        self._songs_catalog = None
        self._setlist_cache = {}  # show_id -> setlist

    def _fetch_api(self, url: str) -> Any:
        """Fetch from Phantasy Tour API with file-based caching."""
        # Build cache key from URL
        cache_key = url.replace(PT_API_BASE, "").replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")
        cache_key = f"pt_{self.band_key}{cache_key}"
        cache_file = CACHE_DIR / f"{cache_key}.json"

        # Check cache (1 hour expiry)
        if cache_file.exists():
            age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if age < 3600:
                with open(cache_file) as f:
                    return json.load(f)

        # Live fetch with throttle + 429 backoff (cache misses only).
        last_err = None
        for attempt in range(PT_MAX_RETRIES):
            time.sleep(PT_REQUEST_DELAY_SEC)
            try:
                req = urllib.request.Request(url, headers={
                    "Accept": "application/json",
                    "User-Agent": "JamMuse/1.0"
                })
                with urllib.request.urlopen(req, timeout=30) as response:
                    data = json.loads(response.read().decode())

                # Cache response
                try:
                    with open(cache_file, 'w') as f:
                        json.dump(data, f)
                except OSError:
                    pass

                return data
            except urllib.error.HTTPError as e:
                last_err = e
                if e.code == 429:
                    retry_after = int(e.headers.get("Retry-After", 2 ** attempt))
                    print(f"  PT 429 — backing off {retry_after}s (attempt {attempt + 1}/{PT_MAX_RETRIES})")
                    time.sleep(retry_after)
                    continue
                break
            except Exception as e:
                last_err = e
                break

        print(f"PT API error for {url}: {last_err}")
        # Stale cache fallback
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)
        return []

    def _get_all_shows(self) -> List[dict]:
        """Get all shows for this band (paginated)."""
        if self._shows_cache is not None:
            return self._shows_cache

        all_shows = []
        page = 1

        while True:
            url = f"{PT_API_BASE}/bands/{self.pt_id}/shows?pageSize=100&page={page}"
            data = self._fetch_api(url)
            if not data or not isinstance(data, list) or len(data) == 0:
                break
            all_shows.extend(data)
            if len(data) < 100:
                break
            page += 1

        self._shows_cache = all_shows
        return all_shows

    def _get_songs_catalog(self) -> List[dict]:
        """Get full song catalog for name resolution."""
        if self._songs_catalog is not None:
            return self._songs_catalog

        url = f"{PT_API_BASE}/songs/?bandId={self.pt_id}"
        data = self._fetch_api(url)
        self._songs_catalog = data if isinstance(data, list) else []
        return self._songs_catalog

    def _get_setlist(self, show_id: int) -> List[dict]:
        """Get setlist (songstats) for a specific show."""
        if show_id in self._setlist_cache:
            return self._setlist_cache[show_id]

        url = f"{PT_API_BASE}/shows/{show_id}/songstats"
        data = self._fetch_api(url)
        setlist = data if isinstance(data, list) else []
        self._setlist_cache[show_id] = setlist
        return setlist

    def _resolve_song_id(self, song_name: str) -> Optional[int]:
        """Resolve a song name to its PT song ID via the catalog."""
        catalog = self._get_songs_catalog()
        song_lower = song_name.lower()

        # Exact match first
        for song in catalog:
            if song.get("Name", "").lower() == song_lower:
                return song.get("Id")

        # Partial match
        for song in catalog:
            sname = song.get("Name", "").lower()
            if song_lower in sname or sname in song_lower:
                return song.get("Id")

        return None

    def _get_song_performances(self, song_name: str) -> tuple:
        """Get performances of a song using /api/songs/{id}/shows.

        Returns (performances_list, total_count) where performances_list contains
        the earliest ~30 shows (API limit) and total_count is the true total.
        """
        song_id = self._resolve_song_id(song_name)
        if not song_id:
            return [], 0

        url = f"{PT_API_BASE}/songs/{song_id}/shows"
        data = self._fetch_api(url)

        if isinstance(data, dict):
            shows_data = data.get("aaData", [])
            total_count = data.get("iTotalRecords", len(shows_data))
        elif isinstance(data, list):
            shows_data = data
            total_count = len(shows_data)
        else:
            return [], 0

        performances = []
        for show in shows_data:
            venue = show.get("Venue", {})
            stats = show.get("SongStats", {})
            performances.append({
                "date": show.get("DateTime", ""),
                "venue": venue.get("Name", "Unknown") if isinstance(venue, dict) else "Unknown",
                "city": venue.get("Locale", "") if isinstance(venue, dict) else "",
                "show_id": show.get("Id"),
                "set_number": stats.get("SetNumber") if isinstance(stats, dict) else None,
                "position": stats.get("Position") if isinstance(stats, dict) else None,
            })

        return performances, total_count

    def _get_song_debut(self, song_name: str) -> Optional[str]:
        """Get debut date from the song catalog."""
        catalog = self._get_songs_catalog()
        song_lower = song_name.lower()
        for song in catalog:
            if song.get("Name", "").lower() == song_lower:
                return song.get("Debut")
        # Partial match
        for song in catalog:
            if song_lower in song.get("Name", "").lower():
                return song.get("Debut")
        return None

    def _find_last_played(self, song_name: str, max_shows: int = 200) -> Optional[dict]:
        """Find the most recent performance by scanning recent shows' setlists.

        Starts from the most recent show and works backward — efficient for
        songs that haven't been shelved for too long.
        """
        shows = self._get_all_shows()
        song_lower = song_name.lower()

        # Shows are typically returned newest-first from PT, but let's sort to be sure
        sorted_shows = sorted(shows, key=lambda s: self._parse_date(s.get("dateTime", "")) or datetime.min, reverse=True)

        checked = 0
        for show in sorted_shows:
            if not show.get("hasSetlist"):
                continue
            show_id = show.get("id")
            if not show_id:
                continue

            setlist = self._get_setlist(show_id)
            for entry in setlist:
                if entry.get("Name", "").lower() == song_lower:
                    venue_name, locale = self._get_venue_info(show)
                    return {
                        "date": show.get("dateTime", ""),
                        "venue": venue_name,
                        "city": locale,
                        "show_id": show_id,
                        "shows_checked": checked,
                    }

            checked += 1
            if checked >= max_shows:
                break

        return None

    def _normalize_song_name(self, query: str) -> Optional[str]:
        """Normalize song name from query using aliases and catalog."""
        query_lower = query.lower().strip()

        # Remove filler words
        filler_phrases = ["how many times", "when did they", "tell me about", "gap on", "gap for"]
        for phrase in filler_phrases:
            query_lower = query_lower.replace(phrase, " ")

        filler_words = ["play", "played", "last", "first", "stats", "have", "they",
                        "has", "been", "info", "about", "statistics"]
        for word in filler_words:
            query_lower = re.sub(rf'\b{word}\b', ' ', query_lower)

        query_lower = re.sub(r'[?!.,]', '', query_lower)
        query_lower = " ".join(query_lower.split()).strip()

        if not query_lower:
            return None

        # Check aliases (exact)
        song_aliases = self.config.get("song_aliases", {})
        if query_lower in song_aliases:
            return song_aliases[query_lower]

        # Partial alias matches
        for alias, song_name in song_aliases.items():
            if alias in query_lower or query_lower in alias:
                return song_name

        # Check song catalog for exact match
        catalog = self._get_songs_catalog()
        for song in catalog:
            if song.get("Name", "").lower() == query_lower:
                return song["Name"]

        # Partial catalog matches
        for song in catalog:
            sname = song.get("Name", "").lower()
            if query_lower in sname or sname in query_lower:
                return song["Name"]

        # Return as-is (title case)
        return query_lower.title()

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse PT ISO date format."""
        if not date_str:
            return None
        try:
            # PT dates: 2025-08-29T19:00:00-07:00
            # Strip timezone offset for parsing
            clean = re.sub(r'[+-]\d{2}:\d{2}$', '', date_str)
            return datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
        except (ValueError, TypeError):
            try:
                return datetime.strptime(date_str[:10], "%Y-%m-%d")
            except (ValueError, TypeError):
                return None

    def _format_date(self, date_str: str) -> str:
        """Format date as YYYY-MM-DD."""
        dt = self._parse_date(date_str)
        if dt:
            return dt.strftime("%Y-%m-%d")
        return date_str[:10] if date_str and len(date_str) >= 10 else date_str

    def _get_venue_info(self, show: dict) -> tuple:
        """Extract venue name and location from a show dict."""
        venue_data = show.get("venue", {})
        if isinstance(venue_data, dict):
            name = venue_data.get("name", "Unknown")
            locale = venue_data.get("locale", "")
        else:
            name = "Unknown"
            locale = ""
        return name, locale

    # =========================================================================
    # QUERY METHODS
    # =========================================================================

    def query_play_count(self, song_name: str, year: int = None) -> QueryResult:
        """How many times has a song been played?"""
        normalized = self._normalize_song_name(song_name) or song_name
        performances, total_count = self._get_song_performances(normalized)

        if total_count == 0:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find any performances of '{normalized}' by {self.band_name}. "
                       f"Try one of their popular songs: {', '.join(self.config['jam_vehicles'][:3])}."
            )

        if year:
            # Year filtering only works on the returned subset (earliest ~30)
            # so we note this limitation for large catalogs
            year_str = str(year)
            year_perfs = [p for p in performances if self._format_date(p["date"]).startswith(year_str)]
            count = len(year_perfs)

            if count == 0:
                answer = f"No performances of {normalized} found in {year} (from available data). It has been played {total_count} times total."
            else:
                year_dates = sorted(self._parse_date(p["date"]) for p in year_perfs if self._parse_date(p["date"]))
                first_date = year_dates[0].strftime("%Y-%m-%d") if year_dates else "?"
                last_date = year_dates[-1].strftime("%Y-%m-%d") if year_dates else "?"
                answer = (
                    f"{self.band_name} played {normalized} at least **{count} times** in {year} "
                    f"({total_count} total performances all-time)."
                )
                answer += f"\n\nFirst in {year}: {first_date}\nLast in {year}: {last_date}"

            return QueryResult(
                success=True,
                band=self.band_name,
                answer=answer,
                highlight=str(total_count),
            )

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"{self.band_name} has played {normalized} **{total_count} times**.",
            highlight=str(total_count),
            card_data={
                "type": "count",
                "title": normalized,
                "stat": total_count,
                "stat_label": "times played"
            },
            related_queries=[
                f"When did they last play {normalized}?",
                f"When did they first play {normalized}?"
            ]
        )

    def query_gap(self, song_name: str) -> QueryResult:
        """How many shows since a song was last played?"""
        normalized = self._normalize_song_name(song_name) or song_name
        _, total_count = self._get_song_performances(normalized)

        if total_count == 0:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find any performances of '{normalized}'."
            )

        # Scan recent shows to find last performance
        last_info = self._find_last_played(normalized)

        if not last_info:
            return QueryResult(
                success=True,
                band=self.band_name,
                answer=f"{normalized} has been played {total_count} times but hasn't appeared in the last 200 shows checked.",
                highlight="200+",
            )

        last_date = self._format_date(last_info["date"])
        venue = last_info["venue"]
        city = last_info["city"]
        location = f"{venue}, {city}" if city else venue

        # Count shows since
        shows = self._get_all_shows()
        last_dt = self._parse_date(last_info["date"])
        shows_since = sum(1 for s in shows if self._parse_date(s.get("dateTime", ""))
                         and self._parse_date(s.get("dateTime", "")) > last_dt)

        if shows_since == 0:
            answer = f"{normalized} was played at the most recent show ({last_date} at {location})."
        else:
            answer = f"{normalized} was last played {shows_since} shows ago on {last_date} at {location}."

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=answer,
            highlight=str(shows_since),
            card_data={
                "type": "gap",
                "title": normalized,
                "stat": shows_since,
                "stat_label": "shows ago",
                "extra": {"last_played": last_date, "venue": location}
            },
            related_queries=[
                f"How many times have they played {normalized}?",
                f"When did they first play {normalized}?"
            ]
        )

    def query_first_played(self, song_name: str) -> QueryResult:
        """When was a song first played (debut)?"""
        normalized = self._normalize_song_name(song_name) or song_name

        # Try catalog debut first (most accurate)
        debut_date = self._get_song_debut(normalized)
        if debut_date:
            first_date = self._format_date(debut_date)
            # Get performances for venue info
            performances, _ = self._get_song_performances(normalized)
            if performances:
                performances.sort(key=lambda x: self._parse_date(x["date"]) or datetime.max)
                first = performances[0]
                venue = first["venue"]
                city = first["city"]
                location = f"{venue}, {city}" if city else venue
                return QueryResult(
                    success=True,
                    band=self.band_name,
                    answer=f"{normalized} debuted on {first_date} at {location}.",
                    card_data={"type": "debut", "title": normalized,
                               "extra": {"debut_date": first_date, "venue": location}},
                    related_queries=[f"How many times have they played {normalized}?",
                                     f"When did they last play {normalized}?"]
                )
            return QueryResult(
                success=True, band=self.band_name,
                answer=f"{normalized} debuted on {first_date}.",
            )

        # Fallback to API data
        performances, total_count = self._get_song_performances(normalized)
        if not performances:
            return QueryResult(success=False, band=self.band_name,
                               answer=f"I couldn't find any performances of '{normalized}'.")

        performances.sort(key=lambda x: self._parse_date(x["date"]) or datetime.max)
        first = performances[0]
        first_date = self._format_date(first["date"])
        venue = first["venue"]
        city = first["city"]
        location = f"{venue}, {city}" if city else venue

        return QueryResult(
            success=True, band=self.band_name,
            answer=f"{normalized} debuted on {first_date} at {location}.",
            card_data={"type": "debut", "title": normalized,
                       "extra": {"debut_date": first_date, "venue": location}},
            related_queries=[f"How many times have they played {normalized}?",
                             f"When did they last play {normalized}?"]
        )

    def query_song_stats(self, song_name: str) -> QueryResult:
        """Get comprehensive stats for a song."""
        normalized = self._normalize_song_name(song_name) or song_name
        performances, total_count = self._get_song_performances(normalized)

        if total_count == 0:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{normalized}' in {self.band_name}'s catalog."
            )

        # Debut from catalog
        debut_date = self._get_song_debut(normalized)
        first_date = self._format_date(debut_date) if debut_date else None

        # Fallback debut from performance data
        if not first_date and performances:
            performances.sort(key=lambda x: self._parse_date(x["date"]) or datetime.max)
            first_date = self._format_date(performances[0]["date"])

        # Last played via recent show scan
        last_info = self._find_last_played(normalized)
        if last_info:
            last_date = self._format_date(last_info["date"])
            shows = self._get_all_shows()
            last_dt = self._parse_date(last_info["date"])
            shows_since = sum(1 for s in shows if self._parse_date(s.get("dateTime", ""))
                             and self._parse_date(s.get("dateTime", "")) > last_dt)
        else:
            last_date = None
            shows_since = None

        lines = [
            f"**{normalized}** - {self.band_name} Stats\n",
            f"Times played: {total_count}",
        ]
        if first_date:
            lines.append(f"First played: {first_date}")
        if last_date:
            lines.append(f"Last played: {last_date}")
        if shows_since is not None and shows_since > 0:
            lines.append(f"Current gap: {shows_since} shows")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=str(total_count),
            card_data={
                "type": "stats",
                "title": normalized,
                "stat": total_count,
                "stat_label": "times played",
                "extra": {
                    "first_played": first_date,
                    "last_played": last_date,
                    "gap": shows_since
                }
            },
            related_queries=[
                f"When did they last play {normalized}?",
                f"When did they first play {normalized}?"
            ]
        )

    def query_show_count(self, year: int = None) -> QueryResult:
        """How many shows has the band played?"""
        shows = self._get_all_shows()

        if year:
            year_str = str(year)
            shows = [s for s in shows if self._format_date(s.get("dateTime", "")).startswith(year_str)]
            answer = f"{self.band_name} played {len(shows)} shows in {year}."
        else:
            answer = f"{self.band_name} has played {len(shows)} shows (in Phantasy Tour database)."

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=answer,
            highlight=str(len(shows))
        )

    def query_random_show(self, year: int = None) -> QueryResult:
        """Return a random show, optionally filtered by year."""
        shows = self._get_all_shows()

        candidates = shows
        if year:
            candidates = [s for s in shows if self._format_date(s.get("dateTime", "")).startswith(str(year))]

        if not candidates:
            if year:
                return QueryResult(success=False, band=self.band_name,
                                   answer=f"No {self.band_name} shows found for {year}.")
            return QueryResult(success=False, band=self.band_name, answer="No show data available.")

        show = _random.choice(candidates)
        date = self._format_date(show.get("dateTime", ""))
        venue_name, locale = self._get_venue_info(show)

        year_display = f" from {year}" if year else ""
        lines = [f"Here's a random {self.band_name} show{year_display} for you!\n"]
        lines.append(f"**{date}** - {venue_name}")
        if locale:
            lines.append(f"*{locale}*")

        # Try to include setlist
        show_id = show.get("id")
        if show_id and show.get("hasSetlist"):
            setlist = self._get_setlist(show_id)
            if setlist:
                lines.append("")
                # Group by set number
                sets = {}
                for entry in setlist:
                    sn = entry.get("SetNumber", 1)
                    sets.setdefault(sn, []).append(entry.get("Name", "?"))

                for sn in sorted(sets.keys()):
                    if sn == 9 or sn == 0 or (isinstance(sn, str) and sn.upper() == "E"):
                        set_label = "Encore"
                    else:
                        set_label = f"Set {sn}"
                    lines.append(f"**{set_label}:** {', '.join(sets[sn])}")

        related = [f"random {self.band_name.lower()} show from {date[:4]}",
                   f"{self.band_name.lower()} setlist {date}"]
        if year:
            related.append(f"random {self.band_name.lower()} show")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=date,
            related_queries=related
        )

    def query_setlist(self, date: str) -> QueryResult:
        """Get setlist for a specific date."""
        date_clean = extract_date_from_query(date) or date
        shows = self._get_all_shows()

        for show in shows:
            show_date = self._format_date(show.get("dateTime", ""))
            if show_date == date_clean:
                venue_name, locale = self._get_venue_info(show)

                lines = [f"**{self.band_name}** - {show_date}"]
                if locale:
                    lines.append(f"*{venue_name}, {locale}*\n")
                else:
                    lines.append(f"*{venue_name}*\n")

                show_id = show.get("id")
                if show_id and show.get("hasSetlist"):
                    setlist = self._get_setlist(show_id)
                    if setlist:
                        sets = {}
                        for entry in setlist:
                            sn = entry.get("SetNumber", 1)
                            sets.setdefault(sn, []).append(entry.get("Name", "?"))

                        for sn in sorted(sets.keys()):
                            if sn == 9 or sn == 0 or (isinstance(sn, str) and sn.upper() == "E"):
                                set_label = "Encore"
                            else:
                                set_label = f"Set {sn}"
                            lines.append(f"**{set_label}** {', '.join(sets[sn])}")
                    else:
                        lines.append("(Setlist not yet entered)")
                else:
                    lines.append("(Setlist not yet entered)")

                return QueryResult(
                    success=True,
                    band=self.band_name,
                    answer="\n".join(lines)
                )

        return QueryResult(
            success=False,
            band=self.band_name,
            answer=f"No setlist found for {self.band_name} on {date_clean}."
        )

    def query_shows_on_date(self, month: int, day: int, holiday_name: str = None) -> QueryResult:
        """Query shows played on a specific month/day across all years."""
        shows = self._get_all_shows()
        matching_shows = []
        for show in shows:
            dt = self._parse_date(show.get("dateTime", ""))
            if dt and dt.month == month and dt.day == day:
                matching_shows.append(show)

        matching_shows.sort(key=lambda x: self._parse_date(x.get("dateTime", "")) or datetime.min)

        count = len(matching_shows)
        date_display = HOLIDAY_DISPLAY.get(holiday_name, f"{month}/{day}") if holiday_name else f"{month}/{day}"

        if count == 0:
            return QueryResult(
                success=True,
                band=self.band_name,
                answer=f"{self.band_name} has never played on {date_display}.",
            )

        years = [str(self._parse_date(s.get("dateTime", "")).year)
                 for s in matching_shows if self._parse_date(s.get("dateTime", ""))]
        first_year = years[0] if years else "?"
        last_year = years[-1] if years else "?"
        venues = set()
        for s in matching_shows:
            vname, _ = self._get_venue_info(s)
            venues.add(vname)

        lines = [f"{self.band_name} has played **{count} shows** on {date_display}!\n"]
        lines.append(f"First: {first_year} | Most Recent: {last_year}")
        lines.append(f"Unique venues: {len(venues)}\n")

        lines.append("Recent shows:")
        for show in reversed(matching_shows[-5:]):
            d = self._format_date(show.get("dateTime", ""))
            vname, locale = self._get_venue_info(show)
            lines.append(f"  \u2022 {d}: {vname}" + (f", {locale}" if locale else ""))

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=str(count),
        )

    # =========================================================================
    # MAIN QUERY ROUTER
    # =========================================================================

    def query(self, question: str) -> QueryResult:
        """Route a natural language question to the appropriate handler."""
        q = question.lower().strip()

        # Random show
        if is_random_show_query(q):
            year = extract_year_filter(question)
            return self.query_random_show(year=year)

        # Shows on a specific month/day
        if is_shows_on_date_query(q):
            month_day = extract_month_day_from_query(question)
            if month_day:
                month, day = month_day
                holiday_name = detect_holiday_name(q)
                return self.query_shows_on_date(month, day, holiday_name)

        # Setlist queries
        if is_setlist_query(q):
            date = extract_date_from_query(question)
            if date:
                return self.query_setlist(date)

        # Gap queries
        if any(word in q for word in ["gap on", "gap for", "how long since", "when did they last",
                                       "last played", "last time", "shows since"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_gap(song)

        # First played / debut queries
        if any(word in q for word in ["first time", "debut", "first played", "when did they first"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_first_played(song)

        # Play count queries
        if any(word in q for word in ["how many times", "how often", "play count", "times played"]):
            year_match = re.search(r'\b(20\d{2}|19\d{2})\b', question)
            year = int(year_match.group(1)) if year_match else None
            song = self._normalize_song_name(question)
            if song:
                return self.query_play_count(song, year=year)

        # Show count queries
        if any(word in q for word in ["how many shows", "show count", "total shows"]):
            year_match = re.search(r'\b(20\d{2}|19\d{2})\b', question)
            year = int(year_match.group(1)) if year_match else None
            return self.query_show_count(year=year)

        # Stats queries
        if any(word in q for word in ["stats", "statistics", "info on", "tell me about"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_song_stats(song)

        # Default: try song stats
        song = self._normalize_song_name(question)
        if song:
            return self.query_song_stats(song)

        # Fallback
        return QueryResult(
            success=False,
            band=self.band_name,
            answer=f"I'm not sure how to answer that about {self.band_name}. Try asking about:\n"
                   f"- Song stats: '{self.config['jam_vehicles'][0]} stats'\n"
                   f"- Play count: 'how many times have they played {self.config['jam_vehicles'][1]}'\n"
                   f"- Gap: 'when did they last play {self.config['jam_vehicles'][2]}'"
        )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_all_pt_aliases() -> Dict[str, str]:
    """Get all song/band aliases mapped to band keys for auto-detection."""
    aliases = {}
    for band_key, config in PT_BANDS.items():
        for alias in config.get("song_aliases", {}).keys():
            aliases[alias.lower()] = band_key
        for alias in config.get("aliases", []):
            aliases[alias.lower()] = band_key
    return aliases


if __name__ == "__main__":
    print("=" * 60)
    print("Phantasy Tour Engine - Testing")
    print("=" * 60)

    # Test SCI
    print("\n--- STRING CHEESE INCIDENT ---")
    sci = PhantasyTourEngine("sci")
    print(f"Shows: {len(sci._get_all_shows())}")
    print(f"Songs in catalog: {len(sci._get_songs_catalog())}")

    for q in ["Black Clouds stats", "how many times have they played Rivertrance?"]:
        print(f"\nQ: {q}")
        result = sci.query(q)
        print(result.answer[:300])
