#!/usr/bin/env python3
"""
JamMuse Query Engine
====================

Multi-band natural language query interface for jam band statistics.
Supports Goose, King Gizzard & the Lizard Wizard, and more via Songfish API.

Each band has its own lore, terminology, and fan language that the engine understands.
"""

import json
import re
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from collections import Counter
from datetime import datetime

# Cache directory for API responses
CACHE_DIR = Path(__file__).parent.parent / "data" / "jammuse_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


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
# BAND CONFIGURATIONS
# =============================================================================

BANDS = {
    "goose": {
        "name": "Goose",
        "api_base": "https://elgoose.net/api/v2",
        "artist_id": 1,  # Filter for main band (not Vasudo)
        "site_url": "https://elgoose.net",

        # Fan terminology and aliases
        "aliases": ["goose", "el goose", "the goose"],

        # Song name aliases (how fans refer to songs)
        "song_aliases": {
            "arcadia": "Arcadia",
            "arc": "Arcadia",
            "hungersite": "Hungersite",
            "hunger": "Hungersite",
            "wysteria": "Wysteria Lane",
            "wysteria lane": "Wysteria Lane",
            "madhuvan": "Madhuvan",
            "madhu": "Madhuvan",
            "tumble": "Tumble",
            "all i need": "All I Need",
            "ain": "All I Need",
            "arrow": "Arrow",
            "creatures": "Creatures",
            "atlas dogs": "Atlas Dogs",
            "atlas": "Atlas Dogs",
            "drive": "Drive",
            "thatch": "Thatch",
            "rosewood": "Rosewood Heart",
            "rosewood heart": "Rosewood Heart",
            "echo of a rose": "Echo of a Rose",
            "echo": "Echo of a Rose",
            "hot tea": "Hot Tea",
            "rockdale": "Rockdale",
            "dripfield": "Dripfield",
            "time to flee": "Time to Flee",
            "earthling or alien": "Earthling or Alien?",
            "earthling": "Earthling or Alien?",
            "empress": "The Empress Of Organos",
            "empress of organos": "The Empress Of Organos",
            "old sea": "This Old Sea",
            "this old sea": "This Old Sea",
            "animal": "Animal",
            "into the myst": "Into the Myst",
            "myst": "Into the Myst",
            "turned clouds": "Turned Clouds",
            "flodown": "Flodown",
            "elmeg": "Elmeg the Wise",
            "elmeg the wise": "Elmeg the Wise",
            "red bird": "Red Bird",
            "butter rum": "Butter Rum",
            "butterrum": "Butter Rum",
            "seekers on the ridge": "Seekers on the Ridge",
            "seekers": "Seekers on the Ridge",
            "yeti": "Yeti",
            "white lights": "White Lights",
            "slow ready": "Slow Ready",
            "factory fiction": "Factory Fiction",
            "borne": "Borne",
            "pancakes": "Pancakes",
            "jive lee": "Jive Lee",
            "jive": "Jive Lee",
            "so ready": "So Ready",
            "doc brown": "Doc Brown",
            "silver rising": "Silver Rising",
        },

        # Venue aliases
        "venue_aliases": {
            "cap": "The Capitol Theatre",
            "capitol": "The Capitol Theatre",
            "cap theatre": "The Capitol Theatre",
            "radio city": "Radio City Music Hall",
            "rcmh": "Radio City Music Hall",
            "msg": "Madison Square Garden",
            "garden": "Madison Square Garden",
            "spac": "Saratoga Performing Arts Center",
            "saratoga": "Saratoga Performing Arts Center",
            "red rocks": "Red Rocks Amphitheatre",
            "peach": "Peach Music Festival",
            "peach fest": "Peach Music Festival",
            "westville": "Westville Music Bowl",
            "nectars": "Nectar's",
            "forest hills": "Forest Hills Stadium",
            "dicks": "Dick's Sporting Goods Park",
            "dicks field": "Dick's Sporting Goods Park",
        },

        # Eras (like Phish 1.0, 2.0, 3.0)
        "eras": {
            "pre-peter": (2014, 2017),    # Before Peter Anspach joined
            "early": (2018, 2019),         # Early Peter era, pre-breakout
            "breakout": (2019, 2021),      # Peach Fest breakthrough
            "arena": (2022, 2030),         # Arena/theater era
        },

        # Special events (like Phish NYE, Halloween)
        "special_events": {
            "goosemas": {"month": 12, "description": "Annual December holiday show"},
            "mustache season": {"months": [5, 6, 7, 8], "description": "Summer tour tradition"},
        },

        # Band members
        "members": {
            "rick": "Rick Mitarotonda",
            "mitarotonda": "Rick Mitarotonda",
            "peter": "Peter Anspach",
            "anspach": "Peter Anspach",
            "trevor": "Trevor Weekz",
            "weekz": "Trevor Weekz",
            "cotter": "Cotter Ellis",
            "ellis": "Cotter Ellis",
            "ben": "Ben Atkind",
            "atkind": "Ben Atkind",
        },

        # Milestone shows
        "milestone_shows": {
            "2019-08-03": "Peach Fest 2019 - The breakout show that put Goose on the map",
            "2022-05-05": "Radio City with Trey Anastasio - 'Passing the torch' moment",
            "2025-06-28": "First MSG headline - Longest set ever played at MSG (4+ hours)",
        },

        # Top jam vehicles (from our jam chart analysis)
        "jam_vehicles": [
            "All I Need", "Madhuvan", "Tumble", "Wysteria Lane", "Drive",
            "Thatch", "Arcadia", "Creatures", "Rosewood Heart", "Arrow",
            "Echo of a Rose", "Hungersite", "Rockdale", "Pancakes", "Hot Tea"
        ],
    },

    "kglw": {
        "name": "King Gizzard & the Lizard Wizard",
        "short_name": "King Gizzard",
        "api_base": "https://kglw.net/api/v2",
        "artist_id": 1,
        "site_url": "https://kglw.net",

        # Fan terminology
        "aliases": ["king gizzard", "kglw", "gizz", "king gizz", "gizzard",
                   "the gizz", "lizard wizard"],

        # Song aliases
        "song_aliases": {
            "head on pill": "Head On/Pill",
            "head on/pill": "Head On/Pill",
            "hop": "Head On/Pill",
            "the river": "The River",
            "river": "The River",
            "dripping tap": "The Dripping Tap",
            "the dripping tap": "The Dripping Tap",
            "tap": "The Dripping Tap",
            "crumbling castle": "Crumbling Castle",
            "crumbling": "Crumbling Castle",
            "am i in heaven": "Am I In Heaven?",
            "heaven": "Am I In Heaven?",
            "gamma knife": "Gamma Knife",
            "gamma": "Gamma Knife",
            "rattlesnake": "Rattlesnake",
            "snake": "Rattlesnake",
            "robot stop": "Robot Stop",
            "robot": "Robot Stop",
            "magma": "Magma",
            "hypertension": "Hypertension",
            "ice v": "Ice V",
            "slow jam 1": "Slow Jam 1",
            "slow jam": "Slow Jam 1",
            "slow jam 2": "Her and I (Slow Jam 2)",
            "her and i": "Her and I (Slow Jam 2)",
            "mind fuzz": "I'm In Your Mind Fuzz",
            "im in your mind": "I'm In Your Mind",
            "nuclear fusion": "Nuclear Fusion",
            "nuclear": "Nuclear Fusion",
            "float along": "Float Along – Fill Your Lungs",
            "peoples vultures": "People-Vultures",
            "people vultures": "People-Vultures",
            "vultures": "People-Vultures",
            "iron lung": "Iron Lung",
            "gaia": "Gaia",
            "mars for the rich": "Mars for the Rich",
            "mars": "Mars for the Rich",
            "superbug": "Superbug",
            "han tyumi": "Han-Tyumi, the Confused Cyborg",
            "han-tyumi": "Han-Tyumi, the Confused Cyborg",
            "bitter boogie": "Bitter Boogie",
            "boogie": "Bitter Boogie",
            "inner cell": "Inner Cell",
            "loyalty": "Loyalty",
            "horology": "Horology",
            "polygondwanaland": "Crumbling Castle",  # Often means the epic opener
            "nonagon infinity": "Robot Stop",  # Often means the album opener
            "magenta mountain": "Magenta Mountain",
            "magenta": "Magenta Mountain",
            "this thing": "This Thing",
            "kepler": "Kepler-22b",
            "kepler-22b": "Kepler-22b",
            "kepler 22b": "Kepler-22b",
            "hot water": "Hot Water",
            "theia": "Theia",
            "extinction": "Extinction",
            "motor spirit": "Motor Spirit",
            "perihelion": "Perihelion",
            "gliese 710": "Gliese 710",
            "gliese": "Gliese 710",
        },

        # Venue aliases
        "venue_aliases": {
            "red rocks": "Red Rocks Amphitheatre",
            "reds": "Red Rocks Amphitheatre",
            "babys": "Baby's All Right",
            "baby's": "Baby's All Right",
            "babys all right": "Baby's All Right",
            "brooklyn": "Baby's All Right",  # Their famous Brooklyn spot
            "forest hills": "Forest Hills Stadium",
            "bonnaroo": "Bonnaroo Music Festival",
            "roo": "Bonnaroo Music Festival",
            "glastonbury": "Worthy Farm",
            "glasto": "Worthy Farm",
            "greek": "William Randolph Hearst Greek Theatre",
            "greek theatre": "William Randolph Hearst Greek Theatre",
            "berkeley": "William Randolph Hearst Greek Theatre",
            "tote": "The Tote Hotel",
            "corner": "Corner Hotel",
            "croxton": "Croxton Park Hotel",
            "enmore": "Enmore Theatre",
            "levitation": "Levitation Festival",
            "desert daze": "Desert Daze",
        },

        # Gizzverse lore
        "lore": {
            "gizzverse": "The interconnected narrative across albums featuring recurring characters and themes",
            "han-tyumi": "A confused cyborg who appears across multiple albums, seeking to become human",
            "nonagon infinity": "The album that loops infinitely - Robot Stop flows into Big Fig Wasp",
            "microtonal": "Albums using microtonal tuning (Flying Microtonal Banana, K.G., L.W.)",
            "marathon": "Their famous 3+ hour career-spanning sets",
            "bootlegger": "Official program where fans can download and distribute live recordings",
        },

        # Eras
        "eras": {
            "garage": (2010, 2013),           # Early garage rock
            "psych": (2014, 2016),            # Psychedelic era (Mind Fuzz, Quarters)
            "prolific": (2017, 2017),         # 5 albums in one year!
            "thrash": (2018, 2020),           # Heavier era (Infest the Rats' Nest)
            "jam": (2021, 2030),              # Extended jam era (Timeland, marathon sets)
        },

        # Band members
        "members": {
            "stu": "Stu Mackenzie",
            "mackenzie": "Stu Mackenzie",
            "ambrose": "Ambrose Kenny-Smith",
            "amby": "Ambrose Kenny-Smith",
            "cook": "Cook Craig",
            "cookie": "Cook Craig",
            "joey": "Joey Walker",
            "lucas": "Lucas Harwood",
            "cavs": "Michael Cavanagh",
            "michael": "Michael Cavanagh",
        },

        # Special show types
        "show_types": {
            "marathon": "3+ hour career-spanning sets",
            "orchestral": "Shows with full orchestra backing",
            "rave": "Electronic/dance focused sets",
            "residency": "Multi-night runs at same venue",
        },

        # Top jam vehicles (from our jam chart analysis)
        "jam_vehicles": [
            "The River", "The Dripping Tap", "Magma", "Head On/Pill",
            "Slow Jam 1", "Hypertension", "Am I In Heaven?", "Sense",
            "Her and I (Slow Jam 2)", "Ice V", "Theia", "Magenta Mountain"
        ],
    }
}


class JamMuseEngine:
    """Multi-band query engine for jam band statistics."""

    def __init__(self, band_key: str):
        """Initialize engine for a specific band."""
        if band_key not in BANDS:
            raise ValueError(f"Unknown band: {band_key}. Available: {list(BANDS.keys())}")

        self.band_key = band_key
        self.config = BANDS[band_key]
        self.band_name = self.config["name"]
        self.api_base = self.config["api_base"]
        self.artist_id = self.config.get("artist_id")

        # Data caches
        self._shows = None
        self._songs = None
        self._setlists = None
        self._jamcharts = None
        self._venues = None

    # =========================================================================
    # API METHODS
    # =========================================================================

    def _fetch_api(self, endpoint: str, params: dict = None) -> dict:
        """Fetch data from Songfish API with caching."""
        cache_key = f"{self.band_key}_{endpoint.replace('/', '_')}"
        if params:
            param_str = "_".join(f"{k}_{v}" for k, v in sorted(params.items()))
            cache_key += f"_{param_str}"
        cache_file = CACHE_DIR / f"{cache_key}.json"

        # Check cache (1 hour expiry for most data)
        if cache_file.exists():
            age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if age < 3600:  # 1 hour
                with open(cache_file) as f:
                    return json.load(f)

        # Fetch from API
        url = f"{self.api_base}/{endpoint}.json"
        if params:
            param_str = "&".join(f"{k}={v}" for k, v in params.items())
            url += f"?{param_str}"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JamMuse/1.0"})
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())

            # Cache response
            with open(cache_file, 'w') as f:
                json.dump(data, f)

            return data
        except Exception as e:
            print(f"API error for {url}: {e}")
            # Try to use stale cache
            if cache_file.exists():
                with open(cache_file) as f:
                    return json.load(f)
            return {"error": True, "data": []}

    @property
    def shows(self) -> List[dict]:
        """Get all shows for this band."""
        if self._shows is None:
            data = self._fetch_api("shows")
            all_shows = data.get("data", [])
            # Filter by artist_id if specified
            if self.artist_id:
                self._shows = [s for s in all_shows if s.get("artist_id") == self.artist_id]
            else:
                self._shows = all_shows
        return self._shows

    @property
    def songs(self) -> List[dict]:
        """Get all songs for this band."""
        if self._songs is None:
            data = self._fetch_api("songs")
            self._songs = data.get("data", [])
        return self._songs

    @property
    def jamcharts(self) -> List[dict]:
        """Get all jam chart entries."""
        if self._jamcharts is None:
            data = self._fetch_api("jamcharts")
            all_jams = data.get("data", [])
            if self.artist_id:
                self._jamcharts = [j for j in all_jams if j.get("artist_id") == self.artist_id]
            else:
                self._jamcharts = all_jams
        return self._jamcharts

    def get_setlist(self, show_date: str) -> List[dict]:
        """Get setlist for a specific show."""
        data = self._fetch_api(f"setlists/showdate/{show_date}")
        setlist = data.get("data", [])
        if self.artist_id:
            setlist = [s for s in setlist if s.get("artist_id") == self.artist_id]
        return sorted(setlist, key=lambda x: (x.get("setnumber", "1"), x.get("position", 0)))

    # =========================================================================
    # NORMALIZATION HELPERS
    # =========================================================================

    def _normalize_song_name(self, query: str) -> Optional[str]:
        """Normalize a song name from query using aliases."""
        query_lower = query.lower().strip()

        # Remove common filler words using word boundaries (multi-word first, then single)
        filler_phrases = [
            "how many times", "when did they", "tell me about",
            "jam chart", "gap on", "gap for", "info on",
        ]
        for phrase in filler_phrases:
            query_lower = query_lower.replace(phrase, " ")

        # Single word fillers - use word boundary regex
        filler_words = [
            "longest", "best", "play", "played", "last", "first",
            "stats", "statistics", "info", "about", "gap",
            "jamchart", "greatest", "version", "top"
        ]
        for word in filler_words:
            query_lower = re.sub(rf'\b{word}\b', ' ', query_lower)

        # Clean up extra spaces and punctuation
        query_lower = re.sub(r'[?!.,]', '', query_lower)
        query_lower = " ".join(query_lower.split()).strip()

        if not query_lower:
            return None

        # Check aliases first (exact match)
        song_aliases = self.config.get("song_aliases", {})
        if query_lower in song_aliases:
            return song_aliases[query_lower]

        # Check if query is contained in an alias or vice versa
        for alias, song_name in song_aliases.items():
            if alias == query_lower or query_lower == alias:
                return song_name

        # Check if it matches a song name directly
        for song in self.songs:
            song_name_lower = song["name"].lower()
            if query_lower == song_name_lower:
                return song["name"]

        # Check partial matches (query in song name or song name in query)
        for song in self.songs:
            song_name_lower = song["name"].lower()
            if query_lower in song_name_lower or song_name_lower in query_lower:
                return song["name"]

        # Last resort: check aliases for partial matches
        for alias, song_name in song_aliases.items():
            if alias in query_lower or query_lower in alias:
                return song_name

        return None

    def _normalize_venue(self, query: str) -> Optional[str]:
        """Normalize a venue name from query using aliases."""
        query_lower = query.lower().strip()

        venue_aliases = self.config.get("venue_aliases", {})
        if query_lower in venue_aliases:
            return venue_aliases[query_lower]

        # Check partial matches
        for alias, venue in venue_aliases.items():
            if alias in query_lower:
                return venue

        return query

    def _match_venue(self, show_venue: str, target_venue: str) -> bool:
        """Check if a show venue matches the target."""
        if not show_venue or not target_venue:
            return False

        show_lower = show_venue.lower()
        target_lower = target_venue.lower()

        # Direct match
        if target_lower in show_lower or show_lower in target_lower:
            return True

        # Check aliases
        normalized = self._normalize_venue(target_venue)
        if normalized and normalized.lower() in show_lower:
            return True

        return False

    # =========================================================================
    # QUERY METHODS
    # =========================================================================

    def query_song_stats(self, song_name: str) -> QueryResult:
        """Get comprehensive stats for a song."""
        # Find song in catalog
        song_match = None
        for song in self.songs:
            if song["name"].lower() == song_name.lower():
                song_match = song
                break

        if not song_match:
            # Try alias
            normalized = self._normalize_song_name(song_name)
            if normalized:
                for song in self.songs:
                    if song["name"].lower() == normalized.lower():
                        song_match = song
                        break

        if not song_match:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find a song called '{song_name}' in the {self.band_name} catalog."
            )

        song_name = song_match["name"]
        song_slug = song_match["slug"]

        # Count performances via setlists API
        data = self._fetch_api(f"setlists/slug/{song_slug}")
        performances = data.get("data", [])
        if self.artist_id:
            performances = [p for p in performances if p.get("artist_id") == self.artist_id]

        play_count = len(performances)

        if play_count == 0:
            return QueryResult(
                success=True,
                band=self.band_name,
                answer=f"{song_name} is in the {self.band_name} catalog but hasn't been played live yet."
            )

        # Get first and last played
        dates = sorted(set(p["showdate"] for p in performances if p.get("showdate")))
        first_played = dates[0] if dates else "Unknown"
        last_played = dates[-1] if dates else "Unknown"

        # Calculate gap
        if last_played != "Unknown" and len(self.shows) > 0:
            shows_since = len([s for s in self.shows if s["showdate"] > last_played])
        else:
            shows_since = 0

        # Jam chart count
        jamchart_entries = [j for j in self.jamcharts if j.get("songname") == song_name]
        jamchart_count = len(jamchart_entries)
        jamchart_rate = (jamchart_count / play_count * 100) if play_count > 0 else 0

        # Check if original
        is_original = song_match.get("isoriginal", 1) == 1
        original_artist = song_match.get("original_artist", "")

        # Build answer
        lines = [f"**{song_name}** - {self.band_name} Stats\n"]

        if not is_original and original_artist:
            lines.append(f"Originally by: {original_artist}")

        lines.append(f"Times played: {play_count}")
        lines.append(f"First played: {first_played}")
        lines.append(f"Last played: {last_played}")

        if shows_since > 0:
            lines.append(f"Current gap: {shows_since} shows")

        if jamchart_count > 0:
            lines.append(f"Jam charted: {jamchart_count} times ({jamchart_rate:.1f}%)")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=str(play_count),
            card_data={
                "type": "stats",
                "title": song_name,
                "stat": play_count,
                "stat_label": "times played",
                "extra": {
                    "first_played": first_played,
                    "last_played": last_played,
                    "gap": shows_since,
                    "jamchart_count": jamchart_count,
                    "jamchart_rate": f"{jamchart_rate:.1f}%"
                }
            },
            related_queries=[
                f"best {song_name}",
                f"gap on {song_name}",
                f"first {song_name}"
            ]
        )

    def query_play_count(self, song_name: str) -> QueryResult:
        """How many times has a song been played?"""
        normalized = self._normalize_song_name(song_name) or song_name

        # Find in songs
        song_slug = None
        for song in self.songs:
            if song["name"].lower() == normalized.lower():
                song_slug = song["slug"]
                normalized = song["name"]
                break

        if not song_slug:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{song_name}' in the {self.band_name} catalog."
            )

        # Get performances
        data = self._fetch_api(f"setlists/slug/{song_slug}")
        performances = data.get("data", [])
        if self.artist_id:
            performances = [p for p in performances if p.get("artist_id") == self.artist_id]

        count = len(set(p["showdate"] for p in performances if p.get("showdate")))

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"{self.band_name} has played {normalized} {count} times.",
            highlight=str(count),
            card_data={
                "type": "count",
                "title": normalized,
                "stat": count,
                "stat_label": "times played"
            }
        )

    def query_gap(self, song_name: str) -> QueryResult:
        """How many shows since a song was last played?"""
        normalized = self._normalize_song_name(song_name) or song_name

        song_slug = None
        for song in self.songs:
            if song["name"].lower() == normalized.lower():
                song_slug = song["slug"]
                normalized = song["name"]
                break

        if not song_slug:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{song_name}'."
            )

        # Get performances
        data = self._fetch_api(f"setlists/slug/{song_slug}")
        performances = data.get("data", [])
        if self.artist_id:
            performances = [p for p in performances if p.get("artist_id") == self.artist_id]

        if not performances:
            return QueryResult(
                success=True,
                band=self.band_name,
                answer=f"{normalized} has never been played live by {self.band_name}."
            )

        # Find last played
        dates = sorted(set(p["showdate"] for p in performances if p.get("showdate")))
        last_played = dates[-1] if dates else None

        if not last_played:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"Couldn't determine when {normalized} was last played."
            )

        # Get last venue
        last_perf = next((p for p in performances if p.get("showdate") == last_played), None)
        venue = last_perf.get("venuename", "Unknown venue") if last_perf else "Unknown venue"

        # Count shows since
        shows_since = len([s for s in self.shows if s["showdate"] > last_played])

        if shows_since == 0:
            answer = f"{normalized} was played at the most recent show ({last_played} at {venue})."
        else:
            answer = f"{normalized} was last played {shows_since} shows ago on {last_played} at {venue}."

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=answer,
            highlight=str(shows_since) if shows_since > 0 else "0",
            card_data={
                "type": "gap",
                "title": normalized,
                "stat": shows_since,
                "stat_label": "shows ago",
                "extra": {
                    "last_played": last_played,
                    "venue": venue
                }
            }
        )

    def query_jamchart(self, song_name: str, limit: int = 5) -> QueryResult:
        """Get jam chart entries for a song (the best versions)."""
        normalized = self._normalize_song_name(song_name) or song_name

        # Find matching jam charts
        entries = [j for j in self.jamcharts
                   if j.get("songname", "").lower() == normalized.lower()]

        if not entries:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"No jam chart entries found for {normalized}. Try one of the top jammed songs: {', '.join(self.config['jam_vehicles'][:5])}."
            )

        # Sort by date (most recent first) and take top N
        entries = sorted(entries, key=lambda x: x.get("showdate", ""), reverse=True)[:limit]

        lines = [f"**Best {normalized} Versions** (Jam Chart)\n"]

        for i, entry in enumerate(entries, 1):
            date = entry.get("showdate", "Unknown")
            venue = entry.get("venuename", "Unknown venue")
            duration = entry.get("tracktime", "")
            note = entry.get("jamchartnote", "")

            line = f"{i}. {date} at {venue}"
            if duration:
                line += f" ({duration})"
            lines.append(line)

            if note:
                # Truncate long notes
                note = note[:200] + "..." if len(note) > 200 else note
                lines.append(f"   *{note}*")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            card_data={
                "type": "jamchart",
                "title": f"Best {normalized}",
                "extra": {"entries": len(entries)}
            },
            related_queries=[
                f"{normalized} stats",
                f"gap on {normalized}",
                f"how many times {normalized}"
            ]
        )

    def _parse_duration(self, tracktime: str) -> float:
        """Parse tracktime string (MM:SS or HH:MM:SS) to minutes."""
        if not tracktime:
            return 0.0
        try:
            parts = tracktime.split(":")
            if len(parts) == 2:
                mins, secs = int(parts[0]), int(parts[1])
                return mins + secs / 60.0
            elif len(parts) == 3:
                hours, mins, secs = int(parts[0]), int(parts[1]), int(parts[2])
                return hours * 60 + mins + secs / 60.0
        except (ValueError, IndexError):
            return 0.0
        return 0.0

    def query_longest(self, song_name: str, limit: int = 10) -> QueryResult:
        """Get the longest versions of a song."""
        normalized = self._normalize_song_name(song_name) or song_name

        # Find song slug
        song_slug = None
        for song in self.songs:
            if song["name"].lower() == normalized.lower():
                song_slug = song["slug"]
                normalized = song["name"]
                break

        if not song_slug:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{song_name}' in the {self.band_name} catalog."
            )

        # Get all performances with durations
        data = self._fetch_api(f"setlists/slug/{song_slug}")
        performances = data.get("data", [])
        if self.artist_id:
            performances = [p for p in performances if p.get("artist_id") == self.artist_id]

        # Filter to those with duration data and parse
        with_duration = []
        for p in performances:
            duration_min = self._parse_duration(p.get("tracktime", ""))
            if duration_min > 0:
                with_duration.append({
                    **p,
                    "duration_min": duration_min
                })

        if not with_duration:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I don't have duration data for {normalized}. Try checking the jam chart versions instead.",
                related_queries=[f"best {normalized}", f"{normalized} stats"]
            )

        # Sort by duration descending
        sorted_perfs = sorted(with_duration, key=lambda x: x["duration_min"], reverse=True)[:limit]

        # Calculate stats
        all_durations = [p["duration_min"] for p in with_duration]
        avg_duration = sum(all_durations) / len(all_durations) if all_durations else 0
        longest = sorted_perfs[0]

        # Format the longest
        mins = int(longest["duration_min"])
        secs = int((longest["duration_min"] - mins) * 60)
        duration_str = f"{mins}:{secs:02d}"

        lines = [f"**Longest {normalized} Versions**\n"]

        for i, perf in enumerate(sorted_perfs, 1):
            date = perf.get("showdate", "Unknown")
            venue = perf.get("venuename", "Unknown venue")
            dur_mins = int(perf["duration_min"])
            dur_secs = int((perf["duration_min"] - dur_mins) * 60)
            dur_str = f"{dur_mins}:{dur_secs:02d}"
            lines.append(f"{i}. {dur_str} - {date} at {venue}")

        lines.append(f"\nAverage {normalized} length: {avg_duration:.1f} minutes")
        lines.append(f"Based on {len(with_duration)} performances with timing data")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=duration_str,
            card_data={
                "type": "longest",
                "title": f"Longest {normalized}",
                "stat": duration_str,
                "subtitle": f"{longest.get('showdate')} at {longest.get('venuename', '')}",
                "extra": {
                    "avg_duration": f"{avg_duration:.1f} min",
                    "performances_with_data": len(with_duration)
                }
            },
            related_queries=[
                f"best {normalized}",
                f"{normalized} stats",
                f"gap on {normalized}"
            ]
        )

    def query_average_duration(self, song_name: str) -> QueryResult:
        """Get the average duration of a song."""
        normalized = self._normalize_song_name(song_name) or song_name

        song_slug = None
        for song in self.songs:
            if song["name"].lower() == normalized.lower():
                song_slug = song["slug"]
                normalized = song["name"]
                break

        if not song_slug:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{song_name}'."
            )

        data = self._fetch_api(f"setlists/slug/{song_slug}")
        performances = data.get("data", [])
        if self.artist_id:
            performances = [p for p in performances if p.get("artist_id") == self.artist_id]

        durations = [self._parse_duration(p.get("tracktime", "")) for p in performances]
        durations = [d for d in durations if d > 0]

        if not durations:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I don't have duration data for {normalized}."
            )

        avg = sum(durations) / len(durations)
        min_dur = min(durations)
        max_dur = max(durations)

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"Average {normalized} length: {avg:.1f} minutes\n"
                   f"Shortest: {min_dur:.1f} min | Longest: {max_dur:.1f} min\n"
                   f"Based on {len(durations)} performances with timing data.",
            highlight=f"{avg:.1f}",
            card_data={
                "type": "duration",
                "title": f"{normalized} Duration",
                "stat": f"{avg:.1f}",
                "stat_label": "avg minutes",
                "extra": {
                    "min": f"{min_dur:.1f} min",
                    "max": f"{max_dur:.1f} min",
                    "count": len(durations)
                }
            }
        )

    def query_longest_overall(self, limit: int = 5) -> QueryResult:
        """Get the longest performances of ANY song (overall longest jams)."""
        all_performances = []

        # Get top jam vehicles first (most likely to have long versions)
        jam_vehicles = self.config.get("jam_vehicles", [])
        songs_to_check = []

        # Add jam vehicles first
        for song in self.songs:
            if song["name"] in jam_vehicles:
                songs_to_check.insert(0, song)
            else:
                songs_to_check.append(song)

        # Limit to top 50 songs to avoid too many API calls
        songs_to_check = songs_to_check[:50]

        for song in songs_to_check:
            song_slug = song["slug"]
            song_name = song["name"]

            # Get performances for this song
            data = self._fetch_api(f"setlists/slug/{song_slug}")
            performances = data.get("data", [])
            if self.artist_id:
                performances = [p for p in performances if p.get("artist_id") == self.artist_id]

            for p in performances:
                duration_min = self._parse_duration(p.get("tracktime", ""))
                if duration_min > 0:
                    all_performances.append({
                        "song": song_name,
                        "date": p.get("showdate", "Unknown"),
                        "venue": p.get("venuename", "Unknown venue"),
                        "duration_min": duration_min,
                        "tracktime": p.get("tracktime", "")
                    })

        if not all_performances:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I don't have duration data for {self.band_name} performances."
            )

        # Sort by duration descending and take top N
        sorted_perfs = sorted(all_performances, key=lambda x: x["duration_min"], reverse=True)[:limit]

        # Format the result
        longest = sorted_perfs[0]
        mins = int(longest["duration_min"])
        secs = int((longest["duration_min"] - mins) * 60)
        duration_str = f"{mins}:{secs:02d}"

        if limit == 1:
            lines = [f"**Longest {self.band_name} Performance Ever**\n"]
        else:
            lines = [f"**Top {limit} Longest {self.band_name} Performances**\n"]

        for i, perf in enumerate(sorted_perfs, 1):
            dur_mins = int(perf["duration_min"])
            dur_secs = int((perf["duration_min"] - dur_mins) * 60)
            dur_str = f"{dur_mins}:{dur_secs:02d}"
            lines.append(f"{i}. **{perf['song']}** - {dur_str} ({perf['date']} at {perf['venue']})")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=duration_str,
            card_data={
                "type": "longest_overall",
                "title": f"Longest {self.band_name} Jams",
                "stat": duration_str,
                "subtitle": f"{longest['song']} - {longest['date']}",
                "extra": {
                    "top_performances": sorted_perfs[:5]
                }
            },
            related_queries=[
                f"longest {sorted_perfs[0]['song']}",
                f"longest {sorted_perfs[1]['song']}" if len(sorted_perfs) > 1 else None,
                f"best {sorted_perfs[0]['song']}"
            ]
        )

    def query_show_count(self, venue: str = None, year: int = None) -> QueryResult:
        """How many shows has the band played?"""
        shows = self.shows

        if year:
            shows = [s for s in shows if str(s.get("show_year")) == str(year)]

        if venue:
            normalized_venue = self._normalize_venue(venue)
            shows = [s for s in shows if self._match_venue(s.get("venuename", ""), normalized_venue)]

        count = len(shows)

        if venue and year:
            answer = f"{self.band_name} played {count} shows at {venue} in {year}."
        elif venue:
            answer = f"{self.band_name} has played {count} shows at {venue}."
        elif year:
            answer = f"{self.band_name} played {count} shows in {year}."
        else:
            answer = f"{self.band_name} has played {count} shows total."

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=answer,
            highlight=str(count)
        )

    def query_setlist(self, date: str) -> QueryResult:
        """Get setlist for a specific date."""
        # Normalize date format
        date = date.replace("/", "-")

        setlist = self.get_setlist(date)

        if not setlist:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"No setlist found for {date}."
            )

        # Group by set
        sets = {}
        venue = None
        for song in setlist:
            setnumber = song.get("setnumber", "1")
            settype = song.get("settype", "Set")
            key = f"{settype} {setnumber}" if settype != "Encore" else "Encore"

            if key not in sets:
                sets[key] = []

            name = song.get("songname", "Unknown")
            transition = song.get("transition", ", ")
            sets[key].append((name, transition))

            if not venue:
                venue = song.get("venuename", "")

        # Format output
        lines = [f"**{self.band_name}** - {date}"]
        if venue:
            lines.append(f"*{venue}*\n")

        for set_name in sorted(sets.keys()):
            songs = sets[set_name]
            song_str = ""
            for i, (name, trans) in enumerate(songs):
                song_str += name
                if i < len(songs) - 1:
                    song_str += trans if trans else ", "
            lines.append(f"**{set_name}:** {song_str}")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines)
        )

    def query_last_played(self, song_name: str) -> QueryResult:
        """When was a song last played?"""
        return self.query_gap(song_name)  # Same logic

    def query_first_played(self, song_name: str) -> QueryResult:
        """When was a song first played (debut)?"""
        normalized = self._normalize_song_name(song_name) or song_name

        song_slug = None
        for song in self.songs:
            if song["name"].lower() == normalized.lower():
                song_slug = song["slug"]
                normalized = song["name"]
                break

        if not song_slug:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{song_name}'."
            )

        # Get performances
        data = self._fetch_api(f"setlists/slug/{song_slug}")
        performances = data.get("data", [])
        if self.artist_id:
            performances = [p for p in performances if p.get("artist_id") == self.artist_id]

        if not performances:
            return QueryResult(
                success=True,
                band=self.band_name,
                answer=f"{normalized} has never been played live."
            )

        # Find first played
        dates = sorted(set(p["showdate"] for p in performances if p.get("showdate")))
        first_played = dates[0] if dates else None

        first_perf = next((p for p in performances if p.get("showdate") == first_played), None)
        venue = first_perf.get("venuename", "Unknown venue") if first_perf else "Unknown venue"

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"{normalized} debuted on {first_played} at {venue}.",
            card_data={
                "type": "debut",
                "title": normalized,
                "extra": {"debut_date": first_played, "venue": venue}
            }
        )

    # =========================================================================
    # MAIN QUERY ROUTER
    # =========================================================================

    def query(self, question: str) -> QueryResult:
        """Route a natural language question to the appropriate handler."""
        q = question.lower().strip()

        # Setlist queries
        date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})', question)
        if date_match and any(word in q for word in ["setlist", "set list", "what did they play"]):
            return self.query_setlist(date_match.group(1))

        # Longest OVERALL queries (no specific song - "longest ever jams", "longest songs ever")
        if any(word in q for word in ["longest", "longest ever"]):
            # Check if this is an "overall" query (no specific song mentioned)
            # Patterns: "longest ever X jams", "longest X songs", "longest X jams ever"
            overall_patterns = [
                r"longest\s+ever\s+(\w+\s+)?(jams?|songs?|versions?|performances?)",  # "longest ever jams" or "longest ever goose jams"
                r"longest\s+(\w+\s+)?(jams?|songs?|versions?|performances?)\s+ever",  # "longest jams ever" or "longest goose jams ever"
                r"longest\s+(jams?|songs?|versions?|performances?)\s+of\s+all\s+time",
                r"longest\s+ever\s+played",
                r"longest\s+ever$",
            ]
            is_overall = any(re.search(pat, q) for pat in overall_patterns)

            # Also check if no song can be identified
            song = self._normalize_song_name(question)

            if is_overall or not song:
                # Determine limit: plural = 5, singular = 1
                if any(word in q for word in ["jams", "songs", "versions", "performances"]):
                    limit = 5
                else:
                    limit = 1
                return self.query_longest_overall(limit=limit)
            elif song:
                return self.query_longest(song)

        # Average duration queries
        if any(word in q for word in ["average length", "avg duration", "typical length", "how long is"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_average_duration(song)

        # Jam chart / best version queries
        if any(word in q for word in ["best", "jam chart", "jamchart", "greatest", "top version"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_jamchart(song)

        # Gap queries
        if any(word in q for word in ["gap on", "gap for", "how long since", "when did they last"]):
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
            song = self._normalize_song_name(question)
            if song:
                return self.query_play_count(song)

        # Show count queries
        if any(word in q for word in ["how many shows", "show count", "total shows"]):
            year_match = re.search(r'\b(20\d{2}|19\d{2})\b', question)
            year = int(year_match.group(1)) if year_match else None
            return self.query_show_count(year=year)

        # Last played queries
        if any(word in q for word in ["last played", "last time", "when was the last"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_last_played(song)

        # Stats queries (explicit)
        if any(word in q for word in ["stats", "statistics", "info on", "tell me about"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_song_stats(song)

        # Default: try to identify a song and give stats
        song = self._normalize_song_name(question)
        if song:
            return self.query_song_stats(song)

        # Fallback
        return QueryResult(
            success=False,
            band=self.band_name,
            answer=f"I'm not sure how to answer that about {self.band_name}. Try asking about:\n"
                   f"- Song stats: '{self.config['jam_vehicles'][0]} stats'\n"
                   f"- Play count: 'how many times {self.config['jam_vehicles'][1]}'\n"
                   f"- Gap: 'gap on {self.config['jam_vehicles'][2]}'\n"
                   f"- Best versions: 'best {self.config['jam_vehicles'][0]}'\n"
                   f"- Setlists: 'setlist 2024-09-07'"
        )


# =============================================================================
# UNIFIED JAMMUSE - AUTO-DETECTS BAND FROM QUERY
# =============================================================================

# Import setlist.fm engine
try:
    from scripts.setlistfm_engine import SetlistFMEngine, SETLISTFM_BANDS, get_all_setlistfm_aliases
    SETLISTFM_AVAILABLE = True
except ImportError:
    SETLISTFM_AVAILABLE = False
    SETLISTFM_BANDS = {}

# Import Archive.org Grateful Dead engine
try:
    from scripts.archive_engine import ArchiveDeadEngine, get_dead_song_aliases
    DEAD_AVAILABLE = True
except ImportError:
    DEAD_AVAILABLE = False


class UnifiedJamMuse:
    """
    Unified query engine that auto-detects which band you're asking about.
    No need to specify - just ask about any song and it figures it out.

    Supports:
    - Phish (via PhishStatsEngine)
    - Goose, King Gizzard (via Songfish/JamMuseEngine)
    - Umphrey's McGee, Widespread Panic, moe., STS9, Billy Strings (via setlist.fm)
    """

    def __init__(self, include_phish: bool = True):
        """Initialize with all band engines."""
        self.engines = {}
        self.setlistfm_engines = {}
        self._song_to_band = {}  # Maps song names to band keys
        self._alias_to_band = {}  # Maps aliases to band keys
        self.include_phish = include_phish
        self._phish_engine = None
        self._dead_engine = None

        # Initialize Songfish-based engines (Goose, King Gizzard)
        for band_key in BANDS:
            self.engines[band_key] = JamMuseEngine(band_key)

        # Initialize setlist.fm engines (Umphrey's, WSP, moe., STS9, Billy Strings)
        if SETLISTFM_AVAILABLE:
            for band_key in SETLISTFM_BANDS:
                self.setlistfm_engines[band_key] = SetlistFMEngine(band_key)

        # Initialize Archive.org Grateful Dead engine
        if DEAD_AVAILABLE:
            try:
                self._dead_engine = ArchiveDeadEngine()
            except Exception as e:
                print(f"Warning: Could not load Dead engine: {e}")
                self._dead_engine = None

        # Build song lookup tables
        self._build_song_index()

    def _build_song_index(self):
        """Build index of all songs across all bands for auto-detection."""
        # Index Songfish bands (Goose, King Gizzard)
        for band_key, engine in self.engines.items():
            config = BANDS[band_key]

            # Index song aliases
            for alias, song_name in config.get("song_aliases", {}).items():
                self._alias_to_band[alias.lower()] = band_key

            # Index actual songs from API
            for song in engine.songs:
                song_name = song["name"].lower()
                self._song_to_band[song_name] = band_key

        # Index setlist.fm bands (Umphrey's, WSP, moe., STS9, Billy Strings)
        if SETLISTFM_AVAILABLE:
            for band_key, config in SETLISTFM_BANDS.items():
                # Index song aliases
                for alias in config.get("song_aliases", {}).keys():
                    self._alias_to_band[alias.lower()] = band_key

                # Index band name aliases
                for alias in config.get("aliases", []):
                    self._alias_to_band[alias.lower()] = band_key

        # If including Phish, add Phish song detection
        if self.include_phish:
            self._add_phish_songs()

        # Add Grateful Dead song aliases for detection
        if DEAD_AVAILABLE and self._dead_engine:
            dead_aliases = get_dead_song_aliases()
            for alias in dead_aliases:
                self._alias_to_band[alias.lower()] = "dead"

    def _add_phish_songs(self):
        """Add Phish songs to the index for detection."""
        # Common Phish song aliases that are unique to Phish
        phish_aliases = {
            "tweezer": "phish", "ghost": "phish", "yem": "phish",
            "you enjoy myself": "phish", "reba": "phish", "divided sky": "phish",
            "fluffhead": "phish", "harry hood": "phish", "hood": "phish",
            "antelope": "phish", "run like an antelope": "phish",
            "slave": "phish", "slave to the traffic light": "phish",
            "bathtub gin": "phish", "gin": "phish", "mike's song": "phish",
            "mikes song": "phish", "mike's": "phish", "weekapaug": "phish",
            "weekapaug groove": "phish", "down with disease": "phish",
            "dwd": "phish", "disease": "phish", "simple": "phish",
            "carini": "phish", "piper": "phish", "sand": "phish",
            "twist": "phish", "drowned": "phish", "wolfman's": "phish",
            "wolfmans brother": "phish", "wolfman's brother": "phish",
            "stash": "phish", "possum": "phish", "cavern": "phish",
            "chalk dust": "phish", "chalkdust": "phish", "chalk dust torture": "phish",
            "bouncing": "phish", "bouncing around the room": "phish",
            "fee": "phish", "sample": "phish", "sample in a jar": "phish",
            "lizards": "phish", "the lizards": "phish", "golgi": "phish",
            "golgi apparatus": "phish", "suzy greenberg": "phish", "suzy": "phish",
            "ac/dc bag": "phish", "acdc bag": "phish", "bag": "phish",
            "maze": "phish", "sparkle": "phish", "esther": "phish",
            "punch you in the eye": "phish", "pyite": "phish",
            "harpua": "phish", "gamehendge": "phish", "mcgrupp": "phish",
            "llama": "phish", "david bowie": "phish", "bowie": "phish",
            "jim": "phish", "halley's comet": "phish", "halleys": "phish",
            "cities": "phish", "mango song": "phish", "mango": "phish",
            "guyute": "phish", "limb by limb": "phish", "limb": "phish",
            "theme": "phish", "theme from the bottom": "phish",
            "free": "phish", "character zero": "phish", "zero": "phish",
            "first tube": "phish", "tube": "phish", "gotta jibboo": "phish",
            "jibboo": "phish", "moma dance": "phish", "moma": "phish",
            "roggae": "phish", "birds of a feather": "phish", "birds": "phish",
            "dirt": "phish", "waste": "phish", "wading": "phish",
            "wading in the velvet sea": "phish", "prince caspian": "phish",
            "caspian": "phish", "vultures": "phish", "sleeping monkey": "phish",
            "scents and subtle sounds": "phish", "scents": "phish",
            "walls of the cave": "phish", "walls": "phish",
            "party time": "phish", "blaze on": "phish", "everything's right": "phish",
            "everythings right": "phish", "ruby waves": "phish", "ruby": "phish",
            "sigma oasis": "phish", "sigma": "phish", "set your soul free": "phish",
            "soul free": "phish", "no men": "phish", "no men in no man's land": "phish",
            "golden age": "phish", "fuego": "phish", "winterqueen": "phish",
            "steam": "phish", "light": "phish", "mercury": "phish",
            "breath and burning": "phish", "miss you": "phish",
            "more": "phish", "say it to me santos": "phish", "santos": "phish",
        }
        for alias, band in phish_aliases.items():
            self._alias_to_band[alias] = band

    def _get_phish_engine(self):
        """Get or lazily load the Phish engine."""
        if self._phish_engine is None and self.include_phish:
            try:
                from scripts.query_engine import PhishStatsEngine
                self._phish_engine = PhishStatsEngine()
                self._phish_engine.load_data()
            except Exception as e:
                print(f"Warning: Could not load Phish engine: {e}")
                self._phish_engine = False  # Mark as failed
        return self._phish_engine if self._phish_engine else None

    def _normalize_query_for_detection(self, query: str) -> List[str]:
        """
        Generate normalized variations of the query for better matching.
        Handles plurals, possessives, and other natural language variations.
        Returns list of query variations to try.
        """
        q_lower = query.lower()
        variations = [q_lower]

        # Create a singularized version by removing trailing 's' from words
        words = q_lower.split()
        singularized_words = []
        for word in words:
            # Strip punctuation for processing
            clean_word = re.sub(r'[?!.,\'"]', '', word)

            # Handle common plural patterns
            if clean_word.endswith('ies') and len(clean_word) > 4:
                # "candies" -> "candy" (but not "series")
                singularized_words.append(clean_word[:-3] + 'y')
            elif clean_word.endswith('es') and len(clean_word) > 3:
                # "boxes" -> "box", "glasses" -> "glass"
                # But be careful: "eyes" -> "eye", not "ey"
                if clean_word.endswith('sses') or clean_word.endswith('xes') or clean_word.endswith('ches') or clean_word.endswith('shes'):
                    singularized_words.append(clean_word[:-2])
                else:
                    singularized_words.append(clean_word[:-1])  # Just remove 's'
            elif clean_word.endswith('s') and len(clean_word) > 2 and not clean_word.endswith('ss'):
                # "cats" -> "cat", but not "glass" -> "glas"
                singularized_words.append(clean_word[:-1])
            else:
                singularized_words.append(clean_word)

        singularized = ' '.join(singularized_words)
        if singularized != q_lower:
            variations.append(singularized)

        # Also try with possessives removed ("mike's song" -> "mikes song" -> "mike song")
        no_possessive = re.sub(r"'s\b", 's', q_lower)  # "mike's" -> "mikes"
        if no_possessive not in variations:
            variations.append(no_possessive)
        no_possessive2 = re.sub(r"'s\b", '', q_lower)  # "mike's" -> "mike"
        if no_possessive2 not in variations:
            variations.append(no_possessive2)

        return variations

    def _detect_band(self, query: str) -> Optional[str]:
        """
        Detect which band the query is about based on song names/aliases.
        Returns band key or None if can't detect.
        """
        q_lower = query.lower()

        # Get normalized variations for matching
        query_variations = self._normalize_query_for_detection(query)

        # Check for explicit band mentions
        if any(word in q_lower for word in ["phish", "trey", "fishman", "page", "mike gordon"]):
            return "phish"
        if any(word in q_lower for word in ["grateful dead", "the dead", "jerry garcia", "jerry", "garcia", "dead played", "gd ", "dead jams", "dead songs", "dead jam", "dead song"]):
            return "dead"
        # Also check for standalone "dead" followed by common query words
        if re.search(r'\bdead\b', q_lower) and any(word in q_lower for word in ["longest", "best", "stats", "how many", "played"]):
            return "dead"
        if any(word in q_lower for word in ["goose", "rick mitarotonda", "el goose"]):
            return "goose"
        if any(word in q_lower for word in ["king gizzard", "kglw", "gizz", "stu mackenzie"]):
            return "kglw"
        # Setlist.fm bands
        if any(word in q_lower for word in ["umphreys", "umphrey", "umph "]):
            return "umphreys"
        if any(word in q_lower for word in ["widespread", "wsp", "panic"]):
            return "wsp"
        if any(word in q_lower for word in ["moe.", "moe "]):
            return "moe"
        if any(word in q_lower for word in ["sts9", "sound tribe", "sector 9"]):
            return "sts9"
        if any(word in q_lower for word in ["billy strings", "bmfs"]):
            return "billy"

        # Check aliases - sort by length (longest first) to avoid partial matches
        # e.g., "fluffhead" should match before "head"
        # Use word boundary matching to avoid "time" matching inside "times"
        sorted_aliases = sorted(self._alias_to_band.items(), key=lambda x: len(x[0]), reverse=True)
        for alias, band_key in sorted_aliases:
            # Try matching against all query variations (original + singularized, etc.)
            for q_var in query_variations:
                if re.search(rf'\b{re.escape(alias)}\b', q_var):
                    return band_key

        # Check song names from catalogs - also longest first
        sorted_songs = sorted(self._song_to_band.items(), key=lambda x: len(x[0]), reverse=True)
        for song_name, band_key in sorted_songs:
            for q_var in query_variations:
                if re.search(rf'\b{re.escape(song_name)}\b', q_var):
                    return band_key

        return None

    def _normalize_question(self, question: str) -> str:
        """
        Normalize the question by converting plurals to singular, etc.
        This helps sub-engines that don't have their own normalization.
        """
        q = question

        # Common plural song name patterns - convert to singular
        # "tweezers" -> "tweezer", "ghosts" -> "ghost", etc.
        plural_fixes = [
            (r'\btweezers\b', 'tweezer'),
            (r'\bghosts\b', 'ghost'),
            (r'\brebas\b', 'reba'),
            (r'\bstashes\b', 'stash'),
            (r'\bcaverns\b', 'cavern'),
            (r'\bmazes\b', 'maze'),
            (r'\bsimples\b', 'simple'),
            (r'\bpipers\b', 'piper'),
            (r'\bcarinis\b', 'carini'),
            (r'\bwastes\b', 'waste'),
        ]

        for pattern, replacement in plural_fixes:
            q = re.sub(pattern, replacement, q, flags=re.IGNORECASE)

        # Generic singularization for words ending in 's' that look like song names
        # Be conservative - only apply to likely song name positions
        words = q.split()
        normalized_words = []
        for i, word in enumerate(words):
            clean = word.lower().rstrip('?!.,')
            # If word ends in 's' and previous word is a query keyword, try to singularize
            if clean.endswith('s') and len(clean) > 3 and not clean.endswith('ss'):
                prev_word = words[i-1].lower() if i > 0 else ''
                if prev_word in ['longest', 'best', 'about', 'on', 'for']:
                    # Try singularizing
                    singular = clean[:-1]
                    normalized_words.append(word[:-1] if word[-1] == 's' else word.replace(clean, singular))
                    continue
            normalized_words.append(word)

        return ' '.join(normalized_words)

    def query(self, question: str) -> QueryResult:
        """
        Answer any jam band question - auto-detects which band.
        """
        # Normalize the question for better matching
        normalized_question = self._normalize_question(question)

        # Detect which band
        band_key = self._detect_band(question)

        if not band_key:
            return QueryResult(
                success=False,
                band=None,
                answer="I couldn't figure out which band you're asking about. "
                       "Try including a song name like:\n"
                       "- 'longest Dark Star' (Grateful Dead)\n"
                       "- 'longest Tweezer' (Phish)\n"
                       "- 'Arcadia stats' (Goose)\n"
                       "- 'gap on Dripping Tap' (King Gizzard)\n"
                       "- 'how many times Mantis' (Umphrey's McGee)\n"
                       "- 'when did they last play Chilly Water' (Widespread Panic)"
            )

        # Route to appropriate engine (use normalized question for better matching)
        if band_key == "phish":
            phish_engine = self._get_phish_engine()
            if phish_engine:
                result = phish_engine.query(normalized_question)
                # Wrap Phish result to add band field
                return QueryResult(
                    success=result.success,
                    answer=result.answer,
                    band="Phish",
                    highlight=result.highlight,
                    card_data=result.card_data,
                    related_queries=result.related_queries,
                    raw_data=getattr(result, 'raw_data', None)
                )
            else:
                return QueryResult(
                    success=False,
                    band="Phish",
                    answer="Phish data is currently unavailable. Try a Goose or King Gizzard query!"
                )
        elif band_key == "dead":
            # Grateful Dead via Archive.org
            if self._dead_engine:
                return self._dead_engine.query(normalized_question)
            else:
                return QueryResult(
                    success=False,
                    band="Grateful Dead",
                    answer="Grateful Dead catalog is still loading. Please try again in a few minutes!"
                )
        elif band_key in self.engines:
            # Songfish bands (Goose, King Gizzard)
            return self.engines[band_key].query(normalized_question)
        elif band_key in self.setlistfm_engines:
            # Setlist.fm bands (Umphrey's, WSP, moe., STS9, Billy Strings)
            return self.setlistfm_engines[band_key].query(normalized_question)
        else:
            return QueryResult(
                success=False,
                band=None,
                answer=f"Unknown band key: {band_key}"
            )

    def get_available_bands(self) -> List[str]:
        """Return list of available bands."""
        bands = list(self.engines.keys())  # Songfish bands
        bands.extend(self.setlistfm_engines.keys())  # Setlist.fm bands
        if self.include_phish:
            bands.insert(0, "phish")
        if self._dead_engine:
            bands.insert(0, "dead")  # Grateful Dead at the top!
        return bands


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Singleton unified engine
_unified_engine = None

def get_unified_engine() -> UnifiedJamMuse:
    """Get the singleton unified JamMuse engine."""
    global _unified_engine
    if _unified_engine is None:
        _unified_engine = UnifiedJamMuse(include_phish=True)
    return _unified_engine

def query_any(question: str) -> QueryResult:
    """Query any band - auto-detects from context."""
    return get_unified_engine().query(question)


# =============================================================================
# CLI FOR TESTING
# =============================================================================

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("JamMuse Unified Engine - Testing")
    print("=" * 60)

    engine = get_unified_engine()

    test_queries = [
        # Phish
        "longest Tweezer",
        "Carini stats",
        "gap on Fluffhead",
        # Goose
        "longest Arcadia",
        "how many times Hungersite",
        "gap on Madhuvan",
        # King Gizzard
        "The River stats",
        "longest Dripping Tap",
        "best Magma",
    ]

    for q in test_queries:
        print(f"\nQ: {q}")
        result = engine.query(q)
        band = result.band or "Unknown"
        answer_preview = result.answer.split('\n')[0][:60]
        print(f"   [{band}] {answer_preview}...")
