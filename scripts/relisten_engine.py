#!/usr/bin/env python3
"""
Relisten Duration Engine
========================

Supplementary duration layer for jam bands whose primary engines (Phantasy Tour,
setlist.fm) lack per-track duration data. Uses the Relisten API (api.relisten.net)
which pulls from Archive.org recordings.

Provides: query_longest(), query_longest_overall(), query_average_duration()

Relisten API (no key needed):
- Years: GET /api/v2/artists/{id}/years
- Shows/year: GET /api/v2/artists/{id}/years/{year}
- Show detail: GET /api/v2/artists/{id}/years/{year}/{date}
  → sources[] → sets[] → tracks[] with duration in seconds
"""

import json
import re
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

# Reuse QueryResult from jammuse_engine to avoid import cycles
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


# Cache directory for Relisten API responses
CACHE_DIR = Path(__file__).parent.parent / "data" / "relisten_cache"
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    pass

# Index directory (same place)
INDEX_DIR = CACHE_DIR

CACHE_EXPIRY = 24 * 3600  # 24 hours

API_BASE = "https://api.relisten.net/api/v2"

# Maps existing band_keys to Relisten artist IDs
RELISTEN_BANDS = {
    "sci": {"relisten_id": 151, "name": "String Cheese Incident"},
    "biscuits": {"relisten_id": 17, "name": "The Disco Biscuits"},
    "lotus": {"relisten_id": 1, "name": "Lotus"},
    "umphreys": {"relisten_id": 163, "name": "Umphrey's McGee"},
    "wsp": {"relisten_id": 5, "name": "Widespread Panic"},
    "moe": {"relisten_id": 161, "name": "moe."},
    "sts9": {"relisten_id": 157, "name": "STS9"},
    "billy": {"relisten_id": 202, "name": "Billy Strings"},
    "spafford": {"relisten_id": 158, "name": "Spafford"},
}


class RelistenDurationEngine:
    """Duration query engine backed by Relisten/Archive.org recordings."""

    def __init__(self, band_key: str, song_aliases: dict = None, jam_vehicles: list = None):
        if band_key not in RELISTEN_BANDS:
            raise ValueError(f"Unknown Relisten band: {band_key}")
        self.band_key = band_key
        config = RELISTEN_BANDS[band_key]
        self.artist_id = config["relisten_id"]
        self.band_name = config["name"]
        self.song_aliases = song_aliases or {}
        self.jam_vehicles = jam_vehicles or []
        self._duration_index = None

    # -------------------------------------------------------------------------
    # API + Caching
    # -------------------------------------------------------------------------

    def _fetch_api(self, url: str) -> Any:
        """HTTP GET with file cache, 24h expiry."""
        # Build cache filename from URL path
        cache_key = url.replace(API_BASE, "").strip("/").replace("/", "_")
        cache_file = CACHE_DIR / f"{self.band_key}_{cache_key}.json"

        # Check cache
        if cache_file.exists():
            age = time.time() - cache_file.stat().st_mtime
            if age < CACHE_EXPIRY:
                try:
                    return json.loads(cache_file.read_text())
                except (json.JSONDecodeError, OSError):
                    pass

        # Fetch from API
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JamMuse/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            # Save to cache
            try:
                cache_file.write_text(json.dumps(data))
            except OSError:
                pass
            return data
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            print(f"Relisten API error for {url}: {e}")
            return None

    # -------------------------------------------------------------------------
    # Data collection
    # -------------------------------------------------------------------------

    def _get_years(self) -> List[str]:
        """Get list of available years."""
        url = f"{API_BASE}/artists/{self.artist_id}/years"
        data = self._fetch_api(url)
        if not data:
            return []
        return [y["year"] for y in data if y.get("year")]

    def _get_show_dates(self, year: str) -> List[dict]:
        """Get all shows for a year with venue info."""
        url = f"{API_BASE}/artists/{self.artist_id}/years/{year}"
        data = self._fetch_api(url)
        if not data:
            return []
        shows = data.get("shows", [])
        results = []
        for s in shows:
            venue = s.get("venue", {}) or {}
            results.append({
                "date": s.get("display_date", ""),
                "venue": venue.get("name", "Unknown"),
                "location": venue.get("location", ""),
                "source_count": s.get("source_count", 0),
            })
        return results

    def _get_show_tracks(self, year: str, date: str) -> List[dict]:
        """Fetch show detail, extract tracks from first source."""
        url = f"{API_BASE}/artists/{self.artist_id}/years/{year}/{date}"
        data = self._fetch_api(url)
        if not data:
            return []
        sources = data.get("sources", [])
        if not sources:
            return []
        # Use first source (usually best available)
        src = sources[0]
        tracks = []
        for s in src.get("sets", []):
            for t in s.get("tracks", []):
                title = t.get("title", "")
                duration = t.get("duration")
                if title and duration and duration > 0:
                    # Skip file artifacts (e.g. "11. Intro 2 -.mp3", numbered files)
                    if re.search(r'\.(mp3|flac|wav|shn|ogg)$', title, re.IGNORECASE):
                        continue
                    if re.match(r'^\d+\.\s', title) and len(title.split()) <= 3:
                        continue
                    # Skip generic non-song tracks
                    lower_title = title.lower().strip()
                    if lower_title in ("intro", "outro", "crowd", "banter",
                                       "tuning", "encore break", "set break",
                                       "applause", "soundcheck"):
                        continue
                    tracks.append({
                        "title": title,
                        "duration": duration,
                    })
        return tracks

    # -------------------------------------------------------------------------
    # Track name normalization
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize_track_name(name: str) -> str:
        """Normalize a Relisten track name.

        Taper-entered names may include transition markers:
        - "Black Clouds >Workin in a Coal Mine>Black Clouds" → "black clouds"
        - "Missin Me>" → "missin me"
        - "Aquamarine >" → "aquamarine"
        """
        # Split on > and take first segment
        parts = name.split(">")
        cleaned = parts[0].strip()
        # Strip trailing punctuation
        cleaned = re.sub(r'[>\-\s]+$', '', cleaned)
        return cleaned.lower()

    def _resolve_song_name(self, query: str) -> str:
        """Resolve a user query to a normalized song name using aliases."""
        q = query.lower().strip()

        # Remove common filler words
        filler_phrases = [
            "how many times", "when did they", "tell me about",
            "jam chart", "gap on", "gap for", "info on",
            "how long is", "how long does", "average duration",
            "avg duration", "average length", "typical length",
        ]
        for phrase in filler_phrases:
            q = q.replace(phrase, " ")

        filler_words = [
            "longest", "best", "play", "played", "last", "first",
            "stats", "statistics", "info", "about", "gap",
            "version", "top", "ever", "average", "avg", "duration",
            "the", "a", "of",
        ]
        for word in filler_words:
            q = re.sub(rf'\b{word}\b', ' ', q)

        # Remove band names
        band_names = [
            "sci", "string cheese", "cheese incident", "string cheese incident",
            "biscuits", "disco biscuits", "the disco biscuits", "bisco",
            "lotus", "umphreys", "umphrey's", "umphrey's mcgee",
            "wsp", "widespread", "widespread panic",
            "moe", "moe.", "sts9", "sound tribe", "sector 9",
            "billy strings", "bmfs", "billy",
            "spafford", "spaff",
        ]
        for bn in sorted(band_names, key=len, reverse=True):
            q = q.replace(bn, " ")

        q = re.sub(r'[?!.,]', '', q)
        q = " ".join(q.split()).strip()

        if not q:
            return None

        # Check song_aliases from PT/setlistfm config
        if q in self.song_aliases:
            return self.song_aliases[q].lower()

        # Check partial alias matches
        for alias, song_name in self.song_aliases.items():
            if alias == q or q == alias:
                return song_name.lower()

        # Check if query is a substring of an alias or vice versa
        for alias, song_name in sorted(self.song_aliases.items(), key=lambda x: len(x[0]), reverse=True):
            if alias in q or q in alias:
                return song_name.lower()

        return q

    # -------------------------------------------------------------------------
    # Duration index
    # -------------------------------------------------------------------------

    def _index_path(self) -> Path:
        return INDEX_DIR / f"{self.band_key}_duration_index.json"

    def _load_or_build_index(self) -> dict:
        """Load cached index or build from scratch."""
        if self._duration_index is not None:
            return self._duration_index

        idx_path = self._index_path()
        if idx_path.exists():
            age = time.time() - idx_path.stat().st_mtime
            if age < CACHE_EXPIRY:
                try:
                    self._duration_index = json.loads(idx_path.read_text())
                    return self._duration_index
                except (json.JSONDecodeError, OSError):
                    pass

        # Build index
        self._duration_index = self._build_duration_index()
        return self._duration_index

    def _build_duration_index(self) -> dict:
        """Iterate all shows, extract {normalized_track_name: [{date, duration_sec, venue, location}]}."""
        print(f"Building Relisten duration index for {self.band_name}... (this may take a minute)")
        index = {}  # normalized_name → [performances]
        display_names = {}  # normalized_name → most common display name

        years = self._get_years()
        total_shows = 0

        for year in years:
            shows = self._get_show_dates(year)
            for show in shows:
                date = show["date"]
                if not date or show.get("source_count", 0) == 0:
                    continue

                tracks = self._get_show_tracks(year, date)
                if not tracks:
                    continue

                total_shows += 1
                for track in tracks:
                    norm = self._normalize_track_name(track["title"])
                    if not norm or len(norm) < 2:
                        continue

                    if norm not in index:
                        index[norm] = []
                        display_names[norm] = {}

                    index[norm].append({
                        "date": date,
                        "duration_sec": track["duration"],
                        "venue": show["venue"],
                        "location": show["location"],
                    })

                    # Track display name frequency
                    raw_title = track["title"].split(">")[0].strip().rstrip(">- ")
                    display_names[norm][raw_title] = display_names[norm].get(raw_title, 0) + 1

        # Pick best display name for each track
        best_names = {}
        for norm, counts in display_names.items():
            best_names[norm] = max(counts, key=counts.get)

        result = {
            "tracks": index,
            "display_names": best_names,
            "total_shows": total_shows,
            "built_at": time.time(),
        }

        # Save index
        try:
            idx_path = self._index_path()
            idx_path.write_text(json.dumps(result))
        except OSError as e:
            print(f"Warning: Could not save index: {e}")

        print(f"Done! Indexed {len(index)} songs across {total_shows} shows.")
        return result

    # -------------------------------------------------------------------------
    # Query methods
    # -------------------------------------------------------------------------

    def _find_song_in_index(self, song_query: str) -> tuple:
        """Find a song in the index, returns (normalized_key, display_name, performances)."""
        index = self._load_or_build_index()
        tracks = index.get("tracks", {})
        display_names = index.get("display_names", {})

        resolved = self._resolve_song_name(song_query)
        if not resolved:
            return None, None, None

        # Direct match
        if resolved in tracks:
            return resolved, display_names.get(resolved, resolved), tracks[resolved]

        # Try matching against display names (lowercased)
        for norm_key, disp in display_names.items():
            if disp.lower() == resolved:
                return norm_key, disp, tracks[norm_key]

        # Substring match (prefer longer matches)
        matches = []
        for norm_key in tracks:
            if resolved in norm_key or norm_key in resolved:
                matches.append(norm_key)
        if matches:
            # Prefer exact-length match, then shortest
            best = min(matches, key=lambda k: abs(len(k) - len(resolved)))
            return best, display_names.get(best, best), tracks[best]

        return None, None, None

    def query_longest(self, song_name: str, limit: int = 10, year: int = None) -> QueryResult:
        """Get the longest versions of a song."""
        norm_key, display_name, performances = self._find_song_in_index(song_name)

        if not performances:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find duration data for '{song_name}' by {self.band_name} on Relisten."
            )

        # Filter by year if specified
        if year:
            performances = [p for p in performances if p["date"].startswith(str(year))]
            if not performances:
                return QueryResult(
                    success=False,
                    band=self.band_name,
                    answer=f"No performances of {display_name} found in {year}."
                )

        # Sort by duration descending
        sorted_perfs = sorted(performances, key=lambda x: x["duration_sec"], reverse=True)

        # Calculate average
        avg_sec = sum(p["duration_sec"] for p in performances) / len(performances)
        avg_min = avg_sec / 60

        top = sorted_perfs[:limit]

        # Format output
        longest = top[0]
        l_min = longest["duration_sec"] // 60
        l_sec = longest["duration_sec"] % 60

        year_str = f" in {year}" if year else ""
        lines = [f"**Top {min(limit, len(top))} Longest {display_name}{year_str}** ({self.band_name})\n"]

        for i, p in enumerate(top, 1):
            mins = p["duration_sec"] // 60
            secs = p["duration_sec"] % 60
            venue_str = p.get("venue", "")
            loc_str = p.get("location", "")
            location = f"{venue_str}, {loc_str}" if loc_str else venue_str
            lines.append(f"{i}. **{mins}:{secs:02d}** — {p['date']} ({location})")

        lines.append(f"\nAverage duration: {avg_min:.1f} min (based on {len(performances)} performances)")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=f"{l_min}:{l_sec:02d}",
            card_data={
                "type": "duration",
                "title": f"Longest {display_name}",
                "stat": f"{l_min}:{l_sec:02d}",
                "stat_label": "longest version",
                "extra": {
                    "avg": f"{avg_min:.1f} min",
                    "count": len(performances),
                    "source": "Relisten/Archive.org"
                }
            }
        )

    def query_longest_overall(self, limit: int = 5) -> QueryResult:
        """Get the longest performances across all songs."""
        index = self._load_or_build_index()
        tracks = index.get("tracks", {})
        display_names = index.get("display_names", {})

        all_perfs = []
        for norm_key, perfs in tracks.items():
            disp = display_names.get(norm_key, norm_key)
            for p in perfs:
                all_perfs.append({
                    "song": disp,
                    "date": p["date"],
                    "duration_sec": p["duration_sec"],
                    "venue": p.get("venue", ""),
                    "location": p.get("location", ""),
                })

        if not all_perfs:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"No duration data available for {self.band_name}."
            )

        sorted_perfs = sorted(all_perfs, key=lambda x: x["duration_sec"], reverse=True)

        # Deduplicate by song — show each song's longest only once
        seen_songs = set()
        unique_top = []
        for p in sorted_perfs:
            song_key = p["song"].lower()
            if song_key not in seen_songs:
                seen_songs.add(song_key)
                unique_top.append(p)
                if len(unique_top) >= limit:
                    break

        lines = [f"**Top {len(unique_top)} Longest {self.band_name} Jams Ever**\n"]

        for i, p in enumerate(unique_top, 1):
            mins = p["duration_sec"] // 60
            secs = p["duration_sec"] % 60
            location = f"{p['venue']}, {p['location']}" if p["location"] else p["venue"]
            lines.append(f"{i}. **{p['song']}** — {mins}:{secs:02d} ({p['date']}, {location})")

        lines.append(f"\nBased on {index.get('total_shows', '?')} shows on Relisten/Archive.org")

        longest = unique_top[0]
        l_min = longest["duration_sec"] // 60
        l_sec = longest["duration_sec"] % 60

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=f"{l_min}:{l_sec:02d}",
            card_data={
                "type": "duration",
                "title": f"Longest {self.band_name} Jams",
                "stat": f"{l_min}:{l_sec:02d}",
                "stat_label": "longest ever",
                "extra": {
                    "song": longest["song"],
                    "date": longest["date"],
                    "source": "Relisten/Archive.org"
                }
            }
        )

    def query_average_duration(self, song_name: str) -> QueryResult:
        """Get the average duration of a song."""
        norm_key, display_name, performances = self._find_song_in_index(song_name)

        if not performances:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find duration data for '{song_name}' by {self.band_name} on Relisten."
            )

        durations = [p["duration_sec"] for p in performances]
        avg_sec = sum(durations) / len(durations)
        min_sec = min(durations)
        max_sec = max(durations)

        avg_min = avg_sec / 60
        min_min = min_sec / 60
        max_min = max_sec / 60

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"Average {display_name} length: {avg_min:.1f} minutes\n"
                   f"Shortest: {min_min:.1f} min | Longest: {max_min:.1f} min\n"
                   f"Based on {len(durations)} performances with timing data (Relisten/Archive.org).",
            highlight=f"{avg_min:.1f}",
            card_data={
                "type": "duration",
                "title": f"{display_name} Duration",
                "stat": f"{avg_min:.1f}",
                "stat_label": "avg minutes",
                "extra": {
                    "min": f"{min_min:.1f} min",
                    "max": f"{max_min:.1f} min",
                    "count": len(durations),
                    "source": "Relisten/Archive.org"
                }
            }
        )
