#!/usr/bin/env python3
"""
PhishStats Query Engine
=======================

Natural language query interface for Phish statistics.
Uses local pre-computed data for fast responses.

The key insight: Rather than hardcoding query patterns, we provide
structured data to an LLM that can interpret any question and
extract the relevant information.

For now, we implement common query patterns directly. The LLM layer
can be added on top for true natural language flexibility.
"""

import json
import re
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
COMPUTED_DIR = DATA_DIR / "computed"


@dataclass
class QueryResult:
    """Structured result from a query."""
    success: bool
    answer: str
    highlight: Optional[str] = None
    card_data: Optional[Dict[str, Any]] = None
    related_queries: Optional[List[str]] = None
    raw_data: Optional[Any] = None


class PhishStatsEngine:
    """Query engine for Phish statistics."""

    # Common venue aliases for matching
    VENUE_ALIASES = {
        "msg": "Madison Square Garden",
        "garden": "Madison Square Garden",
        "madison square": "Madison Square Garden",
        "the garden": "Madison Square Garden",
        "alpine": "Alpine Valley",
        "alpine valley": "Alpine Valley Music Theatre",
        "dicks": "Dick's Sporting Goods Park",
        "dick's": "Dick's Sporting Goods Park",
        "commerce city": "Dick's Sporting Goods Park",
        "shoreline": "Shoreline Amphitheatre",
        "tahoe": "Lake Tahoe Outdoor Arena at Harveys",
        "harveys": "Lake Tahoe Outdoor Arena at Harveys",
        "fenway": "Fenway Park",
        "sphere": "Sphere",
        "las vegas sphere": "Sphere",
        "riviera": "Moon Palace Resort",  # Mexico runs
        "mexico": "Moon Palace Resort",
        "cancun": "Moon Palace Resort",
        "mgm": "MGM Grand Garden Arena",
        "hampton": "Hampton Coliseum",
        "mothership": "Hampton Coliseum",
        "great woods": "Great Woods Center for the Performing Arts",
        "mansfield": "Xfinity Center",  # Various names over the years
        "spac": "Saratoga Performing Arts Center",
        "saratoga": "Saratoga Performing Arts Center",
        "bethel": "Bethel Woods Center for the Arts",
        "darien": "Darien Lake Performing Arts Center",
        "jones beach": "Jones Beach Theater",
        "deer creek": "Deer Creek Music Center",
        "noblesville": "Deer Creek Music Center",
        "ruoff": "Ruoff Music Center",  # New name for Deer Creek
        "merriweather": "Merriweather Post Pavilion",
        "columbia": "Merriweather Post Pavilion",
        "gorge": "The Gorge Amphitheatre",
        "george": "The Gorge Amphitheatre",
        "wrigley": "Wrigley Field",
        "chicago": "Wrigley Field",
        "hollywood bowl": "Hollywood Bowl",
        "greek": "Greek Theatre",
        "berkeley": "Greek Theatre",
        "red rocks": "Red Rocks Amphitheatre",
        "uic": "UIC Pavilion",
        "allstate": "Allstate Arena",
        "rosemont": "Rosemont Horizon",
    }

    def __init__(self):
        self.data_loaded = False
        self.song_stats = {}
        self.duration_stats = {}
        self.venue_stats = {}
        self.shows = []
        self.raw_durations = {}  # All individual performances with venues
        self.songs_metadata = {}  # Song metadata including original artist
        self.artists_to_songs = {}  # Reverse lookup: artist -> list of songs

    def load_data(self):
        """Load all pre-computed data."""
        if self.data_loaded:
            return

        print("Loading PhishStats data...")

        try:
            with open(COMPUTED_DIR / "song_stats.json") as f:
                self.song_stats = json.load(f)

            with open(COMPUTED_DIR / "duration_stats.json") as f:
                self.duration_stats = json.load(f)

            with open(COMPUTED_DIR / "venue_stats.json") as f:
                self.venue_stats = json.load(f)

            with open(RAW_DIR / "shows.json") as f:
                self.shows = json.load(f)

            # Load raw durations for venue-filtered queries
            try:
                with open(RAW_DIR / "durations.json") as f:
                    self.raw_durations = json.load(f)
            except FileNotFoundError:
                self.raw_durations = {}

            # Load song metadata (including artist info for covers)
            try:
                with open(RAW_DIR / "songs_metadata.json") as f:
                    self.songs_metadata = json.load(f)
                    # Build reverse lookup: artist -> songs
                    self.artists_to_songs = {}
                    for song_name, meta in self.songs_metadata.items():
                        artist = meta.get("artist", "Phish")
                        if artist not in self.artists_to_songs:
                            self.artists_to_songs[artist] = []
                        self.artists_to_songs[artist].append(song_name)
            except FileNotFoundError:
                self.songs_metadata = {}
                self.artists_to_songs = {}

            self.data_loaded = True
            print(f"  Loaded {len(self.song_stats)} songs, {len(self.duration_stats)} with durations, {len(self.shows)} shows")

        except FileNotFoundError as e:
            print(f"  Warning: Some data files not found: {e}")
            print("  Run 'python scripts/refresh_data.py --full --durations' to populate data")

    def _normalize_song_name(self, query: str) -> Optional[str]:
        """Try to match a song name from the query."""
        query_lower = query.lower()

        # Common abbreviations/aliases (check first - these are intentional)
        aliases = {
            "yem": "You Enjoy Myself",
            "dwd": "Down with Disease",
            "cdt": "Chalk Dust Torture",
            "mikes": "Mike's Song",
            "mike's": "Mike's Song",
            "weekapaug": "Weekapaug Groove",
            "hood": "Harry Hood",
            "gin": "Bathtub Gin",
            "antelope": "Run Like an Antelope",
            "bowie": "David Bowie",
            "soam": "Split Open and Melt",
            "slave": "Slave to the Traffic Light",
            "fluff": "Fluffhead",
            "tweezer reprise": "Tweezer Reprise",
            "tweeprise": "Tweezer Reprise",
            "2001": "Also Sprach Zarathustra",
            "also sprach": "Also Sprach Zarathustra",
        }

        for alias, full_name in aliases.items():
            # Use word boundary check for aliases
            if re.search(r'\b' + re.escape(alias) + r'\b', query_lower):
                return full_name

        # Direct matches - use word boundary to avoid matching "Time" in "times"
        # Sort by length descending to match longer names first (e.g., "Harry Hood" before "Hood")
        sorted_songs = sorted(self.song_stats.keys(), key=len, reverse=True)
        for song in sorted_songs:
            # Use word boundary matching to avoid false positives
            pattern = r'\b' + re.escape(song.lower()) + r'\b'
            if re.search(pattern, query_lower):
                return song

        return None

    def _song_to_slug(self, song_name: str) -> str:
        """Convert song name to Phish.in slug format."""
        slug = song_name.lower()
        slug = re.sub(r"[''']", "", slug)  # Remove apostrophes
        slug = re.sub(r"[^a-z0-9]+", "-", slug)  # Replace non-alphanumeric with dashes
        slug = slug.strip("-")
        return slug

    def _normalize_venue_name(self, query: str) -> Optional[str]:
        """Try to match a venue name from the query."""
        query_lower = query.lower()

        # Check aliases first
        for alias, full_name in self.VENUE_ALIASES.items():
            if alias in query_lower:
                return full_name

        # Direct partial match against known venues in shows
        venues_seen = set()
        for show in self.shows:
            venues_seen.add(show.get("venue", ""))

        for venue in venues_seen:
            if venue and venue.lower() in query_lower:
                return venue
            # Also check if query is in venue name (e.g., "square garden" in "Madison Square Garden")
            if venue and query_lower in venue.lower():
                return venue

        return None

    def _extract_venue_from_query(self, query: str) -> tuple:
        """
        Extract venue from query like 'longest Tweezer at Madison Square Garden'.
        Returns (query_without_venue, venue_name) or (query, None).
        """
        query_lower = query.lower()

        # Patterns to look for venue markers
        venue_markers = [" at ", " in ", " from "]

        for marker in venue_markers:
            if marker in query_lower:
                parts = query.split(marker, 1)  # Split only on first occurrence
                base_query = parts[0].strip()
                venue_part = parts[1].strip() if len(parts) > 1 else ""

                if venue_part:
                    venue = self._normalize_venue_name(venue_part)
                    if venue:
                        return base_query, venue
                    # If no exact match, use the raw venue part for fuzzy matching later
                    return base_query, venue_part

        return query, None

    def _match_venue(self, perf_venue: str, target_venue: str) -> bool:
        """Check if a performance venue matches the target venue."""
        if not perf_venue or not target_venue:
            return False

        perf_lower = perf_venue.lower()
        target_lower = target_venue.lower()

        # Exact match
        if target_lower in perf_lower or perf_lower in target_lower:
            return True

        # Check against aliases - if target matches an alias, compare with the full name
        for alias, full_name in self.VENUE_ALIASES.items():
            if alias in target_lower:
                if full_name.lower() in perf_lower:
                    return True

        return False

    def query_longest(self, song_name: str) -> QueryResult:
        """Query for the longest version of a song."""
        slug = self._song_to_slug(song_name)

        if slug not in self.duration_stats:
            return QueryResult(
                success=False,
                answer=f"I don't have duration data for {song_name}. Try a common jam vehicle like Tweezer, Ghost, or You Enjoy Myself.",
                related_queries=["longest Tweezer", "longest Ghost", "longest YEM"]
            )

        stats = self.duration_stats[slug]
        longest = stats["longest"]

        # Format duration
        mins = int(longest["duration_min"])
        secs = int((longest["duration_min"] - mins) * 60)
        duration_str = f"{mins}:{secs:02d}"

        # Calculate vs average
        avg = stats["avg_duration_min"]
        vs_avg = longest["duration_min"] / avg

        answer = (
            f"The longest {song_name} was {duration_str} ({longest['duration_min']:.1f} minutes), "
            f"played at {longest['venue']} on {longest['date']}. "
            f"That's {vs_avg:.1f}x longer than average ({avg:.1f} min)."
        )

        return QueryResult(
            success=True,
            answer=answer,
            highlight=duration_str,
            card_data={
                "type": "longest",
                "title": song_name,
                "stat": duration_str,
                "subtitle": f"{longest['date']} • {longest['venue']}",
                "context": f"{vs_avg:.1f}x longer than average ({avg:.1f} min)",
                "extra": {
                    "avg_duration": f"{avg:.1f} min",
                    "total_performances": stats["track_count"],
                    "monster_count": stats["monster_count"],
                    "monster_rate": f"{stats['monster_rate']*100:.1f}%"
                }
            },
            related_queries=[
                f"top 10 longest {song_name}s",
                f"average {song_name} length",
                f"{song_name} monster jams"
            ],
            raw_data=stats
        )

    def query_longest_at_venue(self, song_name: str, venue: str) -> QueryResult:
        """Query for the longest version of a song at a specific venue."""
        slug = self._song_to_slug(song_name)

        if slug not in self.raw_durations:
            return QueryResult(
                success=False,
                answer=f"I don't have duration data for {song_name}.",
                related_queries=["longest Tweezer", "longest Ghost"]
            )

        # Filter performances by venue
        all_perfs = self.raw_durations[slug]
        venue_perfs = [p for p in all_perfs if self._match_venue(p.get("venue", ""), venue)]

        if not venue_perfs:
            # No matches - try to find similar venues
            venues_with_song = set(p.get("venue", "") for p in all_perfs if p.get("venue"))
            return QueryResult(
                success=False,
                answer=f"I couldn't find any performances of {song_name} at {venue}. "
                       f"This song has been played at {len(venues_with_song)} different venues.",
                related_queries=[f"longest {song_name}", f"where has {song_name} been played"]
            )

        # Find longest at this venue
        longest = max(venue_perfs, key=lambda p: p.get("duration_min", 0))
        total_at_venue = len(venue_perfs)

        # Format duration
        mins = int(longest["duration_min"])
        secs = int((longest["duration_min"] - mins) * 60)
        duration_str = f"{mins}:{secs:02d}"

        # Get overall stats for comparison
        overall_stats = self.duration_stats.get(slug, {})
        overall_longest = overall_stats.get("max_duration_min", longest["duration_min"])
        overall_avg = overall_stats.get("avg_duration_min", longest["duration_min"])

        # Build answer
        venue_display = longest.get("venue", venue)
        answer = (
            f"The longest {song_name} at {venue_display} was {duration_str} "
            f"({longest['duration_min']:.1f} minutes) on {longest['date']}. "
            f"They've played {song_name} at this venue {total_at_venue} times."
        )

        # Add context vs overall longest
        if overall_longest > longest["duration_min"] * 1.1:  # If overall is >10% longer
            answer += f" The longest ever was {overall_longest:.1f} min."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=duration_str,
            card_data={
                "type": "longest",
                "title": f"{song_name} at {venue_display}",
                "stat": duration_str,
                "subtitle": f"{longest['date']}",
                "context": f"Played {total_at_venue} times at this venue",
                "extra": {
                    "avg_duration": f"{overall_avg:.1f} min (overall)",
                    "total_performances": total_at_venue,
                    "overall_longest": f"{overall_longest:.1f} min",
                    "venue": venue_display
                }
            },
            related_queries=[
                f"longest {song_name}",
                f"how many times {song_name} at {venue}",
                f"all {song_name}s at {venue}"
            ],
            raw_data={"longest": longest, "venue_performances": venue_perfs}
        )

    def query_longest_song_at_venue(self, venue: str) -> QueryResult:
        """Query for the longest song ever played at a venue (any song)."""
        # Search all songs for performances at this venue
        all_venue_perfs = []

        for song_slug, performances in self.raw_durations.items():
            for perf in performances:
                if self._match_venue(perf.get("venue", ""), venue):
                    all_venue_perfs.append({
                        "song_slug": song_slug,
                        "date": perf["date"],
                        "duration_min": perf["duration_min"],
                        "venue": perf["venue"]
                    })

        if not all_venue_perfs:
            return QueryResult(
                success=False,
                answer=f"I couldn't find any duration data for performances at '{venue}'.",
                related_queries=["longest song at MSG", "longest song at Alpine"]
            )

        # Find the longest
        longest = max(all_venue_perfs, key=lambda p: p["duration_min"])
        venue_display = longest["venue"]

        # Convert slug back to song name
        song_name = longest["song_slug"].replace("-", " ").title()
        # Try to find proper name from song_stats
        for name in self.song_stats.keys():
            if self._song_to_slug(name) == longest["song_slug"]:
                song_name = name
                break

        # Format duration
        mins = int(longest["duration_min"])
        secs = int((longest["duration_min"] - mins) * 60)
        duration_str = f"{mins}:{secs:02d}"

        # Get top 5 for context
        top_5 = sorted(all_venue_perfs, key=lambda p: p["duration_min"], reverse=True)[:5]

        answer = (
            f"The longest song ever played at {venue_display} was {song_name} "
            f"at {duration_str} ({longest['duration_min']:.1f} minutes) on {longest['date']}."
        )

        # Add runners up
        if len(top_5) > 1:
            runners = []
            for p in top_5[1:3]:  # Show 2nd and 3rd
                p_name = p["song_slug"].replace("-", " ").title()
                for name in self.song_stats.keys():
                    if self._song_to_slug(name) == p["song_slug"]:
                        p_name = name
                        break
                runners.append(f"{p_name} ({p['duration_min']:.1f} min, {p['date']})")
            answer += f"\n\nRunners up: {'; '.join(runners)}"

        return QueryResult(
            success=True,
            answer=answer,
            highlight=duration_str,
            card_data={
                "type": "longest",
                "title": f"Longest at {venue_display}",
                "stat": duration_str,
                "subtitle": f"{song_name} • {longest['date']}",
                "context": f"Out of {len(all_venue_perfs)} tracked performances",
                "extra": {
                    "song": song_name,
                    "date": longest["date"],
                    "venue": venue_display,
                    "total_tracked": len(all_venue_perfs)
                }
            },
            related_queries=[
                f"longest {song_name}",
                f"shows at {venue}",
                f"how many times {song_name} at {venue}"
            ],
            raw_data={"longest": longest, "top_5": top_5}
        )

    def query_play_count(self, song_name: str, venue: str = None, year: str = None) -> QueryResult:
        """Query for how many times a song has been played."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database. Check the spelling or try another song."
            )

        stats = self.song_stats[song_name]

        # TODO: Add venue/year filtering when we have that data indexed
        count = stats["play_count"]
        first = stats["first_played"]
        last = stats["last_played"]

        answer = (
            f"{song_name} has been played {count} times. "
            f"First played on {first}, most recently on {last}."
        )

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(count),
            card_data={
                "type": "count",
                "title": song_name,
                "stat": count,
                "stat_label": "times played",
                "subtitle": f"First: {first} • Last: {last}",
                "extra": {
                    "jamchart_count": stats["jamchart_count"],
                    "opener_count": stats["opener_count"],
                    "encore_count": stats["encore_count"],
                    "venue_count": stats["venue_count"]
                }
            },
            related_queries=[
                f"longest {song_name}",
                f"{song_name} as opener",
                f"when did they last play {song_name}"
            ],
            raw_data=stats
        )

    def query_song_stats(self, song_name: str) -> QueryResult:
        """Get comprehensive stats for a song."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        stats = self.song_stats[song_name]
        slug = self._song_to_slug(song_name)
        duration_data = self.duration_stats.get(slug, {})

        parts = [f"{song_name} Statistics:"]
        parts.append(f"• Played {stats['play_count']} times ({stats['first_played']} to {stats['last_played']})")
        parts.append(f"• Jam charted {stats['jamchart_count']} times ({stats['jamchart_count']/stats['play_count']*100:.1f}%)")
        parts.append(f"• Opened {stats['opener_count']} shows, encored {stats['encore_count']} times")

        if duration_data:
            parts.append(f"• Average length: {duration_data['avg_duration_min']:.1f} min")
            parts.append(f"• Longest: {duration_data['max_duration_min']:.1f} min ({duration_data['longest']['date']})")
            parts.append(f"• Monster jams (25+ min): {duration_data['monster_count']} ({duration_data['monster_rate']*100:.1f}%)")

        return QueryResult(
            success=True,
            answer="\n".join(parts),
            card_data={
                "type": "stats",
                "title": song_name,
                "stats": stats,
                "duration_stats": duration_data
            },
            raw_data={"song_stats": stats, "duration_stats": duration_data}
        )

    def query_last_played(self, song_name: str) -> QueryResult:
        """Query for when a song was last played."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        stats = self.song_stats[song_name]
        last = stats["last_played"]
        count = stats["play_count"]

        answer = f"{song_name} was last played on {last}. It has been played {count} times total."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=last,
            card_data={
                "type": "count",
                "title": song_name,
                "stat": last,
                "stat_label": "last played",
                "subtitle": f"Played {count} times total"
            },
            related_queries=[
                f"gap on {song_name}",
                f"longest {song_name}",
                f"{song_name} stats"
            ],
            raw_data=stats
        )

    def query_gap(self, song_name: str) -> QueryResult:
        """Query for how many shows since a song was last played."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        stats = self.song_stats[song_name]
        last_played = stats["last_played"]
        count = stats["play_count"]

        # Calculate gap - count shows after last_played date
        gap = 0
        sorted_shows = sorted(self.shows, key=lambda s: s["showdate"], reverse=True)
        for show in sorted_shows:
            if show["showdate"] > last_played:
                gap += 1
            else:
                break

        if gap == 0:
            answer = f"{song_name} was played at the most recent show ({last_played})!"
        elif gap == 1:
            answer = f"{song_name} has a gap of 1 show. Last played on {last_played}."
        else:
            answer = f"{song_name} has a gap of {gap} shows. Last played on {last_played}."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(gap),
            card_data={
                "type": "count",
                "title": song_name,
                "stat": gap,
                "stat_label": "show gap",
                "subtitle": f"Last played: {last_played}",
                "extra": {
                    "play_count": count,
                    "last_played": last_played
                }
            },
            related_queries=[
                f"when did they last play {song_name}",
                f"{song_name} stats",
                f"longest {song_name}"
            ],
            raw_data={"gap": gap, "last_played": last_played, "stats": stats}
        )

    def query_play_count_at_venue(self, song_name: str, venue: str) -> QueryResult:
        """Query for how many times a song has been played at a specific venue."""
        # Find all shows at this venue
        venue_shows = [s for s in self.shows if self._match_venue(s.get("venue", ""), venue)]

        if not venue_shows:
            return QueryResult(
                success=False,
                answer=f"I couldn't find any shows at '{venue}'.",
                related_queries=[f"how many times has {song_name} been played"]
            )

        # Count how many times song appears at this venue
        count = 0
        dates = []
        venue_display = venue_shows[0].get("venue", venue) if venue_shows else venue

        for show in venue_shows:
            for song_entry in show.get("songs", []):
                if song_entry.get("song", "").lower() == song_name.lower():
                    count += 1
                    dates.append(show["showdate"])
                    break

        total_venue_shows = len(venue_shows)

        if count == 0:
            answer = (
                f"{song_name} has never been played at {venue_display}. "
                f"Phish has played {total_venue_shows} shows at this venue."
            )
        else:
            pct = (count / total_venue_shows) * 100
            answer = (
                f"{song_name} has been played {count} times at {venue_display} "
                f"({pct:.0f}% of {total_venue_shows} shows there)."
            )
            if dates:
                dates.sort(reverse=True)
                answer += f" Most recently on {dates[0]}."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(count),
            card_data={
                "type": "count",
                "title": f"{song_name} at {venue_display}",
                "stat": count,
                "stat_label": "times played",
                "subtitle": f"Out of {total_venue_shows} shows at this venue",
                "extra": {
                    "venue_shows": total_venue_shows,
                    "percentage": f"{(count/total_venue_shows)*100:.1f}%" if total_venue_shows > 0 else "0%"
                }
            },
            related_queries=[
                f"longest {song_name} at {venue}",
                f"how many times has {song_name} been played",
                f"shows at {venue}"
            ],
            raw_data={"count": count, "dates": dates, "venue_shows": total_venue_shows}
        )

    def query_setlist(self, date: str) -> QueryResult:
        """Query for the setlist of a specific show."""
        # Normalize date format (handle various input formats)
        date_normalized = date.strip()

        # Try to find the show
        show = None
        for s in self.shows:
            if s["showdate"] == date_normalized:
                show = s
                break

        if not show:
            # Try partial matches (year-month-day could be entered differently)
            for s in self.shows:
                if date_normalized.replace("/", "-").replace(".", "-") in s["showdate"]:
                    show = s
                    break

        if not show:
            return QueryResult(
                success=False,
                answer=f"I couldn't find a show on {date}. Make sure to use YYYY-MM-DD format (e.g., 1999-12-31).",
                related_queries=["show on 1999-12-31", "show on 2023-12-31"]
            )

        # Build setlist
        venue = show.get("venue", "Unknown Venue")
        city = show.get("city", "")
        state = show.get("state", "")
        location = f"{city}, {state}" if city else ""

        songs = show.get("songs", [])
        sets = {}
        for song in songs:
            set_name = song.get("set", "1")
            if set_name not in sets:
                sets[set_name] = []
            sets[set_name].append(song.get("song", "Unknown"))

        # Format setlist
        setlist_parts = [f"Setlist for {show['showdate']} at {venue}"]
        if location:
            setlist_parts[0] += f" ({location})"

        for set_name in sorted(sets.keys()):
            if set_name == "E":
                setlist_parts.append(f"\nEncore: {', '.join(sets[set_name])}")
            else:
                setlist_parts.append(f"\nSet {set_name}: {', '.join(sets[set_name])}")

        return QueryResult(
            success=True,
            answer="\n".join(setlist_parts),
            card_data={
                "type": "setlist",
                "title": show["showdate"],
                "subtitle": venue,
                "context": location,
                "sets": sets
            },
            related_queries=[
                f"longest song from {show['showdate']}",
                f"other shows at {venue}"
            ],
            raw_data=show
        )

    def query_average_duration(self, song_name: str) -> QueryResult:
        """Query for the average duration of a song."""
        slug = self._song_to_slug(song_name)

        if slug not in self.duration_stats:
            return QueryResult(
                success=False,
                answer=f"I don't have duration data for {song_name}.",
                related_queries=["average Tweezer length", "average Ghost length"]
            )

        stats = self.duration_stats[slug]
        avg = stats["avg_duration_min"]
        median = stats["median_duration_min"]
        track_count = stats["track_count"]
        longest = stats["max_duration_min"]
        shortest = stats["min_duration_min"]

        # Format as mm:ss
        avg_mins = int(avg)
        avg_secs = int((avg - avg_mins) * 60)
        avg_str = f"{avg_mins}:{avg_secs:02d}"

        answer = (
            f"The average {song_name} is {avg_str} ({avg:.1f} minutes). "
            f"Based on {track_count} performances. "
            f"Shortest: {shortest:.1f} min, Longest: {longest:.1f} min."
        )

        return QueryResult(
            success=True,
            answer=answer,
            highlight=avg_str,
            card_data={
                "type": "stats",
                "title": song_name,
                "stat": avg_str,
                "stat_label": "average length",
                "extra": {
                    "median": f"{median:.1f} min",
                    "shortest": f"{shortest:.1f} min",
                    "longest": f"{longest:.1f} min",
                    "track_count": track_count
                }
            },
            related_queries=[
                f"longest {song_name}",
                f"{song_name} stats",
                f"monster {song_name}s"
            ],
            raw_data=stats
        )

    def _extract_year_from_query(self, query: str) -> Optional[int]:
        """Extract a year from query like 'since 1995' or 'in 2023'."""
        # Look for 4-digit years
        match = re.search(r'\b(19[89]\d|20[0-2]\d)\b', query)
        if match:
            return int(match.group(1))
        return None

    def query_show_count(self, since_year: int = None, in_year: int = None, in_decade: str = None) -> QueryResult:
        """Query for how many shows Phish has played."""
        if in_year:
            # Shows in a specific year
            year_shows = [s for s in self.shows if s["showdate"].startswith(str(in_year))]
            count = len(year_shows)

            if count == 0:
                answer = f"Phish didn't play any shows in {in_year}."
            else:
                # Get venues
                venues = set(s["venue"] for s in year_shows)
                answer = f"Phish played {count} shows in {in_year} at {len(venues)} different venues."

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(count),
                card_data={
                    "type": "count",
                    "title": f"Shows in {in_year}",
                    "stat": count,
                    "stat_label": "shows"
                },
                related_queries=[
                    f"unique songs in {in_year}",
                    f"shows in {in_year - 1}",
                    f"shows in {in_year + 1}"
                ]
            )

        elif since_year:
            # Shows since a year
            since_shows = [s for s in self.shows if s["showdate"] >= f"{since_year}-01-01"]
            count = len(since_shows)
            total = len(self.shows)

            answer = f"Phish has played {count} shows since {since_year} (out of {total} total career shows)."

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(count),
                card_data={
                    "type": "count",
                    "title": f"Shows Since {since_year}",
                    "stat": count,
                    "stat_label": "shows",
                    "subtitle": f"Out of {total} total shows"
                },
                related_queries=[
                    f"unique songs since {since_year}",
                    f"shows in {since_year}",
                    "total shows"
                ]
            )

        else:
            # Total shows
            total = len(self.shows)
            sorted_shows = sorted(self.shows, key=lambda s: s["showdate"])
            first = sorted_shows[0]
            last = sorted_shows[-1]

            # By decade
            from collections import Counter
            decades = Counter(s["showdate"][:3] + "0s" for s in self.shows)
            decade_str = ", ".join(f"{d}: {c}" for d, c in sorted(decades.items()))

            answer = (
                f"Phish has played {total} shows from {first['showdate']} to {last['showdate']}.\n"
                f"By decade: {decade_str}"
            )

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(total),
                card_data={
                    "type": "count",
                    "title": "Total Shows",
                    "stat": total,
                    "stat_label": "shows",
                    "subtitle": f"{first['showdate']} to {last['showdate']}",
                    "extra": {
                        "first_show": first["showdate"],
                        "last_show": last["showdate"],
                        "decades": dict(decades)
                    }
                },
                related_queries=[
                    "total unique songs",
                    "shows in the 1990s",
                    "shows since 2020"
                ]
            )

    def query_unique_songs(self, since_year: int = None, in_year: int = None) -> QueryResult:
        """Query for how many unique songs Phish has played."""
        if in_year:
            # Songs played in a specific year
            year_shows = [s for s in self.shows if s["showdate"].startswith(str(in_year))]
            songs_in_year = set()
            for show in year_shows:
                for song in show.get("songs", []):
                    songs_in_year.add(song.get("song", ""))

            count = len(songs_in_year)
            show_count = len(year_shows)

            if count == 0:
                answer = f"Phish didn't play any shows in {in_year}."
            else:
                answer = f"Phish played {count} unique songs in {in_year} across {show_count} shows."

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(count),
                card_data={
                    "type": "count",
                    "title": f"Unique Songs in {in_year}",
                    "stat": count,
                    "stat_label": "songs",
                    "subtitle": f"Across {show_count} shows"
                },
                related_queries=[
                    f"shows in {in_year}",
                    f"unique songs in {in_year - 1}",
                    "total unique songs"
                ]
            )

        elif since_year:
            # Songs played since a year
            since_shows = [s for s in self.shows if s["showdate"] >= f"{since_year}-01-01"]
            songs_since = set()
            for show in since_shows:
                for song in show.get("songs", []):
                    songs_since.add(song.get("song", ""))

            count = len(songs_since)
            total_songs = len(self.song_stats)
            show_count = len(since_shows)

            answer = (
                f"Phish has played {count} unique songs since {since_year} "
                f"(out of {total_songs} total in their catalog) across {show_count} shows."
            )

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(count),
                card_data={
                    "type": "count",
                    "title": f"Unique Songs Since {since_year}",
                    "stat": count,
                    "stat_label": "songs",
                    "subtitle": f"Out of {total_songs} total songs"
                },
                related_queries=[
                    f"shows since {since_year}",
                    "total unique songs",
                    f"unique songs in {since_year}"
                ]
            )

        else:
            # Total unique songs
            total = len(self.song_stats)

            # Find most and least played
            sorted_songs = sorted(self.song_stats.items(), key=lambda x: x[1]["play_count"], reverse=True)
            most_played = sorted_songs[0]
            least_played = [s for s in sorted_songs if s[1]["play_count"] == 1]

            answer = (
                f"Phish has played {total} unique songs in their career.\n"
                f"Most played: {most_played[0]} ({most_played[1]['play_count']} times).\n"
                f"Songs played only once: {len(least_played)}"
            )

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(total),
                card_data={
                    "type": "count",
                    "title": "Total Unique Songs",
                    "stat": total,
                    "stat_label": "songs",
                    "extra": {
                        "most_played": most_played[0],
                        "most_played_count": most_played[1]["play_count"],
                        "one_timers": len(least_played)
                    }
                },
                related_queries=[
                    "most played song",
                    "total shows",
                    "unique songs since 2020"
                ]
            )

    def query_career_stats(self) -> QueryResult:
        """Get overall career statistics."""
        total_shows = len(self.shows)
        total_songs = len(self.song_stats)

        sorted_shows = sorted(self.shows, key=lambda s: s["showdate"])
        first = sorted_shows[0]
        last = sorted_shows[-1]

        # Unique venues
        venues = set(s["venue"] for s in self.shows)

        # By decade
        from collections import Counter
        decades = Counter(s["showdate"][:3] + "0s" for s in self.shows)

        # Most played song
        sorted_songs = sorted(self.song_stats.items(), key=lambda x: x[1]["play_count"], reverse=True)
        most_played = sorted_songs[0]

        answer = (
            f"Phish Career Statistics:\n"
            f"• {total_shows} shows ({first['showdate']} to {last['showdate']})\n"
            f"• {total_songs} unique songs performed\n"
            f"• {len(venues)} unique venues\n"
            f"• Most played: {most_played[0]} ({most_played[1]['play_count']} times)\n"
            f"• Shows by decade: " + ", ".join(f"{d}: {c}" for d, c in sorted(decades.items()))
        )

        return QueryResult(
            success=True,
            answer=answer,
            card_data={
                "type": "stats",
                "title": "Phish Career Stats",
                "extra": {
                    "total_shows": total_shows,
                    "total_songs": total_songs,
                    "unique_venues": len(venues),
                    "first_show": first["showdate"],
                    "last_show": last["showdate"],
                    "decades": dict(decades)
                }
            },
            related_queries=[
                "shows since 2020",
                "unique songs since 2000",
                "most played song"
            ]
        )

    def _normalize_artist_name(self, query: str) -> Optional[str]:
        """Try to match an artist name from the query."""
        query_lower = query.lower()

        # Common artist aliases
        artist_aliases = {
            "stones": "The Rolling Stones",
            "rolling stones": "The Rolling Stones",
            "beatles": "The Beatles",
            "the beatles": "The Beatles",
            "dead": "Grateful Dead",
            "grateful dead": "Grateful Dead",
            "zeppelin": "Led Zeppelin",
            "led zeppelin": "Led Zeppelin",
            "floyd": "Pink Floyd",
            "pink floyd": "Pink Floyd",
            "talking heads": "Talking Heads",
            "the who": "The Who",
            "little feat": "Little Feat",
            "velvet underground": "The Velvet Underground",
            "prince": "Prince",
            "bowie": "David Bowie",
            "david bowie": "David Bowie",
            "neil young": "Neil Young",
            "bob dylan": "Bob Dylan",
            "dylan": "Bob Dylan",
            "trey": "Trey Anastasio",
            "trey anastasio": "Trey Anastasio",
        }

        for alias, full_name in artist_aliases.items():
            if alias in query_lower:
                return full_name

        # Direct match against known artists
        for artist in self.artists_to_songs.keys():
            if artist.lower() in query_lower:
                return artist

        return None

    def query_covers_by_artist(self, artist: str) -> QueryResult:
        """Query for how many songs by a specific artist Phish has covered."""
        if artist not in self.artists_to_songs:
            # Try fuzzy match
            for known_artist in self.artists_to_songs.keys():
                if artist.lower() in known_artist.lower() or known_artist.lower() in artist.lower():
                    artist = known_artist
                    break
            else:
                return QueryResult(
                    success=False,
                    answer=f"I couldn't find any covers of {artist} in the database.",
                    related_queries=["Rolling Stones covers", "Beatles covers", "Talking Heads covers"]
                )

        songs = self.artists_to_songs[artist]
        song_count = len(songs)

        # Get play counts and last played for each song
        songs_with_stats = []
        total_performances = 0
        last_played = None

        for song_name in songs:
            if song_name in self.song_stats:
                stats = self.song_stats[song_name]
                play_count = stats["play_count"]
                total_performances += play_count
                song_last = stats["last_played"]
                songs_with_stats.append({
                    "song": song_name,
                    "play_count": play_count,
                    "last_played": song_last
                })
                if last_played is None or song_last > last_played:
                    last_played = song_last

        # Sort by play count
        songs_with_stats.sort(key=lambda x: -x["play_count"])

        # Build answer
        answer = f"Phish has covered {song_count} {artist} songs, performed a total of {total_performances} times."
        if last_played:
            answer += f"\nLast covered on {last_played}."

        # List top songs
        if songs_with_stats:
            top_songs = songs_with_stats[:5]
            songs_list = ", ".join(f"{s['song']} ({s['play_count']}x)" for s in top_songs)
            answer += f"\n\nTop covers: {songs_list}"
            if len(songs_with_stats) > 5:
                answer += f" ...and {len(songs_with_stats) - 5} more"

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(song_count),
            card_data={
                "type": "count",
                "title": f"{artist} Covers",
                "stat": song_count,
                "stat_label": "songs covered",
                "subtitle": f"{total_performances} total performances",
                "extra": {
                    "total_performances": total_performances,
                    "last_covered": last_played,
                    "top_songs": songs_with_stats[:5]
                }
            },
            related_queries=[
                f"when did they last play {songs_with_stats[0]['song']}" if songs_with_stats else "cover stats",
                "Beatles covers",
                "Rolling Stones covers"
            ],
            raw_data={"artist": artist, "songs": songs_with_stats}
        )

    def query_last_covered_artist(self, artist: str) -> QueryResult:
        """Query for when Phish last covered a specific artist."""
        if artist not in self.artists_to_songs:
            # Try fuzzy match
            for known_artist in self.artists_to_songs.keys():
                if artist.lower() in known_artist.lower() or known_artist.lower() in artist.lower():
                    artist = known_artist
                    break
            else:
                return QueryResult(
                    success=False,
                    answer=f"I couldn't find any covers of {artist} in the database."
                )

        songs = self.artists_to_songs[artist]

        # Find last played across all songs by this artist
        last_played = None
        last_song = None

        for song_name in songs:
            if song_name in self.song_stats:
                stats = self.song_stats[song_name]
                song_last = stats["last_played"]
                if last_played is None or song_last > last_played:
                    last_played = song_last
                    last_song = song_name

        if not last_played:
            return QueryResult(
                success=False,
                answer=f"I couldn't find play history for {artist} covers."
            )

        answer = f"Phish last covered {artist} on {last_played} with \"{last_song}\"."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=last_played,
            card_data={
                "type": "count",
                "title": f"Last {artist} Cover",
                "stat": last_played,
                "stat_label": "date",
                "subtitle": last_song
            },
            related_queries=[
                f"how many {artist} songs",
                f"{last_song} stats",
                "cover stats"
            ]
        )

    def query_song_as_opener(self, song_name: str) -> QueryResult:
        """Query for how many times a song has been played as show opener."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        stats = self.song_stats[song_name]
        opener_count = stats.get("opener_count", 0)
        play_count = stats["play_count"]
        pct = (opener_count / play_count * 100) if play_count > 0 else 0

        if opener_count == 0:
            answer = f"{song_name} has never been played as a show opener (played {play_count} times total)."
        else:
            answer = f"{song_name} has opened {opener_count} shows ({pct:.1f}% of {play_count} total performances)."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(opener_count),
            card_data={
                "type": "count",
                "title": f"{song_name} as Opener",
                "stat": opener_count,
                "stat_label": "times as opener",
                "subtitle": f"{pct:.1f}% of {play_count} performances"
            },
            related_queries=[
                f"{song_name} as encore",
                f"{song_name} stats",
                "most common openers"
            ],
            raw_data=stats
        )

    def query_song_as_encore(self, song_name: str) -> QueryResult:
        """Query for how many times a song has been played as encore."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        stats = self.song_stats[song_name]
        encore_count = stats.get("encore_count", 0)
        play_count = stats["play_count"]
        pct = (encore_count / play_count * 100) if play_count > 0 else 0

        if encore_count == 0:
            answer = f"{song_name} has never been played as an encore (played {play_count} times total)."
        else:
            answer = f"{song_name} has been an encore {encore_count} times ({pct:.1f}% of {play_count} total performances)."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(encore_count),
            card_data={
                "type": "count",
                "title": f"{song_name} as Encore",
                "stat": encore_count,
                "stat_label": "times as encore",
                "subtitle": f"{pct:.1f}% of {play_count} performances"
            },
            related_queries=[
                f"{song_name} as opener",
                f"{song_name} stats",
                "most common encores"
            ],
            raw_data=stats
        )

    def query_last_opener(self, song_name: str) -> QueryResult:
        """Query for when a song was last played as show opener."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        # Search through shows for most recent opener
        opener_shows = []
        for show in self.shows:
            for song in show.get("songs", []):
                if song.get("song") == song_name:
                    # Check if opener (set 1, position 1)
                    if song.get("set") == "1" and song.get("position") == 1:
                        opener_shows.append({
                            "date": show["showdate"],
                            "venue": show.get("venue", ""),
                            "city": show.get("city", ""),
                            "state": show.get("state", "")
                        })

        if not opener_shows:
            stats = self.song_stats[song_name]
            return QueryResult(
                success=True,
                answer=f"{song_name} has never opened a show (played {stats['play_count']} times total).",
                related_queries=[f"last {song_name} as encore", f"{song_name} stats"]
            )

        # Sort by date and get most recent
        opener_shows.sort(key=lambda x: x["date"], reverse=True)
        last = opener_shows[0]
        total_openers = len(opener_shows)

        # Calculate gap (shows since)
        gap = 0
        for show in sorted(self.shows, key=lambda s: s["showdate"], reverse=True):
            if show["showdate"] > last["date"]:
                gap += 1
            else:
                break

        location = f"{last['city']}, {last['state']}" if last['city'] else ""
        answer = f"{song_name} last opened a show on {last['date']} at {last['venue']}"
        if location:
            answer += f" ({location})"
        answer += f".\nThat's {gap} shows ago. They've opened with {song_name} {total_openers} times total."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=last["date"],
            card_data={
                "type": "count",
                "title": f"Last {song_name} Opener",
                "stat": last["date"],
                "stat_label": "date",
                "subtitle": f"{last['venue']}",
                "extra": {
                    "gap": gap,
                    "total_openers": total_openers
                }
            },
            related_queries=[
                f"{song_name} as opener",
                f"last {song_name} as encore",
                f"gap on {song_name}"
            ],
            raw_data={"last": last, "all_openers": opener_shows[:10]}
        )

    def query_last_encore(self, song_name: str) -> QueryResult:
        """Query for when a song was last played as encore."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        # Search through shows for most recent encore
        encore_shows = []
        for show in self.shows:
            for song in show.get("songs", []):
                if song.get("song") == song_name:
                    # Check if encore (set is E, e, E2, etc.)
                    set_name = song.get("set", "")
                    if set_name.lower().startswith("e"):
                        encore_shows.append({
                            "date": show["showdate"],
                            "venue": show.get("venue", ""),
                            "city": show.get("city", ""),
                            "state": show.get("state", "")
                        })

        if not encore_shows:
            stats = self.song_stats[song_name]
            return QueryResult(
                success=True,
                answer=f"{song_name} has never been an encore (played {stats['play_count']} times total).",
                related_queries=[f"last {song_name} as opener", f"{song_name} stats"]
            )

        # Sort by date and get most recent
        encore_shows.sort(key=lambda x: x["date"], reverse=True)
        last = encore_shows[0]
        total_encores = len(encore_shows)

        # Calculate gap (shows since)
        gap = 0
        for show in sorted(self.shows, key=lambda s: s["showdate"], reverse=True):
            if show["showdate"] > last["date"]:
                gap += 1
            else:
                break

        location = f"{last['city']}, {last['state']}" if last['city'] else ""
        answer = f"{song_name} was last an encore on {last['date']} at {last['venue']}"
        if location:
            answer += f" ({location})"
        answer += f".\nThat's {gap} shows ago. They've encored with {song_name} {total_encores} times total."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=last["date"],
            card_data={
                "type": "count",
                "title": f"Last {song_name} Encore",
                "stat": last["date"],
                "stat_label": "date",
                "subtitle": f"{last['venue']}",
                "extra": {
                    "gap": gap,
                    "total_encores": total_encores
                }
            },
            related_queries=[
                f"{song_name} as encore",
                f"last {song_name} as opener",
                f"gap on {song_name}"
            ],
            raw_data={"last": last, "all_encores": encore_shows[:10]}
        )

    def query_opener_stats(self) -> QueryResult:
        """Get aggregate opener statistics."""
        # Find all songs that have been openers
        openers = [(name, stats["opener_count"], stats["play_count"])
                   for name, stats in self.song_stats.items()
                   if stats.get("opener_count", 0) > 0]

        unique_openers = len(openers)
        total_opener_plays = sum(o[1] for o in openers)

        # Sort by opener count
        openers.sort(key=lambda x: -x[1])
        top_10 = openers[:10]

        answer = (
            f"Phish Opener Statistics:\n"
            f"• {unique_openers} unique songs have opened shows\n"
            f"• Top openers:\n"
        )
        for name, count, total in top_10:
            pct = (count / total * 100) if total > 0 else 0
            answer += f"  {name}: {count} times ({pct:.0f}% of plays)\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            highlight=str(unique_openers),
            card_data={
                "type": "stats",
                "title": "Opener Statistics",
                "stat": unique_openers,
                "stat_label": "unique openers",
                "extra": {
                    "top_openers": [(n, c) for n, c, _ in top_10]
                }
            },
            related_queries=[
                "encore stats",
                "Chalk Dust Torture as opener",
                "Runaway Jim as opener"
            ],
            raw_data={"openers": openers[:20]}
        )

    def query_encore_stats(self) -> QueryResult:
        """Get aggregate encore statistics."""
        # Find all songs that have been encores
        encores = [(name, stats["encore_count"], stats["play_count"])
                   for name, stats in self.song_stats.items()
                   if stats.get("encore_count", 0) > 0]

        unique_encores = len(encores)
        total_encore_plays = sum(e[1] for e in encores)

        # Sort by encore count
        encores.sort(key=lambda x: -x[1])
        top_10 = encores[:10]

        answer = (
            f"Phish Encore Statistics:\n"
            f"• {unique_encores} unique songs have been encores\n"
            f"• Top encores:\n"
        )
        for name, count, total in top_10:
            pct = (count / total * 100) if total > 0 else 0
            answer += f"  {name}: {count} times ({pct:.0f}% of plays)\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            highlight=str(unique_encores),
            card_data={
                "type": "stats",
                "title": "Encore Statistics",
                "stat": unique_encores,
                "stat_label": "unique encores",
                "extra": {
                    "top_encores": [(n, c) for n, c, _ in top_10]
                }
            },
            related_queries=[
                "opener stats",
                "Tweezer Reprise as encore",
                "Good Times Bad Times as encore"
            ],
            raw_data={"encores": encores[:20]}
        )

    def query_cover_stats(self) -> QueryResult:
        """Get overall cover song statistics."""
        if not self.artists_to_songs:
            return QueryResult(
                success=False,
                answer="Cover song data is not available. Run refresh with --full to pull song metadata."
            )

        # Count covers vs originals
        phish_songs = self.artists_to_songs.get("Phish", [])
        trey_songs = self.artists_to_songs.get("Trey Anastasio", [])
        original_count = len(phish_songs) + len(trey_songs)

        cover_artists = {k: v for k, v in self.artists_to_songs.items()
                        if k not in ["Phish", "Trey Anastasio", "Traditional"]}
        total_cover_songs = sum(len(songs) for songs in cover_artists.values())

        # Top covered artists
        sorted_artists = sorted(cover_artists.items(), key=lambda x: -len(x[1]))[:10]

        answer = (
            f"Phish Cover Statistics:\n"
            f"• {original_count} original Phish/Trey songs\n"
            f"• {total_cover_songs} cover songs from {len(cover_artists)} different artists\n"
            f"\nTop covered artists:\n"
        )
        for artist, songs in sorted_artists:
            answer += f"• {artist}: {len(songs)} songs\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            card_data={
                "type": "stats",
                "title": "Cover Statistics",
                "extra": {
                    "original_count": original_count,
                    "cover_count": total_cover_songs,
                    "cover_artists": len(cover_artists),
                    "top_artists": [(a, len(s)) for a, s in sorted_artists]
                }
            },
            related_queries=[
                "Beatles covers",
                "Rolling Stones covers",
                "Talking Heads covers"
            ]
        )

    def query_venue_show_count(self, venue: str) -> QueryResult:
        """Query for how many shows Phish has played at a venue."""
        venue_shows = [s for s in self.shows if self._match_venue(s.get("venue", ""), venue)]

        if not venue_shows:
            return QueryResult(
                success=False,
                answer=f"I couldn't find any shows at '{venue}'.",
                related_queries=["shows at MSG", "shows at Alpine"]
            )

        count = len(venue_shows)
        venue_display = venue_shows[0].get("venue", venue)

        # Get date range
        sorted_shows = sorted(venue_shows, key=lambda s: s["showdate"])
        first = sorted_shows[0]["showdate"]
        last = sorted_shows[-1]["showdate"]

        answer = f"Phish has played {count} shows at {venue_display} ({first} to {last})."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=str(count),
            card_data={
                "type": "count",
                "title": f"Shows at {venue_display}",
                "stat": count,
                "stat_label": "shows",
                "subtitle": f"{first} to {last}"
            },
            related_queries=[
                f"longest song at {venue}",
                "shows at MSG",
                "shows at Alpine"
            ],
            raw_data={"count": count, "first": first, "last": last}
        )

    def query_most_played(self, year: int = None) -> QueryResult:
        """Query for the most played song overall or in a specific year."""
        if year:
            # Count plays by song in this year
            year_str = str(year)
            song_counts = {}
            for show in self.shows:
                if show["showdate"].startswith(year_str):
                    for song in show.get("songs", []):
                        name = song.get("song", "")
                        if name:
                            song_counts[name] = song_counts.get(name, 0) + 1

            if not song_counts:
                return QueryResult(
                    success=False,
                    answer=f"No shows found in {year}."
                )

            sorted_songs = sorted(song_counts.items(), key=lambda x: -x[1])
            top = sorted_songs[0]
            top_10 = sorted_songs[:10]

            answer = f"Most played song in {year}: {top[0]} ({top[1]} times)\n\nTop 10:\n"
            for i, (name, count) in enumerate(top_10, 1):
                answer += f"{i}. {name}: {count}\n"

            return QueryResult(
                success=True,
                answer=answer.strip(),
                highlight=top[0],
                card_data={
                    "type": "stats",
                    "title": f"Most Played in {year}",
                    "stat": top[0],
                    "stat_label": f"{top[1]} times",
                    "extra": {"top_10": top_10}
                },
                related_queries=[
                    f"shows in {year}",
                    f"unique songs in {year}",
                    "most played song"
                ]
            )
        else:
            # Overall most played
            sorted_songs = sorted(self.song_stats.items(), key=lambda x: -x[1]["play_count"])
            top = sorted_songs[0]
            top_10 = sorted_songs[:10]

            answer = f"Most played song ever: {top[0]} ({top[1]['play_count']} times)\n\nTop 10:\n"
            for i, (name, stats) in enumerate(top_10, 1):
                answer += f"{i}. {name}: {stats['play_count']}\n"

            return QueryResult(
                success=True,
                answer=answer.strip(),
                highlight=top[0],
                card_data={
                    "type": "stats",
                    "title": "Most Played Songs",
                    "stat": top[0],
                    "stat_label": f"{top[1]['play_count']} times",
                    "extra": {"top_10": [(n, s["play_count"]) for n, s in top_10]}
                },
                related_queries=[
                    "rarest songs",
                    "most played in 2024",
                    "career stats"
                ]
            )

    def query_biggest_gaps(self) -> QueryResult:
        """Query for songs with the longest current gaps."""
        # Get most recent show date
        sorted_shows = sorted(self.shows, key=lambda s: s["showdate"], reverse=True)
        latest_date = sorted_shows[0]["showdate"]

        # Calculate gaps for all songs
        songs_with_gaps = []
        for name, stats in self.song_stats.items():
            last_played = stats["last_played"]
            # Count shows since
            gap = sum(1 for s in sorted_shows if s["showdate"] > last_played)
            if gap > 0:  # Only include songs with a gap
                songs_with_gaps.append({
                    "song": name,
                    "gap": gap,
                    "last_played": last_played,
                    "play_count": stats["play_count"]
                })

        # Sort by gap descending
        songs_with_gaps.sort(key=lambda x: -x["gap"])
        top_20 = songs_with_gaps[:20]

        answer = "Songs with the biggest gaps (shows since last played):\n\n"
        for i, s in enumerate(top_20[:10], 1):
            answer += f"{i}. {s['song']}: {s['gap']} shows (last: {s['last_played']}, played {s['play_count']}x total)\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            highlight=f"{top_20[0]['song']} ({top_20[0]['gap']} shows)",
            card_data={
                "type": "stats",
                "title": "Biggest Gaps",
                "extra": {"top_gaps": top_20}
            },
            related_queries=[
                f"gap on {top_20[0]['song']}",
                "rarest songs",
                "most played song"
            ],
            raw_data=top_20
        )

    def query_first_played(self, song_name: str) -> QueryResult:
        """Query for when a song was first played (debut)."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        stats = self.song_stats[song_name]
        debut = stats["first_played"]
        play_count = stats["play_count"]

        # Find the debut show for venue info
        debut_show = None
        for show in self.shows:
            if show["showdate"] == debut:
                debut_show = show
                break

        venue = debut_show.get("venue", "") if debut_show else ""
        city = debut_show.get("city", "") if debut_show else ""

        answer = f"{song_name} debuted on {debut}"
        if venue:
            answer += f" at {venue}"
            if city:
                answer += f" ({city})"
        answer += f". It has been played {play_count} times since."

        return QueryResult(
            success=True,
            answer=answer,
            highlight=debut,
            card_data={
                "type": "count",
                "title": f"{song_name} Debut",
                "stat": debut,
                "stat_label": "debut date",
                "subtitle": venue if venue else None
            },
            related_queries=[
                f"when did they last play {song_name}",
                f"{song_name} stats",
                f"gap on {song_name}"
            ],
            raw_data=stats
        )

    def query_peak_year(self) -> QueryResult:
        """Query for which year Phish played the most shows."""
        from collections import Counter
        years = Counter(s["showdate"][:4] for s in self.shows)
        sorted_years = sorted(years.items(), key=lambda x: -x[1])

        top = sorted_years[0]
        top_10 = sorted_years[:10]

        answer = f"Phish played the most shows in {top[0]} with {top[1]} shows.\n\nTop 10 years:\n"
        for year, count in top_10:
            answer += f"• {year}: {count} shows\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            highlight=f"{top[0]} ({top[1]} shows)",
            card_data={
                "type": "stats",
                "title": "Peak Touring Years",
                "stat": top[0],
                "stat_label": f"{top[1]} shows",
                "extra": {"by_year": top_10}
            },
            related_queries=[
                f"shows in {top[0]}",
                "career stats",
                "total shows"
            ]
        )

    def query_song_by_set(self, song_name: str, target_set: str = None, year: int = None) -> QueryResult:
        """Query for how many times a song has been played in each set, or a specific set, optionally filtered by year."""
        if song_name not in self.song_stats:
            return QueryResult(
                success=False,
                answer=f"I couldn't find '{song_name}' in the database."
            )

        # Count by set
        set_counts = {"1": 0, "2": 0, "3": 0, "E": 0}
        year_str = str(year) if year else None

        for show in self.shows:
            # Filter by year if specified
            if year_str and not show["showdate"].startswith(year_str):
                continue

            for song in show.get("songs", []):
                if song.get("song") == song_name:
                    set_name = song.get("set", "")
                    # Normalize encore sets
                    if set_name.lower().startswith("e"):
                        set_counts["E"] = set_counts.get("E", 0) + 1
                    elif set_name in set_counts:
                        set_counts[set_name] += 1
                    else:
                        set_counts[set_name] = set_counts.get(set_name, 0) + 1

        total = sum(set_counts.values())
        year_label = f" in {year}" if year else ""

        # If asking about a specific set
        if target_set:
            target_set_normalized = target_set.upper() if target_set.lower().startswith("e") else target_set
            count = set_counts.get(target_set_normalized, 0)
            pct = (count / total * 100) if total > 0 else 0

            set_label = "encore" if target_set_normalized == "E" else f"set {target_set_normalized}"

            if total == 0:
                answer = f"{song_name} was not played{year_label}."
            elif count == 0:
                answer = f"{song_name} was never played in {set_label}{year_label} (played {total} times total{year_label})."
            else:
                answer = f"{song_name} was played in {set_label} {count} times{year_label} ({pct:.1f}% of {total} total performances{year_label})."

            return QueryResult(
                success=True,
                answer=answer,
                highlight=str(count),
                card_data={
                    "type": "count",
                    "title": f"{song_name} in {set_label.title()}{year_label}",
                    "stat": count,
                    "stat_label": f"times in {set_label}",
                    "subtitle": f"{pct:.1f}% of {total} performances{year_label}"
                },
                related_queries=[
                    f"{song_name} set breakdown",
                    f"{song_name} stats",
                    f"{song_name} as opener"
                ],
                raw_data={"set_counts": set_counts, "total": total, "year": year}
            )

        # Full breakdown
        if total == 0:
            answer = f"{song_name} was not played{year_label}."
        else:
            answer = f"{song_name} set breakdown{year_label} ({total} total performances):\n\n"
            for set_name in ["1", "2", "3", "E"]:
                count = set_counts.get(set_name, 0)
                if count > 0:
                    pct = (count / total * 100) if total > 0 else 0
                    label = "Encore" if set_name == "E" else f"Set {set_name}"
                    answer += f"• {label}: {count} times ({pct:.1f}%)\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            card_data={
                "type": "stats",
                "title": f"{song_name} by Set{year_label}",
                "extra": {
                    "set_1": set_counts.get("1", 0),
                    "set_2": set_counts.get("2", 0),
                    "set_3": set_counts.get("3", 0),
                    "encore": set_counts.get("E", 0),
                    "total": total,
                    "year": year
                }
            },
            related_queries=[
                f"{song_name} in set 1",
                f"{song_name} in set 2",
                f"{song_name} as encore"
            ],
            raw_data={"set_counts": set_counts, "total": total, "year": year}
        )

    def query_rarest_songs(self) -> QueryResult:
        """Query for the rarest/least played songs."""
        sorted_songs = sorted(self.song_stats.items(), key=lambda x: x[1]["play_count"])

        # Get songs played only once
        one_timers = [(n, s) for n, s in sorted_songs if s["play_count"] == 1]

        # Get rarest that have been played more than once (more interesting)
        rare_repeats = [(n, s) for n, s in sorted_songs if s["play_count"] > 1][:20]

        answer = f"Rarest songs in Phish's catalog:\n\n"
        answer += f"Songs played only once: {len(one_timers)}\n"
        answer += f"Examples: {', '.join(n for n, s in one_timers[:5])}\n\n"
        answer += "Rarest songs played more than once:\n"
        for name, stats in rare_repeats[:10]:
            answer += f"• {name}: {stats['play_count']} times (last: {stats['last_played']})\n"

        return QueryResult(
            success=True,
            answer=answer.strip(),
            highlight=f"{len(one_timers)} one-timers",
            card_data={
                "type": "stats",
                "title": "Rarest Songs",
                "extra": {
                    "one_timer_count": len(one_timers),
                    "one_timer_examples": [n for n, s in one_timers[:10]],
                    "rare_repeats": [(n, s["play_count"]) for n, s in rare_repeats[:10]]
                }
            },
            related_queries=[
                "most played song",
                "biggest gaps",
                "total unique songs"
            ]
        )

    def _extract_date_from_query(self, query: str) -> Optional[str]:
        """Extract a date from a query like 'setlist from 12/31/1999'."""
        # Common date patterns
        patterns = [
            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY or M/D/YYYY
            r'(\d{1,2}/\d{1,2}/\d{2})',  # MM/DD/YY
        ]

        for pattern in patterns:
            match = re.search(pattern, query)
            if match:
                date_str = match.group(1)
                # Convert to YYYY-MM-DD format
                if '/' in date_str:
                    parts = date_str.split('/')
                    if len(parts[2]) == 2:
                        year = '19' + parts[2] if int(parts[2]) > 50 else '20' + parts[2]
                    else:
                        year = parts[2]
                    return f"{year}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                return date_str

        return None

    def query(self, question: str) -> QueryResult:
        """
        Main query entry point. Parses the question and routes to appropriate handler.

        This is where an LLM could be integrated for true natural language understanding.
        For now, we use pattern matching for common query types.
        """
        self.load_data()

        question_lower = question.lower().strip()

        # Extract venue if present (e.g., "longest Tweezer at MSG")
        base_question, venue = self._extract_venue_from_query(question)

        # Pattern: Career stats - "career stats", "phish stats", "overall stats"
        if any(p in question_lower for p in ["career stats", "phish stats", "overall stats", "band stats"]):
            return self.query_career_stats()

        # Pattern: Opener stats - "opener stats", "most common openers", "unique openers"
        if any(p in question_lower for p in ["opener stats", "opener statistics", "most common opener", "unique opener", "how many opener", "top opener"]):
            return self.query_opener_stats()

        # Pattern: Encore stats - "encore stats", "most common encores", "unique encores"
        if any(p in question_lower for p in ["encore stats", "encore statistics", "most common encore", "unique encore", "how many encore", "top encore"]):
            return self.query_encore_stats()

        # Pattern: Last encore with song - "last time they encored with X", "when did they last encore with"
        if any(p in question_lower for p in ["last encore", "last time they encored", "when did they last encore", "last encored with", "last played as encore"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_last_encore(song)

        # Pattern: Last opener with song - "last time they opened with X", "when did they last open with"
        if any(p in question_lower for p in ["last opener", "last open", "last time they opened", "when did they last open", "last opened with", "last played as opener"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_last_opener(song)

        # Pattern: Song as encore - "Tweezer as encore", "how many times has X been an encore"
        # Check encore BEFORE opener since "encore" is more specific
        if any(p in question_lower for p in ["as encore", "as an encore", "been an encore", "been encore", "encore with", "encored with", "times encored", "close the show", "closed the show", "closed with"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_song_as_encore(song)

        # Pattern: Song as opener - "Tweezer as opener", "how many times has X opened"
        if any(p in question_lower for p in ["as opener", "as an opener", "been an opener", "been opener", "open with", "opened with", "times opened", "open the show", "opened the show"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_song_as_opener(song)

        # Pattern: Cover stats - "cover stats", "how many covers"
        if any(p in question_lower for p in ["cover stats", "cover statistics", "how many covers"]):
            return self.query_cover_stats()

        # Pattern: Covers by artist - "Rolling Stones covers", "how many Beatles songs"
        if any(p in question_lower for p in ["covers", "songs have they covered", "songs have they played"]):
            artist = self._normalize_artist_name(question)
            if artist and artist not in ["Phish", "Trey Anastasio"]:
                return self.query_covers_by_artist(artist)

        # Pattern: Last covered artist - "when did they last cover Rolling Stones"
        if any(p in question_lower for p in ["last cover", "last time they covered", "when did they last cover"]):
            artist = self._normalize_artist_name(question)
            if artist:
                return self.query_last_covered_artist(artist)

        # Pattern: Venue show count - "how many shows at MSG", "how many times played at Alpine"
        if venue and any(p in question_lower for p in ["how many shows", "how many times", "played at", "shows at"]):
            # Check if this is asking about venue, not a song
            song = self._normalize_song_name(base_question)
            if not song:
                return self.query_venue_show_count(venue)

        # Pattern: Most played song - "most played song", "what song is played the most"
        if any(p in question_lower for p in ["most played", "played the most", "most common song", "most frequent"]):
            year = self._extract_year_from_query(question)
            if year:
                return self.query_most_played(year=year)
            return self.query_most_played()

        # Pattern: Biggest gaps - "biggest gap", "longest gap", "what song has the longest gap"
        if any(p in question_lower for p in ["biggest gap", "longest gap", "largest gap", "most overdue"]):
            return self.query_biggest_gaps()

        # Pattern: Rarest songs - "rarest songs", "least played", "what is the rarest"
        if any(p in question_lower for p in ["rarest", "least played", "most rare", "fewest times"]):
            return self.query_rarest_songs()

        # Pattern: Peak year - "what year did they play the most", "busiest year"
        if any(p in question_lower for p in ["most shows", "busiest year", "peak year", "which year"]) and "year" in question_lower:
            return self.query_peak_year()

        # Pattern: First played / debut - "when did they first play", "debut of", "first time they played"
        if any(p in question_lower for p in ["first play", "when did they first", "debut", "first time they played", "first time played"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_first_played(song)

        # Pattern: Song by set - "Carini in set 1", "Reba in the second set", "how many times in set II"
        set_one_patterns = ["set 1", "set one", "first set", "1st set", "set i"]
        set_two_patterns = ["set 2", "set two", "second set", "2nd set", "set ii"]
        set_three_patterns = ["set 3", "set three", "third set", "3rd set", "set iii"]
        all_set_patterns = set_one_patterns + set_two_patterns + set_three_patterns + ["set breakdown"]

        if any(p in question_lower for p in all_set_patterns):
            song = self._normalize_song_name(base_question)
            if song:
                # Determine which set they're asking about
                # Check in reverse order (III before II before I) to avoid substring matching issues
                target_set = None
                if any(p in question_lower for p in set_three_patterns):
                    target_set = "3"
                elif any(p in question_lower for p in set_two_patterns):
                    target_set = "2"
                elif any(p in question_lower for p in set_one_patterns):
                    target_set = "1"
                # Extract year if present
                year = self._extract_year_from_query(question)
                # If just "set breakdown" or general, target_set stays None for full breakdown
                return self.query_song_by_set(song, target_set, year)

        # Pattern: Show count - "how many shows" (check before setlist patterns)
        if any(p in question_lower for p in ["how many shows", "total shows", "number of shows", "show count"]):
            year = self._extract_year_from_query(question)
            if "since" in question_lower and year:
                return self.query_show_count(since_year=year)
            elif year:
                return self.query_show_count(in_year=year)
            else:
                return self.query_show_count()

        # Pattern: Unique songs count - "how many songs", "unique songs"
        if any(p in question_lower for p in ["how many songs", "unique songs", "total songs", "different songs"]):
            year = self._extract_year_from_query(question)
            if "since" in question_lower and year:
                return self.query_unique_songs(since_year=year)
            elif year:
                return self.query_unique_songs(in_year=year)
            else:
                return self.query_unique_songs()

        # Pattern: Setlist lookup - "setlist from 12/31/1999" or "show on 1999-12-31"
        if any(p in question_lower for p in ["setlist", "show on", "show from", "what did they play on"]):
            date = self._extract_date_from_query(question)
            if date:
                return self.query_setlist(date)
            return QueryResult(
                success=False,
                answer="I couldn't find a date in your question. Try 'setlist from 1999-12-31' or 'show on 12/31/1999'."
            )

        # Pattern: Gap query - "gap on [song]" or "how long since [song]"
        if any(p in question_lower for p in ["gap on", "gap for", "how long since", "shows since"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_gap(song)
            return QueryResult(
                success=False,
                answer="I couldn't identify which song you're asking about. Try 'gap on Harpua'."
            )

        # Pattern: "when did they last play" or "last time they played"
        if any(p in question_lower for p in ["last play", "when did they last", "last time", "most recent"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_last_played(song)
            return QueryResult(
                success=False,
                answer="I couldn't identify which song you're asking about."
            )

        # Pattern: "average [song] length" or "how long is [song] usually"
        if any(p in question_lower for p in ["average", "avg", "typical", "usually"]) and any(p in question_lower for p in ["length", "long", "duration", "min"]):
            song = self._normalize_song_name(base_question)
            if song:
                return self.query_average_duration(song)
            return QueryResult(
                success=False,
                answer="I couldn't identify which song you're asking about. Try 'average Ghost length'."
            )

        # Pattern: "longest [song]" or "what's the longest [song]"
        if "longest" in question_lower:
            # Check if asking for longest song at venue (no specific song)
            if venue and ("longest song" in question_lower or "longest jam" in question_lower):
                return self.query_longest_song_at_venue(venue)

            song = self._normalize_song_name(base_question)
            if song:
                if venue:
                    return self.query_longest_at_venue(song, venue)
                return self.query_longest(song)

            # If we have a venue but no song identified, assume they want longest song at venue
            if venue:
                return self.query_longest_song_at_venue(venue)

            return QueryResult(
                success=False,
                answer="I couldn't identify which song you're asking about. Try 'longest Tweezer' or 'longest song at MSG'."
            )

        # Pattern: "how many times" / "play count" / "times played" (with venue support)
        if any(p in question_lower for p in ["how many times", "play count", "times played", "how often"]):
            song = self._normalize_song_name(base_question)
            if song:
                if venue:
                    return self.query_play_count_at_venue(song, venue)
                return self.query_play_count(song)
            return QueryResult(
                success=False,
                answer="I couldn't identify which song you're asking about."
            )

        # Pattern: "[song] stats" or "tell me about [song]"
        if any(p in question_lower for p in ["stats", "statistics", "tell me about", "info on"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_song_stats(song)

        # Default: Try to identify a song and give stats
        song = self._normalize_song_name(question)
        if song:
            return self.query_song_stats(song)

        return QueryResult(
            success=False,
            answer="I'm not sure how to answer that. Try asking about:\n• Longest version of a song ('longest Tweezer')\n• Play counts ('how many times has Ghost been played')\n• Song stats ('Tweezer stats')\n• Gap on a song ('gap on Harpua')\n• Setlists ('setlist from 1999-12-31')\n• Average length ('average Ghost length')",
            related_queries=["longest Tweezer", "Ghost play count", "gap on Harpua", "setlist from 1999-12-31"]
        )


def main():
    """Interactive query mode for testing."""
    engine = PhishStatsEngine()
    engine.load_data()

    print("\n" + "=" * 60)
    print("PhishStats Query Engine")
    print("Type a question or 'quit' to exit")
    print("=" * 60 + "\n")

    while True:
        try:
            question = input("Ask: ").strip()
            if question.lower() in ["quit", "exit", "q"]:
                break

            result = engine.query(question)

            print(f"\n{result.answer}")

            if result.highlight:
                print(f"\n  >>> {result.highlight}")

            if result.related_queries:
                print(f"\n  Related: {', '.join(result.related_queries)}")

            print()

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

    print("Goodbye!")


if __name__ == "__main__":
    main()
