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

    def __init__(self):
        self.data_loaded = False
        self.song_stats = {}
        self.duration_stats = {}
        self.venue_stats = {}
        self.shows = []

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

            self.data_loaded = True
            print(f"  Loaded {len(self.song_stats)} songs, {len(self.duration_stats)} with durations, {len(self.shows)} shows")

        except FileNotFoundError as e:
            print(f"  Warning: Some data files not found: {e}")
            print("  Run 'python scripts/refresh_data.py --full --durations' to populate data")

    def _normalize_song_name(self, query: str) -> Optional[str]:
        """Try to match a song name from the query."""
        query_lower = query.lower()

        # Direct matches
        for song in self.song_stats.keys():
            if song.lower() in query_lower:
                return song

        # Common abbreviations/aliases
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
            if alias in query_lower:
                return full_name

        return None

    def _song_to_slug(self, song_name: str) -> str:
        """Convert song name to Phish.in slug format."""
        slug = song_name.lower()
        slug = re.sub(r"[''']", "", slug)  # Remove apostrophes
        slug = re.sub(r"[^a-z0-9]+", "-", slug)  # Replace non-alphanumeric with dashes
        slug = slug.strip("-")
        return slug

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

    def query(self, question: str) -> QueryResult:
        """
        Main query entry point. Parses the question and routes to appropriate handler.

        This is where an LLM could be integrated for true natural language understanding.
        For now, we use pattern matching for common query types.
        """
        self.load_data()

        question_lower = question.lower().strip()

        # Pattern: "longest [song]" or "what's the longest [song]"
        if "longest" in question_lower:
            song = self._normalize_song_name(question)
            if song:
                return self.query_longest(song)
            return QueryResult(
                success=False,
                answer="I couldn't identify which song you're asking about. Try 'longest Tweezer' or 'longest Ghost'."
            )

        # Pattern: "how many times" / "play count" / "times played"
        if any(p in question_lower for p in ["how many times", "play count", "times played", "how often"]):
            song = self._normalize_song_name(question)
            if song:
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
            answer="I'm not sure how to answer that. Try asking about:\n• Longest version of a song ('longest Tweezer')\n• Play counts ('how many times has Ghost been played')\n• Song stats ('Tweezer stats')",
            related_queries=["longest Tweezer", "Ghost play count", "YEM stats"]
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
