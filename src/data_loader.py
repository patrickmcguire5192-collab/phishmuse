"""
Data Loader for Phish Statistics

Transforms raw API data into clean data models for analysis.
"""

import pickle
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from tqdm import tqdm

from .api import PhishNetAPI
from .models import (
    Show, Song, SongPerformance, Tour, TourBreak,
    parse_date
)


def safe_int(value, default=0):
    """Safely convert a value to int, returning default if not possible."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_bool_int(value, default=False):
    """Safely convert a value to bool via int."""
    try:
        return bool(int(value))
    except (ValueError, TypeError):
        return default


class PhishDataLoader:
    """Load and structure Phish data for analysis."""

    def __init__(self, api: Optional[PhishNetAPI] = None, data_dir: str = "data"):
        self.api = api or PhishNetAPI()
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Loaded data
        self.shows: List[Show] = []
        self.songs: Dict[str, Song] = {}
        self.tours: Dict[int, Tour] = {}
        self.tour_breaks: List[TourBreak] = []

    def load_all_data(self, start_year: int = 1983, end_year: int = None,
                      force_refresh: bool = False) -> 'PhishDataLoader':
        """Load all data from API and structure it."""

        cache_file = self.data_dir / f"phish_data_{start_year}_{end_year or 'current'}.pkl"

        # Try to load from cache
        if not force_refresh and cache_file.exists():
            print(f"Loading from cache: {cache_file}")
            with open(cache_file, 'rb') as f:
                cached = pickle.load(f)
                self.shows = cached['shows']
                self.songs = cached['songs']
                self.tours = cached['tours']
                self.tour_breaks = cached['tour_breaks']
                print(f"Loaded {len(self.shows)} shows, {len(self.songs)} songs")
                return self

        end_year = end_year or datetime.now().year

        # Fetch raw data
        print("Fetching setlist data from API...")
        raw_setlists = self.api.get_all_setlists(start_year, end_year)

        # Filter to Phish only (artistid = 1)
        phish_setlists = [s for s in raw_setlists if s.get('artistid') == 1]
        print(f"Filtered to {len(phish_setlists)} Phish setlist entries")

        # Build shows
        self._build_shows(phish_setlists)

        # Build song catalog
        self._build_songs()

        # Identify tours and breaks
        self._identify_tours_and_breaks()

        # Cache the processed data
        with open(cache_file, 'wb') as f:
            pickle.dump({
                'shows': self.shows,
                'songs': self.songs,
                'tours': self.tours,
                'tour_breaks': self.tour_breaks
            }, f)
        print(f"Cached processed data to {cache_file}")

        return self

    def _build_shows(self, raw_setlists: List[Dict]):
        """Build Show objects from raw setlist data."""
        show_map: Dict[int, Show] = {}

        for entry in tqdm(raw_setlists, desc="Building shows"):
            show_id = entry['showid']

            if show_id not in show_map:
                show_map[show_id] = Show(
                    show_id=show_id,
                    show_date=parse_date(entry['showdate']),
                    venue_name=entry.get('venue', 'Unknown'),
                    venue_id=entry.get('venueid', 0),
                    city=entry.get('city', ''),
                    state=entry.get('state', ''),
                    country=entry.get('country', 'USA'),
                    tour_id=entry.get('tourid'),
                    tour_name=entry.get('tourname')
                )

            # Create song performance
            perf = SongPerformance(
                song_name=entry['song'],
                song_slug=entry.get('slug', entry['song'].lower().replace(' ', '-')),
                show_id=show_id,
                show_date=parse_date(entry['showdate']),
                position=safe_int(entry.get('position'), 0),
                set_type=entry.get('set', '1'),
                gap=safe_int(entry.get('gap'), 0),
                duration_seconds=safe_int(entry.get('tracktime')) if entry.get('tracktime') else None,
                is_jamchart=safe_bool_int(entry.get('isjamchart')),
                venue_id=entry.get('venueid'),
                tour_id=entry.get('tourid'),
                notes=entry.get('footnote')
            )

            show_map[show_id].songs.append(perf)

        # Sort shows by date
        self.shows = sorted(show_map.values(), key=lambda x: x.show_date)
        print(f"Built {len(self.shows)} shows")

    def _build_songs(self):
        """Build Song catalog from show data."""
        song_perfs: Dict[str, List[SongPerformance]] = defaultdict(list)

        for show in self.shows:
            for perf in show.songs:
                song_perfs[perf.song_name].append(perf)

        for song_name, perfs in song_perfs.items():
            sorted_perfs = sorted(perfs, key=lambda x: x.show_date)
            slug = sorted_perfs[0].song_slug if sorted_perfs else song_name.lower().replace(' ', '-')

            self.songs[song_name] = Song(
                name=song_name,
                slug=slug,
                times_played=len(perfs),
                debut_date=sorted_perfs[0].show_date if sorted_perfs else None,
                last_played=sorted_perfs[-1].show_date if sorted_perfs else None,
                performances=sorted_perfs
            )

        print(f"Cataloged {len(self.songs)} unique songs")

    def _identify_tours_and_breaks(self, break_threshold_days: int = 14):
        """
        Identify tours and breaks between them.

        A 'tour break' is defined as a gap of more than break_threshold_days
        between consecutive shows.
        """
        if len(self.shows) < 2:
            return

        # Group shows by tour_id from API (if available)
        tour_shows: Dict[int, List[Show]] = defaultdict(list)
        for show in self.shows:
            if show.tour_id:
                tour_shows[show.tour_id].append(show)

        # Build Tour objects
        for tour_id, shows in tour_shows.items():
            sorted_shows = sorted(shows, key=lambda x: x.show_date)
            if sorted_shows:
                tour_name = sorted_shows[0].tour_name or f"Tour {tour_id}"
                self.tours[tour_id] = Tour(
                    tour_id=tour_id,
                    name=tour_name,
                    start_date=sorted_shows[0].show_date,
                    end_date=sorted_shows[-1].show_date,
                    shows=sorted_shows
                )

        # Identify tour breaks (gaps > threshold between consecutive shows)
        for i in range(1, len(self.shows)):
            prev_show = self.shows[i - 1]
            curr_show = self.shows[i]
            days_between = (curr_show.show_date - prev_show.show_date).days

            if days_between > break_threshold_days:
                # Count shows in previous and next "run"
                prev_tour_shows = self._count_consecutive_shows_before(i - 1, break_threshold_days)
                next_tour_shows = self._count_consecutive_shows_after(i, break_threshold_days)

                self.tour_breaks.append(TourBreak(
                    end_show=prev_show,
                    start_show=curr_show,
                    days_between=days_between,
                    shows_in_previous_tour=prev_tour_shows,
                    shows_in_next_tour=next_tour_shows
                ))

        print(f"Identified {len(self.tours)} tours and {len(self.tour_breaks)} tour breaks")

    def _count_consecutive_shows_before(self, idx: int, threshold: int) -> int:
        """Count shows in the consecutive run before this index."""
        count = 1
        for i in range(idx, 0, -1):
            days = (self.shows[i].show_date - self.shows[i - 1].show_date).days
            if days > threshold:
                break
            count += 1
        return count

    def _count_consecutive_shows_after(self, idx: int, threshold: int) -> int:
        """Count shows in the consecutive run after this index."""
        count = 1
        for i in range(idx, len(self.shows) - 1):
            days = (self.shows[i + 1].show_date - self.shows[i].show_date).days
            if days > threshold:
                break
            count += 1
        return count

    def get_show_index(self, show: Show) -> int:
        """Get the chronological index of a show."""
        for i, s in enumerate(self.shows):
            if s.show_id == show.show_id:
                return i
        return -1

    def is_tour_break_between(self, show1: Show, show2: Show,
                               min_days: int = 14) -> Tuple[bool, int]:
        """
        Check if there's a tour break between two shows.
        Returns (is_break, days_between).
        """
        days = abs((show2.show_date - show1.show_date).days)
        return (days > min_days, days)

    def get_performances_with_context(self, song_name: str) -> List[Dict]:
        """
        Get all performances of a song with tour break context.

        Returns list of dicts with:
        - performance: SongPerformance
        - prev_performance: SongPerformance or None
        - gap_shows: int (shows between performances)
        - gap_days: int (days between performances)
        - crosses_tour_break: bool
        - tour_break_days: int (if crosses_tour_break)
        """
        if song_name not in self.songs:
            return []

        song = self.songs[song_name]
        perfs = sorted(song.performances, key=lambda x: x.show_date)

        results = []
        for i, perf in enumerate(perfs):
            context = {
                'performance': perf,
                'prev_performance': None,
                'gap_shows': 0,
                'gap_days': 0,
                'crosses_tour_break': False,
                'tour_break_days': 0,
                'show_index': self._get_show_index_by_id(perf.show_id)
            }

            if i > 0:
                prev = perfs[i - 1]
                context['prev_performance'] = prev
                context['gap_shows'] = perf.gap  # From API
                context['gap_days'] = (perf.show_date - prev.show_date).days

                # Check for tour break
                is_break, days = self.is_tour_break_between(
                    self._get_show_by_id(prev.show_id),
                    self._get_show_by_id(perf.show_id)
                )
                context['crosses_tour_break'] = is_break
                if is_break:
                    context['tour_break_days'] = days

            results.append(context)

        return results

    def _get_show_by_id(self, show_id: int) -> Optional[Show]:
        """Get a show by its ID."""
        for show in self.shows:
            if show.show_id == show_id:
                return show
        return None

    def _get_show_index_by_id(self, show_id: int) -> int:
        """Get chronological index of a show by ID."""
        for i, show in enumerate(self.shows):
            if show.show_id == show_id:
                return i
        return -1


if __name__ == "__main__":
    # Quick test
    loader = PhishDataLoader()
    loader.load_all_data(start_year=2015, end_year=2024)

    print(f"\nLoaded {len(loader.shows)} shows")
    print(f"Cataloged {len(loader.songs)} songs")
    print(f"Found {len(loader.tour_breaks)} tour breaks")

    # Check a popular song
    if "Tweezer" in loader.songs:
        tweezer = loader.songs["Tweezer"]
        print(f"\nTweezer: {tweezer.times_played} performances")
        print(f"Debut: {tweezer.debut_date}")
        print(f"Last: {tweezer.last_played}")
