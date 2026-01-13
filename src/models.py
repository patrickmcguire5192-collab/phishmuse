"""
Data Models for Phish Statistics Analysis

Clean data structures that make analysis straightforward.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional, Dict
from enum import Enum


class SetType(Enum):
    SET1 = "1"
    SET2 = "2"
    SET3 = "3"
    ENCORE = "e"
    ENCORE2 = "e2"
    SOUNDCHECK = "s"


@dataclass
class SongPerformance:
    """A single performance of a song at a show."""
    song_name: str
    song_slug: str
    show_id: int
    show_date: date
    position: int
    set_type: str
    gap: int  # Shows since last played (from API)
    duration_seconds: Optional[int] = None
    is_jamchart: bool = False
    venue_id: Optional[int] = None
    tour_id: Optional[int] = None
    notes: Optional[str] = None

    @property
    def is_opener(self) -> bool:
        return self.set_type == "1" and self.position == 1

    @property
    def is_encore(self) -> bool:
        return self.set_type in ("e", "e2", "3")

    @property
    def is_set1(self) -> bool:
        return self.set_type == "1"

    @property
    def is_set2(self) -> bool:
        return self.set_type == "2"


@dataclass
class Show:
    """A single Phish show."""
    show_id: int
    show_date: date
    venue_name: str
    venue_id: int
    city: str
    state: str
    country: str
    tour_id: Optional[int] = None
    tour_name: Optional[str] = None
    songs: List[SongPerformance] = field(default_factory=list)

    @property
    def opener(self) -> Optional[SongPerformance]:
        for song in self.songs:
            if song.is_opener:
                return song
        return None

    @property
    def encores(self) -> List[SongPerformance]:
        return [s for s in self.songs if s.is_encore]

    @property
    def set1_songs(self) -> List[SongPerformance]:
        return sorted([s for s in self.songs if s.is_set1], key=lambda x: x.position)

    @property
    def set2_songs(self) -> List[SongPerformance]:
        return sorted([s for s in self.songs if s.is_set2], key=lambda x: x.position)

    def get_song_by_name(self, name: str) -> Optional[SongPerformance]:
        for song in self.songs:
            if song.song_name.lower() == name.lower():
                return song
        return None


@dataclass
class Tour:
    """A tour/run of shows."""
    tour_id: int
    name: str
    start_date: date
    end_date: date
    shows: List[Show] = field(default_factory=list)

    @property
    def num_shows(self) -> int:
        return len(self.shows)

    @property
    def duration_days(self) -> int:
        return (self.end_date - self.start_date).days


@dataclass
class Song:
    """A song in the Phish catalog."""
    name: str
    slug: str
    times_played: int = 0
    debut_date: Optional[date] = None
    last_played: Optional[date] = None
    gap: int = 0  # Current gap
    is_original: bool = True
    performances: List[SongPerformance] = field(default_factory=list)

    def gaps_between_performances(self) -> List[int]:
        """Calculate gaps between all performances of this song."""
        if len(self.performances) < 2:
            return []

        sorted_perfs = sorted(self.performances, key=lambda x: x.show_date)
        gaps = []
        for i in range(1, len(sorted_perfs)):
            # This gives us the gap that existed BEFORE each performance
            gaps.append(sorted_perfs[i].gap)
        return gaps


@dataclass
class TourBreak:
    """Represents a break between tours."""
    end_show: Show
    start_show: Show
    days_between: int
    shows_in_previous_tour: int
    shows_in_next_tour: int

    @property
    def is_significant_break(self) -> bool:
        """A significant break is > 30 days (roughly a month off)."""
        return self.days_between > 30


@dataclass
class SongGapAnalysis:
    """Analysis of song gaps with tour context."""
    song_name: str
    performances: List[SongPerformance]

    # Gap statistics
    all_gaps: List[int] = field(default_factory=list)
    intra_tour_gaps: List[int] = field(default_factory=list)  # Within same tour
    cross_tour_gaps: List[int] = field(default_factory=list)  # Across tour breaks

    # Additional context
    gaps_with_context: List[Dict] = field(default_factory=list)  # Full context for each gap


def parse_date(date_str: str) -> date:
    """Parse a date string from the API."""
    if isinstance(date_str, date):
        return date_str
    return datetime.strptime(date_str, "%Y-%m-%d").date()
