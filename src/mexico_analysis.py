"""
Mexico Analysis Module

Statistical analysis of Phish performances at Mexico (Riviera Maya / Cancun).
Used to generate prop bets for PhanDuel.

Key findings:
- Sand has been played at EVERY Mexico run (8/8)
- Wilson has NEVER been played in Mexico (0/40 shows)
- Mexican Cousin played only ONCE (2016) - the irony!
- Average 3.5 water-themed songs per run (using 9-song OBVIOUS list)
"""

from collections import Counter, defaultdict
from typing import List, Dict, Set
import numpy as np

from .api import PhishNetAPI


# The official water-themed song list (OBVIOUS tier only)
# These are songs with water/ocean/sea/wave/sand explicitly in the title
WATER_SONGS_OBVIOUS = {
    'Sand',
    'A Song I Heard the Ocean Sing',
    'Wading in the Velvet Sea',
    'Beneath a Sea of Stars Part 1',
    'Ruby Waves',
    'Waves',
    'A Wave of Hope',
    'The Ocean',
    'Sea and Sand',
}

# Extended list (includes CLEAR tier - defensible water connections)
WATER_SONGS_EXTENDED = WATER_SONGS_OBVIOUS | {
    'Bathtub Gin',      # Bathtub = water vessel
    'Prince Caspian',   # Narnia sea voyage, "wash upon the shore"
    'Down with Disease', # "drift upon the sea" lyric
    'The Wedge',        # Named after Newport surfing spot
    'Shipwreck',        # Nautical
    'Foam',             # Ocean foam / surf
}

# Longshot bets - songs with thematic connection to Mexico
MEXICO_LONGSHOTS = {
    'Mexican Cousin': {
        'times_played': 1,
        'total_shows': 40,
        'last_played': '2016-01-15',
        'note': 'Only played once in Mexico history - the irony!'
    },
    'Wilson': {
        'times_played': 0,
        'total_shows': 40,
        'last_played': None,
        'note': 'Never been played in Mexico - would be historic debut'
    },
    'Llama': {
        'times_played': 0,
        'total_shows': 40,
        'last_played': None,
        'note': 'Another bustout candidate for Mexico'
    },
    'Fluffhead': {
        'times_played': 1,
        'total_shows': 40,
        'last_played': '2017-01-14',
        'note': 'Beach Fluffhead would be epic'
    }
}


class MexicoAnalyzer:
    """Analyze Phish Mexico performances for prop bet generation."""

    def __init__(self, api: PhishNetAPI = None):
        self.api = api or PhishNetAPI()
        self.mexico_shows = []
        self.runs_by_year = {}

    def load_mexico_data(self, start_year: int = 2016, end_year: int = 2026) -> 'MexicoAnalyzer':
        """Load all Mexico show data."""
        all_shows = []
        for year in range(start_year, end_year):
            shows = self.api.get_shows_by_year(year)
            all_shows.extend(shows)

        self.mexico_shows = sorted(
            [s for s in all_shows if s.get('country') == 'Mexico'],
            key=lambda x: x['showdate']
        )

        # Group by year (each year = one run)
        self.runs_by_year = defaultdict(list)
        for show in self.mexico_shows:
            year = show['showdate'][:4]
            self.runs_by_year[year].append(show)

        print(f"Loaded {len(self.mexico_shows)} Mexico shows across {len(self.runs_by_year)} runs")
        return self

    def get_setlist(self, date: str) -> List[str]:
        """Get song names for a specific show date."""
        setlist = self.api.get_setlist_by_date(date)
        return [e['song'] for e in setlist if e.get('artistid') == 1]

    def analyze_water_songs_per_show(self, water_list: Set[str] = None) -> Dict:
        """Analyze water song frequency per show."""
        water_list = water_list or WATER_SONGS_OBVIOUS

        water_per_show = []
        for show in self.mexico_shows:
            songs = set(self.get_setlist(show['showdate']))
            water_count = len(songs & water_list)
            water_per_show.append(water_count)

        over_1_5 = sum(1 for x in water_per_show if x > 1.5)

        return {
            'total_shows': len(water_per_show),
            'water_songs_used': len(water_list),
            'mean': np.mean(water_per_show),
            'median': np.median(water_per_show),
            'min': min(water_per_show),
            'max': max(water_per_show),
            'over_1_5_count': over_1_5,
            'over_1_5_pct': 100 * over_1_5 / len(water_per_show),
            'under_1_5_pct': 100 * (len(water_per_show) - over_1_5) / len(water_per_show),
            'distribution': dict(Counter(water_per_show))
        }

    def analyze_water_songs_per_run(self, water_list: Set[str] = None) -> Dict:
        """Analyze water song frequency per Mexico run (year)."""
        water_list = water_list or WATER_SONGS_OBVIOUS

        run_stats = []
        for year, shows in sorted(self.runs_by_year.items()):
            all_water_this_run = set()
            for show in shows:
                songs = set(self.get_setlist(show['showdate']))
                all_water_this_run.update(songs & water_list)

            run_stats.append({
                'year': year,
                'num_shows': len(shows),
                'water_songs': len(all_water_this_run),
                'which': sorted(all_water_this_run)
            })

        water_counts = [r['water_songs'] for r in run_stats]
        over_4_5 = sum(1 for x in water_counts if x > 4.5)

        return {
            'total_runs': len(run_stats),
            'water_songs_used': len(water_list),
            'mean': np.mean(water_counts),
            'median': np.median(water_counts),
            'min': min(water_counts),
            'max': max(water_counts),
            'over_4_5_count': over_4_5,
            'over_4_5_pct': 100 * over_4_5 / len(water_counts),
            'under_4_5_pct': 100 * (len(water_counts) - over_4_5) / len(water_counts),
            'by_year': run_stats
        }

    def get_top_mexico_songs(self, n: int = 20) -> List[tuple]:
        """Get the most played songs at Mexico shows."""
        all_songs = []
        for show in self.mexico_shows:
            all_songs.extend(self.get_setlist(show['showdate']))

        return Counter(all_songs).most_common(n)

    def get_never_played_in_mexico(self, min_overall_plays: int = 20) -> List[str]:
        """Get popular songs that have never been played in Mexico."""
        mexico_songs = set()
        for show in self.mexico_shows:
            mexico_songs.update(self.get_setlist(show['showdate']))

        # Get recent popular songs
        recent = self.api.get_setlists_by_year(2023)
        recent_counts = Counter(e['song'] for e in recent if e.get('artistid') == 1)

        never_in_mexico = []
        for song, count in recent_counts.most_common(100):
            if song not in mexico_songs and count >= min_overall_plays:
                never_in_mexico.append((song, count))

        return never_in_mexico

    def generate_prop_bets(self) -> Dict:
        """Generate recommended prop bets for Mexico shows."""
        per_show = self.analyze_water_songs_per_show()
        per_run = self.analyze_water_songs_per_run()

        return {
            'per_show_bets': {
                'over_1_5_water': {
                    'description': 'Over 1.5 water-themed songs (per show)',
                    'probability': per_show['over_1_5_pct'],
                    'fair_odds': 100 / per_show['over_1_5_pct'] if per_show['over_1_5_pct'] > 0 else 999,
                    'recommended_odds': 2.0,
                    'water_songs': sorted(WATER_SONGS_OBVIOUS)
                },
                'under_1_5_water': {
                    'description': 'Under 1.5 water-themed songs (per show)',
                    'probability': per_show['under_1_5_pct'],
                    'fair_odds': 100 / per_show['under_1_5_pct'] if per_show['under_1_5_pct'] > 0 else 999,
                    'recommended_odds': 2.0
                }
            },
            'per_run_bets': {
                'over_4_5_water': {
                    'description': 'Over 4.5 unique water songs for entire run',
                    'probability': per_run['over_4_5_pct'],
                    'fair_odds': 100 / per_run['over_4_5_pct'] if per_run['over_4_5_pct'] > 0 else 999,
                    'recommended_odds': 2.0
                },
                'under_4_5_water': {
                    'description': 'Under 4.5 unique water songs for entire run',
                    'probability': per_run['under_4_5_pct'],
                    'fair_odds': 100 / per_run['under_4_5_pct'] if per_run['under_4_5_pct'] > 0 else 999,
                    'recommended_odds': 2.0
                }
            },
            'longshot_bets': {
                'mexican_cousin': {
                    'description': 'Mexican Cousin played',
                    'probability': 2.5,  # 1/40 shows
                    'fair_odds': 40,
                    'recommended_odds': 40,
                    'note': 'Only played once in Mexico (2016) - ultimate irony bet!'
                },
                'wilson': {
                    'description': 'Wilson played',
                    'probability': 0,  # Never played
                    'fair_odds': 50,
                    'recommended_odds': 50,
                    'note': 'Never been played in Mexico - historic debut potential'
                }
            }
        }


def run_mexico_analysis():
    """Run full Mexico analysis and print results."""
    analyzer = MexicoAnalyzer()
    analyzer.load_mexico_data()

    print("\n" + "="*60)
    print("  TOP 20 SONGS AT PHISH MEXICO")
    print("="*60)
    for i, (song, count) in enumerate(analyzer.get_top_mexico_songs(20), 1):
        print(f"  {i:2}. {song}: {count}")

    print("\n" + "="*60)
    print("  WATER SONGS PER SHOW (OBVIOUS 9-SONG LIST)")
    print("="*60)
    per_show = analyzer.analyze_water_songs_per_show()
    print(f"  Mean: {per_show['mean']:.2f}")
    print(f"  Over 1.5: {per_show['over_1_5_pct']:.0f}%")
    print(f"  Under 1.5: {per_show['under_1_5_pct']:.0f}%")

    print("\n" + "="*60)
    print("  WATER SONGS PER RUN")
    print("="*60)
    per_run = analyzer.analyze_water_songs_per_run()
    print(f"  Mean: {per_run['mean']:.2f}")
    print(f"  Over 4.5: {per_run['over_4_5_pct']:.0f}%")
    print(f"  Under 4.5: {per_run['under_4_5_pct']:.0f}%")

    for r in per_run['by_year']:
        print(f"  {r['year']}: {r['water_songs']} songs - {', '.join(r['which'])}")

    print("\n" + "="*60)
    print("  RECOMMENDED PROP BETS")
    print("="*60)
    bets = analyzer.generate_prop_bets()

    print("\n  Per-Show Bets:")
    for key, bet in bets['per_show_bets'].items():
        print(f"    {bet['description']}: {bet['recommended_odds']}x")

    print("\n  Per-Run Bets:")
    for key, bet in bets['per_run_bets'].items():
        print(f"    {bet['description']}: {bet['recommended_odds']}x")

    print("\n  Longshot Bets:")
    for key, bet in bets['longshot_bets'].items():
        print(f"    {bet['description']}: {bet['recommended_odds']}x - {bet['note']}")

    return analyzer


if __name__ == "__main__":
    run_mexico_analysis()
