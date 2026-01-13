#!/usr/bin/env python3
"""
Phish Stats - Entry Point

Quick way to run analyses.

Usage:
    python run.py                    # Run tour break analysis (default)
    python run.py --years 2015-2024  # Specify year range
    python run.py --song "Tweezer"   # Analyze specific song
"""

import argparse
import sys
import os

# Ensure we can import from src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data_loader import PhishDataLoader
from src.tour_gap_analysis import TourGapAnalyzer


def main():
    parser = argparse.ArgumentParser(description='Phish Statistics Analysis')
    parser.add_argument('--years', type=str, default='2010-2024',
                        help='Year range (e.g., 2015-2024)')
    parser.add_argument('--song', type=str, default=None,
                        help='Analyze specific song')
    parser.add_argument('--refresh', action='store_true',
                        help='Force refresh data from API')

    args = parser.parse_args()

    # Parse years
    start_year, end_year = map(int, args.years.split('-'))

    print("=" * 50)
    print("  PHISH STATS ANALYZER")
    print("=" * 50)

    # Load data
    print(f"\nLoading data for {start_year}-{end_year}...")
    loader = PhishDataLoader()
    loader.load_all_data(start_year=start_year, end_year=end_year,
                         force_refresh=args.refresh)

    print(f"Loaded {len(loader.shows)} shows, {len(loader.songs)} songs")

    analyzer = TourGapAnalyzer(loader)

    if args.song:
        # Analyze specific song
        if args.song not in loader.songs:
            print(f"Song '{args.song}' not found!")
            print("\nDid you mean one of these?")
            matches = [s for s in loader.songs.keys()
                       if args.song.lower() in s.lower()][:5]
            for m in matches:
                print(f"  - {m}")
            return

        comp = analyzer.analyze_song(args.song)
        print(f"\n{args.song} Analysis:")
        print(f"  Total performances: {loader.songs[args.song].times_played}")
        print(f"\n  Intra-tour gaps: {len(comp.intra_tour_gaps)} observations")
        print(f"    Mean: {comp.intra_mean_gap:.1f} shows")
        print(f"    Median: {comp.intra_median_gap:.1f} shows")
        print(f"\n  Cross-tour gaps: {len(comp.cross_tour_gaps)} observations")
        print(f"    Mean: {comp.cross_mean_gap:.1f} shows")
        print(f"    Median: {comp.cross_median_gap:.1f} shows")
        print(f"\n  Mann-Whitney p-value: {comp.mannwhitney_pvalue:.4f}")

    else:
        # Run full analysis
        from analysis.run_tour_break_analysis import run_analysis
        run_analysis(start_year=start_year, end_year=end_year)


if __name__ == "__main__":
    main()
