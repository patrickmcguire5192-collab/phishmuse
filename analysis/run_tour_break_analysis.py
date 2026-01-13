#!/usr/bin/env python3
"""
Tour Break Analysis for PhanDuel Odds Engine

This script investigates whether tour breaks affect song gap patterns.

Key question: Should the recency penalty be the same whether a song was played:
- 1 night ago (same tour/run)
- 3 months ago (different tour)

Run with: python -m analysis.run_tour_break_analysis
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from src.data_loader import PhishDataLoader
from src.tour_gap_analysis import TourGapAnalyzer, to_dataframe


def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)


def run_analysis(start_year: int = 2010, end_year: int = 2024):
    """Run the full tour break analysis."""

    print_header("PHISH TOUR BREAK ANALYSIS")
    print(f"\nAnalyzing data from {start_year} to {end_year}")
    print("This may take a moment on first run (fetching API data)...\n")

    # Load data
    loader = PhishDataLoader()
    loader.load_all_data(start_year=start_year, end_year=end_year)

    print(f"\nLoaded {len(loader.shows)} shows")
    print(f"Cataloged {len(loader.songs)} unique songs")
    print(f"Identified {len(loader.tour_breaks)} tour breaks (>14 day gaps)")

    # Initialize analyzer
    analyzer = TourGapAnalyzer(loader, tour_break_threshold_days=14)

    # ==================== AGGREGATE ANALYSIS ====================
    print_header("AGGREGATE GAP ANALYSIS")

    agg = analyzer.aggregate_analysis(min_performances=30)

    print(f"\nSongs analyzed (played 30+ times): {agg['total_songs_analyzed']}")
    print(f"Total intra-tour gap observations: {agg['total_intra_tour_gaps']:,}")
    print(f"Total cross-tour gap observations: {agg['total_cross_tour_gaps']:,}")

    print("\n--- Intra-Tour Gaps (within same tour/run) ---")
    print(f"  Mean gap:   {agg['intra_tour']['mean_gap_shows']:.2f} shows")
    print(f"  Median gap: {agg['intra_tour']['median_gap_shows']:.1f} shows")
    print(f"  Std dev:    {agg['intra_tour']['std_gap_shows']:.2f} shows")
    print(f"  Mean days:  {agg['intra_tour']['mean_gap_days']:.1f} days")

    print("\n--- Cross-Tour Gaps (after tour break) ---")
    print(f"  Mean gap:   {agg['cross_tour']['mean_gap_shows']:.2f} shows")
    print(f"  Median gap: {agg['cross_tour']['median_gap_shows']:.1f} shows")
    print(f"  Std dev:    {agg['cross_tour']['std_gap_shows']:.2f} shows")
    print(f"  Mean days:  {agg['cross_tour']['mean_gap_days']:.1f} days")

    print("\n--- Statistical Significance ---")
    print(f"  T-test p-value:       {agg.get('ttest_pvalue', 'N/A'):.4f}")
    print(f"  Mann-Whitney p-value: {agg.get('mannwhitney_pvalue', 'N/A'):.4f}")

    if agg.get('mannwhitney_pvalue', 1) < 0.05:
        print("  ** STATISTICALLY SIGNIFICANT at p < 0.05 **")
    else:
        print("  (Not statistically significant at p < 0.05)")

    # ==================== REPEAT PATTERN ANALYSIS ====================
    print_header("SONG REPEAT PATTERN ANALYSIS")

    for window in [3, 5, 10]:
        repeat_analysis = analyzer.analyze_repeat_patterns(n_shows_window=window)

        print(f"\n--- Within {window} shows ---")
        print(f"  Same tour context:")
        print(f"    Repeat probability: {repeat_analysis['same_tour']['repeat_probability']*100:.1f}%")
        print(f"    Avg gap when repeated: {repeat_analysis['same_tour']['avg_gap_when_repeated']:.2f} shows")

        print(f"  Tour start context:")
        print(f"    Repeat probability: {repeat_analysis['tour_start']['repeat_probability']*100:.1f}%")
        print(f"    Avg gap when repeated: {repeat_analysis['tour_start']['avg_gap_when_repeated']:.2f} shows")

        if 'chi2_pvalue' in repeat_analysis:
            print(f"  Chi-square p-value: {repeat_analysis['chi2_pvalue']:.4f}")

    # ==================== RECOMMENDATION ====================
    print_header("RECOMMENDATION FOR PHANDUEL ODDS ENGINE")

    rec = analyzer.calculate_optimal_penalty_adjustment()

    print(f"\nGap Ratio (cross-tour / intra-tour): {rec['gap_ratio']:.2f}")
    print(f"\n{rec['interpretation']}")
    print(f"\nRecommended penalty adjustment factor: {rec['recommended_adjustment']:.3f}")

    if abs(rec['recommended_adjustment'] - 1.0) > 0.1:
        print("\n>>> ACTIONABLE: Consider implementing tour-break-aware penalties <<<")
        print(f"    For songs played before a tour break, multiply recency penalty by {rec['recommended_adjustment']:.2f}")
    else:
        print("\n>>> Current uniform penalty approach appears appropriate <<<")

    # ==================== PER-SONG BREAKDOWN ====================
    print_header("PER-SONG ANALYSIS (Top 20 by statistical significance)")

    comparisons = analyzer.analyze_all_songs(min_performances=50)
    df = to_dataframe(comparisons)

    # Sort by statistical significance
    df_sorted = df.sort_values('mannwhitney_p').head(20)

    print("\n" + df_sorted[['song', 'intra_mean', 'cross_mean', 'gap_ratio', 'mannwhitney_p']].to_string(index=False))

    # ==================== SPECIFIC SONG DEEP DIVES ====================
    print_header("DEEP DIVE: KEY JAM VEHICLES")

    jam_vehicles = ['Tweezer', 'Down with Disease', 'Ghost', 'Light', 'Ruby Waves']

    for song in jam_vehicles:
        if song in loader.songs:
            comp = analyzer.analyze_song(song)
            print(f"\n{song}:")
            print(f"  Intra-tour: {len(comp.intra_tour_gaps)} obs, mean={comp.intra_mean_gap:.1f} shows")
            print(f"  Cross-tour: {len(comp.cross_tour_gaps)} obs, mean={comp.cross_mean_gap:.1f} shows")
            if comp.intra_mean_gap > 0:
                ratio = comp.cross_mean_gap / comp.intra_mean_gap
                print(f"  Gap ratio: {ratio:.2f}x")
            print(f"  Mann-Whitney p: {comp.mannwhitney_pvalue:.4f}")

    # ==================== SAVE RESULTS ====================
    print_header("SAVING RESULTS")

    # Save detailed comparison data
    output_dir = "data/analysis_results"
    os.makedirs(output_dir, exist_ok=True)

    full_df = to_dataframe(comparisons)
    full_df.to_csv(f"{output_dir}/tour_gap_analysis.csv", index=False)
    print(f"Saved: {output_dir}/tour_gap_analysis.csv")

    # Save summary
    summary = {
        'analysis_period': f"{start_year}-{end_year}",
        'shows_analyzed': len(loader.shows),
        'songs_analyzed': agg['total_songs_analyzed'],
        'intra_tour_mean_gap': agg['intra_tour']['mean_gap_shows'],
        'cross_tour_mean_gap': agg['cross_tour']['mean_gap_shows'],
        'gap_ratio': rec['gap_ratio'],
        'recommended_adjustment': rec['recommended_adjustment'],
        'statistically_significant': agg.get('mannwhitney_pvalue', 1) < 0.05
    }

    pd.DataFrame([summary]).to_csv(f"{output_dir}/tour_gap_summary.csv", index=False)
    print(f"Saved: {output_dir}/tour_gap_summary.csv")

    print("\n" + "=" * 60)
    print("  ANALYSIS COMPLETE")
    print("=" * 60)

    return {
        'aggregate': agg,
        'recommendation': rec,
        'comparisons': comparisons
    }


if __name__ == "__main__":
    # Default: analyze last 15 years (covers 3.0 and 4.0 era)
    run_analysis(start_year=2010, end_year=2024)
