"""
Tour Gap Analysis Module

Investigates whether song gaps behave differently within tours vs. across tour breaks.

Key question: Should the recency penalty be the same whether a song was played
1 night ago (same tour) vs. 3 months ago (different tour)?
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

from .data_loader import PhishDataLoader
from .models import SongPerformance, Show


@dataclass
class GapComparison:
    """Comparison of gaps within tours vs across tour breaks."""
    song_name: str

    # Intra-tour gaps (show gaps when no tour break between performances)
    intra_tour_gaps: List[int]
    intra_tour_days: List[int]

    # Cross-tour gaps (show gaps when tour break exists between performances)
    cross_tour_gaps: List[int]
    cross_tour_days: List[int]

    # Statistics
    intra_mean_gap: float = 0.0
    intra_median_gap: float = 0.0
    intra_std_gap: float = 0.0

    cross_mean_gap: float = 0.0
    cross_median_gap: float = 0.0
    cross_std_gap: float = 0.0

    # Statistical tests
    ttest_pvalue: float = 1.0
    mannwhitney_pvalue: float = 1.0

    def compute_stats(self):
        """Compute statistics for this comparison."""
        if self.intra_tour_gaps:
            self.intra_mean_gap = np.mean(self.intra_tour_gaps)
            self.intra_median_gap = np.median(self.intra_tour_gaps)
            self.intra_std_gap = np.std(self.intra_tour_gaps)

        if self.cross_tour_gaps:
            self.cross_mean_gap = np.mean(self.cross_tour_gaps)
            self.cross_median_gap = np.median(self.cross_tour_gaps)
            self.cross_std_gap = np.std(self.cross_tour_gaps)

        # Statistical tests (only if we have enough data)
        if len(self.intra_tour_gaps) >= 5 and len(self.cross_tour_gaps) >= 5:
            try:
                _, self.ttest_pvalue = stats.ttest_ind(
                    self.intra_tour_gaps, self.cross_tour_gaps
                )
                _, self.mannwhitney_pvalue = stats.mannwhitneyu(
                    self.intra_tour_gaps, self.cross_tour_gaps,
                    alternative='two-sided'
                )
            except Exception:
                pass


@dataclass
class TourBreakEffect:
    """Quantifies the effect of tour breaks on song repetition."""

    # After tour break, what's the probability a song repeats in first N shows?
    repeat_prob_no_break: Dict[int, float]  # {n_shows: probability}
    repeat_prob_after_break: Dict[int, float]

    # Average gap when song IS repeated
    avg_gap_no_break: float
    avg_gap_after_break: float

    # Sample sizes
    n_samples_no_break: int
    n_samples_after_break: int


class TourGapAnalyzer:
    """Analyze how tour breaks affect song gap patterns."""

    def __init__(self, loader: PhishDataLoader, tour_break_threshold_days: int = 14):
        self.loader = loader
        self.break_threshold = tour_break_threshold_days

    def analyze_song(self, song_name: str) -> GapComparison:
        """
        Analyze gap patterns for a specific song.

        Separates gaps into:
        1. Intra-tour: Performances within the same tour/run
        2. Cross-tour: Performances separated by a tour break
        """
        contexts = self.loader.get_performances_with_context(song_name)

        intra_gaps = []
        intra_days = []
        cross_gaps = []
        cross_days = []

        for ctx in contexts:
            if ctx['prev_performance'] is None:
                continue  # Skip first performance (no gap)

            gap_shows = ctx['gap_shows']
            gap_days = ctx['gap_days']

            if ctx['crosses_tour_break']:
                cross_gaps.append(gap_shows)
                cross_days.append(gap_days)
            else:
                intra_gaps.append(gap_shows)
                intra_days.append(gap_days)

        comparison = GapComparison(
            song_name=song_name,
            intra_tour_gaps=intra_gaps,
            intra_tour_days=intra_days,
            cross_tour_gaps=cross_gaps,
            cross_tour_days=cross_days
        )
        comparison.compute_stats()

        return comparison

    def analyze_all_songs(self, min_performances: int = 50) -> List[GapComparison]:
        """Analyze gap patterns for all songs with sufficient data."""
        results = []

        for song_name, song in self.loader.songs.items():
            if song.times_played < min_performances:
                continue

            comparison = self.analyze_song(song_name)

            # Only include if we have meaningful data in both categories
            if len(comparison.intra_tour_gaps) >= 5 and len(comparison.cross_tour_gaps) >= 5:
                results.append(comparison)

        return sorted(results, key=lambda x: x.mannwhitney_pvalue)

    def aggregate_analysis(self, min_performances: int = 30) -> Dict:
        """
        Aggregate analysis across all songs.

        Returns overall statistics about how tour breaks affect gaps.
        """
        all_intra_gaps = []
        all_cross_gaps = []
        all_intra_days = []
        all_cross_days = []

        song_comparisons = []

        for song_name, song in self.loader.songs.items():
            if song.times_played < min_performances:
                continue

            comparison = self.analyze_song(song_name)

            if comparison.intra_tour_gaps and comparison.cross_tour_gaps:
                all_intra_gaps.extend(comparison.intra_tour_gaps)
                all_cross_gaps.extend(comparison.cross_tour_gaps)
                all_intra_days.extend(comparison.intra_tour_days)
                all_cross_days.extend(comparison.cross_tour_days)
                song_comparisons.append(comparison)

        # Aggregate statistics
        result = {
            'total_songs_analyzed': len(song_comparisons),
            'total_intra_tour_gaps': len(all_intra_gaps),
            'total_cross_tour_gaps': len(all_cross_gaps),

            'intra_tour': {
                'mean_gap_shows': np.mean(all_intra_gaps) if all_intra_gaps else 0,
                'median_gap_shows': np.median(all_intra_gaps) if all_intra_gaps else 0,
                'std_gap_shows': np.std(all_intra_gaps) if all_intra_gaps else 0,
                'mean_gap_days': np.mean(all_intra_days) if all_intra_days else 0,
                'median_gap_days': np.median(all_intra_days) if all_intra_days else 0,
            },

            'cross_tour': {
                'mean_gap_shows': np.mean(all_cross_gaps) if all_cross_gaps else 0,
                'median_gap_shows': np.median(all_cross_gaps) if all_cross_gaps else 0,
                'std_gap_shows': np.std(all_cross_gaps) if all_cross_gaps else 0,
                'mean_gap_days': np.mean(all_cross_days) if all_cross_days else 0,
                'median_gap_days': np.median(all_cross_days) if all_cross_days else 0,
            },

            'song_comparisons': song_comparisons
        }

        # Statistical tests on aggregate data
        if all_intra_gaps and all_cross_gaps:
            _, result['ttest_pvalue'] = stats.ttest_ind(all_intra_gaps, all_cross_gaps)
            _, result['mannwhitney_pvalue'] = stats.mannwhitneyu(
                all_intra_gaps, all_cross_gaps, alternative='two-sided'
            )

        return result

    def analyze_repeat_patterns(self, n_shows_window: int = 3) -> Dict:
        """
        Analyze: After a tour break, how does the pattern of song repetition change?

        Key question: If DWD was played at the last show of Summer Tour,
        what's the probability it appears in the first N shows of Fall Tour
        vs. if it was played mid-tour?
        """
        # Track: for each song played, did it repeat within next N shows?
        repeats_same_tour = []  # (song_name, did_repeat, gap_if_repeated)
        repeats_cross_tour = []

        for i, show in enumerate(self.loader.shows):
            # Check if this show starts a new tour segment
            is_tour_start = False
            if i > 0:
                prev_show = self.loader.shows[i - 1]
                days_since_last = (show.show_date - prev_show.show_date).days
                is_tour_start = days_since_last > self.break_threshold

            # For each song in this show
            for song_perf in show.songs:
                song_name = song_perf.song_name

                # Look at next N shows to see if repeated
                repeated = False
                repeat_gap = None

                for j in range(1, n_shows_window + 1):
                    if i + j >= len(self.loader.shows):
                        break

                    next_show = self.loader.shows[i + j]

                    # Check if there's a tour break between current and next show
                    # (only count within same tour segment for "same tour" category)
                    cumulative_days = (next_show.show_date - show.show_date).days

                    for next_song in next_show.songs:
                        if next_song.song_name == song_name:
                            repeated = True
                            repeat_gap = j
                            break

                    if repeated:
                        break

                if is_tour_start:
                    repeats_cross_tour.append({
                        'song': song_name,
                        'repeated': repeated,
                        'gap': repeat_gap
                    })
                else:
                    repeats_same_tour.append({
                        'song': song_name,
                        'repeated': repeated,
                        'gap': repeat_gap
                    })

        # Compute probabilities
        same_tour_df = pd.DataFrame(repeats_same_tour)
        cross_tour_df = pd.DataFrame(repeats_cross_tour)

        result = {
            'window_size': n_shows_window,
            'same_tour': {
                'total_observations': len(same_tour_df),
                'repeat_probability': same_tour_df['repeated'].mean() if len(same_tour_df) > 0 else 0,
                'avg_gap_when_repeated': same_tour_df[same_tour_df['repeated']]['gap'].mean() if len(same_tour_df) > 0 else 0
            },
            'tour_start': {
                'total_observations': len(cross_tour_df),
                'repeat_probability': cross_tour_df['repeated'].mean() if len(cross_tour_df) > 0 else 0,
                'avg_gap_when_repeated': cross_tour_df[cross_tour_df['repeated']]['gap'].mean() if len(cross_tour_df) > 0 else 0
            }
        }

        # Chi-square test for independence
        if len(same_tour_df) > 0 and len(cross_tour_df) > 0:
            contingency = [
                [same_tour_df['repeated'].sum(), (~same_tour_df['repeated']).sum()],
                [cross_tour_df['repeated'].sum(), (~cross_tour_df['repeated']).sum()]
            ]
            chi2, pvalue, dof, expected = stats.chi2_contingency(contingency)
            result['chi2_statistic'] = chi2
            result['chi2_pvalue'] = pvalue

        return result

    def calculate_optimal_penalty_adjustment(self) -> Dict:
        """
        Based on the data, calculate what the penalty adjustment should be
        for cross-tour gaps vs intra-tour gaps.

        Returns recommended multipliers for PhanDuel odds engine.
        """
        agg = self.aggregate_analysis()

        intra_mean = agg['intra_tour']['mean_gap_shows']
        cross_mean = agg['cross_tour']['mean_gap_shows']

        # The ratio tells us how gaps compare
        # If cross_tour gaps are longer, songs are less likely to repeat after tour break
        if intra_mean > 0:
            gap_ratio = cross_mean / intra_mean
        else:
            gap_ratio = 1.0

        # Convert to penalty adjustment
        # gap_ratio > 1 means songs have longer gaps after tour breaks
        # This suggests the current penalty (same for both) overcounts recent plays after tour breaks

        result = {
            'intra_tour_mean_gap': intra_mean,
            'cross_tour_mean_gap': cross_mean,
            'gap_ratio': gap_ratio,
            'interpretation': '',
            'recommended_adjustment': 1.0
        }

        if gap_ratio > 1.2:
            result['interpretation'] = (
                f"Cross-tour gaps are {gap_ratio:.1f}x longer than intra-tour gaps. "
                "This suggests tour breaks DO 'reset' song rotation somewhat. "
                "Consider reducing the recency penalty for songs played before a tour break."
            )
            # If gaps are 2x longer after tour break, the "effective" recency is half
            result['recommended_adjustment'] = 1.0 / gap_ratio

        elif gap_ratio < 0.8:
            result['interpretation'] = (
                f"Cross-tour gaps are only {gap_ratio:.1f}x the intra-tour gaps. "
                "This suggests songs played before tour breaks are MORE likely to return quickly. "
                "Consider increasing the recency penalty for pre-break songs."
            )
            result['recommended_adjustment'] = 1.0 / gap_ratio

        else:
            result['interpretation'] = (
                f"Gap ratio is {gap_ratio:.1f}, suggesting tour breaks don't significantly affect patterns. "
                "Current uniform penalty approach is likely appropriate."
            )
            result['recommended_adjustment'] = 1.0

        return result


def to_dataframe(comparisons: List[GapComparison]) -> pd.DataFrame:
    """Convert list of GapComparisons to a DataFrame for analysis."""
    data = []
    for c in comparisons:
        data.append({
            'song': c.song_name,
            'intra_n': len(c.intra_tour_gaps),
            'intra_mean': c.intra_mean_gap,
            'intra_median': c.intra_median_gap,
            'intra_std': c.intra_std_gap,
            'cross_n': len(c.cross_tour_gaps),
            'cross_mean': c.cross_mean_gap,
            'cross_median': c.cross_median_gap,
            'cross_std': c.cross_std_gap,
            'ttest_p': c.ttest_pvalue,
            'mannwhitney_p': c.mannwhitney_pvalue,
            'gap_ratio': c.cross_mean_gap / c.intra_mean_gap if c.intra_mean_gap > 0 else np.nan
        })
    return pd.DataFrame(data)


if __name__ == "__main__":
    # Quick test
    from .data_loader import PhishDataLoader

    print("Loading data...")
    loader = PhishDataLoader()
    loader.load_all_data(start_year=2015, end_year=2024)

    print("\nAnalyzing tour gap patterns...")
    analyzer = TourGapAnalyzer(loader)

    # Aggregate analysis
    agg = analyzer.aggregate_analysis(min_performances=30)
    print(f"\nAnalyzed {agg['total_songs_analyzed']} songs")
    print(f"Intra-tour mean gap: {agg['intra_tour']['mean_gap_shows']:.1f} shows")
    print(f"Cross-tour mean gap: {agg['cross_tour']['mean_gap_shows']:.1f} shows")

    # Recommendation
    rec = analyzer.calculate_optimal_penalty_adjustment()
    print(f"\n{rec['interpretation']}")
    print(f"Recommended penalty adjustment: {rec['recommended_adjustment']:.2f}")
