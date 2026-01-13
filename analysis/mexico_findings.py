"""
MEXICO ANALYSIS FINDINGS
========================
Analysis conducted: January 2025
Data range: 2016-2025 (40 shows across 8 runs)

This documents the statistical findings used to generate Mexico-specific
prop bets for PhanDuel.
"""

# =============================================================================
# KEY FINDINGS
# =============================================================================

FINDINGS = {
    'water_songs': {
        'summary': 'Phish leans into beach themes at Mexico shows',
        'official_list': [
            'Sand',                          # 8x in Mexico (EVERY run!)
            'A Song I Heard the Ocean Sing', # 4x
            'Wading in the Velvet Sea',      # 4x
            'Beneath a Sea of Stars Part 1', # 4x
            'Ruby Waves',                    # 3x
            'Waves',                         # 2x
            'A Wave of Hope',                # 2x
            'The Ocean',                     # 1x
            'Sea and Sand',                  # 1x
        ],
        'per_show_stats': {
            'mean': 0.70,
            'over_1_5_probability': 0.18,  # 18%
            'under_1_5_probability': 0.82,  # 82%
        },
        'per_run_stats': {
            'mean': 3.50,
            'median': 4.0,
            'min': 1,   # 2019
            'max': 6,   # 2024
            'over_4_5_probability': 0.125,  # 12.5%
            'under_4_5_probability': 0.875, # 87.5%
        }
    },

    'longshots': {
        'mexican_cousin': {
            'times_played': 1,
            'total_shows': 40,
            'probability': 0.025,  # 2.5%
            'last_played': '2016-01-15',
            'insight': 'The ultimate irony bet - Mexican Cousin barely played IN Mexico!'
        },
        'wilson': {
            'times_played': 0,
            'total_shows': 40,
            'probability': 0.0,
            'insight': 'Never played in Mexico - would be historic debut'
        },
        'llama': {
            'times_played': 0,
            'total_shows': 40,
            'probability': 0.0,
            'insight': 'Another notable bustout candidate'
        }
    },

    'top_mexico_songs': [
        ('Jam', 10),
        ('Free', 9),
        ('Ghost', 9),
        ('Sand', 8),
        ('Chalk Dust Torture', 8),
        ('Tube', 8),
        ('Also Sprach Zarathustra', 7),
        ('Possum', 7),
        ('Simple', 7),
        ('46 Days', 7),
    ],

    'constants': {
        'sand': 'Played in ALL 8 Mexico runs (100%)',
    }
}

# =============================================================================
# PROP BETS FOR PHANDUEL
# =============================================================================

PHANDUEL_MEXICO_BETS = [
    {
        'bet': 'Over 1.5 water-themed songs (per show)',
        'odds': 2.0,
        'type': 'per_show',
        'description': 'Water songs: Sand, Ocean Sing, Velvet Sea, Beneath a Sea of Stars, Ruby Waves, Waves, Wave of Hope, The Ocean, Sea and Sand'
    },
    {
        'bet': 'Under 1.5 water-themed songs (per show)',
        'odds': 2.0,
        'type': 'per_show',
        'description': 'Water songs: Sand, Ocean Sing, Velvet Sea, Beneath a Sea of Stars, Ruby Waves, Waves, Wave of Hope, The Ocean, Sea and Sand'
    },
    {
        'bet': 'Over 4.5 water songs for entire run',
        'odds': 2.0,
        'type': 'per_run',
        'description': 'Total unique water-themed songs played across all Mexico shows'
    },
    {
        'bet': 'Under 4.5 water songs for entire run',
        'odds': 2.0,
        'type': 'per_run',
        'description': 'Total unique water-themed songs played across all Mexico shows'
    },
    {
        'bet': 'Mexican Cousin played',
        'odds': 40.0,
        'type': 'longshot',
        'description': 'Only played once in Mexico history (2016). The irony!'
    },
    {
        'bet': 'Wilson played',
        'odds': 50.0,
        'type': 'longshot',
        'description': 'Never been played in Mexico - would be a historic debut!'
    }
]

# =============================================================================
# WATER SONGS BY MEXICO RUN
# =============================================================================

WATER_BY_YEAR = {
    2016: {'shows': 3, 'water_songs': 4, 'songs': ['Sand', 'A Song I Heard the Ocean Sing', 'Wading in the Velvet Sea', 'The Ocean']},
    2017: {'shows': 4, 'water_songs': 3, 'songs': ['Sand', 'A Song I Heard the Ocean Sing', 'Wading in the Velvet Sea']},
    2019: {'shows': 4, 'water_songs': 1, 'songs': ['Sand']},  # Lowest!
    2020: {'shows': 6, 'water_songs': 4, 'songs': ['Sand', 'Beneath a Sea of Stars Part 1', 'Sea and Sand', 'Waves']},
    2022: {'shows': 7, 'water_songs': 4, 'songs': ['Sand', 'A Song I Heard the Ocean Sing', 'Beneath a Sea of Stars Part 1', 'Wading in the Velvet Sea']},
    2023: {'shows': 4, 'water_songs': 4, 'songs': ['Sand', 'Ruby Waves', 'Waves', 'A Wave of Hope']},
    2024: {'shows': 6, 'water_songs': 6, 'songs': ['Sand', 'A Song I Heard the Ocean Sing', 'A Wave of Hope', 'Beneath a Sea of Stars Part 1', 'Ruby Waves', 'Wading in the Velvet Sea']},  # Highest!
    2025: {'shows': 6, 'water_songs': 2, 'songs': ['Sand', 'Beneath a Sea of Stars Part 1']},
}

# =============================================================================
# METHODOLOGY NOTES
# =============================================================================

"""
WATER SONG SELECTION CRITERIA:

We used a tiered approach to identify water-themed songs:

TIER 1 - OBVIOUS (Used for betting):
  Songs with water/ocean/sea/wave/sand explicitly in the title.
  This is the most defensible list for prop betting purposes.

TIER 2 - CLEAR (Not used, but defensible):
  Songs with clear water connections in lyrics or meaning:
  - Bathtub Gin (bathtub)
  - Prince Caspian (Narnia sea voyage)
  - Down with Disease ("drift upon the sea")
  - The Wedge (surfing term)
  - Shipwreck (nautical)
  - Foam (surf)

TIER 3 - STRETCH (Not used):
  Tenuous connections that would be disputed.

The OBVIOUS list was chosen because:
1. No arguments about what counts
2. Easy for bettors to track
3. Historical data shows meaningful variance (0-6 per run)
"""

if __name__ == '__main__':
    print("Mexico Analysis Findings")
    print("="*50)
    print(f"\nTop songs: {FINDINGS['top_mexico_songs'][:5]}")
    print(f"\nWater songs per run mean: {FINDINGS['water_songs']['per_run_stats']['mean']}")
    print(f"\nMexican Cousin plays in Mexico: {FINDINGS['longshots']['mexican_cousin']['times_played']}")
    print(f"Wilson plays in Mexico: {FINDINGS['longshots']['wilson']['times_played']}")
