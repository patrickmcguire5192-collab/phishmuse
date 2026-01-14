"""
Combined Analysis: Base Probability + Monster Multiplier
"""
import json
import urllib.request
import time

API_KEY = '69F3065FB7F44C387CE5'

# Monster rates from Phish.in (already calculated)
MONSTER_RATES = {
    'Ruby Waves': 0.146,
    'Soul Planet': 0.080,
    'You Enjoy Myself': 0.071,
    'Drowned': 0.064,
    'Tweezer': 0.059,
    'Down with Disease': 0.036,
    'Ghost': 0.031,
    'Simple': 0.023,
    'Mercury': 0.023,
    'David Bowie': 0.022,
    'Bathtub Gin': 0.013,
    'Carini': 0.013,
    'Sand': 0.013,
    'Chalk Dust Torture': 0.010,
    'Piper': 0.010,
    'Harry Hood': 0.005,
    'Fluffhead': 0.004,
    'Split Open and Melt': 0.003,
    'Stash': 0.002,
    'Light': 0.0,
    'Run Like an Antelope': 0.0,
    'Slave to the Traffic Light': 0.0,
    'Reba': 0.0,
    "Everything's Right": 0.02,  # Estimate - no data
    "Mike's Song": 0.01,  # Estimate - no data
}

def get_monster_multiplier(monster_rate):
    """Convert monster rate to multiplier using ranges"""
    if monster_rate >= 0.10:      # 10%+ (Ruby Waves)
        return 1.30
    elif monster_rate >= 0.05:    # 5-10% (Soul Planet, YEM, Drowned, Tweezer)
        return 1.20
    elif monster_rate >= 0.03:    # 3-5% (DwD, Ghost)
        return 1.15
    elif monster_rate >= 0.01:    # 1-3% (Simple, Mercury, Bowie, Gin, etc.)
        return 1.10
    else:                         # <1% (no monster potential)
        return 1.00

# Fetch Phish.net data for frequency and jam rate
JAM_VEHICLES = [
    'You Enjoy Myself', 'Ruby Waves', 'Soul Planet', 'Fluffhead', 'Drowned',
    'Mercury', "Everything's Right", 'Ghost', 'David Bowie', 'Tweezer',
    'Harry Hood', 'Down with Disease', 'Simple', 'Piper', 'Bathtub Gin',
    'Light', 'Carini', 'Stash', 'Run Like an Antelope', 'Chalk Dust Torture',
    'Split Open and Melt', 'Sand', 'Slave to the Traffic Light', "Mike's Song", 'Reba'
]

print("Fetching Phish.net data...")
years = [2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]
all_entries = []

for year in years:
    url = f'https://api.phish.net/v5/setlists/showyear/{year}.json?apikey={API_KEY}'
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            if data.get('data'):
                all_entries.extend(data['data'])
    except:
        pass

phish_entries = [e for e in all_entries if e.get('artistid') == 1]
show_ids = set(e['showid'] for e in phish_entries)
total_shows = len(show_ids)

# Count plays and jam charts
from collections import defaultdict
song_stats = defaultdict(lambda: {'plays': 0, 'jamcharts': 0})

for entry in phish_entries:
    song = entry.get('song', '')
    if song in JAM_VEHICLES:
        song_stats[song]['plays'] += 1
        if entry.get('isjamchart') == '1' or entry.get('isjamchart') == 1:
            song_stats[song]['jamcharts'] += 1

# Build combined table
results = []
for song in JAM_VEHICLES:
    stats = song_stats[song]
    plays = stats['plays']
    jamcharts = stats['jamcharts']
    
    frequency = plays / total_shows if total_shows > 0 else 0
    jam_rate = jamcharts / plays if plays > 0 else 0
    when_played = min(1.0, 0.4 + jam_rate)
    base_prob = frequency * when_played
    
    monster_rate = MONSTER_RATES.get(song, 0)
    monster_mult = get_monster_multiplier(monster_rate)
    
    final_prob = base_prob * monster_mult
    
    results.append({
        'song': song,
        'plays': plays,
        'frequency': frequency,
        'jam_rate': jam_rate,
        'when_played': when_played,
        'base_prob': base_prob,
        'monster_rate': monster_rate,
        'monster_mult': monster_mult,
        'final_prob': final_prob
    })

# Sort by final_prob descending
results.sort(key=lambda x: x['final_prob'], reverse=True)

print(f"\nData from {total_shows} shows (2016-2024)")
print("\n" + "="*115)
print(f"{'Song':<26} {'Freq':>6} {'JamRate':>8} {'WhenPld':>8} {'BaseProb':>9} {'MonRate':>8} {'MonMult':>8} {'FINAL':>9} {'Odds':>6}")
print("="*115)

for r in results:
    odds = 1 / r['final_prob'] if r['final_prob'] > 0 else 999
    mult_indicator = "🔥" if r['monster_mult'] > 1.0 else "  "
    print(f"{r['song']:<26} {r['frequency']*100:>5.1f}% {r['jam_rate']*100:>7.1f}% {r['when_played']*100:>7.0f}% {r['base_prob']*100:>8.2f}% {r['monster_rate']*100:>7.1f}% {r['monster_mult']:>7.2f}x {r['final_prob']*100:>8.2f}% {odds:>5.1f}x {mult_indicator}")

print("="*115)
total_final = sum(r['final_prob'] for r in results)
print(f"{'TOTAL':<26} {'':>6} {'':>8} {'':>8} {'':>9} {'':>8} {'':>8} {total_final*100:>8.1f}%")

# Show top 15 after normalization to 85%
print("\n" + "="*80)
print("TOP 15 AFTER NORMALIZATION (to 85%)")
print("="*80)
top15 = results[:15]
top15_sum = sum(r['final_prob'] for r in top15)
norm_factor = 0.85 / top15_sum if top15_sum > 0 else 1

print(f"{'Song':<26} {'Raw':>10} {'Normalized':>12} {'Final Odds':>12}")
print("-"*80)
for r in top15:
    norm_prob = r['final_prob'] * norm_factor
    odds = 1 / norm_prob if norm_prob > 0 else 999
    mult_indicator = "🔥" if r['monster_mult'] > 1.0 else "  "
    print(f"{r['song']:<26} {r['final_prob']*100:>9.2f}% {norm_prob*100:>11.2f}% {odds:>11.1f}x {mult_indicator}")

print("-"*80)
print(f"{'Other':<26} {'':>10} {'15.00':>11}% {'6.7':>11}x")
