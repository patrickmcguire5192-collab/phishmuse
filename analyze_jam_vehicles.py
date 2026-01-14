"""Analyze jam vehicle base probabilities (no recency)"""
import json
import urllib.request
from collections import defaultdict

API_KEY = '69F3065FB7F44C387CE5'

# Our 25 jam vehicle candidates
JAM_VEHICLES = [
    'You Enjoy Myself', 'Ruby Waves', 'Soul Planet', 'Fluffhead', 'Drowned',
    'Mercury', "Everything's Right", 'Ghost', 'David Bowie', 'Tweezer',
    'Harry Hood', 'Down with Disease', 'Simple', 'Piper', 'Bathtub Gin',
    'Light', 'Carini', 'Stash', 'Run Like an Antelope', 'Chalk Dust Torture',
    'Split Open and Melt', 'Sand', 'Slave to the Traffic Light', "Mike's Song", 'Reba'
]

# Fetch setlist data
years = [2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]
all_entries = []

print("Fetching data from Phish.net API...")
for year in years:
    url = f'https://api.phish.net/v5/setlists/showyear/{year}.json?apikey={API_KEY}'
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            if data.get('data'):
                all_entries.extend(data['data'])
                print(f"  {year}: {len(data['data'])} entries")
    except Exception as e:
        print(f"  {year}: Error - {e}")

# Filter to Phish only
phish_entries = [e for e in all_entries if e.get('artistid') == 1]
print(f"\nTotal Phish entries: {len(phish_entries)}")

# Count unique shows
show_ids = set(e['showid'] for e in phish_entries)
total_shows = len(show_ids)
print(f"Total shows: {total_shows}")

# Count plays and jam charts for each song
song_stats = defaultdict(lambda: {'plays': 0, 'jamcharts': 0})

for entry in phish_entries:
    song = entry.get('song', '')
    if song in JAM_VEHICLES:
        song_stats[song]['plays'] += 1
        if entry.get('isjamchart') == '1' or entry.get('isjamchart') == 1:
            song_stats[song]['jamcharts'] += 1

# Calculate and display table
print("\n" + "="*90)
print(f"{'Song':<28} {'Plays':>6} {'JamCharts':>10} {'Frequency':>10} {'JamRate':>8} {'WhenPlayed':>11} {'BaseProb':>9}")
print("="*90)

results = []
for song in JAM_VEHICLES:
    stats = song_stats[song]
    plays = stats['plays']
    jamcharts = stats['jamcharts']
    
    frequency = plays / total_shows if total_shows > 0 else 0
    jam_rate = jamcharts / plays if plays > 0 else 0
    when_played = min(1.0, 0.4 + jam_rate)
    base_prob = frequency * when_played
    
    results.append({
        'song': song,
        'plays': plays,
        'jamcharts': jamcharts,
        'frequency': frequency,
        'jam_rate': jam_rate,
        'when_played': when_played,
        'base_prob': base_prob
    })

# Sort by base_prob descending
results.sort(key=lambda x: x['base_prob'], reverse=True)

for r in results:
    print(f"{r['song']:<28} {r['plays']:>6} {r['jamcharts']:>10} {r['frequency']*100:>9.1f}% {r['jam_rate']*100:>7.1f}% {r['when_played']*100:>10.0f}% {r['base_prob']*100:>8.2f}%")

print("="*90)
total_base = sum(r['base_prob'] for r in results)
print(f"{'TOTAL':<28} {'':<6} {'':<10} {'':<10} {'':<8} {'':<11} {total_base*100:>8.1f}%")

# Show raw odds (1/baseProb)
print("\n" + "="*70)
print("RAW ODDS (before recency, before normalization)")
print("="*70)
for r in results[:15]:
    raw_odds = 1 / r['base_prob'] if r['base_prob'] > 0 else 999
    print(f"{r['song']:<28} {r['base_prob']*100:>6.2f}%  →  {raw_odds:>5.1f}x")
