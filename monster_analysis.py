"""
Monster Multiplier Analysis
---------------------------
Calculate what % of each song's performances went over 25 minutes.
Data from Phish.in API (has actual track durations in milliseconds).
"""
import json
import urllib.request
import time

# 25 minutes in milliseconds
MONSTER_THRESHOLD_MS = 25 * 60 * 1000  # 1,500,000 ms

# Our 25 jam vehicle candidates with their Phish.in slugs
JAM_VEHICLES = [
    ('You Enjoy Myself', 'you-enjoy-myself'),
    ('Ruby Waves', 'ruby-waves'),
    ('Soul Planet', 'soul-planet'),
    ('Fluffhead', 'fluffhead'),
    ('Drowned', 'drowned'),
    ('Mercury', 'mercury'),
    ("Everything's Right", 'everythings-right'),
    ('Ghost', 'ghost'),
    ('David Bowie', 'david-bowie'),
    ('Tweezer', 'tweezer'),
    ('Harry Hood', 'harry-hood'),
    ('Down with Disease', 'down-with-disease'),
    ('Simple', 'simple'),
    ('Piper', 'piper'),
    ('Bathtub Gin', 'bathtub-gin'),
    ('Light', 'light'),
    ('Carini', 'carini'),
    ('Stash', 'stash'),
    ('Run Like an Antelope', 'run-like-an-antelope'),
    ('Chalk Dust Torture', 'chalk-dust-torture'),
    ('Split Open and Melt', 'split-open-and-melt'),
    ('Sand', 'sand'),
    ('Slave to the Traffic Light', 'slave-to-the-traffic-light'),
    ("Mike's Song", 'mikes-song'),
    ('Reba', 'reba'),
]

def fetch_song_tracks(slug):
    """Fetch all track performances for a song from Phish.in"""
    url = f'https://phish.in/api/v2/tracks?song_slug={slug}&per_page=1000'
    try:
        req = urllib.request.Request(url, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            return data.get('tracks', [])
    except Exception as e:
        print(f"  Error fetching {slug}: {e}")
        return []

print("Fetching duration data from Phish.in API...")
print("="*95)

results = []

for song_name, slug in JAM_VEHICLES:
    tracks = fetch_song_tracks(slug)
    time.sleep(0.3)  # Be nice to the API
    
    if not tracks:
        print(f"  {song_name}: No data")
        continue
    
    # Filter to tracks with duration data
    with_duration = [t for t in tracks if t.get('duration') and t['duration'] > 0]
    
    if not with_duration:
        print(f"  {song_name}: No duration data")
        continue
    
    # Calculate stats
    durations_ms = [t['duration'] for t in with_duration]
    durations_min = [d / 60000 for d in durations_ms]
    
    total_plays = len(with_duration)
    monsters = sum(1 for d in durations_ms if d >= MONSTER_THRESHOLD_MS)
    monster_rate = monsters / total_plays if total_plays > 0 else 0
    
    avg_duration = sum(durations_min) / len(durations_min)
    max_duration = max(durations_min)
    
    results.append({
        'song': song_name,
        'slug': slug,
        'total_plays': total_plays,
        'monsters': monsters,
        'monster_rate': monster_rate,
        'avg_min': avg_duration,
        'max_min': max_duration
    })
    
    print(f"  {song_name}: {total_plays} plays, {monsters} monsters ({monster_rate*100:.1f}%), avg {avg_duration:.1f} min, max {max_duration:.1f} min")

# Sort by monster rate descending
results.sort(key=lambda x: x['monster_rate'], reverse=True)

print("\n" + "="*95)
print(f"{'Song':<28} {'Plays':>7} {'Monsters':>9} {'MonsterRate':>12} {'Avg':>8} {'Max':>8}")
print(f"{'':28} {'':>7} {'(≥25min)':>9} {'':>12} {'(min)':>8} {'(min)':>8}")
print("="*95)

for r in results:
    print(f"{r['song']:<28} {r['total_plays']:>7} {r['monsters']:>9} {r['monster_rate']*100:>11.1f}% {r['avg_min']:>8.1f} {r['max_min']:>8.1f}")

print("="*95)

# Show the WhenPlayed calculation with monster rate instead of jam rate
print("\n" + "="*95)
print("PROPOSED: WhenPlayed = 0.4 + MonsterRate (capped at 1.0)")
print("="*95)
print(f"{'Song':<28} {'MonsterRate':>12} {'WhenPlayed':>12}")
print("="*95)
for r in results:
    when_played = min(1.0, 0.4 + r['monster_rate'])
    print(f"{r['song']:<28} {r['monster_rate']*100:>11.1f}% {when_played*100:>11.0f}%")
