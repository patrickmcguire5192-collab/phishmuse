#!/usr/bin/env python3
"""
Archive.org Grateful Dead Bootstrap Script
==========================================

Builds a local index of all Grateful Dead shows from Archive.org.
This creates a JSON catalog with song names, durations, and show metadata
for fast querying without hitting the API on every request.

Run this once to build the catalog, then use archive_engine.py for queries.
"""

import json
import urllib.request
import urllib.parse
import time
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re

# Output file
CATALOG_PATH = Path(__file__).parent.parent / "data" / "grateful_dead_catalog.json"

# Archive.org API base
ARCHIVE_API = "https://archive.org"

def search_shows(year: int = None, page: int = 1, rows: int = 100) -> dict:
    """Search for Grateful Dead shows on Archive.org."""
    query = "collection:GratefulDead"
    if year:
        query += f" AND year:{year}"

    params = {
        "q": query,
        "output": "json",
        "rows": rows,
        "page": page,
        "fl": "identifier,date,venue,coverage,avg_rating,title"
    }

    url = f"{ARCHIVE_API}/advancedsearch.php?" + urllib.parse.urlencode(params)

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"  Error searching: {e}")
        return {"response": {"docs": [], "numFound": 0}}


def get_show_metadata(identifier: str) -> dict:
    """Get full metadata for a specific recording."""
    url = f"{ARCHIVE_API}/metadata/{identifier}"

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"  Error fetching {identifier}: {e}")
        return {}


def normalize_song_name(name: str) -> str:
    """Normalize song names for consistent matching."""
    if not name:
        return ""

    # Remove leading/trailing whitespace
    name = name.strip()

    # Remove transition indicators but track them
    name = re.sub(r'\s*[>→]\s*$', '', name)
    name = re.sub(r'^\s*[>→]\s*', '', name)

    # Common abbreviations and variations
    replacements = {
        "Playin' In The Band": "Playing in the Band",
        "Playin'": "Playing in the Band",
        "PITB": "Playing in the Band",
        "Truckin'": "Truckin",
        "Goin' Down The Road Feeling Bad": "Going Down the Road Feeling Bad",
        "GDTRFB": "Going Down the Road Feeling Bad",
        "NFA": "Not Fade Away",
        "IKYR": "I Know You Rider",
        "I Know You Rider": "I Know You Rider",
        "China Cat Sunflower": "China Cat Sunflower",
        "CCS": "China Cat Sunflower",
        "Morning Dew": "Morning Dew",
        "He's Gone": "He's Gone",
        "Hes Gone": "He's Gone",
        "Jack-A-Roe": "Jack-A-Roe",
        "Jack A Roe": "Jack-A-Roe",
        "St. Stephen": "St. Stephen",
        "Saint Stephen": "St. Stephen",
        "St Stephen": "St. Stephen",
        "Sugar Magnolia": "Sugar Magnolia",
        "Sugaree": "Sugaree",
        "Estimated Prophet": "Estimated Prophet",
        "Eyes Of The World": "Eyes of the World",
        "Scarlet Begonias": "Scarlet Begonias",
        "Fire On The Mountain": "Fire on the Mountain",
        "Wharf Rat": "Wharf Rat",
        "Weather Report Suite": "Weather Report Suite",
        "WRS": "Weather Report Suite",
        "Uncle John's Band": "Uncle John's Band",
        "UJB": "Uncle John's Band",
        "The Other One": "The Other One",
        "TOO": "The Other One",
        "Drums": "Drums",
        "Space": "Space",
        "Drums/Space": "Drums",  # We'll track these separately
    }

    # Check for exact replacements
    for old, new in replacements.items():
        if name.lower() == old.lower():
            return new

    return name


def parse_duration(length_str) -> float:
    """Parse duration string to seconds."""
    if not length_str:
        return 0

    try:
        # If it's already a number (seconds)
        if isinstance(length_str, (int, float)):
            return float(length_str)

        length_str = str(length_str).strip()

        # Format: MM:SS or HH:MM:SS
        if ':' in length_str:
            parts = length_str.split(':')
            if len(parts) == 2:
                return int(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])

        # Just seconds
        return float(length_str)
    except:
        return 0


def extract_tracks(metadata: dict) -> list:
    """Extract track list with durations from show metadata."""
    files = metadata.get('files', [])
    tracks = []
    seen_titles = set()

    for f in sorted(files, key=lambda x: x.get('name', '')):
        title = f.get('title', '')
        length = f.get('length')
        name = f.get('name', '')

        # Skip non-audio or already seen
        if not title or not length:
            continue

        # Prefer MP3 files (they have cleaner MM:SS format)
        # Skip duplicate titles (different formats of same track)
        if title in seen_titles:
            continue

        # Skip metadata files
        if any(skip in name.lower() for skip in ['.txt', '.xml', '.ffp', '.md5']):
            continue

        seen_titles.add(title)

        duration_sec = parse_duration(length)
        if duration_sec < 10:  # Skip very short tracks (announcements, etc)
            continue

        normalized = normalize_song_name(title)
        if not normalized or normalized.lower() in ['stage announcements', 'stage anouncements', 'tuning', 'crowd']:
            continue

        tracks.append({
            'title': title,
            'song': normalized,
            'duration': duration_sec,
            'duration_str': f"{int(duration_sec // 60)}:{int(duration_sec % 60):02d}"
        })

    return tracks


def build_catalog():
    """Build the complete Grateful Dead catalog."""
    print("=" * 60)
    print("Building Grateful Dead Catalog from Archive.org")
    print("=" * 60)
    print()

    # Create data directory if needed
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # First, get all unique show dates and best recording for each
    print("Step 1: Finding all unique show dates...")

    shows_by_date = defaultdict(list)

    # Get total count
    result = search_shows(rows=0)
    total = result['response']['numFound']
    print(f"  Total recordings in collection: {total:,}")

    # Fetch all recordings (paginated)
    # Use smaller batches and slower rate to avoid rate limiting
    page = 1
    rows = 200  # Smaller batches
    fetched = 0

    while fetched < total:
        # Try up to 3 times with exponential backoff
        result = None
        for attempt in range(3):
            result = search_shows(page=page, rows=rows)
            if 'response' in result:
                break
            wait_time = 2 ** (attempt + 1)  # 2, 4, 8 seconds
            print(f"\n  API error on page {page}, attempt {attempt + 1}/3, waiting {wait_time}s...", flush=True)
            time.sleep(wait_time)

        if not result or 'response' not in result:
            print(f"\n  Skipping page {page} after 3 failed attempts", flush=True)
            page += 1
            fetched += rows  # Approximate skip
            continue

        docs = result['response']['docs']

        if not docs:
            break

        for doc in docs:
            date = doc.get('date', '')[:10]  # YYYY-MM-DD
            if date and date.startswith('19'):  # Valid GD date
                shows_by_date[date].append({
                    'identifier': doc['identifier'],
                    'rating': doc.get('avg_rating', 0) or 0,
                    'venue': doc.get('venue', ''),
                    'coverage': doc.get('coverage', ''),
                    'title': doc.get('title', '')
                })

        fetched += len(docs)
        if fetched % 1000 == 0:
            print(f"\n  Fetched {fetched:,}/{total:,} recordings...", flush=True)
        page += 1
        time.sleep(1.0)  # 1 second between requests - be nice to Archive.org

    print(f"\n  Found {len(shows_by_date):,} unique show dates")
    print()

    # Step 2: For each date, pick best recording and extract tracks
    print("Step 2: Extracting setlists and durations...")
    print("  (This will take a while - ~2500 API calls)")
    print()

    catalog = {
        'metadata': {
            'created': datetime.now().isoformat(),
            'source': 'archive.org',
            'band': 'Grateful Dead'
        },
        'shows': [],
        'songs': defaultdict(lambda: {
            'performances': [],
            'total_plays': 0,
            'total_duration': 0
        })
    }

    dates = sorted(shows_by_date.keys())
    errors = 0

    for i, date in enumerate(dates):
        # Pick best recording (highest rating)
        recordings = shows_by_date[date]
        best = max(recordings, key=lambda x: x['rating'])

        # Get full metadata
        metadata = get_show_metadata(best['identifier'])
        if not metadata:
            errors += 1
            continue

        # Extract tracks
        tracks = extract_tracks(metadata)

        if tracks:
            show_meta = metadata.get('metadata', {})
            venue = show_meta.get('venue', best['venue'])
            if isinstance(venue, list):
                venue = venue[0] if venue else ''

            coverage = show_meta.get('coverage', best['coverage'])
            if isinstance(coverage, list):
                coverage = coverage[0] if coverage else ''

            show = {
                'date': date,
                'venue': venue,
                'location': coverage,
                'identifier': best['identifier'],
                'rating': best['rating'],
                'tracks': tracks
            }
            catalog['shows'].append(show)

            # Index songs
            for track in tracks:
                song_name = track['song']
                catalog['songs'][song_name]['performances'].append({
                    'date': date,
                    'duration': track['duration'],
                    'duration_str': track['duration_str'],
                    'venue': venue
                })
                catalog['songs'][song_name]['total_plays'] += 1
                catalog['songs'][song_name]['total_duration'] += track['duration']

        # Progress
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1:,}/{len(dates):,} shows ({len(catalog['shows']):,} with tracks)...")

        time.sleep(0.3)  # Rate limit - be nice to Archive.org

    print(f"\n  Processed {len(dates):,} dates, got {len(catalog['shows']):,} shows with track data")
    print(f"  Errors: {errors}")

    # Convert defaultdict to regular dict for JSON
    catalog['songs'] = dict(catalog['songs'])

    # Calculate song stats
    print("\nStep 3: Calculating song statistics...")
    for song_name, data in catalog['songs'].items():
        plays = data['total_plays']
        if plays > 0:
            data['avg_duration'] = data['total_duration'] / plays

            # Find longest version
            performances = sorted(data['performances'], key=lambda x: x['duration'], reverse=True)
            data['longest'] = performances[0] if performances else None
            data['first_played'] = min(p['date'] for p in performances)
            data['last_played'] = max(p['date'] for p in performances)

    print(f"  Indexed {len(catalog['songs']):,} unique songs")

    # Save catalog
    print(f"\nSaving catalog to {CATALOG_PATH}...")
    with open(CATALOG_PATH, 'w') as f:
        json.dump(catalog, f, indent=2, default=str)

    file_size = CATALOG_PATH.stat().st_size / (1024 * 1024)
    print(f"  Saved! ({file_size:.1f} MB)")

    # Print summary
    print("\n" + "=" * 60)
    print("CATALOG COMPLETE!")
    print("=" * 60)
    print(f"Shows: {len(catalog['shows']):,}")
    print(f"Songs: {len(catalog['songs']):,}")

    # Top 10 most played
    print("\nTop 10 Most Played Songs:")
    top_songs = sorted(catalog['songs'].items(), key=lambda x: x[1]['total_plays'], reverse=True)[:10]
    for song, data in top_songs:
        print(f"  {song}: {data['total_plays']} times")

    # Longest jams
    print("\nTop 10 Longest Jams:")
    all_performances = []
    for song, data in catalog['songs'].items():
        if data.get('longest'):
            all_performances.append((song, data['longest']))

    longest = sorted(all_performances, key=lambda x: x[1]['duration'], reverse=True)[:10]
    for song, perf in longest:
        mins = int(perf['duration'] // 60)
        secs = int(perf['duration'] % 60)
        print(f"  {song}: {mins}:{secs:02d} ({perf['date']})")

    print()
    return catalog


if __name__ == '__main__':
    build_catalog()
