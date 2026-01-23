#!/usr/bin/env python3
"""
Archive.org Grateful Dead Bootstrap Script
==========================================

Builds a local index of all Grateful Dead shows from Archive.org.
This creates a JSON catalog with song names, durations, and show metadata
for fast querying without hitting the API on every request.

RESUME SUPPORT:
- Saves progress after each batch of shows
- Automatically resumes from where it left off
- Run multiple times to incrementally build the catalog

Usage:
  python archive_bootstrap.py          # Resume/continue building
  python archive_bootstrap.py --status # Show current progress
  python archive_bootstrap.py --reset  # Start fresh
"""

import json
import urllib.request
import urllib.parse
import time
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re

# Output files
DATA_DIR = Path(__file__).parent.parent / "data"
CATALOG_PATH = DATA_DIR / "grateful_dead_catalog.json"
CHECKPOINT_PATH = DATA_DIR / "dead_checkpoint.json"

# Archive.org API base
ARCHIVE_API = "https://archive.org"

# Rate limiting - be nice to Archive.org
SEARCH_DELAY = 2.0     # Seconds between search API calls
METADATA_DELAY = 1.5   # Seconds between metadata API calls
SAVE_EVERY = 20        # Save checkpoint every N shows

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


def load_checkpoint():
    """Load checkpoint for resume capability."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH) as f:
                return json.load(f)
        except:
            pass
    return {'processed_dates': [], 'shows_by_date': {}}


def save_checkpoint(checkpoint):
    """Save checkpoint."""
    checkpoint['updated'] = datetime.now().isoformat()
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(checkpoint, f)


def load_catalog():
    """Load existing catalog or create empty one."""
    if CATALOG_PATH.exists():
        try:
            with open(CATALOG_PATH) as f:
                return json.load(f)
        except:
            pass
    return {
        'metadata': {'created': datetime.now().isoformat(), 'source': 'archive.org', 'band': 'Grateful Dead'},
        'shows': [],
        'songs': {}
    }


def save_catalog(catalog):
    """Save catalog and recalculate song stats."""
    catalog['metadata']['updated'] = datetime.now().isoformat()

    # Recalculate song stats
    for song_name, data in catalog['songs'].items():
        perfs = data.get('performances', [])
        if perfs:
            data['total_plays'] = len(perfs)
            data['total_duration'] = sum(p['duration'] for p in perfs)
            data['avg_duration'] = data['total_duration'] / len(perfs)
            sorted_perfs = sorted(perfs, key=lambda x: x['duration'], reverse=True)
            data['longest'] = sorted_perfs[0]
            data['first_played'] = min(p['date'] for p in perfs)
            data['last_played'] = max(p['date'] for p in perfs)

    with open(CATALOG_PATH, 'w') as f:
        json.dump(catalog, f, indent=2, default=str)


def show_status():
    """Show current build progress."""
    checkpoint = load_checkpoint()
    catalog = load_catalog()

    processed = len(checkpoint.get('processed_dates', []))
    total_dates = len(checkpoint.get('shows_by_date', {}))

    print("\n" + "=" * 60)
    print("GRATEFUL DEAD CATALOG STATUS")
    print("=" * 60)
    print(f"Show dates discovered: {total_dates:,}")
    print(f"Show dates processed:  {processed:,}")
    if total_dates > 0:
        print(f"Progress:              {100*processed/total_dates:.1f}%")
        print(f"Remaining:             {total_dates - processed:,}")
    print()
    print(f"Catalog shows:  {len(catalog.get('shows', [])):,}")
    print(f"Catalog songs:  {len(catalog.get('songs', {})):,}")

    if catalog.get('songs'):
        print("\nTop 5 most played:")
        top = sorted(catalog['songs'].items(), key=lambda x: x[1].get('total_plays', 0), reverse=True)[:5]
        for song, data in top:
            print(f"  {song}: {data.get('total_plays', 0)} times")
    print()


def build_catalog(reset=False):
    """Build catalog with resume support."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if reset:
        print("Resetting - starting fresh...")
        CHECKPOINT_PATH.unlink(missing_ok=True)
        CATALOG_PATH.unlink(missing_ok=True)

    print("=" * 60)
    print("Building Grateful Dead Catalog from Archive.org")
    print("=" * 60)

    checkpoint = load_checkpoint()
    catalog = load_catalog()

    processed_dates = set(checkpoint.get('processed_dates', []))
    shows_by_date = checkpoint.get('shows_by_date', {})

    print(f"Resuming: {len(processed_dates)} dates already processed, {len(catalog.get('shows', []))} shows in catalog")

    # =========================================================================
    # PHASE 1: Discover all show dates (if not done)
    # =========================================================================

    if not shows_by_date:
        print("\nPhase 1: Discovering all show dates...")

        result = search_shows(rows=0)
        total = result['response']['numFound']
        print(f"Total recordings in collection: {total:,}")

        page = 1
        rows = 100
        fetched = 0

        while fetched < total:
            result = None
            for attempt in range(5):
                result = search_shows(page=page, rows=rows)
                if 'response' in result and result['response'].get('docs'):
                    break
                wait = min(2 ** (attempt + 1), 30)
                print(f"  Retry {attempt+1}/5, waiting {wait}s...", flush=True)
                time.sleep(wait)

            if not result or 'response' not in result:
                print(f"  Stopping at page {page} due to errors. Run again to resume.")
                break

            docs = result['response']['docs']
            if not docs:
                break

            for doc in docs:
                date = doc.get('date', '')[:10]
                if date and date.startswith('19'):
                    if date not in shows_by_date:
                        shows_by_date[date] = []
                    shows_by_date[date].append({
                        'identifier': doc['identifier'],
                        'rating': doc.get('avg_rating', 0) or 0,
                        'venue': doc.get('venue', ''),
                        'coverage': doc.get('coverage', '')
                    })

            fetched += len(docs)
            if fetched % 500 == 0:
                print(f"  {fetched:,}/{total:,} recordings, {len(shows_by_date):,} unique dates...", flush=True)
                # Save progress
                checkpoint['shows_by_date'] = shows_by_date
                save_checkpoint(checkpoint)

            page += 1
            time.sleep(SEARCH_DELAY)

        checkpoint['shows_by_date'] = shows_by_date
        save_checkpoint(checkpoint)
        print(f"\nDiscovered {len(shows_by_date):,} unique show dates!")

    else:
        print(f"\nPhase 1 complete: {len(shows_by_date):,} show dates already discovered")

    # =========================================================================
    # PHASE 2: Extract setlists (resumable)
    # =========================================================================

    print("\nPhase 2: Extracting setlists and durations...")

    dates_to_process = sorted(set(shows_by_date.keys()) - processed_dates)
    print(f"Remaining dates to process: {len(dates_to_process):,}")

    if not dates_to_process:
        print("\nAll dates processed! Catalog complete.")
        show_status()
        return catalog

    added = 0
    errors = 0

    for i, date in enumerate(dates_to_process):
        recordings = shows_by_date[date]
        best = max(recordings, key=lambda x: x['rating'])

        metadata = get_show_metadata(best['identifier'])
        if not metadata:
            errors += 1
            processed_dates.add(date)
            continue

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
                song = track['song']
                if song not in catalog['songs']:
                    catalog['songs'][song] = {'performances': []}
                catalog['songs'][song]['performances'].append({
                    'date': date,
                    'duration': track['duration'],
                    'duration_str': track['duration_str'],
                    'venue': venue
                })

            added += 1

        processed_dates.add(date)

        # Save progress periodically
        if (i + 1) % SAVE_EVERY == 0:
            checkpoint['processed_dates'] = list(processed_dates)
            save_checkpoint(checkpoint)
            save_catalog(catalog)
            pct = 100 * len(processed_dates) / len(shows_by_date)
            print(f"  {i+1}/{len(dates_to_process)} this session | Total: {len(catalog['shows'])} shows, {len(catalog['songs'])} songs ({pct:.1f}%)")

        time.sleep(METADATA_DELAY)

    # Final save
    checkpoint['processed_dates'] = list(processed_dates)
    save_checkpoint(checkpoint)
    save_catalog(catalog)

    print(f"\nSession complete! Added {added} shows, {errors} errors")
    show_status()
    return catalog


if __name__ == '__main__':
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ['--status', '-s', 'status']:
            show_status()
        elif arg in ['--reset', '-r', 'reset']:
            build_catalog(reset=True)
        elif arg in ['--help', '-h']:
            print(__doc__)
        else:
            build_catalog()
    else:
        build_catalog()
