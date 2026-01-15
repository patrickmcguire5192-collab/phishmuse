#!/usr/bin/env python3
"""
PhishStats Data Refresh Script
==============================

Pulls data from Phish.net and Phish.in APIs, saves locally as JSON.
Can run as full refresh or incremental (new shows only).

Usage:
    python scripts/refresh_data.py --full     # Full historical pull
    python scripts/refresh_data.py --recent   # Last 30 days only
    python scripts/refresh_data.py --compute  # Just recompute stats
"""

import json
import argparse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import time

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
COMPUTED_DIR = DATA_DIR / "computed"

# API Keys
PHISHNET_API_KEY = "69F3065FB7F44C387CE5"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
COMPUTED_DIR.mkdir(parents=True, exist_ok=True)


def fetch_json(url, headers=None):
    """Fetch JSON from URL with error handling."""
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None


def pull_phishnet_shows(years=None):
    """Pull show/setlist data from Phish.net API."""
    if years is None:
        # Full historical pull: 1983-present
        years = range(1983, datetime.now().year + 1)

    all_shows = {}
    all_setlist_entries = []

    print(f"Pulling Phish.net data for {len(list(years))} years...")

    for year in years:
        url = f"https://api.phish.net/v5/setlists/showyear/{year}.json?apikey={PHISHNET_API_KEY}"
        data = fetch_json(url)

        if data and data.get("data"):
            entries = [e for e in data["data"] if e.get("artistid") == 1]  # Phish only
            all_setlist_entries.extend(entries)

            # Group by show
            for entry in entries:
                show_id = entry["showid"]
                if show_id not in all_shows:
                    all_shows[show_id] = {
                        "showid": show_id,
                        "showdate": entry["showdate"],
                        "venue": entry.get("venue"),
                        "city": entry.get("city"),
                        "state": entry.get("state"),
                        "country": entry.get("country"),
                        "venueid": entry.get("venueid"),
                        "songs": []
                    }

                all_shows[show_id]["songs"].append({
                    "song": entry["song"],
                    "position": entry.get("position"),
                    "set": entry.get("set"),
                    "isjamchart": entry.get("isjamchart"),
                    "gap": entry.get("gap")
                })

            print(f"  {year}: {len(entries)} setlist entries")

        time.sleep(0.2)  # Be nice to the API

    return list(all_shows.values()), all_setlist_entries


def pull_phishin_durations(song_slugs):
    """Pull duration data from Phish.in API for specified songs."""
    all_durations = {}

    print(f"Pulling Phish.in durations for {len(song_slugs)} songs...")

    for slug in song_slugs:
        url = f"https://phish.in/api/v2/tracks?song_slug={slug}&per_page=2000"
        data = fetch_json(url, headers={"Accept": "application/json"})

        if data and data.get("tracks"):
            tracks = data["tracks"]
            all_durations[slug] = [
                {
                    "date": t.get("show_date"),
                    "duration_ms": t.get("duration"),
                    "duration_min": t.get("duration", 0) / 60000,
                    "venue": t.get("venue_name"),
                    "position": t.get("position")
                }
                for t in tracks if t.get("duration")
            ]
            print(f"  {slug}: {len(all_durations[slug])} tracks with duration")

        time.sleep(0.3)  # Be nice to the API

    return all_durations


def compute_song_stats(setlist_entries, durations):
    """Compute aggregate statistics for each song."""
    print("Computing song statistics...")

    song_stats = defaultdict(lambda: {
        "play_count": 0,
        "jamchart_count": 0,
        "opener_count": 0,
        "encore_count": 0,
        "first_played": None,
        "last_played": None,
        "venues": set(),
        "years": set()
    })

    for entry in setlist_entries:
        song = entry["song"]
        stats = song_stats[song]

        stats["play_count"] += 1

        if entry.get("isjamchart") in ["1", 1]:
            stats["jamchart_count"] += 1

        if entry.get("set") == "1" and entry.get("position") == 1:
            stats["opener_count"] += 1

        if entry.get("set") in ["e", "e2", "E"]:
            stats["encore_count"] += 1

        date = entry.get("showdate")
        if date:
            if stats["first_played"] is None or date < stats["first_played"]:
                stats["first_played"] = date
            if stats["last_played"] is None or date > stats["last_played"]:
                stats["last_played"] = date
            stats["years"].add(date[:4])

        if entry.get("venueid"):
            stats["venues"].add(entry["venueid"])

    # Convert sets to lists for JSON serialization
    for song, stats in song_stats.items():
        stats["venues"] = list(stats["venues"])
        stats["years"] = sorted(list(stats["years"]))
        stats["venue_count"] = len(stats["venues"])
        stats["year_count"] = len(stats["years"])

    return dict(song_stats)


def compute_duration_stats(durations):
    """Compute duration statistics from Phish.in data."""
    print("Computing duration statistics...")

    duration_stats = {}

    for slug, tracks in durations.items():
        if not tracks:
            continue

        times = [t["duration_min"] for t in tracks]

        # Find longest
        longest_track = max(tracks, key=lambda t: t["duration_ms"])

        # Count monsters (25+ min)
        monsters = [t for t in tracks if t["duration_min"] >= 25]

        duration_stats[slug] = {
            "track_count": len(tracks),
            "avg_duration_min": sum(times) / len(times),
            "median_duration_min": sorted(times)[len(times) // 2],
            "min_duration_min": min(times),
            "max_duration_min": max(times),
            "longest": {
                "date": longest_track["date"],
                "duration_min": longest_track["duration_min"],
                "venue": longest_track["venue"]
            },
            "monster_count": len(monsters),
            "monster_rate": len(monsters) / len(tracks) if tracks else 0,
            "monsters": [
                {"date": t["date"], "duration_min": round(t["duration_min"], 1), "venue": t["venue"]}
                for t in sorted(monsters, key=lambda x: -x["duration_min"])[:10]
            ]
        }

    return duration_stats


def compute_venue_stats(shows):
    """Compute statistics by venue."""
    print("Computing venue statistics...")

    venue_stats = defaultdict(lambda: {
        "show_count": 0,
        "songs_played": set(),
        "first_show": None,
        "last_show": None,
        "name": None,
        "city": None,
        "state": None,
        "country": None
    })

    for show in shows:
        venue_id = show.get("venueid")
        if not venue_id:
            continue

        stats = venue_stats[venue_id]
        stats["show_count"] += 1
        stats["name"] = show.get("venue")
        stats["city"] = show.get("city")
        stats["state"] = show.get("state")
        stats["country"] = show.get("country")

        date = show.get("showdate")
        if date:
            if stats["first_show"] is None or date < stats["first_show"]:
                stats["first_show"] = date
            if stats["last_show"] is None or date > stats["last_show"]:
                stats["last_show"] = date

        for song_entry in show.get("songs", []):
            stats["songs_played"].add(song_entry["song"])

    # Convert sets to counts
    for venue_id, stats in venue_stats.items():
        stats["unique_songs"] = len(stats["songs_played"])
        stats["songs_played"] = list(stats["songs_played"])

    return dict(venue_stats)


def save_json(data, filepath):
    """Save data as formatted JSON."""
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Saved: {filepath}")


def main():
    parser = argparse.ArgumentParser(description="PhishStats Data Refresh")
    parser.add_argument("--full", action="store_true", help="Full historical pull (1983-present)")
    parser.add_argument("--recent", action="store_true", help="Recent data only (current year)")
    parser.add_argument("--compute", action="store_true", help="Just recompute stats from existing data")
    parser.add_argument("--durations", action="store_true", help="Pull duration data from Phish.in")
    args = parser.parse_args()

    # Default to recent if no args
    if not any([args.full, args.recent, args.compute, args.durations]):
        args.recent = True

    print("=" * 60)
    print("PhishStats Data Refresh")
    print("=" * 60)

    shows = []
    setlist_entries = []

    # Pull Phish.net data
    if args.full:
        print("\n[1/4] Full historical pull from Phish.net...")
        shows, setlist_entries = pull_phishnet_shows()
        save_json(shows, RAW_DIR / "shows.json")
        save_json(setlist_entries, RAW_DIR / "setlists.json")

    elif args.recent:
        print("\n[1/4] Recent data pull from Phish.net...")
        current_year = datetime.now().year
        shows, setlist_entries = pull_phishnet_shows(years=[current_year - 1, current_year])

        # Merge with existing if available
        existing_shows_path = RAW_DIR / "shows.json"
        if existing_shows_path.exists():
            with open(existing_shows_path) as f:
                existing = json.load(f)
            existing_ids = {s["showid"] for s in existing}
            new_shows = [s for s in shows if s["showid"] not in existing_ids]
            shows = existing + new_shows
            print(f"  Merged {len(new_shows)} new shows with {len(existing)} existing")

        save_json(shows, RAW_DIR / "shows.json")

    elif args.compute:
        print("\n[1/4] Loading existing data...")
        with open(RAW_DIR / "shows.json") as f:
            shows = json.load(f)
        with open(RAW_DIR / "setlists.json") as f:
            setlist_entries = json.load(f)

    # Pull Phish.in durations
    if args.durations or args.full:
        print("\n[2/4] Pulling duration data from Phish.in...")

        # Key songs we want duration data for
        song_slugs = [
            "you-enjoy-myself", "tweezer", "down-with-disease", "ghost",
            "david-bowie", "harry-hood", "ruby-waves", "soul-planet",
            "everythings-right", "mercury", "fluffhead", "drowned",
            "simple", "piper", "bathtub-gin", "run-like-an-antelope",
            "chalk-dust-torture", "stash", "reba", "slave-to-the-traffic-light",
            "light", "carini", "fuego", "sand", "wolfmans-brother",
            "twist", "split-open-and-melt", "mikes-song", "maze",
            "weekapaug-groove", "prince-caspian", "tube", "crosseyed-and-painless",
            "possum", "free", "blaze-on", "waves", "no-mens-land",
            "set-your-soul-free", "2001", "golden-age", "birds-of-a-feather",
            "steam", "cities", "scents-and-subtle-sounds", "theme-from-the-bottom",
            "limb-by-limb", "chalkdust-torture-reprise", "ya-mar"
        ]

        durations = pull_phishin_durations(song_slugs)
        save_json(durations, RAW_DIR / "durations.json")
    else:
        # Load existing durations if available
        durations_path = RAW_DIR / "durations.json"
        if durations_path.exists():
            with open(durations_path) as f:
                durations = json.load(f)
        else:
            durations = {}

    # Compute stats
    print("\n[3/4] Computing aggregate statistics...")

    if setlist_entries or (RAW_DIR / "setlists.json").exists():
        if not setlist_entries:
            with open(RAW_DIR / "setlists.json") as f:
                setlist_entries = json.load(f)

        song_stats = compute_song_stats(setlist_entries, durations)
        save_json(song_stats, COMPUTED_DIR / "song_stats.json")

    if durations:
        duration_stats = compute_duration_stats(durations)
        save_json(duration_stats, COMPUTED_DIR / "duration_stats.json")

    if shows or (RAW_DIR / "shows.json").exists():
        if not shows:
            with open(RAW_DIR / "shows.json") as f:
                shows = json.load(f)

        venue_stats = compute_venue_stats(shows)
        save_json(venue_stats, COMPUTED_DIR / "venue_stats.json")

    # Create metadata file
    print("\n[4/4] Writing metadata...")
    metadata = {
        "last_refresh": datetime.now().isoformat(),
        "show_count": len(shows) if shows else 0,
        "song_count": len(song_stats) if 'song_stats' in dir() else 0,
        "duration_songs": len(durations) if durations else 0,
        "refresh_type": "full" if args.full else "recent" if args.recent else "compute"
    }
    save_json(metadata, DATA_DIR / "metadata.json")

    print("\n" + "=" * 60)
    print("Refresh complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
