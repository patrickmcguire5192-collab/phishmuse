#!/usr/bin/env python3
"""
Pre-compute Relisten duration indexes for all bands.

Run this locally to refresh the cached duration indexes that ship in git.
The cached indexes load in milliseconds; rebuilding from raw API/cache
takes 10-30s per band (the cold-start cost we want to avoid on Vercel).

Usage:
    python scripts/precompute_relisten_indexes.py            # refresh stale only
    python scripts/precompute_relisten_indexes.py --force    # rebuild all
    python scripts/precompute_relisten_indexes.py --band sci # one band
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.relisten_engine import RelistenDurationEngine, RELISTEN_BANDS, INDEX_DIR

STALE_AFTER_DAYS = 7  # rebuild any index older than this in the default mode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Rebuild all bands even if fresh")
    parser.add_argument("--band", help="Rebuild only this band key")
    args = parser.parse_args()

    targets = [args.band] if args.band else list(RELISTEN_BANDS.keys())
    print(f"Targeting {len(targets)} band(s): {', '.join(targets)}")
    print()

    rebuilt = []
    skipped = []
    for band_key in targets:
        idx_path = INDEX_DIR / f"{band_key}_duration_index.json"
        if idx_path.exists() and not args.force:
            try:
                loaded = json.loads(idx_path.read_text())
                age_days = (time.time() - loaded.get("built_at", 0)) / 86400
                if age_days < STALE_AFTER_DAYS:
                    print(f"  {band_key:>10}: fresh ({age_days:.1f}d old, {len(loaded.get('tracks', {}))} songs) — skip")
                    skipped.append(band_key)
                    continue
                print(f"  {band_key:>10}: stale ({age_days:.1f}d old) — rebuilding...")
            except (json.JSONDecodeError, OSError):
                print(f"  {band_key:>10}: corrupt — rebuilding...")
        else:
            print(f"  {band_key:>10}: not cached — building...")

        engine = RelistenDurationEngine(band_key)
        # Force rebuild by deleting then loading
        if idx_path.exists():
            idx_path.unlink()
        engine._duration_index = None  # clear in-memory
        index = engine._load_or_build_index()
        rebuilt.append((band_key, len(index.get("tracks", {})), index.get("total_shows", 0)))

    print()
    print(f"Rebuilt: {len(rebuilt)} band(s)")
    for band_key, songs, shows in rebuilt:
        print(f"  {band_key:>10}: {songs:>6} songs, {shows:>5} shows")
    print(f"Skipped: {len(skipped)} fresh band(s)")
    print()
    print("Commit the updated *_duration_index.json files and push.")


if __name__ == "__main__":
    main()
