#!/usr/bin/env python3
"""
Refresh ALL band data caches that ship in git, so the deployed snapshot is current.

JamMuse serves a *checked-in snapshot* of each band's data — production (Vercel) has
a read-only filesystem and there is no live refresh on the request path. This script
re-pulls every source and rewrites the cache files; commit + push them to redeploy.

Cache layers refreshed:
  - Setlist.fm setlists   (umphreys, wsp, moe, sts9, billy)  -> data/setlistfm_cache/
  - Phantasy Tour shows   (sci, biscuits)                     -> data/pt_cache/
  - Relisten durations    (all 9 relisten bands)              -> data/relisten_cache/
  - Phish (.net/.in)      via refresh_data.py --recent        -> data/...

Usage:
    python scripts/refresh_all.py                 # everything (default)
    python scripts/refresh_all.py --skip-phish    # all non-Phish bands
    python scripts/refresh_all.py --bands umphreys,wsp
    python scripts/refresh_all.py --relisten-only # just duration indexes
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from scripts.setlistfm_engine import SetlistFMEngine, SETLISTFM_BANDS
from scripts.phantasytour_engine import PhantasyTourEngine, PT_BANDS
from scripts.relisten_engine import RELISTEN_BANDS


def _latest_setlistfm_date(setlists):
    """Max eventDate (stored DD-MM-YYYY) as a sortable YYYY-MM-DD string."""
    best = None
    for s in setlists:
        ev = s.get("eventDate")
        if not ev or len(ev) != 10:
            continue
        d, m, y = ev[:2], ev[3:5], ev[6:]
        iso = f"{y}-{m}-{d}"
        if best is None or iso > best:
            best = iso
    return best or "unknown"


def refresh_setlistfm(bands):
    print(f"\n=== Setlist.fm ({len(bands)} bands) ===")
    results = []
    for band in bands:
        try:
            eng = SetlistFMEngine(band)
            eng._setlists_cache = None  # force live re-pull (caches are >1h old)
            t0 = time.time()
            setlists = eng._get_all_setlists()
            latest = _latest_setlistfm_date(setlists)
            print(f"  {band:>10}: {len(setlists):>5} shows, latest {latest}  ({time.time()-t0:.0f}s)")
            results.append((band, len(setlists), latest))
        except Exception as e:
            print(f"  {band:>10}: FAILED — {e}")
            results.append((band, 0, "FAILED"))
    return results


def refresh_pt(bands):
    print(f"\n=== Phantasy Tour ({len(bands)} bands) ===")
    results = []
    for band in bands:
        try:
            eng = PhantasyTourEngine(band)
            eng._shows_cache = None
            t0 = time.time()
            shows = eng._get_all_shows()
            print(f"  {band:>10}: {len(shows):>5} shows  ({time.time()-t0:.0f}s)")
            results.append((band, len(shows)))
        except Exception as e:
            print(f"  {band:>10}: FAILED — {e}")
            results.append((band, 0))
    return results


def refresh_relisten(bands):
    print(f"\n=== Relisten duration indexes ({len(bands)} bands) ===")
    cmd = [sys.executable, str(ROOT / "scripts" / "precompute_relisten_indexes.py"), "--force"]
    if len(bands) == 1:
        cmd += ["--band", bands[0]]
    # precompute already loops all RELISTEN_BANDS when no --band is given
    rc = subprocess.call(cmd, cwd=str(ROOT))
    if rc != 0:
        print(f"  WARNING: precompute exited with code {rc}")
    return rc


def refresh_phish():
    print("\n=== Phish (.net + .in) ===")
    cmd = [sys.executable, str(ROOT / "scripts" / "refresh_data.py"), "--recent"]
    rc = subprocess.call(cmd, cwd=str(ROOT))
    if rc != 0:
        print(f"  WARNING: refresh_data exited with code {rc}")
    return rc


def main():
    parser = argparse.ArgumentParser(description="Refresh all JamMuse band data caches")
    parser.add_argument("--bands", help="Comma-separated band keys to limit to (across all sources)")
    parser.add_argument("--skip-phish", action="store_true", help="Skip the Phish refresh")
    parser.add_argument("--relisten-only", action="store_true", help="Only rebuild Relisten duration indexes")
    args = parser.parse_args()

    only = set(b.strip() for b in args.bands.split(",")) if args.bands else None

    def pick(keys):
        return [b for b in keys if (only is None or b in only)]

    t_start = time.time()

    if args.relisten_only:
        refresh_relisten(pick(RELISTEN_BANDS.keys()))
    else:
        sf = refresh_setlistfm(pick(SETLISTFM_BANDS.keys()))
        pt = refresh_pt(pick(PT_BANDS.keys()))
        refresh_relisten(pick(RELISTEN_BANDS.keys()))
        if not args.skip_phish and (only is None or "phish" in only):
            refresh_phish()

    print(f"\nDone in {time.time()-t_start:.0f}s. Commit the updated data/ files and push to redeploy.")


if __name__ == "__main__":
    main()
