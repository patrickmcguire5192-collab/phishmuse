#!/usr/bin/env python3
"""
Build the PhanDuel duration index.

Iterates every Phish track in Phish.in v2, groups by show to compute the
"longest of show" for every date, then aggregates per-song stats for the
jam-vehicle roster PhanDuel actually predicts on.

Output shape:
{
  "meta": {
    "generated_at": "...",
    "shows_analyzed": <int>,
    "total_tracks": <int>,
    "jam_vehicles": [...],
    "source": "phish.in v2"
  },
  "songs": {
    "Tweezer": {
      "plays": 456,
      "longest_of_show_count": 87,
      "longest_of_show_rate": 0.191,
      "fifteen_plus_count": 132,
      "twenty_plus_count": 61,
      "twenty_five_plus_count": 27,
      "twenty_plus_rate": 0.134,
      "twenty_five_plus_rate": 0.059,
      "mean_min": 12.4,
      "median_min": 10.9,
      "p90_min": 22.3,
      "max_min": 51.7,
      "slug": "tweezer"
    },
    ...
  }
}

Consumed by phanduel-app/src/services/durationEngine.js (checked into
phanduel-app/public/duration_index.json).

Not run automatically. Refresh manually when you want to fold in recent
shows (e.g. mid-tour, after a monster jam night).
"""
from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import mean, median

import urllib.request
import urllib.error

# The 28 songs the current App.js hardcodes as jam-vehicle candidates.
# Keep this list in sync with App.js's jamVehicleData dict.
JAM_VEHICLES = [
    "You Enjoy Myself", "Ruby Waves", "Soul Planet", "Fluffhead", "Drowned",
    "Mercury", "Everything's Right", "Ghost", "David Bowie", "Tweezer",
    "Harry Hood", "Down with Disease", "Simple", "Piper", "Bathtub Gin",
    "Light", "Carini", "Stash", "Run Like an Antelope", "Chalk Dust Torture",
    "Split Open and Melt", "Sand", "Slave to the Traffic Light", "Mike's Song",
    "Reba", "Fuego", "No Men In No Man's Land", "Waves",
]

# Alias table for names that don't slugify cleanly. Phish.in slugs are
# stable so we can hand-curate the awkward ones.
SLUG_OVERRIDES = {
    "No Men In No Man's Land": "no-men-in-no-mans-land",
}


def song_to_slug(name: str) -> str:
    if name in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[name]
    s = name.lower()
    s = re.sub(r"[‘’']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


USER_AGENT = "phanduel-duration-index/1.0 (+https://phanduel-app.vercel.app)"


def fetch_json(url: str, retries: int = 3) -> dict:
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"fetch_json failed after {retries} tries: {url}\n  {last_err}")


def paginate_all_tracks() -> list[dict]:
    """Pull every Phish track from Phish.in v2. ~26k tracks / ~130 pages."""
    per_page = 200
    page = 1
    all_tracks: list[dict] = []
    while True:
        url = f"https://phish.in/api/v2/tracks?per_page={per_page}&page={page}"
        data = fetch_json(url)
        tracks = data.get("tracks", [])
        if not tracks:
            break
        for t in tracks:
            all_tracks.append({
                "date": t.get("show_date"),
                "slug": t.get("slug"),
                "title": t.get("title"),
                "duration_ms": t.get("duration") or 0,
                "position": t.get("position"),
                "venue": t.get("venue_name"),
                "exclude": bool(t.get("exclude_from_stats")),
            })
        total_pages = data.get("total_pages") or 1
        print(f"  page {page}/{total_pages}  cum {len(all_tracks)}", file=sys.stderr)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.25)  # be polite
    return all_tracks


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    frac = k - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def aggregate(tracks: list[dict], jam_vehicles: list[str]) -> dict:
    # Only real performances contribute — Phish.in flags jam breakouts,
    # crowd noise, tuning, etc. with exclude_from_stats.
    tracks = [t for t in tracks if not t["exclude"] and t["duration_ms"] > 0 and t["date"]]

    # Longest track per show (by date) — used to count "was longest of show".
    longest_by_date: dict[str, dict] = {}
    for t in tracks:
        cur = longest_by_date.get(t["date"])
        if cur is None or t["duration_ms"] > cur["duration_ms"]:
            longest_by_date[t["date"]] = t

    # Index tracks by canonical title for the jam vehicles.
    # Phish.in titles match Phish.net titles closely, but we normalize case
    # and strip whitespace to avoid surprises.
    canonical = {name.strip().lower(): name for name in jam_vehicles}
    per_song: dict[str, dict] = {name: {
        "plays": 0, "longest_of_show_count": 0,
        "fifteen_plus_count": 0, "twenty_plus_count": 0, "twenty_five_plus_count": 0,
        "durations_min": [],
        "slug": song_to_slug(name),
    } for name in jam_vehicles}

    for t in tracks:
        title = (t["title"] or "").strip().lower()
        display = canonical.get(title)
        if not display:
            continue
        s = per_song[display]
        d_min = t["duration_ms"] / 60000.0
        s["plays"] += 1
        s["durations_min"].append(d_min)
        if d_min >= 15: s["fifteen_plus_count"] += 1
        if d_min >= 20: s["twenty_plus_count"] += 1
        if d_min >= 25: s["twenty_five_plus_count"] += 1
        # was this THE longest of its show?
        longest = longest_by_date.get(t["date"])
        if longest and longest.get("title") and longest["title"].strip().lower() == title \
                and longest.get("duration_ms") == t.get("duration_ms"):
            s["longest_of_show_count"] += 1

    out_songs = {}
    for name, s in per_song.items():
        durations = s["durations_min"]
        plays = s["plays"]
        if plays == 0:
            print(f"  ⚠️  {name}: 0 plays — slug '{s['slug']}' may be wrong", file=sys.stderr)
            continue
        out_songs[name] = {
            "slug": s["slug"],
            "plays": plays,
            "longest_of_show_count": s["longest_of_show_count"],
            "longest_of_show_rate": round(s["longest_of_show_count"] / plays, 4),
            "fifteen_plus_count": s["fifteen_plus_count"],
            "twenty_plus_count": s["twenty_plus_count"],
            "twenty_five_plus_count": s["twenty_five_plus_count"],
            "fifteen_plus_rate": round(s["fifteen_plus_count"] / plays, 4),
            "twenty_plus_rate": round(s["twenty_plus_count"] / plays, 4),
            "twenty_five_plus_rate": round(s["twenty_five_plus_count"] / plays, 4),
            "mean_min": round(mean(durations), 2),
            "median_min": round(median(durations), 2),
            "p90_min": round(percentile(durations, 0.90), 2),
            "max_min": round(max(durations), 2),
        }

    return {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "shows_analyzed": len(longest_by_date),
            "total_tracks": len(tracks),
            "jam_vehicles": jam_vehicles,
            "source": "phish.in v2",
            "endpoint": "/api/v2/tracks",
        },
        "songs": out_songs,
    }


def main():
    out_arg = None
    for i, a in enumerate(sys.argv[1:]):
        if a == "--out" and i + 1 < len(sys.argv) - 1:
            out_arg = sys.argv[i + 2]

    print("Pulling every Phish track from Phish.in v2...", file=sys.stderr)
    t0 = time.time()
    tracks = paginate_all_tracks()
    dt = time.time() - t0
    print(f"Pulled {len(tracks)} tracks in {dt:.1f}s", file=sys.stderr)

    print("Aggregating jam-vehicle stats...", file=sys.stderr)
    result = aggregate(tracks, JAM_VEHICLES)

    if out_arg:
        Path(out_arg).write_text(json.dumps(result, indent=2))
        print(f"Wrote {out_arg}", file=sys.stderr)
    else:
        json.dump(result, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
