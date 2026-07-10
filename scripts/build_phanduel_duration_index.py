#!/usr/bin/env python3
"""
Build the PhanDuel duration index (v2 — time-decay weighted).

Iterates every Phish track in Phish.in v2, groups by show to compute the
"longest of show" for every date, then aggregates per-song stats with an
EXPONENTIAL TIME DECAY so the index models the band as it plays *today*,
not the 43-year institution. A 1994 Bowie should not out-vote a 2026 Fuego.

Weighting: each performance gets weight 0.5 ** (age_years / half_life).
With the default 1.5y half-life, a show from last summer weighs ~0.6,
a 2021 show ~0.1, and the 90s effectively zero. Rates are shrunk toward
the pooled prior (Bayesian, K effective plays) so a song with 3 recent
plays and 2 longest-of-show wins ranks high but doesn't run the table.

The candidate list is DATA-DERIVED (top N by weighted longest-of-show
score), not hand-picked — this is how What's Going Through Your Mind,
A Wave of Hope, and A Song I Heard the Ocean Sing get in, and how
David Bowie (99 all-time wins, none since the 90s mattered) drops out.

Output shape:
{
  "meta": {
    "generated_at", "reference_date", "source", "endpoint",
    "half_life_years", "shrink_k", "candidate_floor_weighted_plays",
    "shows_analyzed", "total_tracks", "weighted_shows",
    "prior_longest_rate", "prior_twenty_plus_rate",
    "coverage_by_topn": {"10": 0.55, "15": 0.68, ...}   # weighted share of
        # shows whose longest song is inside the top-N emitted candidates —
        # the engine uses this to price the "Other" bucket honestly.
  },
  "songs": {
    "Tweezer": {
      "weighted_plays": 19.3,
      "play_rate": 0.234,            # weighted P(played on a given night)
      "longest_rate": 0.535,         # weighted+shrunk P(longest of show | played)
      "twenty_plus_rate": 0.301,     # weighted+shrunk P(20+ min | played)
      "recent_mean_min": 15.8,       # last-3y unweighted display stats
      "recent_p90_min": 25.1,
      "recent_max_min": 30.3,
      "plays_alltime": 429,
      "longest_of_show_alltime": 95,
      "max_min_alltime": 50.3,
      "last_played": "2026-04-25"
    }, ...
  }
}

Consumed by phanduel-app/src/services/durationEngine.js (checked into
phanduel-app/public/duration_index.json).

Usage:
  python3 scripts/build_phanduel_duration_index.py --out .../duration_index.json
  # iterate on weights without re-pulling ~39k tracks (130 API pages):
  python3 ... --tracks-cache /tmp/all_tracks.json

Not run automatically. Refresh manually mid-tour (a couple minutes) so the
decay reference date tracks the latest show.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from statistics import mean

import urllib.request
import urllib.error

USER_AGENT = "phanduel-duration-index/2.0 (+https://phanduel-app.vercel.app)"

DEFAULT_HALF_LIFE_YEARS = 1.5
DEFAULT_SHRINK_K = 3.0
DEFAULT_CANDIDATE_FLOOR = 1.0   # min weighted plays to be considered at all
DEFAULT_TOP_N = 40              # candidates emitted to JSON
RECENT_WINDOW_YEARS = 3         # unweighted display stats window


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
    """Pull every Phish track from Phish.in v2. ~39k tracks."""
    per_page = 500
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
                "title": t.get("title"),
                "slug": t.get("slug"),
                "duration_ms": t.get("duration") or 0,
                "exclude": bool(t.get("exclude_from_stats")),
            })
        total_pages = data.get("total_pages") or 1
        print(f"  page {page}/{total_pages}  cum {len(all_tracks)}", file=sys.stderr)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.25)
    return all_tracks


def parse_date(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    frac = k - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def aggregate(tracks: list[dict], half_life_years: float, shrink_k: float,
              candidate_floor: float, top_n: int) -> dict:
    # Only real performances — Phish.in flags soundchecks/banter/etc.
    tracks = [t for t in tracks if not t["exclude"] and t["duration_ms"] > 0 and t["date"]]

    ref_date = max(parse_date(t["date"]) for t in tracks)
    hl_days = 365.25 * half_life_years

    def weight(date_str: str) -> float:
        return 0.5 ** ((ref_date - parse_date(date_str)).days / hl_days)

    # Group by show; find the longest track of each show.
    by_show: dict[str, list[dict]] = {}
    for t in tracks:
        by_show.setdefault(t["date"], []).append(t)
    longest_of: dict[str, str] = {
        d: max(ts, key=lambda x: x["duration_ms"])["title"] for d, ts in by_show.items()
    }

    show_weights = {d: weight(d) for d in by_show}
    weighted_shows = sum(show_weights.values())

    recent_cutoff = date(ref_date.year - RECENT_WINDOW_YEARS, ref_date.month, ref_date.day).isoformat()

    # Per-song accumulation.
    agg: dict[str, dict] = {}
    for t in tracks:
        s = agg.setdefault(t["title"], {
            "wplays": 0.0, "wlongest": 0.0, "wtwenty": 0.0,
            "plays_alltime": 0, "longest_alltime": 0,
            "max_ms_alltime": 0, "last_played": t["date"],
            "recent_durations_min": [], "slug": t["slug"],
        })
        w = weight(t["date"])
        s["wplays"] += w
        s["plays_alltime"] += 1
        s["max_ms_alltime"] = max(s["max_ms_alltime"], t["duration_ms"])
        s["last_played"] = max(s["last_played"], t["date"])
        if longest_of[t["date"]] == t["title"]:
            s["wlongest"] += w
            s["longest_alltime"] += 1
        if t["duration_ms"] >= 20 * 60000:
            s["wtwenty"] += w
        if t["date"] >= recent_cutoff:
            s["recent_durations_min"].append(t["duration_ms"] / 60000.0)

    # Pooled priors. Exactly one longest per show → prior P(longest|played)
    # is weighted_shows / total weighted plays (~1/22 songs a night).
    total_wplays = sum(s["wplays"] for s in agg.values())
    prior_longest = weighted_shows / total_wplays
    prior_twenty = sum(s["wtwenty"] for s in agg.values()) / total_wplays

    def shrunk(hits: float, n: float, prior: float) -> float:
        return (hits + shrink_k * prior) / (n + shrink_k)

    # Score + rank candidates.
    scored = []
    for title, s in agg.items():
        if s["wplays"] < candidate_floor:
            continue
        play_rate = s["wplays"] / weighted_shows
        longest_rate = shrunk(s["wlongest"], s["wplays"], prior_longest)
        score = play_rate * longest_rate
        scored.append((score, title))
    scored.sort(reverse=True)
    candidates = [title for _, title in scored[:top_n]]

    # Coverage of the top-N emitted sets — lets the engine price "Other"
    # from measurement instead of a hardcoded 85%.
    coverage_by_topn = {}
    for n in range(10, top_n + 1, 5):
        names = set(candidates[:n])
        cov = sum(w for d, w in show_weights.items() if longest_of[d] in names) / weighted_shows
        coverage_by_topn[str(n)] = round(cov, 4)

    out_songs = {}
    for title in candidates:
        s = agg[title]
        recent = s["recent_durations_min"]
        out_songs[title] = {
            "slug": s["slug"],
            "weighted_plays": round(s["wplays"], 2),
            "play_rate": round(s["wplays"] / weighted_shows, 4),
            "longest_rate": round(shrunk(s["wlongest"], s["wplays"], prior_longest), 4),
            "twenty_plus_rate": round(shrunk(s["wtwenty"], s["wplays"], prior_twenty), 4),
            "recent_mean_min": round(mean(recent), 2) if recent else None,
            "recent_p90_min": round(percentile(recent, 0.90), 2) if recent else None,
            "recent_max_min": round(max(recent), 2) if recent else None,
            "recent_plays": len(recent),
            "plays_alltime": s["plays_alltime"],
            "longest_of_show_alltime": s["longest_alltime"],
            "max_min_alltime": round(s["max_ms_alltime"] / 60000.0, 2),
            "last_played": s["last_played"],
        }

    # ---- Over/Under markets, from the same weighted per-show distributions ----
    # Longest-of-show minutes: line ladder around the weighted median.
    longest_min_by_show = {d: max(x["duration_ms"] for x in ts) / 60000.0
                           for d, ts in by_show.items()}
    sorted_pairs = sorted((longest_min_by_show[d], show_weights[d]) for d in by_show)
    cum, wmedian = 0.0, sorted_pairs[-1][0]
    for v, wt in sorted_pairs:
        cum += wt
        if cum >= weighted_shows / 2:
            wmedian = v
            break
    wmean_longest = sum(longest_min_by_show[d] * show_weights[d] for d in by_show) / weighted_shows

    def p_over_line(line: float) -> float:
        return sum(show_weights[d] for d in by_show
                   if longest_min_by_show[d] > line) / weighted_shows

    # Half-minute lines only (no pushes); pick whichever brackets the
    # weighted median with p_over closest to a fair coin flip.
    lo_line = int(wmedian) + 0.5 if wmedian % 1 >= 0.5 else int(wmedian) - 0.5
    hi_line = lo_line + 1
    primary_line = min((lo_line, hi_line), key=lambda l: abs(p_over_line(l) - 0.5))
    line_ladder = []
    for line in [primary_line - 2, primary_line, primary_line + 2]:
        line_ladder.append({"line": round(line, 1), "p_over": round(p_over_line(line), 4)})

    # 20+ min jams per show: weighted empirical PMF. Modern data is UNDER-
    # dispersed vs Poisson (var/mean ~0.7): most shows have exactly one big
    # jam — the band rations, it doesn't cluster. Price from the PMF, never
    # from a Poisson fit (it would overprice 0 and 3+ and underprice 1).
    count20_by_show = {d: sum(1 for x in ts if x["duration_ms"] >= 20 * 60000)
                       for d, ts in by_show.items()}
    pmf_w: dict[int, float] = {}
    for d in by_show:
        k = min(count20_by_show[d], 3)
        pmf_w[k] = pmf_w.get(k, 0.0) + show_weights[d]
    pmf = {("3plus" if k == 3 else str(k)): round(v / weighted_shows, 4)
           for k, v in sorted(pmf_w.items())}
    mean20 = sum(count20_by_show[d] * show_weights[d] for d in by_show) / weighted_shows
    var20 = sum((count20_by_show[d] - mean20) ** 2 * show_weights[d] for d in by_show) / weighted_shows
    p0 = pmf.get("0", 0.0)
    p_over_05 = round(1 - p0, 4)
    p_over_15 = round(pmf.get("2", 0.0) + pmf.get("3plus", 0.0), 4)

    over_unders = {
        "longest_minutes": {
            "weighted_mean": round(wmean_longest, 2),
            "weighted_median": round(wmedian, 2),
            "primary_line": primary_line,
            "lines": line_ladder,
        },
        "twenty_plus_count": {
            "weighted_mean": round(mean20, 3),
            "variance": round(var20, 3),
            "dispersion_vs_poisson": round(var20 / mean20, 3) if mean20 else None,
            "pmf": pmf,
            "lines": [
                {"line": 0.5, "p_over": p_over_05},
                {"line": 1.5, "p_over": p_over_15},
            ],
        },
    }

    return {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "reference_date": ref_date.isoformat(),
            "source": "phish.in v2",
            "endpoint": "/api/v2/tracks",
            "half_life_years": half_life_years,
            "shrink_k": shrink_k,
            "candidate_floor_weighted_plays": candidate_floor,
            "recent_window_years": RECENT_WINDOW_YEARS,
            "shows_analyzed": len(by_show),
            "total_tracks": len(tracks),
            "weighted_shows": round(weighted_shows, 1),
            "prior_longest_rate": round(prior_longest, 4),
            "prior_twenty_plus_rate": round(prior_twenty, 4),
            "coverage_by_topn": coverage_by_topn,
        },
        "songs": out_songs,
        "over_unders": over_unders,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", help="output JSON path (stdout if omitted)")
    ap.add_argument("--half-life", type=float, default=DEFAULT_HALF_LIFE_YEARS)
    ap.add_argument("--shrink-k", type=float, default=DEFAULT_SHRINK_K)
    ap.add_argument("--floor", type=float, default=DEFAULT_CANDIDATE_FLOOR)
    ap.add_argument("--top", type=int, default=DEFAULT_TOP_N)
    ap.add_argument("--tracks-cache",
                    help="path to cache the raw track pull; reused if it exists")
    args = ap.parse_args()

    tracks = None
    if args.tracks_cache and Path(args.tracks_cache).exists():
        print(f"Loading cached tracks from {args.tracks_cache}", file=sys.stderr)
        raw = json.loads(Path(args.tracks_cache).read_text())
        # Accept both this script's cache shape and the scratch shape {ms, excl}
        tracks = [{
            "date": t["date"], "title": t["title"], "slug": t.get("slug"),
            "duration_ms": t.get("duration_ms", t.get("ms", 0)),
            "exclude": t.get("exclude", t.get("excl", False)),
        } for t in raw]
    if tracks is None:
        print("Pulling every Phish track from Phish.in v2...", file=sys.stderr)
        t0 = time.time()
        tracks = paginate_all_tracks()
        print(f"Pulled {len(tracks)} tracks in {time.time()-t0:.1f}s", file=sys.stderr)
        if args.tracks_cache:
            Path(args.tracks_cache).write_text(json.dumps(tracks))
            print(f"Cached tracks to {args.tracks_cache}", file=sys.stderr)

    print(f"Aggregating (half-life {args.half_life}y, K={args.shrink_k}, "
          f"floor {args.floor}, top {args.top})...", file=sys.stderr)
    result = aggregate(tracks, args.half_life, args.shrink_k, args.floor, args.top)

    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2))
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        json.dump(result, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
