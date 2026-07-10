#!/usr/bin/env python3
"""
Build the PhanDuel specials index — data-derived odds for the Special Bets
board, replacing the made-up numbers with measured per-show event rates.

Same weighting philosophy as build_phanduel_duration_index.py: every show
gets weight 0.5 ** (age_years / half_life) (default 1.5y) so the rates model
the band as it plays today, and each rate gets Bayesian shrinkage (K effective
shows) toward the pooled all-time rate so rare events don't swing wildly.

Markets and their data sources:
  same_song_twice     Phish.in tracks — duplicate titles within a show
                      (HYHU excluded: the Fishman-song sandwich is routine)
  longest_first_set   Phish.in — longest track of show sits in Set 1
  five_song_set2      Phish.in — Set 2 has 5 or fewer tracks
  secret_language     Phish.in "Signal" tag on any track of the show
  a_cappella          Phish.in "A Cappella" tag on any track of the show
  gamehendge          Phish.in "Gamehendge" tag (full-narration shows)
  vacuum              Phish.net setlist notes mention "vacuum"
  bustout_100         Phish.net per-song gap field ≥ 100 on any track

Phish.net data is only fetched for years where the decay weight is
non-negligible (weight ≥ ~0.01 → last ~10 years); older shows contribute
effectively nothing to a 1.5y half-life anyway.

Output: specials_index.json
{
  "meta": { generated_at, reference_date, half_life_years, shrink_k,
            weighted_shows, phishnet_years_fetched, ... },
  "markets": {
    "<key>": {
      "label": "...",
      "prob": 0.113,              # weighted + shrunk P(event per show)
      "raw_rate": 0.121,          # weighted, unshrunk (for the curious)
      "alltime_rate": 0.145,      # unweighted all-time
      "occurrences_recent": 9,    # count in the last 3 calendar years
      "last_occurred": "2026-04-25",
      "old_odds": 15,             # the vibes-era line, for the "was" display
      "note": "..."               # definition fine print shown in the UI
    }, ...
  }
}

Usage:
  python3 scripts/build_phanduel_specials_index.py \
      --tracks-cache /path/all_tracks_tagged.json \
      --out ~/phanduel-app/public/specials_index.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import urllib.request
import urllib.error

USER_AGENT = "phanduel-specials-index/1.0 (+https://phanduel-app.vercel.app)"
PHISHNET_API_KEY = "69F3065FB7F44C387CE5"

DEFAULT_HALF_LIFE_YEARS = 1.5
DEFAULT_SHRINK_K = 5.0          # a bit stiffer than duration index — these are rarer events
MIN_YEAR_WEIGHT = 0.01          # fetch Phish.net only for years that still matter
RECENT_WINDOW_YEARS = 3

# The vibes-era board, so the UI can show "was 15x".
OLD_ODDS = {
    "same_song_twice": 15,
    "longest_first_set": 12,
    "five_song_set2": 20,
    "vacuum": 30,
    "bustout_100": 80,
    "gamehendge": 10000,
    "secret_language": 40,
    "a_cappella": None,          # new market — no old line
}

LABELS = {
    "same_song_twice": "Band plays same song multiple times",
    "longest_first_set": "Longest song is in first set",
    "five_song_set2": "Five song second set (5 or fewer)",
    "vacuum": "Fishman on vacuum",
    "bustout_100": "Bustout (100+ show gap)",
    "gamehendge": "Band plays entirety of Gamehendge",
    "secret_language": "Secret language appearance",
    "a_cappella": "A cappella song performed",
}

NOTES = {
    "same_song_twice": "Same song appears twice on the official Phish.net setlist — includes in-set returns (Simple > jam > Simple). Hold Your Head Up excluded; the Fishman-song sandwich is routine.",
    "longest_first_set": "The longest performance of the night happens in Set 1 (by Phish.in track timings).",
    "five_song_set2": "Second set contains 5 or fewer songs — the mega-jam set.",
    "vacuum": "Phish.net setlist notes mention a vacuum solo.",
    "bustout_100": "Any song returns after a gap of 100+ shows (Phish.net gap data).",
    "gamehendge": "Full Gamehendge narration show (Phish.in Gamehendge tag).",
    "secret_language": "Any secret-language signal (Phish.in Signal tag — community-maintained and may lag recent shows; none tagged since 2011).",
    "a_cappella": "At least one a cappella performance (Phish.in tag).",
}


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


def parse_date(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def load_tracks(cache_path: str) -> list[dict]:
    raw = json.loads(Path(cache_path).read_text())
    tracks = []
    for t in raw:
        if t.get("excl") or not t.get("date") or not (t.get("ms") or 0) > 0:
            continue
        tracks.append({
            "date": t["date"],
            "title": t["title"],
            "ms": t["ms"],
            "set": t.get("set") or "",
            "tags": t.get("tags") or [],
        })
    return tracks


def fetch_phishnet_year_data(years: list[int]) -> dict:
    """Per-show facts from the OFFICIAL setlist (Phish.net rows).

    date -> {vacuum, bustout, dup, five_set2}

    Phish.net is the settlement source for song-count markets: Phish.in
    splits long jams into multiple tracks when the band returns to a song
    (Fuego 37m + Fuego 5m) and carries non-song "Banter" tracks, which
    inflate duplicate-title and set-length counts.
    """
    shows: dict[str, dict] = {}
    for y in years:
        url = (f"https://api.phish.net/v5/setlists/showyear/{y}.json"
               f"?apikey={PHISHNET_API_KEY}&order_by=showdate")
        data = fetch_json(url)
        rows = [r for r in (data.get("data") or []) if r.get("artistid") == 1]
        per_show: dict[str, list[dict]] = {}
        for r in rows:
            d = r.get("showdate")
            if d:
                per_show.setdefault(d, []).append(r)
        for d, rs in per_show.items():
            songs = [(r.get("song") or "").strip() for r in rs]
            non_hyhu = [s for s in songs if s.lower() != "hold your head up"]
            dup = len(non_hyhu) != len(set(non_hyhu))
            set2_count = sum(1 for r in rs if str(r.get("set")) == "2")
            bustout = False
            vacuum = False
            for r in rs:
                try:
                    if int(r.get("gap") or 0) >= 100:
                        bustout = True
                except (TypeError, ValueError):
                    pass
                notes = (r.get("setlistnotes") or "") + " " + (r.get("footnote") or "")
                if re.search(r"vacuum", notes, re.IGNORECASE):
                    vacuum = True
            encore_count = sum(1 for r in rs if str(r.get("set")) in ("e", "e2"))
            shows[d] = {
                "vacuum": vacuum,
                "bustout": bustout,
                "dup": dup,
                "five_set2": 0 < set2_count <= 5,
                "set2_count": set2_count,
                "encore_count": encore_count,
            }
        print(f"  phish.net {y}: {len(rows)} rows / {len(per_show)} shows", file=sys.stderr)
        time.sleep(0.5)
    return shows


def build(tracks: list[dict], half_life: float, shrink_k: float) -> dict:
    ref_date = max(parse_date(t["date"]) for t in tracks)
    hl_days = 365.25 * half_life

    def weight(ds: str) -> float:
        return 0.5 ** ((ref_date - parse_date(ds)).days / hl_days)

    by_show: dict[str, list[dict]] = {}
    for t in tracks:
        by_show.setdefault(t["date"], []).append(t)
    show_weights = {d: weight(d) for d in by_show}
    weighted_shows = sum(show_weights.values())

    # Which Phish.net years still matter under the decay?
    pn_years = [y for y in range(1983, ref_date.year + 1)
                if 0.5 ** (max(0, (ref_date - date(y, 12, 31)).days) / hl_days) >= MIN_YEAR_WEIGHT]
    print(f"Fetching Phish.net years {pn_years[0]}..{pn_years[-1]} for setlist-settled markets...",
          file=sys.stderr)
    pn_shows = fetch_phishnet_year_data(pn_years)

    # Resolve each market per show. Song-count markets settle on the official
    # Phish.net setlist; duration/tag markets settle on Phish.in.
    def resolve(d: str, ts: list[dict]) -> dict:
        longest = max(ts, key=lambda x: x["ms"])
        all_tags = {tag for t in ts for tag in t["tags"]}
        pn = pn_shows.get(d) or {}
        return {
            "same_song_twice": bool(pn.get("dup")),
            "longest_first_set": longest["set"] == "Set 1",
            "five_song_set2": bool(pn.get("five_set2")),
            "secret_language": "Signal" in all_tags,
            "a_cappella": "A Cappella" in all_tags,
            "gamehendge": "Gamehendge" in all_tags,
            "vacuum": bool(pn.get("vacuum")),
            "bustout_100": bool(pn.get("bustout")),
        }

    market_keys = list(LABELS.keys())
    w_hits = {k: 0.0 for k in market_keys}
    raw_hits_alltime = {k: 0 for k in market_keys}
    recent_hits = {k: 0 for k in market_keys}
    last_occurred = {k: None for k in market_keys}
    recent_cutoff = date(ref_date.year - RECENT_WINDOW_YEARS, ref_date.month, ref_date.day).isoformat()

    # Phish.net-settled markets have no signal before the fetched year range —
    # restrict their denominators to shows within that range so absence of
    # data isn't scored as absence of events.
    pn_min_date = f"{pn_years[0]}-01-01"
    pn_weighted_shows = sum(w for d, w in show_weights.items() if d >= pn_min_date)
    pn_alltime_shows = sum(1 for d in by_show if d >= pn_min_date)
    PN_MARKETS = {"vacuum", "bustout_100", "same_song_twice", "five_song_set2"}

    for d, ts in by_show.items():
        w = show_weights[d]
        res = resolve(d, ts)
        for k, hit in res.items():
            if k in PN_MARKETS and d < pn_min_date:
                continue
            if hit:
                w_hits[k] += w
                raw_hits_alltime[k] += 1
                if d >= recent_cutoff:
                    recent_hits[k] += 1
                if last_occurred[k] is None or d > last_occurred[k]:
                    last_occurred[k] = d

    markets = {}
    for k in market_keys:
        denom_w = pn_weighted_shows if k in PN_MARKETS else weighted_shows
        denom_n = pn_alltime_shows if k in PN_MARKETS else len(by_show)
        raw_rate = w_hits[k] / denom_w if denom_w else 0.0
        alltime = raw_hits_alltime[k] / denom_n if denom_n else 0.0
        prob = (w_hits[k] + shrink_k * alltime) / (denom_w + shrink_k)
        markets[k] = {
            "label": LABELS[k],
            "prob": round(prob, 4),
            "raw_rate": round(raw_rate, 4),
            "alltime_rate": round(alltime, 4),
            "occurrences_recent": recent_hits[k],
            "last_occurred": last_occurred[k],
            "old_odds": OLD_ODDS[k],
            "note": NOTES[k],
        }

    # ---- Setlist-settled Over/Under markets (official Phish.net counts) ----
    # Weighted distributions over shows that have Phish.net rows; the balanced
    # primary line is whichever half-count bracket of the median prices
    # closest to a coin flip.
    def ou_market(count_by_date: dict, ladder_offsets: list[float]) -> dict:
        ds = [d for d in count_by_date if d in show_weights]
        w_total = sum(show_weights[d] for d in ds)

        def p_over(line: float) -> float:
            return sum(show_weights[d] for d in ds if count_by_date[d] > line) / w_total

        pairs = sorted((count_by_date[d], show_weights[d]) for d in ds)
        cum, med = 0.0, pairs[-1][0]
        for v, wt in pairs:
            cum += wt
            if cum >= w_total / 2:
                med = v
                break
        wmean = sum(count_by_date[d] * show_weights[d] for d in ds) / w_total
        lo, hi = med - 0.5, med + 0.5
        primary = min((lo, hi), key=lambda l: abs(p_over(l) - 0.5))
        lines = [{"line": round(primary + off, 1), "p_over": round(p_over(primary + off), 4)}
                 for off in ladder_offsets]
        return {
            "weighted_mean": round(wmean, 2),
            "weighted_median": round(med, 1),
            "primary_line": primary,
            "lines": lines,
        }

    set2_counts = {d: v["set2_count"] for d, v in pn_shows.items() if v["set2_count"] > 0}
    encore_counts = {d: v["encore_count"] for d, v in pn_shows.items()}
    over_unders = {
        "set2_songs": ou_market(set2_counts, [-1, 0, 1]),
        "encore_songs": ou_market(encore_counts, [0]),
    }

    return {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "reference_date": ref_date.isoformat(),
            "source": "phish.in v2 tags/tracks + phish.net setlists",
            "half_life_years": half_life,
            "shrink_k": shrink_k,
            "shows_analyzed": len(by_show),
            "weighted_shows": round(weighted_shows, 1),
            "phishnet_years_fetched": [pn_years[0], pn_years[-1]],
            "recent_window_years": RECENT_WINDOW_YEARS,
        },
        "markets": markets,
        "over_unders": over_unders,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tracks-cache", required=True,
                    help="path to the tagged track pull (all_tracks_tagged.json)")
    ap.add_argument("--out", help="output JSON path (stdout if omitted)")
    ap.add_argument("--half-life", type=float, default=DEFAULT_HALF_LIFE_YEARS)
    ap.add_argument("--shrink-k", type=float, default=DEFAULT_SHRINK_K)
    args = ap.parse_args()

    tracks = load_tracks(args.tracks_cache)
    print(f"Loaded {len(tracks)} tracks from cache", file=sys.stderr)
    result = build(tracks, args.half_life, args.shrink_k)

    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2))
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        json.dump(result, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
