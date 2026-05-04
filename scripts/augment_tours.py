#!/usr/bin/env python3
"""
One-time migration: enrich data/raw/shows.json with tourid / tourname / tourwhen
from Phish.net setlist API, and write data/raw/tours.json catalog.

Safe to re-run: only modifies tour fields, leaves other show data untouched.
"""

import json
import time
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
SHOWS_PATH = RAW_DIR / "shows.json"
TOURS_PATH = RAW_DIR / "tours.json"
PHISHNET_API_KEY = "69F3065FB7F44C387CE5"


def fetch_json(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "PhishStats/1.0",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def main():
    with open(SHOWS_PATH) as f:
        shows = json.load(f)
    print(f"Loaded {len(shows)} shows")

    by_id = {s["showid"]: s for s in shows}
    years = sorted({s["showdate"][:4] for s in shows if s.get("showdate")})
    print(f"Spanning {len(years)} years: {years[0]}–{years[-1]}")

    tour_catalog = {}  # tourid -> {name, when, show_count, first_show, last_show}
    updated = 0

    for year in years:
        url = f"https://api.phish.net/v5/setlists/showyear/{year}.json?apikey={PHISHNET_API_KEY}"
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"  {year}: fetch failed ({e})")
            continue

        if not data or not data.get("data"):
            print(f"  {year}: no data")
            continue

        seen_in_year = 0
        for entry in data["data"]:
            if entry.get("artistid") != 1:
                continue
            sid = entry["showid"]
            if sid not in by_id:
                continue
            show = by_id[sid]
            tourid = entry.get("tourid")
            tourname = entry.get("tourname")
            tourwhen = entry.get("tourwhen")
            if tourid is None:
                continue

            if "tourid" not in show or show.get("tourid") != tourid:
                show["tourid"] = tourid
                show["tourname"] = tourname
                show["tourwhen"] = tourwhen
                updated += 1
            seen_in_year += 1

            if tourid not in tour_catalog:
                tour_catalog[tourid] = {
                    "tourid": tourid,
                    "name": tourname,
                    "when": tourwhen,
                    "show_dates": set(),
                }
            tour_catalog[tourid]["show_dates"].add(show["showdate"])

        print(f"  {year}: enriched {seen_in_year} entries")
        time.sleep(0.2)

    # Finalize tour catalog
    catalog_out = []
    for tid, info in tour_catalog.items():
        dates = sorted(info["show_dates"])
        catalog_out.append({
            "tourid": tid,
            "name": info["name"],
            "when": info["when"],
            "show_count": len(dates),
            "first_show": dates[0] if dates else None,
            "last_show": dates[-1] if dates else None,
            "show_dates": dates,
        })
    catalog_out.sort(key=lambda t: t.get("first_show") or "0000-00-00")

    with open(SHOWS_PATH, "w") as f:
        json.dump(shows, f, indent=2)
    with open(TOURS_PATH, "w") as f:
        json.dump(catalog_out, f, indent=2)

    print(f"\nUpdated {updated} show records with tour info")
    print(f"Wrote {len(catalog_out)} tours to {TOURS_PATH}")
    print(f"Sample tours:")
    for t in catalog_out[:5] + catalog_out[-5:]:
        print(f"  {t['tourid']:>4} | {t['when'] or '?':<25} | {t['show_count']:>3} shows | {t['name']}")


if __name__ == "__main__":
    main()
