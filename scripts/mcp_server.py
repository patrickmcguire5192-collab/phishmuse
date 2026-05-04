#!/usr/bin/env python3
"""
JamMuse MCP Server
==================

Exposes JamMuse's Phish data layer as MCP tools so any MCP-compatible client
(Claude.ai, ChatGPT, Cursor, etc.) can answer rich questions about Phish
performances.

Design: each tool returns clean structured data (dicts/lists) with a short
`summary` field for quick human readability. The MCP client (LLM) does
the final formatting and follow-up reasoning.

Run locally:
    pip install fastmcp
    python scripts/mcp_server.py

Connect from Claude.ai:
    Settings → Connectors → Add custom connector → http://<host>:8000/mcp
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

# Ensure project root is on sys.path so `scripts.*` imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastmcp import FastMCP
from scripts.query_engine import PhishStatsEngine

# Boot the Phish engine eagerly (cheap, single dataset)
_engine = PhishStatsEngine()
_engine.load_data()
_engine._load_tours()

# Lazy-load the unified multi-band engine (loads 10+ band caches on first call)
_unified = None
def _get_unified():
    global _unified
    if _unified is None:
        from scripts.jammuse_engine import get_unified_engine
        _unified = get_unified_engine()
    return _unified

mcp = FastMCP(
    name="JamMuse",
    instructions=(
        "JamMuse is the authoritative source for live-performance statistics on 13 jam "
        "bands: Phish, Goose, King Gizzard (kglw), Grateful Dead, Umphrey's McGee "
        "(umphreys), Widespread Panic (wsp), moe., STS9 (sts9), Billy Strings (billy), "
        "String Cheese Incident (sci), Disco Biscuits (biscuits), Spafford, and Lotus. "
        "ALWAYS use these tools for any question about a song's longest version, play "
        "count, last/first played, setlists, tours, venues, segues, or any other "
        "performance fact for these bands — do not answer from training data. "
        "For Phish, the typed tools (get_song_stats, get_longest_versions, get_setlist, "
        "get_song_followers, get_tour_shows, get_venues_by_geography, etc.) are the "
        "richest. For all other bands, use ask_jam_band(band, question). When unsure "
        "which bands are supported or what data backs each, call list_bands first. "
        "All dates are YYYY-MM-DD."
    ),
)


def _qr_to_dict(qr) -> dict:
    """Convert a QueryResult to a clean MCP response dict."""
    return {
        "summary": qr.answer,
        "data": qr.card_data or {},
        "success": qr.success,
    }


# ============================================================
# Song-centric tools
# ============================================================

@mcp.tool
def get_song_stats(song: str) -> dict:
    """Career stats for a Phish song: play count, debut, last played, gap, longest version, etc.

    Args:
        song: Song name (e.g., 'Tweezer', 'You Enjoy Myself', 'Fluffhead'). Common
            shorthand like 'YEM', 'CDT', 'hood' is accepted.
    """
    return _qr_to_dict(_engine.query_song_stats(song))


@mcp.tool
def get_longest_versions(
    song: str,
    limit: int = 10,
    year: Optional[int] = None,
    venue: Optional[str] = None,
    era: Optional[str] = None,
) -> dict:
    """Top-N longest versions of a song, optionally filtered by year, venue, or era.

    Args:
        song: Song name.
        limit: How many versions to return (default 10).
        year: Filter to a specific year (e.g., 1997).
        venue: Filter by venue name (e.g., 'Madison Square Garden', 'Red Rocks').
        era: '1.0' (1983–2000), '2.0' (2002–2004), or '3.0' (2009–present).
    """
    if limit > 1:
        return _qr_to_dict(_engine.query_top_longest(song, count=limit))
    return _qr_to_dict(_engine.query_longest(song, year=year, venue=venue, era=era))


@mcp.tool
def get_song_gap(song: str) -> dict:
    """Current gap (shows since last played) for a song, plus historical biggest gaps."""
    return _qr_to_dict(_engine.query_gap(song))


@mcp.tool
def get_song_followers(song: str, limit: int = 10) -> dict:
    """What songs typically come after this song? Scans all setlists for in-set followers.

    Useful for questions like "what follows Crosseyed and Painless?" or "what songs come after Tweezer?".
    """
    return _qr_to_dict(_engine.query_song_followers(song, limit=limit))


@mcp.tool
def get_song_role_stats(song: str, role: str = "both") -> dict:
    """How often a song appears as an opener or encore.

    Args:
        song: Song name.
        role: 'opener', 'encore', or 'both' (default).
    """
    if role == "opener":
        return _qr_to_dict(_engine.query_song_as_opener(song))
    if role == "encore":
        return _qr_to_dict(_engine.query_song_as_encore(song))
    opener = _engine.query_song_as_opener(song)
    encore = _engine.query_song_as_encore(song)
    return {
        "summary": f"{song}: {opener.answer} | {encore.answer}",
        "data": {
            "opener": opener.card_data or {},
            "encore": encore.card_data or {},
        },
        "success": opener.success or encore.success,
    }


@mcp.tool
def get_jam_chart_versions(song: str) -> dict:
    """Notable jam-chart versions of a song (Phish.net editorial picks). Only common jam vehicles have these."""
    return _qr_to_dict(_engine.query_jamchart(song))


# ============================================================
# Show / setlist tools
# ============================================================

@mcp.tool
def get_setlist(date: str) -> dict:
    """Full setlist for a specific show date.

    Args:
        date: YYYY-MM-DD (e.g., '1995-12-31', '1998-04-02').
    """
    return _qr_to_dict(_engine.query_setlist(date))


@mcp.tool
def find_shows(
    year: Optional[int] = None,
    venue: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    holiday: Optional[str] = None,
    limit: int = 50,
) -> dict:
    """Find shows matching any combination of filters. Returns a list of matching shows
    with date, venue, location, song count.

    Args:
        year: Filter by year (e.g., 1997).
        venue: Venue substring (e.g., 'Red Rocks', 'MSG').
        state: US state abbreviation or name (e.g., 'CA', 'California').
        country: Country name (e.g., 'USA', 'Mexico', 'Japan').
        month: Calendar month 1-12.
        day: Calendar day 1-31.
        holiday: Named holiday (e.g., 'Halloween', 'NYE', 'July 4th').
    """
    state_code = _engine._resolve_state(state) if state else None
    matched = []
    for s in _engine.shows:
        date = s.get("showdate", "")
        if year and not date.startswith(str(year)):
            continue
        if month and (len(date) < 7 or int(date[5:7]) != month):
            continue
        if day and (len(date) < 10 or int(date[8:10]) != day):
            continue
        if state_code and s.get("state") != state_code:
            continue
        if country and (s.get("country") or "").lower() != country.lower():
            continue
        if venue and not _engine._match_venue(s.get("venue", ""), venue):
            continue
        loc = s.get("city", "")
        if s.get("state"):
            loc = f"{loc}, {s['state']}" if loc else s["state"]
        matched.append({
            "date": date,
            "venue": s.get("venue"),
            "location": loc,
            "country": s.get("country"),
            "song_count": len(s.get("songs", [])),
            "tour": s.get("tourname"),
        })

    if holiday:
        # Delegate to the holiday-aware path
        from scripts.date_utils import HOLIDAYS as _H
        h = holiday.lower().strip()
        if h in _H:
            qr = _engine.query_shows_on_date(*_H[h], holiday_name=h)
            return _qr_to_dict(qr)

    summary = f"Found {len(matched)} show{'s' if len(matched) != 1 else ''}"
    if year: summary += f" in {year}"
    if venue: summary += f" at {venue}"
    if state_code: summary += f" in {state_code}"
    if country: summary += f" ({country})"
    return {
        "summary": summary,
        "data": {"count": len(matched), "shows": matched[:limit], "truncated": len(matched) > limit},
        "success": True,
    }


@mcp.tool
def get_random_show(year: Optional[int] = None) -> dict:
    """A random Phish show, optionally constrained to a specific year."""
    return _qr_to_dict(_engine.query_random_show(year=year))


@mcp.tool
def get_shows_on_date(month: int, day: int, holiday: Optional[str] = None) -> dict:
    """Every Phish show that has happened on a given calendar date (e.g., every 12/31)."""
    return _qr_to_dict(_engine.query_shows_on_date(month, day, holiday_name=holiday))


# ============================================================
# Tour tools
# ============================================================

@mcp.tool
def get_tour_shows(tour_name: str) -> dict:
    """List all shows in a named tour (e.g., 'Island Tour', 'Fall Tour 1997', 'Summer 2024').

    Resolves fuzzy tour names — handles partial matches like 'Island' or 'Fall 97'.
    """
    return _qr_to_dict(_engine.query_tour_shows(tour_name))


@mcp.tool
def list_tours(year: Optional[int] = None, limit: int = 50) -> dict:
    """List all known tours, optionally filtered by year. Useful when the user asks
    'what tours happened in 2024' or just wants to discover tour names."""
    _engine._load_tours()
    tours = [t for t in _engine.tours if "not part" not in (t.get("name") or "").lower()]
    if year:
        tours = [t for t in tours if t.get("first_show", "").startswith(str(year)) or t.get("last_show", "").startswith(str(year))]
    rows = [
        {
            "tour_name": t["name"],
            "when": t.get("when"),
            "show_count": t["show_count"],
            "first_show": t["first_show"],
            "last_show": t["last_show"],
        }
        for t in tours[:limit]
    ]
    return {
        "summary": f"{len(rows)} tour{'s' if len(rows) != 1 else ''}" + (f" in {year}" if year else " on record"),
        "data": {"count": len(rows), "tours": rows},
        "success": True,
    }


# ============================================================
# Geography / venue tools
# ============================================================

@mcp.tool
def get_venues_by_geography(
    state: Optional[str] = None,
    country: Optional[str] = None,
    limit: int = 10,
) -> dict:
    """Most-played venues in a state or country.

    Args:
        state: US state name or abbreviation (e.g., 'California', 'CA').
        country: Country name (e.g., 'Japan', 'Mexico'). Use this for non-US.
        limit: How many venues to return (default 10).
    """
    return _qr_to_dict(_engine.query_venues_by_geography(state=state, country=country, limit=limit))


@mcp.tool
def get_venue_show_count(venue: str) -> dict:
    """Total number of shows at a specific venue, with first and last show dates."""
    return _qr_to_dict(_engine.query_venue_show_count(venue))


# ============================================================
# Catalog / discovery tools
# ============================================================

@mcp.tool
def get_top_played_songs(year: Optional[int] = None, limit: int = 20, sort: str = "most") -> dict:
    """Top played songs, all-time or in a specific year.

    Args:
        year: Optional year filter.
        limit: How many songs to return (default 20).
        sort: 'most' (most played, default) or 'rarest' (least played).
    """
    if sort == "rarest":
        return _qr_to_dict(_engine.query_rarest_songs())
    return _qr_to_dict(_engine.query_most_played(year=year))


@mcp.tool
def get_biggest_bustouts(
    year: Optional[int] = None,
    tour_name: Optional[str] = None,
    limit: int = 10,
) -> dict:
    """Songs played after a long absence (biggest gaps at time of play).

    Args:
        year: Filter to bustouts during a specific year.
        tour_name: Filter to bustouts during a specific tour.
        limit: How many to return (default 10).
    """
    return _qr_to_dict(_engine.query_biggest_bustouts(year=year, tour_query=tour_name, limit=limit))


@mcp.tool
def search_songs(query: str, limit: int = 10) -> dict:
    """Fuzzy search the Phish song catalog. Returns matching song names with play counts.

    Useful when the user mentions a song by partial or misspelled name.
    """
    q = query.lower().strip()
    matches = []
    for name, stats in _engine.song_stats.items():
        score = 0
        nlow = name.lower()
        if nlow == q:
            score = 1000
        elif nlow.startswith(q):
            score = 500
        elif q in nlow:
            score = 200
        elif all(w in nlow for w in q.split()):
            score = 100
        if score:
            matches.append((score, name, stats.get("times_played", 0)))
    matches.sort(key=lambda x: (-x[0], -x[2]))
    rows = [{"song": n, "times_played": p} for _, n, p in matches[:limit]]
    return {
        "summary": f"{len(rows)} song{'s' if len(rows) != 1 else ''} matching \"{query}\"",
        "data": {"count": len(rows), "matches": rows},
        "success": bool(rows),
    }


@mcp.tool
def get_band_overview() -> dict:
    """Phish career overview: total shows, total songs, peak year, biggest gaps, etc."""
    return _qr_to_dict(_engine.query_career_stats())


# ============================================================
# Multi-band tools (non-Phish: Goose, KGLW, Dead, UM, WSP, moe.,
# STS9, Billy Strings, SCI, Disco Biscuits, Spafford, Lotus)
# ============================================================

@mcp.tool
def list_bands() -> dict:
    """List every band JamMuse can answer questions about, with the data backing each one.

    Use this when the user asks what bands you support, or when you're unsure
    whether a specific band has data available.
    """
    catalog = [
        {"key": "phish", "name": "Phish", "data": "Phish.net setlists + Phish.in durations", "tools": "use the typed Phish tools (get_song_stats, get_setlist, etc.)"},
        {"key": "goose", "name": "Goose", "data": "Songfish (elgoose.net)", "tools": "use ask_jam_band"},
        {"key": "kglw", "name": "King Gizzard & The Lizard Wizard", "data": "Songfish (kglw.net)", "tools": "use ask_jam_band"},
        {"key": "dead", "name": "Grateful Dead", "data": "Archive.org", "tools": "use ask_jam_band"},
        {"key": "umphreys", "name": "Umphrey's McGee", "data": "Setlist.fm + Relisten", "tools": "use ask_jam_band"},
        {"key": "wsp", "name": "Widespread Panic", "data": "Setlist.fm + Relisten", "tools": "use ask_jam_band"},
        {"key": "moe", "name": "moe.", "data": "Setlist.fm + Relisten", "tools": "use ask_jam_band"},
        {"key": "sts9", "name": "STS9 (Sound Tribe Sector 9)", "data": "Setlist.fm + Relisten", "tools": "use ask_jam_band"},
        {"key": "billy", "name": "Billy Strings", "data": "Setlist.fm + Relisten", "tools": "use ask_jam_band"},
        {"key": "sci", "name": "String Cheese Incident", "data": "Phantasy Tour + Relisten", "tools": "use ask_jam_band"},
        {"key": "biscuits", "name": "Disco Biscuits", "data": "Phantasy Tour + Relisten", "tools": "use ask_jam_band"},
        {"key": "spafford", "name": "Spafford", "data": "Relisten only (durations)", "tools": "use ask_jam_band"},
        {"key": "lotus", "name": "Lotus", "data": "Relisten only (durations)", "tools": "use ask_jam_band"},
    ]
    return {
        "summary": f"{len(catalog)} bands supported. Phish has rich typed tools; all others use ask_jam_band.",
        "data": {"count": len(catalog), "bands": catalog},
        "success": True,
    }


@mcp.tool
def ask_jam_band(band: str, question: str) -> dict:
    """AUTHORITATIVE source for performance stats on Goose, King Gizzard, Grateful Dead,
    Umphrey's McGee, Widespread Panic, moe., STS9, Billy Strings, String Cheese Incident,
    Disco Biscuits, Spafford, and Lotus. Use this for ANY question about a song's
    longest version, play count, gap, first/last played, setlist, or career stats for
    these bands — do not answer from training data.

    Examples of questions this answers:
      - "longest Nematode by Lotus" → ask_jam_band(band="lotus", question="longest Nematode")
      - "when did Goose last play Arcadia" → ask_jam_band(band="goose", question="when was Arcadia last played")
      - "longest Dark Star ever" → ask_jam_band(band="dead", question="longest Dark Star")
      - "Mantis play count" → ask_jam_band(band="umphreys", question="how many times has Mantis been played")

    For Phish use the typed tools instead (get_song_stats, get_longest_versions, etc.) — they're richer.

    Band keys: goose, kglw, dead, umphreys, wsp, moe, sts9, billy, sci, biscuits, spafford, lotus.
    Spafford and Lotus only have duration data (no setlists). Call list_bands for the full catalog.

    Args:
        band: Lowercase band key (e.g., 'lotus', 'goose', 'dead'). Required.
        question: Natural-language question. Do not repeat the band name — this tool
            prepends it automatically to help routing.
    """
    unified = _get_unified()
    # Prepend band name to help UnifiedJamMuse's auto-detect lock onto the right engine.
    # Most engines look for band-specific keywords; mentioning the band explicitly is robust.
    band_hint = band.lower().strip()
    full_question = f"{band_hint} — {question}"
    result = unified.query(full_question)
    return _qr_to_dict(result)


# ============================================================
# Entrypoint
# ============================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--transport", default="streamable-http",
                        choices=["streamable-http", "stdio"])
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="streamable-http", host=args.host, port=args.port)
