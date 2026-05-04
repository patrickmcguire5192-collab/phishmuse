# JamMuse / PhishStats

## What This App Is
A Flask web app providing **natural language query interfaces and dashboards** for jam band statistics. Think "StatMuse for jam bands." Users ask questions in plain English (powered by Claude) and get stats back, or browse visual dashboards.

**IMPORTANT:** When you make changes to this codebase, update the "Recent Changes" section at the bottom of this file so future sessions have context.

## Architecture Overview

### Data Source Layer (per band)
Each band pulls data from one or more sources. This is the most important thing to understand — **bands use DIFFERENT backends**:

| Band | Setlist Source | Duration Source | Engine File |
|------|---------------|-----------------|-------------|
| **Phish** | Phish.net API v5 | Phish.in API v2 | `scripts/query_engine.py` |
| **Goose** | Songfish API (`elgoose.net/api/v2`) | Songfish API | `scripts/jammuse_engine.py` |
| **King Gizzard** | Songfish API (`kglw.net/api/v2`) | Songfish API | `scripts/jammuse_engine.py` |
| **Grateful Dead** | Archive.org | Archive.org | `scripts/archive_engine.py` |
| **Umphrey's McGee** | Setlist.fm | **Relisten** | `scripts/setlistfm_engine.py` + `scripts/relisten_engine.py` |
| **String Cheese** | Phantasy Tour | **Relisten** | `scripts/phantasytour_engine.py` + `scripts/relisten_engine.py` |
| **Disco Biscuits** | Phantasy Tour | **Relisten** | `scripts/phantasytour_engine.py` + `scripts/relisten_engine.py` |
| **Widespread Panic** | Setlist.fm | **Relisten** | `scripts/setlistfm_engine.py` + `scripts/relisten_engine.py` |
| **Spafford** | (Relisten only) | **Relisten** | `scripts/relisten_engine.py` |
| **Billy Strings** | Setlist.fm | **Relisten** | `scripts/setlistfm_engine.py` + `scripts/relisten_engine.py` |
| **moe.** | Setlist.fm | **Relisten** | `scripts/setlistfm_engine.py` + `scripts/relisten_engine.py` |
| **STS9** | Setlist.fm | **Relisten** | `scripts/setlistfm_engine.py` + `scripts/relisten_engine.py` |
| **Lotus** | (Relisten only) | **Relisten** | `scripts/relisten_engine.py` |

### Key Concepts
- **Relisten bands** get duration data from pre-built indexes in `data/relisten_cache/{band}_duration_index.json`. These are large JSON files mapping song names to arrays of performances with date, duration, venue.
- **Songfish bands** (Goose, KGLW) have a unified API for both setlists and durations. Cached in `data/jammuse_cache/`.
- **Setlist.fm bands** use MusicBrainz IDs (mbid) to fetch setlists. Cached in `data/setlistfm_cache/`.
- **Phantasy Tour bands** use PT internal IDs. Cached in `data/pt_cache/`.
- The `UnifiedJamMuse` class in `jammuse_engine.py` orchestrates ALL band engines and provides auto-detection of which band a query is about.

### File Structure
```
app.py                          # Flask app, API routes, dashboard endpoints (801 lines)
scripts/
  query_engine.py               # Phish-specific engine (3407 lines)
  jammuse_engine.py             # Songfish engine + UnifiedJamMuse orchestrator (2309 lines)
  setlistfm_engine.py           # Setlist.fm engine for UM, WSP, moe, STS9, Billy (1199 lines)
  phantasytour_engine.py        # Phantasy Tour engine for SCI, Biscuits (1040 lines)
  relisten_engine.py            # Relisten duration engine, supplements PT/setlistfm bands (562 lines)
  archive_engine.py             # Grateful Dead via Archive.org (1166 lines)
  date_utils.py                 # Show date utilities (286 lines)
  refresh_data.py               # Data refresh scripts (455 lines)
ui/
  jammuse.html                  # Main frontend
  app.html                      # Legacy PhishStats frontend
  ARCHITECTURE.md               # Original architecture doc
data/
  jammuse_cache/                # Songfish API cache (Goose, KGLW)
  setlistfm_cache/              # Setlist.fm API cache
  pt_cache/                     # Phantasy Tour cache
  relisten_cache/               # Relisten duration indexes
  grateful_dead_catalog.json    # Dead song/show catalog
```

### API Routes
- `POST /api/jammuse/query` — Main natural language query endpoint (Claude-powered)
- `GET /api/dashboard/{band}/*` — Dashboard endpoints (top-songs, summary, monsters, shows-per-year, songs-list, duration-trend, song-deep-dive)
- Dashboards exist for: phish (custom), dead (custom), umphreys, sci, wsp, spafford (generic Relisten-based)

### Data Quirks to Know
- Relisten duration indexes contain ALL track names including banter, intros, encore breaks, and annotation variants (e.g., "song @", "song &^8"). The raw track count is NOT the same as unique songs played. For Umphrey's: ~6,150 raw entries but ~980 songs played more than once, ~128 core staples (50+ plays).
- Song aliases are defined per-band in each engine's config dict for normalizing fan shorthand (e.g., "hbb" -> "Hurt Bird Bath").

## MCP Server (Phish only, v1)

JamMuse exposes its Phish data layer as an MCP server so any MCP client (Claude.ai,
ChatGPT, Cursor) can answer rich questions without hitting our REST endpoints.

- Implementation: `scripts/mcp_server.py` (FastMCP, 18 tools)
- Vercel handler: `api/mcp.py` (ASGI, stateless, json_response)
- Public URL: `https://<deploy>/mcp`
- Local testing: `.venv-mcp/bin/python3 scripts/mcp_server.py --port 8765`

The MCP tools wrap engine methods on `PhishStatsEngine`. New engine methods added
to support MCP tools are ALSO routed through the rule-based NL parser at
`/api/query`, so the existing UI gains the same capabilities.

## Recent Changes
<!-- When you make changes, add an entry here with the date and a brief description. Keep the 5 most recent. -->
- **2026-05-03 (latest)**: Fixed Vercel cold-start hot-path. Relisten indexes were being rebuilt every cold start because the engine had a 30-day expiry on the cached JSON files (and Vercel's ephemeral disk meant the cache was always stale by that measure on a fresh container). Removed the auto-expiry; cached indexes are refreshed manually via `scripts/precompute_relisten_indexes.py` and checked in. Cold-start per-band Relisten cost dropped from ~30s to ~500ms.
- **2026-05-03 (later)**: MCP server now multi-band. Added `list_bands` (catalog of all 13 supported bands) and `ask_jam_band(band, question)` tools that delegate to `UnifiedJamMuse.query()`. Phish keeps its 18 typed tools; everything else (Goose, KGLW, Dead, UM, WSP, moe, STS9, Billy, SCI, Biscuits, Spafford, Lotus) routes through the unified engine. UnifiedJamMuse is lazy-loaded.
- **2026-05-03**: Added MCP server (18 tools) at `/mcp` for Claude.ai / ChatGPT custom-connector use. Added 4 new engine methods (`query_tour_shows`, `query_song_followers`, `query_venues_by_geography`, `query_biggest_bustouts`), augmented `data/raw/shows.json` with `tourid/tourname/tourwhen` and wrote `data/raw/tours.json` (117 tours). NL parser routes for tour/segue/geography/bustout queries. Migration script: `scripts/augment_tours.py`.
- **2026-03-27**: Created this CLAUDE.md file for project context.
