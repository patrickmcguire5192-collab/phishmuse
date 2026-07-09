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

## Data Refresh (IMPORTANT — data is a checked-in snapshot, NOT live)

Production (Vercel) has a **read-only filesystem** and there is **no live refresh on the
request path**. Every band serves a *snapshot of cache files committed to git*. The 1-hour
(setlist.fm/PT) and 7-day (Relisten) cache expiries only matter when running locally; on
Vercel they can't rewrite the bundled cache, so data is frozen at the last deploy. If a band
looks stale (e.g. "Umphrey's has only played 4 shows since January"), the snapshot is old.

**To refresh:** run locally, then commit `data/` and push (push → Vercel redeploy).
```
python scripts/refresh_all.py                 # all bands (setlist.fm + PT + Relisten + Phish)
python scripts/refresh_all.py --skip-phish     # non-Phish only
python scripts/refresh_all.py --bands umphreys  # one band, across all its sources
python scripts/refresh_all.py --relisten-only   # just duration indexes
```
`scripts/refresh_all.py` reuses each engine's loader (which re-pulls live because the caches
are past their expiry) and rewrites the cache files. Relisten rebuilds delegate to
`precompute_relisten_indexes.py --force`.

**Automated:** `.github/workflows/weekly-refresh.yml` runs the full refresh every Monday
08:00 UTC and commits the result (no secrets needed — all API keys are currently hardcoded
in the engines; see security note below). Trigger manually from the Actions tab anytime.

> Security note: Phish.net, setlist.fm keys are hardcoded in `scripts/refresh_data.py` and
> `scripts/setlistfm_engine.py`. Fine for now (no secret wiring needed for CI) but worth
> moving to env vars / GH secrets eventually.

## Recent Changes
<!-- When you make changes, add an entry here with the date and a brief description. Keep the 5 most recent. -->
- **2026-07-08 (latest)**: `build_phanduel_duration_index.py` v2 — time-decay weighting. The all-time version failed the fan eye test (all-time YEM wins 80% of shows played; modern YEM wins ~30%; David Bowie had 99 wins, none relevant since the 90s). Now every performance gets weight `0.5^(age/half-life)` (default 1.5y, chosen by eye-testing 1.5/2.5/4.0 against actual 2024-2026 longest-of-show winners), rates get Bayesian shrinkage (K=3) toward pooled priors, and the candidate list is **data-derived** (top 40 by weighted score) instead of the hardcoded 28 — which is how What's Going Through Your Mind, A Wave of Hope, and A Song I Heard the Ocean Sing got in. Also emits `coverage_by_topn` (weighted share of shows whose longest is in the top-N candidates; top-15 ≈ 68%) so PhanDuel prices the "Other" bucket from measurement. Supports `--tracks-cache` to iterate on weights without re-pulling ~39k tracks.
- **2026-07-07**: Added `scripts/build_phanduel_duration_index.py` — pulls every Phish track from Phish.in v2, groups by show date to identify the true longest song of each show, and aggregates per-song duration stats. Writes JSON to `--out` (typically `phanduel-app/public/duration_index.json`). Not run automatically. Consumed by PhanDuel's "Longest (v2)" tab.
- **2026-06-07**: Added `scripts/refresh_all.py` — one orchestrator that refreshes every band's
  cache (setlist.fm, Phantasy Tour, Relisten, Phish) and a weekly GitHub Action
  (`.github/workflows/weekly-refresh.yml`) that runs it + commits, so data no longer goes stale
  silently. Root issue was that the deployed snapshot was frozen at the last manual deploy (UM
  data had stopped in mid-March). Added gentle live-fetch throttling + HTTP 429 backoff to the
  setlist.fm and Phantasy Tour engines (cache-miss path only; no effect on production serving).
  Deleted the outdated `ui/ARCHITECTURE.md` (described an aspirational Claude-powered query path;
  the live `/api/jammuse/query` is actually the rule-based `UnifiedJamMuse.query()`).
- **2026-05-13 (latest)**: Fixed year-filtered duration queries for Relisten bands (`longest ocean billy in 2021` etc.). `relisten_engine._resolve_song_name()` was stripping filler words and band names from the raw question but leaving year tokens behind — so the song string passed downstream became `"ocean in 2021"`, which didn't match the index. Added regex year-stripping at the top of the resolver (mirrors `date_utils.extract_year_filter` patterns); the year itself is still pulled out upstream and passed via `year=` kwarg. Verified across UM, WSP, others.
- **2026-05-03**: Fixed Vercel cold-start hot-path. Relisten indexes were being rebuilt every cold start because the engine had a 30-day expiry on the cached JSON files (and Vercel's ephemeral disk meant the cache was always stale by that measure on a fresh container). Removed the auto-expiry; cached indexes are refreshed manually via `scripts/precompute_relisten_indexes.py` and checked in. Cold-start per-band Relisten cost dropped from ~30s to ~500ms.
- **2026-05-03 (later)**: MCP server now multi-band. Added `list_bands` (catalog of all 13 supported bands) and `ask_jam_band(band, question)` tools that delegate to `UnifiedJamMuse.query()`. Phish keeps its 18 typed tools; everything else (Goose, KGLW, Dead, UM, WSP, moe, STS9, Billy, SCI, Biscuits, Spafford, Lotus) routes through the unified engine. UnifiedJamMuse is lazy-loaded.
- **2026-05-03**: Added MCP server (18 tools) at `/mcp` for Claude.ai / ChatGPT custom-connector use. Added 4 new engine methods (`query_tour_shows`, `query_song_followers`, `query_venues_by_geography`, `query_biggest_bustouts`), augmented `data/raw/shows.json` with `tourid/tourname/tourwhen` and wrote `data/raw/tours.json` (117 tours). NL parser routes for tour/segue/geography/bustout queries. Migration script: `scripts/augment_tours.py`.
- **2026-03-27**: Created this CLAUDE.md file for project context.
