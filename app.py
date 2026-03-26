#!/usr/bin/env python3
"""
PhishStats & JamMuse Web App
============================

Flask app that serves both PhishStats and JamMuse query interfaces.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sys
import statistics
from pathlib import Path
from collections import defaultdict

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))
from scripts.query_engine import PhishStatsEngine
from scripts.jammuse_engine import UnifiedJamMuse, BANDS

app = Flask(__name__, static_folder='ui')
CORS(app)

# Initialize PhishStats query engine (for legacy /api/query endpoint)
engine = PhishStatsEngine()
engine.load_data()

# Load Dead catalog and Umphrey's duration index for dashboards
import json as _json

_dead_catalog = None
_relisten_indexes = {}

def _get_dead_catalog():
    global _dead_catalog
    if _dead_catalog is None:
        with open(Path(__file__).parent / 'data' / 'grateful_dead_catalog.json') as f:
            _dead_catalog = _json.load(f)
        print(f"  Loaded Dead catalog: {len(_dead_catalog['songs'])} songs, {len(_dead_catalog['shows'])} shows")
    return _dead_catalog

def _get_relisten_index(band_key):
    """Load any Relisten duration index by band key."""
    if band_key not in _relisten_indexes:
        fpath = Path(__file__).parent / 'data' / 'relisten_cache' / f'{band_key}_duration_index.json'
        with open(fpath) as f:
            _relisten_indexes[band_key] = _json.load(f)
        idx = _relisten_indexes[band_key]
        print(f"  Loaded {band_key} index: {len(idx['tracks'])} tracks, {idx['total_shows']} shows")
    return _relisten_indexes[band_key]

# Filter junk tracks from Relisten indexes
RELISTEN_SKIP_TRACKS = {'intro', 'encore break', 'jam', 'set break', 'intermission',
                        'crowd', 'tuning', 'banter', 'stage banter', 'soundcheck',
                        'drums', 'space', 'encore', 'introduction'}
DEAD_SKIP_SONGS = {'Drums', 'Space', 'Drums/Space', 'Jam', 'Tuning', 'Introduction'}

# Initialize unified JamMuse engine (handles all bands including Phish)
unified_engine = None

@app.route('/')
def index():
    """Serve the main UI - JamMuse (includes all PhishMuse functionality)."""
    return send_from_directory('ui', 'jammuse.html')

@app.route('/images/<path:filename>')
def serve_image(filename):
    """Serve band images."""
    return send_from_directory('ui/images', filename)

@app.route('/api/query', methods=['POST'])
def query():
    """Handle a natural language query."""
    data = request.get_json()
    question = data.get('question', '')

    if not question:
        return jsonify({'error': 'No question provided'}), 400

    result = engine.query(question)

    return jsonify({
        'success': result.success,
        'answer': result.answer,
        'highlight': result.highlight,
        'card': result.card_data,
        'related': result.related_queries
    })

@app.route('/api/suggest', methods=['GET'])
def suggest():
    """Return suggested queries."""
    return jsonify({
        'suggestions': [
            "longest Tweezer",
            "longest Ghost",
            "YEM stats",
            "how many times has Sand been played",
            "Reba play count",
            "longest Down with Disease"
        ]
    })


# =============================================================================
# JAMMUSE ROUTES - UNIFIED MULTI-BAND ENGINE
# =============================================================================

def get_unified_engine() -> UnifiedJamMuse:
    """Get or create the unified JamMuse engine."""
    global unified_engine
    if unified_engine is None:
        unified_engine = UnifiedJamMuse(include_phish=True)
    return unified_engine


@app.route('/jammuse')
def jammuse_index():
    """Serve the JamMuse UI."""
    return send_from_directory('ui', 'jammuse.html')


@app.route('/api/jammuse/query', methods=['POST'])
def jammuse_query():
    """Handle a JamMuse natural language query - auto-detects band from context."""
    data = request.get_json()
    question = data.get('question', '')

    if not question:
        return jsonify({'error': 'No question provided'}), 400

    try:
        engine = get_unified_engine()
        result = engine.query(question)

        return jsonify({
            'success': result.success,
            'answer': result.answer,
            'band': result.band,
            'highlight': result.highlight,
            'card': result.card_data,
            'related': result.related_queries
        })
    except Exception as e:
        return jsonify({'error': f'Query failed: {str(e)}'}), 500


@app.route('/api/jammuse/bands', methods=['GET'])
def jammuse_bands():
    """Return available bands."""
    bands = [
        {'key': 'phish', 'name': 'Phish'},
        {'key': 'goose', 'name': 'Goose'},
        {'key': 'kglw', 'name': 'King Gizzard & the Lizard Wizard'}
    ]
    return jsonify({'bands': bands})


# =============================================================================
# DASHBOARD API ENDPOINTS - PHISH STATS DASHBOARD
# =============================================================================

# Helper: build slug-to-name lookup from songs_metadata
def _slug_to_name():
    mapping = {}
    for name, meta in engine.songs_metadata.items():
        slug = meta.get('slug', '')
        if slug:
            mapping[slug] = name
    return mapping


@app.route('/api/dashboard/top-songs')
def dashboard_top_songs():
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)
    limit = request.args.get('limit', 25, type=int)

    if start_year or end_year:
        song_counts = defaultdict(int)
        for show in engine.shows:
            year = int(show['showdate'][:4])
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
            for s in show.get('songs', []):
                name = s.get('song', '')
                if name:
                    song_counts[name] += 1
        songs = [{'name': n, 'play_count': c} for n, c in
                 sorted(song_counts.items(), key=lambda x: -x[1])[:limit]]
    else:
        songs = [{'name': n, 'play_count': s['play_count'],
                  'jamchart_count': s.get('jamchart_count', 0)}
                 for n, s in sorted(engine.song_stats.items(),
                                    key=lambda x: -x[1]['play_count'])[:limit]]
    return jsonify({'songs': songs})


@app.route('/api/dashboard/monsters')
def dashboard_monsters():
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)
    min_duration = request.args.get('min_duration', 20.0, type=float)

    slug_names = _slug_to_name()
    song_monster_counts = defaultdict(int)
    performances = []

    for slug, perfs in engine.raw_durations.items():
        song_name = slug_names.get(slug, slug.replace('-', ' ').title())
        for p in perfs:
            dur = p.get('duration_min', 0)
            if dur < min_duration:
                continue
            year = int(p['date'][:4])
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
            song_monster_counts[song_name] += 1
            performances.append({
                'song': song_name,
                'date': p['date'],
                'duration_min': round(dur, 1),
                'venue': p.get('venue', '')
            })

    # Top songs by monster count
    top_songs = [{'name': n, 'count': c} for n, c in
                 sorted(song_monster_counts.items(), key=lambda x: -x[1])[:20]]

    return jsonify({
        'top_songs': top_songs,
        'total_count': len(performances),
        'unique_songs': len(song_monster_counts)
    })


@app.route('/api/dashboard/duration-trend')
def dashboard_duration_trend():
    song_slug = request.args.get('song', '')
    if not song_slug or song_slug not in engine.raw_durations:
        return jsonify({'error': 'Song not found'}), 404

    slug_names = _slug_to_name()
    song_name = slug_names.get(song_slug, song_slug.replace('-', ' ').title())

    by_year = defaultdict(list)
    for p in engine.raw_durations[song_slug]:
        year = int(p['date'][:4])
        by_year[year].append(p['duration_min'])

    years = []
    for year in sorted(by_year.keys()):
        durations = by_year[year]
        years.append({
            'year': year,
            'avg_min': round(statistics.mean(durations), 2),
            'max_min': round(max(durations), 2),
            'min_min': round(min(durations), 2),
            'count': len(durations)
        })

    return jsonify({'song_name': song_name, 'slug': song_slug, 'years': years})


@app.route('/api/dashboard/summary')
def dashboard_summary():
    """Phish KPI summary — year-filterable."""
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)

    total_shows = 0
    unique_songs = set()
    for show in engine.shows:
        year = int(show['showdate'][:4])
        if start_year and year < start_year:
            continue
        if end_year and year > end_year:
            continue
        total_shows += 1
        for s in show.get('songs', []):
            name = s.get('song', '')
            if name:
                unique_songs.add(name)

    slug_names = _slug_to_name()
    monster_count = 0
    monster_songs = set()
    for slug, perfs in engine.raw_durations.items():
        song_name = slug_names.get(slug, slug)
        for p in perfs:
            if p.get('duration_min', 0) < 20:
                continue
            year = int(p['date'][:4])
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
            monster_count += 1
            monster_songs.add(song_name)

    return jsonify({
        'total_shows': total_shows,
        'unique_songs': len(unique_songs),
        'monster_count': monster_count,
        'monster_songs': len(monster_songs)
    })


@app.route('/api/dashboard/songs-list')
def dashboard_songs_list():
    slug_names = _slug_to_name()
    songs = []
    for slug in sorted(engine.raw_durations.keys()):
        name = slug_names.get(slug, slug.replace('-', ' ').title())
        songs.append({'name': name, 'slug': slug})
    songs.sort(key=lambda x: x['name'])
    return jsonify({'songs': songs})


@app.route('/api/dashboard/shows-per-year')
def dashboard_shows_per_year():
    by_year = defaultdict(lambda: {'show_count': 0, 'unique_songs': set()})
    for show in engine.shows:
        year = int(show['showdate'][:4])
        by_year[year]['show_count'] += 1
        for s in show.get('songs', []):
            by_year[year]['unique_songs'].add(s.get('song', ''))

    years = []
    for year in sorted(by_year.keys()):
        d = by_year[year]
        years.append({
            'year': year,
            'show_count': d['show_count'],
            'unique_songs': len(d['unique_songs'])
        })
    return jsonify({'years': years})


@app.route('/api/dashboard/song-deep-dive')
def dashboard_song_deep_dive():
    song_slug = request.args.get('song', '')
    if not song_slug:
        return jsonify({'error': 'Song slug required'}), 400

    slug_names = _slug_to_name()
    song_name = slug_names.get(song_slug, song_slug.replace('-', ' ').title())

    # Walk all shows to find this song's set positions
    set_positions = defaultdict(int)

    for show in engine.shows:
        songs_in_show = show.get('songs', [])
        sets = defaultdict(list)
        for s in songs_in_show:
            sets[s.get('set', '')].append(s)

        for s in songs_in_show:
            if s.get('song', '') != song_name:
                continue

            set_num = s.get('set', '')
            pos = s.get('position', 0)
            set_songs = sets.get(set_num, [])
            positions_in_set = [x['position'] for x in set_songs]

            if set_num == 'e':
                set_positions['Encore'] += 1
            elif pos == min(positions_in_set) if positions_in_set else False:
                set_positions[f'Set {set_num} Opener'] += 1
            elif pos == max(positions_in_set) if positions_in_set else False:
                set_positions[f'Set {set_num} Closer'] += 1
            else:
                set_positions[f'Set {set_num} Mid'] += 1

    total_plays = sum(set_positions.values())

    # Longest versions from raw durations
    longest_versions = []
    if song_slug in engine.raw_durations:
        sorted_perfs = sorted(engine.raw_durations[song_slug],
                              key=lambda p: -p.get('duration_min', 0))
        for p in sorted_perfs[:10]:
            longest_versions.append({
                'date': p['date'],
                'duration_min': round(p.get('duration_min', 0), 1),
                'venue': p.get('venue', '')
            })

    return jsonify({
        'song_name': song_name,
        'slug': song_slug,
        'total_plays': total_plays,
        'set_positions': dict(set_positions),
        'longest_versions': longest_versions
    })


@app.route('/api/dashboard/bustouts')
def dashboard_bustouts():
    min_gap = request.args.get('min_gap', 50, type=int)

    # Walk shows chronologically, track per-song gap
    song_last_show = {}  # song -> (show_index, date, venue)
    bustouts = []

    for i, show in enumerate(engine.shows):
        for s in show.get('songs', []):
            song_name = s.get('song', '')
            if not song_name:
                continue
            if song_name in song_last_show:
                prev_idx, prev_date, prev_venue = song_last_show[song_name]
                gap = i - prev_idx
                if gap >= min_gap:
                    bustouts.append({
                        'song': song_name,
                        'date': show['showdate'],
                        'venue': show.get('venue', ''),
                        'gap_shows': gap,
                        'previous_date': prev_date
                    })
            song_last_show[song_name] = (i, show['showdate'], show.get('venue', ''))

    bustouts.sort(key=lambda x: -x['gap_shows'])
    return jsonify({'bustouts': bustouts[:50]})


# =============================================================================
# GRATEFUL DATA - DEAD DASHBOARD ENDPOINTS
# =============================================================================

@app.route('/api/dashboard/dead/top-songs')
def dead_top_songs():
    catalog = _get_dead_catalog()
    limit = request.args.get('limit', 10, type=int)
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)

    if start_year or end_year:
        song_counts = defaultdict(int)
        for show in catalog['shows']:
            year = int(show['date'][:4])
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
            for t in show.get('tracks', []):
                name = t.get('song', '')
                if name and name not in DEAD_SKIP_SONGS:
                    song_counts[name] += 1
        songs = [{'name': n, 'play_count': c} for n, c in
                 sorted(song_counts.items(), key=lambda x: -x[1])[:limit]]
    else:
        songs_data = catalog['songs']
        top = sorted(((n, s) for n, s in songs_data.items() if n not in DEAD_SKIP_SONGS),
                     key=lambda x: -x[1].get('total_plays', 0))[:limit]
        songs = [{'name': n, 'play_count': s['total_plays']} for n, s in top]
    return jsonify({'songs': songs})


@app.route('/api/dashboard/dead/summary')
def dead_summary():
    """KPI summary: total shows, unique songs, monsters — all year-filterable."""
    catalog = _get_dead_catalog()
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)

    total_shows = 0
    unique_songs = set()
    for show in catalog['shows']:
        year = int(show['date'][:4])
        if start_year and year < start_year:
            continue
        if end_year and year > end_year:
            continue
        total_shows += 1
        for t in show.get('tracks', []):
            name = t.get('song', '')
            if name and name not in DEAD_SKIP_SONGS:
                unique_songs.add(name)

    # Monsters from song performances
    monster_count = 0
    monster_songs = set()
    for name, sdata in catalog['songs'].items():
        if name in DEAD_SKIP_SONGS:
            continue
        for p in sdata.get('performances', []):
            dur_sec = p.get('duration', 0)
            if dur_sec < 1200:  # 20 min
                continue
            year = int(p['date'][:4])
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
            monster_count += 1
            monster_songs.add(name)

    return jsonify({
        'total_shows': total_shows,
        'unique_songs': len(unique_songs),
        'monster_count': monster_count,
        'monster_songs': len(monster_songs)
    })


@app.route('/api/dashboard/dead/monsters')
def dead_monsters():
    catalog = _get_dead_catalog()
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)
    song_monster_counts = defaultdict(int)
    total = 0
    for name, sdata in catalog['songs'].items():
        if name in DEAD_SKIP_SONGS:
            continue
        for p in sdata.get('performances', []):
            dur_sec = p.get('duration', 0)
            if dur_sec < 1200:
                continue
            year = int(p['date'][:4])
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
            song_monster_counts[name] += 1
            total += 1
    top_songs = [{'name': n, 'count': c} for n, c in
                 sorted(song_monster_counts.items(), key=lambda x: -x[1])[:15]]
    return jsonify({'top_songs': top_songs, 'total_count': total, 'unique_songs': len(song_monster_counts)})


@app.route('/api/dashboard/dead/shows-per-year')
def dead_shows_per_year():
    catalog = _get_dead_catalog()
    by_year = defaultdict(int)
    for show in catalog['shows']:
        by_year[int(show['date'][:4])] += 1
    years = [{'year': y, 'show_count': c} for y, c in sorted(by_year.items())]
    return jsonify({'years': years})


@app.route('/api/dashboard/dead/songs-list')
def dead_songs_list():
    catalog = _get_dead_catalog()
    # Only songs with enough performances to make a meaningful chart
    songs = [{'name': n, 'slug': n} for n, s in catalog['songs'].items()
             if len(s.get('performances', [])) >= 10 and n not in DEAD_SKIP_SONGS]
    songs.sort(key=lambda x: x['name'])
    return jsonify({'songs': songs})


@app.route('/api/dashboard/dead/duration-trend')
def dead_duration_trend():
    catalog = _get_dead_catalog()
    song_name = request.args.get('song', '')
    if not song_name or song_name not in catalog['songs']:
        return jsonify({'error': 'Song not found'}), 404

    song_data = catalog['songs'][song_name]
    by_year = defaultdict(list)
    for p in song_data.get('performances', []):
        dur_sec = p.get('duration', 0)
        if dur_sec <= 0:
            continue
        year = int(p['date'][:4])
        by_year[year].append(dur_sec / 60.0)

    years = []
    for year in sorted(by_year.keys()):
        durations = by_year[year]
        years.append({
            'year': year,
            'avg_min': round(statistics.mean(durations), 2),
            'max_min': round(max(durations), 2),
            'min_min': round(min(durations), 2),
            'count': len(durations)
        })

    return jsonify({'song_name': song_name, 'slug': song_name, 'years': years})


@app.route('/api/dashboard/dead/song-deep-dive')
def dead_song_deep_dive():
    catalog = _get_dead_catalog()
    song_name = request.args.get('song', '')
    if not song_name or song_name not in catalog['songs']:
        return jsonify({'error': 'Song not found'}), 404

    song_data = catalog['songs'][song_name]
    perfs = song_data.get('performances', [])

    # Set positions from show data
    set_positions = defaultdict(int)
    for show in catalog['shows']:
        tracks = show.get('tracks', [])
        song_indices = [i for i, t in enumerate(tracks) if t.get('song', '') == song_name]
        for idx in song_indices:
            if idx == 0:
                set_positions['Opener'] += 1
            elif idx == len(tracks) - 1:
                set_positions['Closer'] += 1
            else:
                set_positions['Mid-Set'] += 1

    total_plays = song_data.get('total_plays', len(perfs))

    # Longest versions
    sorted_perfs = sorted(perfs, key=lambda p: -p.get('duration', 0))
    longest_versions = [{'date': p['date'],
                         'duration_min': round(p.get('duration', 0) / 60.0, 1),
                         'venue': p.get('venue', '')}
                        for p in sorted_perfs[:10] if p.get('duration', 0) > 0]

    return jsonify({
        'song_name': song_name,
        'slug': song_name,
        'total_plays': total_plays,
        'set_positions': dict(set_positions),
        'longest_versions': longest_versions
    })


# =============================================================================
# GENERIC RELISTEN DASHBOARD ENDPOINTS
# =============================================================================

def _register_relisten_dashboard(route_prefix, band_key):
    """Register top-songs, summary, songs-list, duration-trend, song-deep-dive
    endpoints for any band with a Relisten duration index."""

    @app.route(f'/api/dashboard/{route_prefix}/top-songs', endpoint=f'{route_prefix}_top_songs')
    def top_songs():
        limit = request.args.get('limit', 10, type=int)
        start_year = request.args.get('start_year', type=int)
        end_year = request.args.get('end_year', type=int)
        idx = _get_relisten_index(band_key)
        counts = []
        for key, perfs in idx['tracks'].items():
            name = idx['display_names'].get(key, key)
            if name.lower() in RELISTEN_SKIP_TRACKS:
                continue
            if start_year or end_year:
                perfs = [p for p in perfs
                         if (not start_year or int(p['date'][:4]) >= start_year)
                         and (not end_year or int(p['date'][:4]) <= end_year)]
            if perfs:
                counts.append({'name': name, 'play_count': len(perfs)})
        counts.sort(key=lambda x: -x['play_count'])
        return jsonify({'songs': counts[:limit]})

    @app.route(f'/api/dashboard/{route_prefix}/summary', endpoint=f'{route_prefix}_summary')
    def summary():
        idx = _get_relisten_index(band_key)
        start_year = request.args.get('start_year', type=int)
        end_year = request.args.get('end_year', type=int)
        show_dates = set()
        unique_songs = set()
        monster_count = 0
        monster_songs = set()
        for key, perfs in idx['tracks'].items():
            name = idx['display_names'].get(key, key)
            if name.lower() in RELISTEN_SKIP_TRACKS:
                continue
            for p in perfs:
                year = int(p['date'][:4])
                if start_year and year < start_year:
                    continue
                if end_year and year > end_year:
                    continue
                show_dates.add(p['date'])
                unique_songs.add(name)
                if p.get('duration_sec', 0) >= 1200:
                    monster_count += 1
                    monster_songs.add(name)
        return jsonify({
            'total_shows': len(show_dates),
            'unique_songs': len(unique_songs),
            'monster_count': monster_count,
            'monster_songs': len(monster_songs)
        })

    @app.route(f'/api/dashboard/{route_prefix}/monsters', endpoint=f'{route_prefix}_monsters')
    def monsters():
        idx = _get_relisten_index(band_key)
        start_year = request.args.get('start_year', type=int)
        end_year = request.args.get('end_year', type=int)
        song_monster_counts = defaultdict(int)
        total = 0
        for key, perfs in idx['tracks'].items():
            name = idx['display_names'].get(key, key)
            if name.lower() in RELISTEN_SKIP_TRACKS:
                continue
            for p in perfs:
                if p.get('duration_sec', 0) < 1200:
                    continue
                year = int(p['date'][:4])
                if start_year and year < start_year:
                    continue
                if end_year and year > end_year:
                    continue
                song_monster_counts[name] += 1
                total += 1
        top_songs = [{'name': n, 'count': c} for n, c in
                     sorted(song_monster_counts.items(), key=lambda x: -x[1])[:15]]
        return jsonify({'top_songs': top_songs, 'total_count': total, 'unique_songs': len(song_monster_counts)})

    @app.route(f'/api/dashboard/{route_prefix}/shows-per-year', endpoint=f'{route_prefix}_shows_per_year')
    def shows_per_year():
        idx = _get_relisten_index(band_key)
        by_year = defaultdict(set)
        for key, perfs in idx['tracks'].items():
            name = idx['display_names'].get(key, key)
            if name.lower() in RELISTEN_SKIP_TRACKS:
                continue
            for p in perfs:
                by_year[int(p['date'][:4])].add(p['date'])
        years = [{'year': y, 'show_count': len(dates)} for y, dates in sorted(by_year.items())]
        return jsonify({'years': years})

    @app.route(f'/api/dashboard/{route_prefix}/songs-list', endpoint=f'{route_prefix}_songs_list')
    def songs_list():
        idx = _get_relisten_index(band_key)
        songs = []
        for key, perfs in idx['tracks'].items():
            name = idx['display_names'].get(key, key)
            if name.lower() in RELISTEN_SKIP_TRACKS:
                continue
            if len(perfs) >= 10:
                songs.append({'name': name, 'slug': key})
        songs.sort(key=lambda x: x['name'])
        return jsonify({'songs': songs})

    @app.route(f'/api/dashboard/{route_prefix}/duration-trend', endpoint=f'{route_prefix}_duration_trend')
    def duration_trend():
        idx = _get_relisten_index(band_key)
        song_key = request.args.get('song', '')
        if not song_key or song_key not in idx['tracks']:
            return jsonify({'error': 'Song not found'}), 404
        song_name = idx['display_names'].get(song_key, song_key)
        by_year = defaultdict(list)
        for p in idx['tracks'][song_key]:
            dur_sec = p.get('duration_sec', 0)
            if dur_sec <= 0:
                continue
            by_year[int(p['date'][:4])].append(dur_sec / 60.0)
        years = [{'year': y, 'avg_min': round(statistics.mean(d), 2),
                  'max_min': round(max(d), 2), 'min_min': round(min(d), 2),
                  'count': len(d)} for y, d in sorted(by_year.items())]
        return jsonify({'song_name': song_name, 'slug': song_key, 'years': years})

    @app.route(f'/api/dashboard/{route_prefix}/song-deep-dive', endpoint=f'{route_prefix}_song_deep_dive')
    def song_deep_dive():
        idx = _get_relisten_index(band_key)
        song_key = request.args.get('song', '')
        if not song_key or song_key not in idx['tracks']:
            return jsonify({'error': 'Song not found'}), 404
        song_name = idx['display_names'].get(song_key, song_key)
        perfs = idx['tracks'][song_key]
        sorted_perfs = sorted(perfs, key=lambda p: -p.get('duration_sec', 0))
        longest = [{'date': p['date'],
                    'duration_min': round(p.get('duration_sec', 0) / 60.0, 1),
                    'venue': p.get('venue', '')}
                   for p in sorted_perfs[:10] if p.get('duration_sec', 0) > 0]
        return jsonify({
            'song_name': song_name, 'slug': song_key,
            'total_plays': len(perfs), 'set_positions': {},
            'longest_versions': longest
        })


# Register all Relisten-backed band dashboards
_register_relisten_dashboard('umphreys', 'umphreys')
_register_relisten_dashboard('sci', 'sci')
_register_relisten_dashboard('wsp', 'wsp')
_register_relisten_dashboard('spafford', 'spafford')


if __name__ == '__main__':
    print("\n" + "=" * 50)
    print("PhishStats is running!")
    print("Open http://localhost:5050 in your browser")
    print("=" * 50 + "\n")
    app.run(debug=True, port=5050)
