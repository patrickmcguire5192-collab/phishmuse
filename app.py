#!/usr/bin/env python3
"""
PhishStats & JamMuse Web App
============================

Flask app that serves both PhishStats and JamMuse query interfaces.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sys
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))
from scripts.query_engine import PhishStatsEngine
from scripts.jammuse_engine import UnifiedJamMuse, BANDS

app = Flask(__name__, static_folder='ui')
CORS(app)

# Initialize PhishStats query engine (for legacy /api/query endpoint)
engine = PhishStatsEngine()
engine.load_data()

# Initialize unified JamMuse engine (handles all bands including Phish)
unified_engine = None

@app.route('/')
def index():
    """Serve the main UI."""
    return send_from_directory('ui', 'app.html')

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


if __name__ == '__main__':
    print("\n" + "=" * 50)
    print("PhishStats is running!")
    print("Open http://localhost:5050 in your browser")
    print("=" * 50 + "\n")
    app.run(debug=True, port=5050)
