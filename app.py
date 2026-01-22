#!/usr/bin/env python3
"""
PhishStats Web App
==================

Simple Flask app that serves the PhishStats query interface.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sys
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))
from scripts.query_engine import PhishStatsEngine

app = Flask(__name__, static_folder='ui')
CORS(app)

# Initialize query engine
engine = PhishStatsEngine()
engine.load_data()

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

if __name__ == '__main__':
    print("\n" + "=" * 50)
    print("PhishStats is running!")
    print("Open http://localhost:5050 in your browser")
    print("=" * 50 + "\n")
    app.run(debug=True, port=5050)
