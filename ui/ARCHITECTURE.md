# PhishStats Architecture

## Overview

PhishStats is a natural language query interface for Phish statistics, inspired by StatMuse. Users ask questions in plain English and get instant, beautifully formatted answers.

## The Key Insight: LLM-Powered Query Parsing

Instead of writing regex patterns or specific handlers for each query type, we use Claude to:

1. **Parse** the natural language query
2. **Determine** which API(s) to call
3. **Extract** the relevant parameters
4. **Format** the response for display

This makes the system inherently flexible - it can handle queries we never explicitly programmed for.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        USER INTERFACE                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  "What's the longest Tweezer ever played?"              │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     QUERY PROCESSOR (Claude)                     │
│                                                                  │
│  Input: Natural language query                                   │
│  Output: Structured intent + API calls needed                    │
│                                                                  │
│  Example output:                                                 │
│  {                                                               │
│    "intent": "longest_performance",                              │
│    "song": "Tweezer",                                            │
│    "apis": ["phishin_tracks"],                                   │
│    "response_template": "The longest {song} was {duration}       │
│                          minutes, played on {date} at {venue}."  │
│  }                                                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        API LAYER                                 │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   Phish.net API  │    │   Phish.in API   │                   │
│  │                  │    │                  │                   │
│  │  • Setlists      │    │  • Durations     │                   │
│  │  • Shows         │    │  • Track times   │                   │
│  │  • Songs         │    │  • Audio URLs    │                   │
│  │  • Venues        │    │  • Jam charts    │                   │
│  │  • Jam charts    │    │                  │                   │
│  └──────────────────┘    └──────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RESPONSE FORMATTER                            │
│                                                                  │
│  Takes raw API data + response template                          │
│  Outputs: Formatted answer + visualization data                  │
│                                                                  │
│  {                                                               │
│    "answer": "The longest Tweezer was 50.3 minutes...",         │
│    "highlight": "50.3 minutes",                                  │
│    "card": {                                                     │
│      "title": "Tweezer",                                         │
│      "stat": "50:18",                                            │
│      "subtitle": "December 31, 1999 • Big Cypress",              │
│      "context": "This is 3.7x longer than average (13.6 min)"   │
│    }                                                             │
│  }                                                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        USER INTERFACE                            │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ╔═══════════════════════════════════════════════════╗  │    │
│  │  ║  TWEEZER                                          ║  │    │
│  │  ║  ─────────────────────────────────────────────────║  │    │
│  │  ║           50:18                                   ║  │    │
│  │  ║  December 31, 1999 • Big Cypress                  ║  │    │
│  │  ║                                                   ║  │    │
│  │  ║  This is 3.7x longer than average (13.6 min)     ║  │    │
│  │  ╚═══════════════════════════════════════════════════╝  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Available Data Sources

### Phish.net API (v5)
- **Setlists**: Every song played at every show
- **Shows**: Dates, venues, ratings
- **Songs**: Metadata, debut dates, gap info
- **Venues**: Location data, show counts
- **Jam Charts**: Curator-selected notable jams

### Phish.in API (v2)
- **Track Durations**: Exact length in milliseconds
- **Show Durations**: Total show length
- **Audio URLs**: MP3 streaming links
- **Waveform Images**: Visual representation

## Example Queries & How They'd Be Processed

### "What's the longest Tweezer ever?"
→ Phish.in `/tracks?song_slug=tweezer` → Sort by duration → Return max

### "How many times has Sand been played in Mexico?"
→ Phish.net `/setlists` filtered by song + venue country → Count

### "Show me all 20+ minute Ghosts from 2023"
→ Phish.in `/tracks?song_slug=ghost` → Filter by year + duration > 20min

### "What songs have never been played at MSG?"
→ Phish.net all songs - songs played at MSG venue

### "What's the average gap for Fluffhead?"
→ Phish.net `/songs/fluffhead` → Get gap data

### "When was the last time they opened with Tweezer?"
→ Phish.net `/setlists` → Filter position=1, song=Tweezer → Most recent

## Why This Architecture is Flexible

1. **No hardcoded query patterns**: Claude interprets intent
2. **New query types automatically work**: If the data exists, Claude can find it
3. **Natural language variations handled**: "longest", "most lengthy", "biggest" all work
4. **Compound queries possible**: "longest Tweezer in the last 5 years at outdoor venues"
5. **Context awareness**: Follow-up questions can reference previous answers

## Tech Stack

- **Frontend**: React (chat UI with StatMuse-inspired design)
- **Query Processing**: Claude API (Haiku for speed, Sonnet for complex queries)
- **Data APIs**: Phish.net v5 + Phish.in v2
- **Caching**: Local cache for frequently accessed data
