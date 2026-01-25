#!/usr/bin/env python3
"""
Setlist.fm Engine for JamMuse
=============================

Query engine for bands available through the setlist.fm API.
Supports Umphrey's McGee, Widespread Panic, moe., STS9, Billy Strings, and more.

Note: setlist.fm doesn't have duration data or jam chart ratings like Songfish,
so we can answer play counts, gaps, setlists, debuts - but not "longest" or "best" queries.
"""

import json
import re
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
from collections import Counter

# Cache directory
CACHE_DIR = Path(__file__).parent.parent / "data" / "setlistfm_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# API Key
SETLISTFM_API_KEY = "TRwP_Xfw6E0DTrr4WA7rDsbHluE-1zA4v5hE"


@dataclass
class QueryResult:
    """Structured result from a query."""
    success: bool
    answer: str
    band: str = None
    highlight: Optional[str] = None
    card_data: Optional[Dict[str, Any]] = None
    related_queries: Optional[List[str]] = None
    raw_data: Optional[Any] = None


# =============================================================================
# BAND CONFIGURATIONS - Setlist.fm bands
# =============================================================================

SETLISTFM_BANDS = {
    "umphreys": {
        "name": "Umphrey's McGee",
        "mbid": "3826a6e0-9ea5-4007-941c-25b9dd943981",
        "aliases": ["umphreys", "umph", "umphreys mcgee", "um", "umphrey's"],

        # Song aliases - how fans refer to songs
        "song_aliases": {
            "anchor drops": "Anchor Drops",
            "anchor": "Anchor Drops",
            "abd": "Anchor Drops",
            "all in time": "All in Time",
            "ait": "All in Time",
            "bad poker": "Bad Poker",
            "booth love": "Booth Love",
            "booth": "Booth Love",
            "bridgeless": "Bridgeless",
            "bright lights": "Bright Lights, Big City",
            "cemetery walk": "Cemetery Walk",
            "cem walk": "Cemetery Walk",
            "conduit": "Conduit",
            "cut the cable": "Cut the Cable",
            "ctc": "Cut the Cable",
            "day nurse": "Day Nurse",
            "dump city": "Dump City",
            "front porch": "Front Porch",
            "glory": "Glory",
            "hajimemashite": "Hajimemashite",
            "haji": "Hajimemashite",
            "higgins": "Higgins",
            "hurt bird bath": "Hurt Bird Bath",
            "hbb": "Hurt Bird Bath",
            "in the kitchen": "In the Kitchen",
            "itk": "In the Kitchen",
            "jai alai": "Jai Alai",
            "jai": "Jai Alai",
            "jajunk": "JaJunk",
            "jimmy stewart": "Jimmy Stewart",
            "jimmy": "Jimmy Stewart",
            "kabump": "Kabump",
            "mantis": "Mantis",
            "miami virtue": "Miami Virtue",
            "miami": "Miami Virtue",
            "miss tinkles": "Miss Tinkle's Overture",
            "mto": "Miss Tinkle's Overture",
            "mulches oddity": "Mulche's Odyssey",
            "mulches": "Mulche's Odyssey",
            "nemo": "Nemo",
            "nothing too fancy": "Nothing Too Fancy",
            "ntf": "Nothing Too Fancy",
            "ocean billy": "Ocean Billy",
            "padgett's profile": "Padgett's Profile",
            "padgetts": "Padgett's Profile",
            "pay the snucka": "Pay the Snucka",
            "snucka": "Pay the Snucka",
            "phil's farm": "Phil's Farm",
            "phils farm": "Phil's Farm",
            "plunger": "Plunger",
            "prowler": "Prowler",
            "puppet string": "Puppet String",
            "push the pig": "Push the Pig",
            "ptp": "Push the Pig",
            "red tape": "Red Tape",
            "resolution": "Resolution",
            "ringo": "Ringo",
            "robot world": "Robot World",
            "rocker": "The Rocker",
            "the rocker": "The Rocker",
            "search 4": "Search 4",
            "slacker": "Slacker",
            "small world": "Small World",
            "sociable jimmy": "Sociable Jimmy",
            "stinko's ascension": "Stinko's Ascension",
            "stinkos": "Stinko's Ascension",
            "stranger danger": "Stranger Danger",
            "summer camp": "Summer Camp",
            "syncopated strangers": "Syncopated Strangers",
            "the floor": "The Floor",
            "floor": "The Floor",
            "the linear": "The Linear",
            "linear": "The Linear",
            "the triple wide": "The Triple Wide",
            "triple wide": "The Triple Wide",
            "ttw": "The Triple Wide",
            "thin air": "Thin Air",
            "tokyo": "Tokio",
            "tokio": "Tokio",
            "tribute to the spinal shaft": "Tribute to the Spinal Shaft",
            "spinal shaft": "Tribute to the Spinal Shaft",
            "ttss": "Tribute to the Spinal Shaft",
            "turn and run": "Turn & Run",
            "tuar": "Turn & Run",
            "utopian fir": "Utopian Fir",
            "wappy sprayberry": "Wappy Sprayberry",
            "wappy": "Wappy Sprayberry",
            "wife soup": "Wife Soup",
            "wizard burial ground": "Wizard Burial Ground",
            "wbg": "Wizard Burial Ground",
            "women wine and song": "Women Wine and Song",
            "wws": "Women Wine and Song",
            "words": "Words",
            "zonkey": "Zonkey",
            "2x2": "2x2",
            "40s theme": "40's Theme",
            "1348": "1348",
        },

        "jam_vehicles": [
            "Wizard Burial Ground", "Mantis", "Prowler", "Tribute to the Spinal Shaft",
            "2x2", "All in Time", "Anchor Drops", "The Triple Wide", "Bridgeless",
            "Booth Love", "In the Kitchen", "Conduit", "Higgins"
        ],
    },

    "wsp": {
        "name": "Widespread Panic",
        "mbid": "3797a6d0-7700-44bf-96fb-f44386bc9ab2",
        "aliases": ["widespread", "wsp", "panic", "widespread panic"],

        "song_aliases": {
            "aint life grand": "Ain't Life Grand",
            "airplane": "Airplane",
            "all time low": "All Time Low",
            "angels on high": "Angels on High",
            "apache": "Apache",
            "arleen": "Arleen",
            "aunt avis": "Aunt Avis",
            "barstools": "Barstools and Dreamers",
            "barstools and dreamers": "Barstools and Dreamers",
            "bear's gone fishin": "Bear's Gone Fishin'",
            "bears gone fishin": "Bear's Gone Fishin'",
            "big wooly mammoth": "Big Wooly Mammoth",
            "blackout blues": "Blackout Blues",
            "blue indian": "Blue Indian",
            "bowlegged woman": "Bowlegged Woman",
            "bust it big": "Bust It Big",
            "can't get high": "Can't Get High",
            "cant get high": "Can't Get High",
            "chilly water": "Chilly Water",
            "chilly": "Chilly Water",
            "climb to safety": "Climb to Safety",
            "coconut": "Coconut",
            "contentment blues": "Contentment Blues",
            "counting train cars": "Counting Train Cars",
            "ctc": "Counting Train Cars",
            "diner": "Diner",
            "disco": "Disco",
            "don't wanna lose you": "Don't Wanna Lose You",
            "drums": "Drums",
            "driving song": "Driving Song",
            "driving": "Driving Song",
            "expiration day": "Expiration Day",
            "fishwater": "Fishwater",
            "gimme": "Gimme",
            "goodpeople": "Goodpeople",
            "greta": "Greta",
            "hatfield": "Hatfield",
            "henry parsons died": "Henry Parsons Died",
            "henry parsons": "Henry Parsons Died",
            "holden oversoul": "Holden Oversoul",
            "holden": "Holden Oversoul",
            "hope in a hopeless world": "Hope in a Hopeless World",
            "i'm not alone": "I'm Not Alone",
            "imitation leather shoes": "Imitation Leather Shoes",
            "impossible": "Impossible",
            "jack": "Jack",
            "jamais vu": "Jamais Vu",
            "junior": "Junior",
            "lake wailin": "Lake Wailin'",
            "lawyers guns and money": "Lawyers, Guns and Money",
            "lets get down to business": "Let's Get Down to Business",
            "lets get the show on the road": "Let's Get the Show on the Road",
            "little kin": "Little Kin",
            "love tractor": "Love Tractor",
            "loves got a hold on me": "Loves Got a Hold on Me",
            "machine": "Machine",
            "maggot brain": "Maggot Brain",
            "makes sense to me": "Makes Sense to Me",
            "melon head": "Melon Head",
            "mercy": "Mercy",
            "mikey d": "Mikey D",
            "nene": "Nene",
            "north": "North",
            "old neighborhood": "Old Neighborhood",
            "one arm steve": "One Arm Steve",
            "party at your mama's house": "Party at Your Mama's House",
            "pickin up the pieces": "Pickin' Up the Pieces",
            "pigeons": "Pigeons",
            "pilgrims": "Pilgrims",
            "porch song": "Porch Song",
            "postcard": "Postcard",
            "protein drink": "Protein Drink",
            "proving ground": "Proving Ground",
            "rebirtha": "Rebirtha",
            "red hot mama": "Red Hot Mama",
            "ride me high": "Ride Me High",
            "ribs and whiskey": "Ribs and Whiskey",
            "rock": "Rock",
            "sampson's church": "Sampson's Church",
            "shut up and drive": "Shut Up and Drive",
            "sleepy monkey": "Sleepy Monkey",
            "space wrangler": "Space Wrangler",
            "stop breakin down": "Stop-Breakin' Down",
            "surprise valley": "Surprise Valley",
            "tall boy": "Tall Boy",
            "the take out": "The Take Out",
            "this part of town": "This Part of Town",
            "thought sausage": "Thought Sausage",
            "three weeks": "Three Weeks",
            "time keeps on slipping": "Time Keeps on Slipping",
            "time zones": "Time Zones",
            "tired bones": "Tired Bones",
            "travelin light": "Travelin' Light",
            "up all night": "Up All Night",
            "vacation": "Vacation",
            "waker": "Waker",
            "walkin": "Walkin' (For Your Love)",
            "weapons": "Weapons",
            "wonderin": "Wonderin'",
            "wont lose again": "Won't Lose Again",
            "wurm": "Wurm",
        },

        "jam_vehicles": [
            "Chilly Water", "Holden Oversoul", "Porch Song", "Space Wrangler",
            "Driving Song", "Pilgrims", "Climb to Safety", "North", "Fishwater",
            "Ain't Life Grand", "Tall Boy", "Surprise Valley"
        ],
    },

    "moe": {
        "name": "moe.",
        "mbid": "5fab339d-5dd4-42b0-8d70-496a4493ed59",
        "aliases": ["moe", "moe."],

        "song_aliases": {
            "akimbo": "Akimbo",
            "all roads": "All Roads",
            "annihilation blues": "Annihilation Blues",
            "brent black": "Brent Black",
            "buster": "Buster",
            "captain america": "Captain America",
            "chromatic": "Chromatic",
            "code talker": "Code Talker",
            "crab eyes": "Crab Eyes",
            "dr gundys slow intravenous drip": "Dr. Gundys Slow Intravenous Drip",
            "dr gundys": "Dr. Gundys Slow Intravenous Drip",
            "dr. gundys": "Dr. Gundys Slow Intravenous Drip",
            "down boy": "Down Boy",
            "four": "Four",
            "happy hour hero": "Happy Hour Hero",
            "head": "Head",
            "hi and lo": "Hi & Lo",
            "hi & lo": "Hi & Lo",
            "interstellar overdrive": "Interstellar Overdrive",
            "jazz wank": "Jazz Wank",
            "joe the bee": "Joe the Bee",
            "kids": "Kids",
            "kyle": "Kyle's Song",
            "kyles song": "Kyle's Song",
            "kyle's song": "Kyle's Song",
            "lazarus": "Lazarus",
            "letter home": "Letter Home",
            "locomotive": "Locomotive",
            "meat": "Meat",
            "moth": "Moth",
            "nebraska": "Nebraska",
            "new york city": "New York City",
            "not coming down": "Not Coming Down",
            "ncd": "Not Coming Down",
            "opium": "Opium",
            "okayalright": "Okayalright",
            "plane crash": "Plane Crash",
            "queen of everything": "Queen of Everything",
            "rebubula": "Rebubula",
            "recreational chemistry": "Recreational Chemistry",
            "rec chem": "Recreational Chemistry",
            "run away": "Run Away",
            "st augustine": "St. Augustine",
            "san ber'dino": "San Ber'dino",
            "spine of a dog": "Spine of a Dog",
            "stranger than fiction": "Stranger Than Fiction",
            "stf": "Stranger Than Fiction",
            "the bones of babelfish": "The Bones of Babelfish",
            "babelfish": "The Bones of Babelfish",
            "the pit": "The Pit",
            "timmy tucker": "Timmy Tucker",
            "toboggan": "Toboggan",
            "tubing": "Tubing",
            "waiting room": "Waiting Room",
            "wormwood": "Wormwood",
            "yodelittle": "Yodelittle",
            "32 things": "32 Things",
        },

        "jam_vehicles": [
            "Rebubula", "Buster", "Meat", "Spine of a Dog", "The Pit",
            "Recreational Chemistry", "Kyle's Song", "Timmy Tucker",
            "Head", "Plane Crash", "Stranger Than Fiction"
        ],
    },

    "sts9": {
        "name": "STS9",
        "mbid": "8d07ac81-0b49-4ec3-9402-2b8b479649a2",
        "aliases": ["sts9", "sound tribe", "sector 9", "tribe"],

        "song_aliases": {
            "abcees": "ABCees",
            "arigato": "Arigato",
            "artifact": "Artifact",
            "beautiful day": "Beautiful Day",
            "breathe in": "Breathe In",
            "central": "Central",
            "click lang echo": "Click Lang Echo",
            "circus": "Circus",
            "dance": "Dance",
            "disco": "Disco",
            "domino": "Domino",
            "ebb and flow": "Ebb & Flow",
            "empires": "Empires",
            "equinox": "Equinox",
            "evasive pursuit": "Evasive Pursuit",
            "f word": "F Word",
            "firewall": "Firewall",
            "four year puma": "Four Year Puma",
            "fyp": "Four Year Puma",
            "gibberish": "Gibberish",
            "glass": "Glass",
            "golden gate": "Golden Gate",
            "gobnugget": "Gobnugget",
            "hubble": "Hubble",
            "instant classic": "Instant Classic",
            "kamuy": "Kamuy",
            "luma daylight": "Luma Daylight",
            "magnetic flux": "Magnetic Flux",
            "moon socket": "Moon Socket",
            "moonsocket": "Moon Socket",
            "mps drum jam": "MPS Drum Jam",
            "mps": "MPS Drum Jam",
            "ngrt": "NGRT",
            "new soma": "New Soma",
            "now": "Now",
            "once in a lifetime": "Once in a Lifetime",
            "open e": "Open E",
            "phoneme": "Phoneme",
            "pianomosity": "Pianomosity",
            "poseidon": "Poseidon",
            "ramone and emiglio": "Ramone and Emiglio",
            "rent": "Rent",
            "ruff it up": "Ruff It Up",
            "scheme": "Scheme",
            "simulator": "Simulator",
            "shock doctrine": "Shock Doctrine",
            "shipwreck": "Shipwreck",
            "six pack": "Six Pack",
            "something": "Something",
            "tap in": "Tap In",
            "the unquestionable supremacy": "The Unquestionable Supremacy",
            "totem": "Totem",
            "vibyl": "Vibyl",
            "walk to the light": "Walk to the Light",
            "warrior": "Warrior",
            "when the dust settles": "When the Dust Settles",
            "wtds": "When the Dust Settles",
            "world go round": "World Go Round",
            "wgr": "World Go Round",
        },

        "jam_vehicles": [
            "Scheme", "Kamuy", "Circus", "World Go Round", "Tap In",
            "When the Dust Settles", "Phoneme", "Gobnugget", "Artifact",
            "Central", "Click Lang Echo", "Poseidon"
        ],
    },

    "billy": {
        "name": "Billy Strings",
        "mbid": "640db492-34c4-47df-be14-96e2cd4b9fe4",
        "aliases": ["billy strings", "billy", "bmfs"],

        "song_aliases": {
            "away from the mire": "Away from the Mire",
            "black mountain rag": "Black Mountain Rag",
            "brown dog": "Brown Dog",
            "catch and release": "Catch and Release",
            "china doll": "China Doll",
            "cocaine blues": "Cocaine Blues",
            "deja vu": "Deja Vu",
            "dos banjos": "Dos Banjos",
            "dust in a baggie": "Dust in a Baggie",
            "enough to leave": "Enough to Leave",
            "fire line": "Fire Line",
            "fire on my tongue": "Fire on My Tongue",
            "freeborn man": "Freeborn Man",
            "guitar peace": "Guitar Peace",
            "hartford": "Hartford",
            "heartbeat of america": "Heartbeat of America",
            "hollow heart": "Hollow Heart",
            "home": "Home",
            "ice bridges": "Ice Bridges",
            "in the morning light": "In the Morning Light",
            "know it all": "Know It All",
            "leaders": "Leaders",
            "lonesome midnight waltz": "Lonesome Midnight Waltz",
            "love and regret": "Love and Regret",
            "love like me": "Love Like Me",
            "meet me at the creek": "Meet Me at the Creek",
            "creek": "Meet Me at the Creek",
            "must be seven": "Must Be Seven",
            "ole slew foot": "Ole Slew-Foot",
            "on the line": "On the Line",
            "pyramid country": "Pyramid Country",
            "red daisy": "Red Daisy",
            "redwood": "Redwood",
            "renewal": "Renewal",
            "revolution": "Revolution",
            "ride or die": "Ride or Die",
            "rock of ages": "Rock of Ages",
            "running": "Running",
            "secrets": "Secrets",
            "show me the door": "Show Me the Door",
            "slow train": "Slow Train",
            "small doses": "Small Doses",
            "snow on the pine tops": "Snow on the Pines",
            "so much for trying": "So Much for Trying",
            "split lip": "Split Lip",
            "taking water": "Taking Water",
            "the great divide": "The Great Divide",
            "this old world": "This Old World",
            "thunder": "Thunder",
            "turmoil and tinfoil": "Turmoil and Tinfoil",
            "watch it fall": "Watch It Fall",
            "wargasm": "Wargasm",
            "while im waiting here": "While I'm Waiting Here",
        },

        "jam_vehicles": [
            "Dust in a Baggie", "Meet Me at the Creek", "Away from the Mire",
            "Taking Water", "Fire Line", "Thunder", "Turmoil and Tinfoil",
            "Red Daisy", "Know It All", "Secrets"
        ],
    },
}


class SetlistFMEngine:
    """Query engine for setlist.fm bands."""

    def __init__(self, band_key: str):
        """Initialize engine for a specific band."""
        if band_key not in SETLISTFM_BANDS:
            raise ValueError(f"Unknown band: {band_key}. Available: {list(SETLISTFM_BANDS.keys())}")

        self.band_key = band_key
        self.config = SETLISTFM_BANDS[band_key]
        self.band_name = self.config["name"]
        self.mbid = self.config["mbid"]

        # Caches
        self._setlists_cache = None
        self._songs_cache = None

    def _fetch_api(self, endpoint: str, params: dict = None) -> dict:
        """Fetch from setlist.fm API with caching."""
        cache_key = f"{self.band_key}_{endpoint.replace('/', '_')}"
        if params:
            param_str = "_".join(f"{k}_{v}" for k, v in sorted(params.items()))
            cache_key += f"_{param_str}"
        cache_file = CACHE_DIR / f"{cache_key}.json"

        # Check cache (1 hour expiry)
        if cache_file.exists():
            age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if age < 3600:
                with open(cache_file) as f:
                    return json.load(f)

        # Build URL
        base_url = "https://api.setlist.fm/rest/1.0"
        url = f"{base_url}/{endpoint}"
        if params:
            param_str = "&".join(f"{k}={v}" for k, v in params.items())
            url += f"?{param_str}"

        try:
            req = urllib.request.Request(url, headers={
                "x-api-key": SETLISTFM_API_KEY,
                "Accept": "application/json",
                "User-Agent": "JamMuse/1.0"
            })
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())

            # Cache response
            with open(cache_file, 'w') as f:
                json.dump(data, f)

            return data
        except Exception as e:
            print(f"API error for {url}: {e}")
            if cache_file.exists():
                with open(cache_file) as f:
                    return json.load(f)
            return {}

    def _get_all_setlists(self, max_pages: int = 50) -> List[dict]:
        """Get all setlists for this band (paginated)."""
        if self._setlists_cache is not None:
            return self._setlists_cache

        all_setlists = []
        page = 1

        while page <= max_pages:
            data = self._fetch_api(f"artist/{self.mbid}/setlists", {"p": str(page)})
            setlists = data.get("setlist", [])
            if not setlists:
                break
            all_setlists.extend(setlists)

            total = data.get("total", 0)
            if len(all_setlists) >= total:
                break
            page += 1

        self._setlists_cache = all_setlists
        return all_setlists

    def _get_song_performances(self, song_name: str) -> List[dict]:
        """Get all performances of a specific song."""
        # Search setlists for this song
        # Note: setlist.fm API doesn't have a direct song search, so we use cached setlists
        setlists = self._get_all_setlists()

        performances = []
        song_lower = song_name.lower()

        for setlist in setlists:
            sets = setlist.get("sets", {}).get("set", [])
            for s in sets:
                songs = s.get("song", [])
                for song in songs:
                    if song.get("name", "").lower() == song_lower:
                        performances.append({
                            "date": setlist.get("eventDate"),
                            "venue": setlist.get("venue", {}).get("name", "Unknown"),
                            "city": setlist.get("venue", {}).get("city", {}).get("name", ""),
                            "setlist_id": setlist.get("id")
                        })
                        break  # Only count once per show

        return performances

    def _normalize_song_name(self, query: str) -> Optional[str]:
        """Normalize song name from query using aliases."""
        query_lower = query.lower().strip()

        # Remove filler words
        filler_phrases = ["how many times", "when did they", "tell me about", "gap on", "gap for"]
        for phrase in filler_phrases:
            query_lower = query_lower.replace(phrase, " ")

        filler_words = ["play", "played", "last", "first", "stats", "have", "they", "has", "been"]
        for word in filler_words:
            query_lower = re.sub(rf'\b{word}\b', ' ', query_lower)

        query_lower = re.sub(r'[?!.,]', '', query_lower)
        query_lower = " ".join(query_lower.split()).strip()

        if not query_lower:
            return None

        # Check aliases
        song_aliases = self.config.get("song_aliases", {})
        if query_lower in song_aliases:
            return song_aliases[query_lower]

        # Check partial matches in aliases
        for alias, song_name in song_aliases.items():
            if alias in query_lower or query_lower in alias:
                return song_name

        # Return as-is (title case) if no alias found
        return query_lower.title()

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse setlist.fm date format (DD-MM-YYYY)."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%d-%m-%Y")
        except:
            return None

    def _format_date(self, date_str: str) -> str:
        """Format date for display."""
        dt = self._parse_date(date_str)
        if dt:
            return dt.strftime("%Y-%m-%d")
        return date_str

    # =========================================================================
    # QUERY METHODS
    # =========================================================================

    def query_play_count(self, song_name: str) -> QueryResult:
        """How many times has a song been played?"""
        normalized = self._normalize_song_name(song_name) or song_name
        performances = self._get_song_performances(normalized)
        count = len(performances)

        if count == 0:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find any performances of '{normalized}' by {self.band_name}. "
                       f"Try one of their popular songs: {', '.join(self.config['jam_vehicles'][:3])}."
            )

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"{self.band_name} has played {normalized} {count} times.",
            highlight=str(count),
            card_data={
                "type": "count",
                "title": normalized,
                "stat": count,
                "stat_label": "times played"
            },
            related_queries=[
                f"When did they last play {normalized}?",
                f"When did they first play {normalized}?"
            ]
        )

    def query_gap(self, song_name: str) -> QueryResult:
        """How many shows since a song was last played?"""
        normalized = self._normalize_song_name(song_name) or song_name
        performances = self._get_song_performances(normalized)

        if not performances:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find any performances of '{normalized}'."
            )

        # Sort by date (most recent first)
        performances.sort(key=lambda x: self._parse_date(x["date"]) or datetime.min, reverse=True)
        last = performances[0]
        last_date = self._format_date(last["date"])
        venue = last["venue"]
        city = last["city"]

        # Count shows since
        setlists = self._get_all_setlists()
        last_dt = self._parse_date(last["date"])
        shows_since = 0
        for sl in setlists:
            sl_dt = self._parse_date(sl.get("eventDate"))
            if sl_dt and last_dt and sl_dt > last_dt:
                shows_since += 1

        location = f"{venue}, {city}" if city else venue

        if shows_since == 0:
            answer = f"{normalized} was played at the most recent show ({last_date} at {location})."
        else:
            answer = f"{normalized} was last played {shows_since} shows ago on {last_date} at {location}."

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=answer,
            highlight=str(shows_since),
            card_data={
                "type": "gap",
                "title": normalized,
                "stat": shows_since,
                "stat_label": "shows ago",
                "extra": {
                    "last_played": last_date,
                    "venue": location
                }
            },
            related_queries=[
                f"How many times have they played {normalized}?",
                f"When did they first play {normalized}?"
            ]
        )

    def query_first_played(self, song_name: str) -> QueryResult:
        """When was a song first played (debut)?"""
        normalized = self._normalize_song_name(song_name) or song_name
        performances = self._get_song_performances(normalized)

        if not performances:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find any performances of '{normalized}'."
            )

        # Sort by date (oldest first)
        performances.sort(key=lambda x: self._parse_date(x["date"]) or datetime.max)
        first = performances[0]
        first_date = self._format_date(first["date"])
        venue = first["venue"]
        city = first["city"]

        location = f"{venue}, {city}" if city else venue

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=f"{normalized} debuted on {first_date} at {location}.",
            card_data={
                "type": "debut",
                "title": normalized,
                "extra": {
                    "debut_date": first_date,
                    "venue": location
                }
            },
            related_queries=[
                f"How many times have they played {normalized}?",
                f"When did they last play {normalized}?"
            ]
        )

    def query_song_stats(self, song_name: str) -> QueryResult:
        """Get comprehensive stats for a song."""
        normalized = self._normalize_song_name(song_name) or song_name
        performances = self._get_song_performances(normalized)

        if not performances:
            return QueryResult(
                success=False,
                band=self.band_name,
                answer=f"I couldn't find '{normalized}' in {self.band_name}'s catalog."
            )

        count = len(performances)

        # Sort for first/last
        performances.sort(key=lambda x: self._parse_date(x["date"]) or datetime.max)
        first = performances[0]
        last = performances[-1]

        first_date = self._format_date(first["date"])
        last_date = self._format_date(last["date"])

        # Count shows since last
        setlists = self._get_all_setlists()
        last_dt = self._parse_date(last["date"])
        shows_since = sum(1 for sl in setlists if self._parse_date(sl.get("eventDate", "")) and
                         self._parse_date(sl.get("eventDate", "")) > last_dt)

        lines = [
            f"**{normalized}** - {self.band_name} Stats\n",
            f"Times played: {count}",
            f"First played: {first_date}",
            f"Last played: {last_date}",
        ]

        if shows_since > 0:
            lines.append(f"Current gap: {shows_since} shows")

        return QueryResult(
            success=True,
            band=self.band_name,
            answer="\n".join(lines),
            highlight=str(count),
            card_data={
                "type": "stats",
                "title": normalized,
                "stat": count,
                "stat_label": "times played",
                "extra": {
                    "first_played": first_date,
                    "last_played": last_date,
                    "gap": shows_since
                }
            },
            related_queries=[
                f"When did they last play {normalized}?",
                f"When did they first play {normalized}?"
            ]
        )

    def query_show_count(self, year: int = None) -> QueryResult:
        """How many shows has the band played?"""
        setlists = self._get_all_setlists()

        if year:
            setlists = [sl for sl in setlists if str(year) in sl.get("eventDate", "")]
            answer = f"{self.band_name} played {len(setlists)} shows in {year}."
        else:
            answer = f"{self.band_name} has played {len(setlists)} shows (in setlist.fm database)."

        return QueryResult(
            success=True,
            band=self.band_name,
            answer=answer,
            highlight=str(len(setlists))
        )

    def query_setlist(self, date: str) -> QueryResult:
        """Get setlist for a specific date."""
        # Normalize date format
        date_clean = date.replace("/", "-")

        # Try to find the show
        setlists = self._get_all_setlists()

        for sl in setlists:
            sl_date = sl.get("eventDate", "")
            formatted = self._format_date(sl_date)
            if formatted == date_clean or sl_date == date_clean:
                venue = sl.get("venue", {}).get("name", "Unknown")
                city = sl.get("venue", {}).get("city", {}).get("name", "")

                lines = [f"**{self.band_name}** - {formatted}"]
                if city:
                    lines.append(f"*{venue}, {city}*\n")
                else:
                    lines.append(f"*{venue}*\n")

                sets = sl.get("sets", {}).get("set", [])
                for i, s in enumerate(sets):
                    set_name = s.get("name", f"Set {i+1}")
                    songs = [song.get("name", "?") for song in s.get("song", [])]
                    if songs:
                        lines.append(f"**{set_name}** {', '.join(songs)}")

                if not sets:
                    lines.append("(Setlist not yet entered)")

                return QueryResult(
                    success=True,
                    band=self.band_name,
                    answer="\n".join(lines)
                )

        return QueryResult(
            success=False,
            band=self.band_name,
            answer=f"No setlist found for {self.band_name} on {date}."
        )

    # =========================================================================
    # MAIN QUERY ROUTER
    # =========================================================================

    def query(self, question: str) -> QueryResult:
        """Route a natural language question to the appropriate handler."""
        q = question.lower().strip()

        # Setlist queries
        date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})', question)
        if date_match and any(word in q for word in ["setlist", "set list", "what did they play"]):
            return self.query_setlist(date_match.group(1))

        # Gap queries
        if any(word in q for word in ["gap on", "gap for", "how long since", "when did they last", "last played", "last time"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_gap(song)

        # First played / debut queries
        if any(word in q for word in ["first time", "debut", "first played", "when did they first"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_first_played(song)

        # Play count queries
        if any(word in q for word in ["how many times", "how often", "play count", "times played"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_play_count(song)

        # Show count queries
        if any(word in q for word in ["how many shows", "show count", "total shows"]):
            year_match = re.search(r'\b(20\d{2}|19\d{2})\b', question)
            year = int(year_match.group(1)) if year_match else None
            return self.query_show_count(year=year)

        # Stats queries
        if any(word in q for word in ["stats", "statistics", "info on", "tell me about"]):
            song = self._normalize_song_name(question)
            if song:
                return self.query_song_stats(song)

        # Default: try song stats
        song = self._normalize_song_name(question)
        if song:
            return self.query_song_stats(song)

        # Fallback
        return QueryResult(
            success=False,
            band=self.band_name,
            answer=f"I'm not sure how to answer that about {self.band_name}. Try asking about:\n"
                   f"- Song stats: '{self.config['jam_vehicles'][0]} stats'\n"
                   f"- Play count: 'how many times have they played {self.config['jam_vehicles'][1]}'\n"
                   f"- Gap: 'when did they last play {self.config['jam_vehicles'][2]}'"
        )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_all_setlistfm_aliases() -> Dict[str, str]:
    """Get all song aliases mapped to band keys for auto-detection."""
    aliases = {}
    for band_key, config in SETLISTFM_BANDS.items():
        for alias in config.get("song_aliases", {}).keys():
            aliases[alias.lower()] = band_key
        # Also add band name aliases
        for alias in config.get("aliases", []):
            aliases[alias.lower()] = band_key
    return aliases


if __name__ == "__main__":
    print("=" * 60)
    print("Setlist.fm Engine - Testing")
    print("=" * 60)

    # Test Umphrey's
    print("\n--- UMPHREY'S MCGEE ---")
    um = SetlistFMEngine("umphreys")

    for q in ["Wizard Burial Ground stats", "how many times have they played Mantis?", "when did they last play Prowler?"]:
        print(f"\nQ: {q}")
        result = um.query(q)
        print(result.answer[:300])
