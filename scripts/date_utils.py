"""Shared date extraction and normalization for all JamMuse engines."""
import re
from typing import Optional

MONTHS = {
    'january': '01', 'jan': '01',
    'february': '02', 'feb': '02',
    'march': '03', 'mar': '03',
    'april': '04', 'apr': '04',
    'may': '05',
    'june': '06', 'jun': '06',
    'july': '07', 'jul': '07',
    'august': '08', 'aug': '08',
    'september': '09', 'sep': '09', 'sept': '09',
    'october': '10', 'oct': '10',
    'november': '11', 'nov': '11',
    'december': '12', 'dec': '12',
}

# Keywords that indicate a setlist/show-date query
SETLIST_KEYWORDS = [
    "setlist", "set list", "show on", "show from",
    "what did they play on", "what did they play",
    "what was played", "what show",
]

_MONTH_NAMES = '|'.join(MONTHS.keys())


def extract_date_from_query(query: str) -> Optional[str]:
    """
    Extract a date from a query string and return it in YYYY-MM-DD format.

    Supports:
    - Natural language: "May 18th 1991", "18 May 1991"
    - Slash dates: "12/31/1999", "12/31/99"
    - ISO dates: "1999-12-31"
    """
    query_lower = query.lower()

    # Pattern 1: "May 18th 1991", "May 18, 1991", "may 18 1991"
    month_pattern = rf'({_MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s*(\d{{4}})'
    match = re.search(month_pattern, query_lower)
    if match:
        month = MONTHS[match.group(1)]
        day = match.group(2).zfill(2)
        year = match.group(3)
        return f"{year}-{month}-{day}"

    # Pattern 2: "18th May 1991", "18 May 1991"
    day_first_pattern = rf'(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES}),?\s*(\d{{4}})'
    match = re.search(day_first_pattern, query_lower)
    if match:
        day = match.group(1).zfill(2)
        month = MONTHS[match.group(2)]
        year = match.group(3)
        return f"{year}-{month}-{day}"

    # Pattern 3: Numeric date patterns
    numeric_patterns = [
        r'(\d{4}-\d{2}-\d{2})',       # YYYY-MM-DD
        r'(\d{1,2}/\d{1,2}/\d{4})',   # MM/DD/YYYY
        r'(\d{1,2}/\d{1,2}/\d{2})',   # MM/DD/YY
    ]
    for pattern in numeric_patterns:
        match = re.search(pattern, query)
        if match:
            date_str = match.group(1)
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts[2]) == 2:
                    year = '19' + parts[2] if int(parts[2]) > 50 else '20' + parts[2]
                else:
                    year = parts[2]
                return f"{year}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            return date_str

    return None


def is_setlist_query(question_lower: str) -> bool:
    """Check if a query is asking for a setlist/show."""
    return any(kw in question_lower for kw in SETLIST_KEYWORDS)


def has_date(query: str) -> bool:
    """Quick check whether a query contains any recognizable date."""
    return extract_date_from_query(query) is not None


# =============================================================================
# RANDOM SHOW QUERIES
# =============================================================================

RANDOM_SHOW_KEYWORDS = [
    "show to listen to", "show to check out",
    "surprise me", "surprise show",
]

# Patterns that match even with a band name inserted (e.g., "random phish show")
RANDOM_SHOW_PATTERNS = [
    r'random\s+[\w\s]*show',           # "random show", "random phish show"
    r'random\s+[\w\s]*concert',        # "random concert", "random goose concert"
    r'give me a\s+[\w\s]*show',        # "give me a show", "give me a dead show"
    r'give me a\s+[\w\s]*concert',     # "give me a concert"
    r'recommend\s+[\w\s]*show',        # "recommend a show", "recommend a dead show"
    r'pick\s+[\w\s]*show',             # "pick a show", "pick me a phish show"
]


def is_random_show_query(question_lower: str) -> bool:
    """Check if a query is asking for a random show recommendation."""
    if any(kw in question_lower for kw in RANDOM_SHOW_KEYWORDS):
        return True
    return any(re.search(pat, question_lower) for pat in RANDOM_SHOW_PATTERNS)


def extract_year_filter(query: str) -> Optional[int]:
    """Extract a year filter from queries like 'random show from 1994' or 'show from 97'."""
    query_lower = query.lower()
    # "from 1994", "in 1994", "of 1994"
    match = re.search(r'(?:from|in|of)\s+((?:19|20)\d{2})\b', query_lower)
    if match:
        return int(match.group(1))
    # "from 94", "in 97" (2-digit year)
    match = re.search(r'(?:from|in|of)\s+(\d{2})\b', query_lower)
    if match:
        yr = int(match.group(1))
        return 1900 + yr if yr > 50 else 2000 + yr
    # Standalone 4-digit year at end or after band name
    match = re.search(r'\b((?:19|20)\d{2})\b', query_lower)
    if match:
        return int(match.group(1))
    return None


# =============================================================================
# MONTH/DAY EXTRACTION (no year - for "on this day" queries)
# =============================================================================

HOLIDAYS = {
    "halloween": (10, 31),
    "new years eve": (12, 31),
    "new year's eve": (12, 31),
    "nye": (12, 31),
    "new years": (12, 31),
    "new year's": (12, 31),
    "christmas": (12, 25),
    "christmas eve": (12, 24),
    "thanksgiving": None,  # Variable date
    "july 4th": (7, 4),
    "july 4": (7, 4),
    "fourth of july": (7, 4),
    "4th of july": (7, 4),
    "independence day": (7, 4),
    "valentines day": (2, 14),
    "valentine's day": (2, 14),
    "st patricks day": (3, 17),
    "st. patrick's day": (3, 17),
    "april fools": (4, 1),
    "april fool's": (4, 1),
    "friday the 13th": None,  # Variable
}

HOLIDAY_DISPLAY = {
    "halloween": "Halloween",
    "new years eve": "New Year's Eve",
    "new year's eve": "New Year's Eve",
    "nye": "New Year's Eve",
    "new years": "New Year's Eve",
    "new year's": "New Year's Eve",
    "christmas": "Christmas",
    "christmas eve": "Christmas Eve",
    "july 4th": "July 4th",
    "july 4": "July 4th",
    "fourth of july": "July 4th",
    "4th of july": "July 4th",
    "independence day": "Independence Day",
    "valentines day": "Valentine's Day",
    "valentine's day": "Valentine's Day",
    "st patricks day": "St. Patrick's Day",
    "st. patrick's day": "St. Patrick's Day",
    "april fools": "April Fools' Day",
    "april fool's": "April Fools' Day",
}

_MONTHS_INT = {
    'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
    'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
    'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
    'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'sept': 9,
    'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
    'december': 12, 'dec': 12,
}

# Keywords that indicate a "shows on this date" query
SHOWS_ON_DATE_KEYWORDS = [
    "shows on", "played on", "shows played on",
    "shows today",
    "on halloween", "on nye", "on new year", "on christmas",
    "on july 4", "on 4th of july",
]

# Patterns for "shows 3/13", "shows march 13", "shows 12/31"
_SHOWS_DATE_PATTERNS = [
    r'shows\s+\d{1,2}/\d{1,2}',                          # "shows 3/13", "shows 12/31"
    r'shows\s+(?:' + _MONTH_NAMES + r')\s+\d{1,2}',      # "shows march 13"
    r'shows\s+(?:' + _MONTH_NAMES + r')$',                # "shows october" (month only — future use)
]


def extract_month_day_from_query(query: str) -> tuple:
    """
    Extract month and day (no year) from a query.
    Returns (month, day) tuple or None.

    Supports: "October 31", "Oct 31st", "31st of October", "10/31", "today", holidays.
    """
    query_lower = query.lower()

    # "today" → use current date
    if 'today' in query_lower:
        from datetime import datetime
        now = datetime.now()
        return (now.month, now.day)

    # Check holiday names first (longer names first to avoid substring issues)
    for holiday in sorted(HOLIDAYS.keys(), key=len, reverse=True):
        if holiday in query_lower:
            return HOLIDAYS[holiday]  # May be None for variable holidays

    # Pattern: "October 31", "Oct 31st"
    month_pattern = rf'({_MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?(?!\d)'
    match = re.search(month_pattern, query_lower)
    if match:
        return (_MONTHS_INT[match.group(1)], int(match.group(2)))

    # Pattern: "31st of October", "31 October"
    day_first_pattern = rf'(\d{{1,2}})(?:st|nd|rd|th)?\s+(?:of\s+)?({_MONTH_NAMES})(?!\s*\d{{4}})'
    match = re.search(day_first_pattern, query_lower)
    if match:
        return (int(match.group(1)), _MONTHS_INT[match.group(2)])

    # Pattern: "10/31" (MM/DD without year)
    numeric_pattern = r'(?:^|[^\d])(\d{1,2})/(\d{1,2})(?:[^\d/]|$)'
    match = re.search(numeric_pattern, query)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return (month, day)

    return None


def is_shows_on_date_query(question_lower: str) -> bool:
    """Check if a query is asking about shows on a specific month/day."""
    if any(kw in question_lower for kw in SHOWS_ON_DATE_KEYWORDS):
        return True
    return any(re.search(pat, question_lower) for pat in _SHOWS_DATE_PATTERNS)


def detect_holiday_name(question_lower: str) -> str:
    """Detect if the query mentions a holiday. Returns the holiday key or None."""
    for holiday in sorted(HOLIDAYS.keys(), key=len, reverse=True):
        if holiday in question_lower:
            return holiday
    return None


# =============================================================================
# "ON THIS DAY" QUERIES
# =============================================================================

ON_THIS_DAY_KEYWORDS = [
    "on this day",
    "today in jam",
    "today in history",
    "today in jam history",
    "shows today",
]


def is_on_this_day_query(question_lower: str) -> bool:
    """Check if a query is asking for 'on this day' across all bands."""
    return any(kw in question_lower for kw in ON_THIS_DAY_KEYWORDS)
