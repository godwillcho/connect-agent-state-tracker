"""
Agent State Enrichment Module

Classifies agent statuses into categories and transforms raw state
change events into pre-computed, query-ready records.

Status classification:
  - ROUTABLE:      Agent can receive contacts (Available)
  - NOT_ROUTABLE:  Custom statuses (Break, Lunch, Training, etc.)
                   or contact-handling states (On Contact, ACW, etc.)
  - OFFLINE:       Agent is logged out

Any status created or removed in the Connect instance is automatically
captured — classification falls back to NOT_ROUTABLE for unknown statuses.
"""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# All timestamps are converted to US/Eastern for reporting
EASTERN = ZoneInfo('America/New_York')

# ──────────────────────────────────────────────
# STATUS CLASSIFICATION SETS
# ──────────────────────────────────────────────
ROUTABLE_STATUSES = frozenset({'Available'})

OFFLINE_STATUSES = frozenset({'Offline', 'Logged Out'})

CONTACT_HANDLING_STATUSES = frozenset({
    'On Contact', 'Incoming', 'Connecting', 'Connected',
    'On Hold', 'After Contact Work', 'ACW',
    'Joined', 'Busy', 'Missed', 'Error',
})

PRODUCTIVE_STATUSES = ROUTABLE_STATUSES | CONTACT_HANDLING_STATUSES


# ──────────────────────────────────────────────
# CLASSIFICATION
# ──────────────────────────────────────────────
def classify_status(status_name):
    """
    Classify an agent status into a category.

    Any custom status (Break, Lunch, Training, or anything newly created
    in the Connect instance) will be classified as NOT_ROUTABLE by default.

    Returns:
        str: One of 'ROUTABLE', 'NOT_ROUTABLE', or 'OFFLINE'
    """
    if status_name in ROUTABLE_STATUSES:
        return 'ROUTABLE'
    if status_name in OFFLINE_STATUSES:
        return 'OFFLINE'
    return 'NOT_ROUTABLE'


def is_productive(status_name):
    """Check if a status is considered productive (Available or handling contacts)."""
    return status_name in PRODUCTIVE_STATUSES


def is_contact_handling(status_name):
    """Check if a status is a contact-handling state."""
    return status_name in CONTACT_HANDLING_STATUSES


# ──────────────────────────────────────────────
# TIMESTAMP PARSING
# ──────────────────────────────────────────────
def parse_iso_timestamp(ts_str):
    """
    Parse an ISO 8601 timestamp string to a timezone-aware datetime.

    Handles formats:
      - 2026-02-20T14:30:00.000Z
      - 2026-02-20T14:30:00Z
      - 2026-02-20T14:30:00+00:00

    Returns:
        datetime or None if parsing fails.
    """
    if not ts_str:
        return None
    try:
        normalized = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(normalized)
    except (ValueError, TypeError):
        logger.warning("Failed to parse timestamp: %s", ts_str)
        return None


# ──────────────────────────────────────────────
# EVENT ENRICHMENT
# ──────────────────────────────────────────────
def enrich_event(evt, agent_arn, agent_name, session_id):
    """
    Transform a raw state change event into a fully enriched,
    query-ready record with pre-computed fields.

    The client sends per event:
      - new_status:               status the agent transitioned TO
      - previous_status:          status the agent was IN before
      - timestamp:                ISO 8601 when the transition occurred
      - previous_start_timestamp: ISO 8601 when the agent entered
                                  the previous status

    The enrichment computes:
      - duration_seconds:    time spent in the previous status
      - status_category:     ROUTABLE / NOT_ROUTABLE / OFFLINE
      - is_productive:       Available or contact-handling
      - is_contact_handling: On Contact, ACW, Incoming, etc.
      - date_str:            YYYY-MM-DD for easy filtering
      - hour_of_day:         0-23 for hourly analysis
      - day_of_week:         Monday-Sunday

    Returns:
        dict: enriched record ready for Parquet serialization.
    """
    transition_ts_str = evt.get('timestamp', '')
    prev_start_str = evt.get('previous_start_timestamp', '')
    new_status = evt.get('new_status', 'Unknown')
    previous_status = evt.get('previous_status', 'Unknown')

    transition_ts = parse_iso_timestamp(transition_ts_str)
    prev_start_ts = parse_iso_timestamp(prev_start_str)

    # Duration = time spent in the PREVIOUS status before this transition
    duration_seconds = 0
    if transition_ts and prev_start_ts:
        delta = transition_ts - prev_start_ts
        duration_seconds = max(0, int(delta.total_seconds()))

    # Convert UTC → US/Eastern for all derived fields
    eastern_ts = transition_ts.astimezone(EASTERN) if transition_ts else None
    tz_label = eastern_ts.strftime('%Z') if eastern_ts else 'US/Eastern'  # EST or EDT

    # Derive time components from Eastern timestamp
    date_str = eastern_ts.strftime('%Y-%m-%d') if eastern_ts else ''
    hour_of_day = eastern_ts.hour if eastern_ts else 0
    day_of_week = eastern_ts.strftime('%A') if eastern_ts else ''

    # Store the transition timestamp in Eastern time with offset
    eastern_ts_str = eastern_ts.isoformat() if eastern_ts else transition_ts_str

    # Classify the previous status (since duration measures time in it)
    prev_category = classify_status(previous_status)

    # Partition columns (based on Eastern date)
    year = eastern_ts.year if eastern_ts else 1970
    month = eastern_ts.month if eastern_ts else 1
    day = eastern_ts.day if eastern_ts else 1

    logger.debug(
        "Enriched event: agent=%s prev=%s new=%s duration=%ds",
        agent_name, previous_status, new_status, duration_seconds,
    )

    return {
        'agent_arn': agent_arn,
        'agent_name': agent_name,
        'status_name': previous_status,
        'status_category': prev_category,
        'previous_status_name': previous_status,
        'new_status_name': new_status,
        'transition_timestamp': eastern_ts_str,
        'timezone': tz_label,
        'duration_seconds': duration_seconds,
        'is_productive': is_productive(previous_status),
        'is_contact_handling': is_contact_handling(previous_status),
        'date_str': date_str,
        'hour_of_day': hour_of_day,
        'day_of_week': day_of_week,
        'source': 'agent_workspace_service',
        'session_id': session_id,
        # Partition keys (used for S3 path, not written to Parquet columns)
        'year': year,
        'month': month,
        'day': day,
    }
