"""
Amazon Connect Agent State Tracker - Lambda Function URL Handler

Entry point for the Lambda Function URL. Routes requests by HTTP method:

  GET  -> Serves the headless 3P service page (HTML/JS) that runs inside
          the Amazon Connect Agent Workspace as a hidden iframe.

  POST -> Receives batched agent state change events, enriches them with
          pre-computed fields (duration, category, productivity flags),
          converts to Parquet, and writes to S3 for Athena querying.

Environment Variables:
    S3_BUCKET:                 Target S3 bucket for Parquet files
    S3_PREFIX:                 S3 key prefix (default: agent-state-events)
    CONNECT_INSTANCE_DOMAIN:   Connect instance domain for CSP headers

Module dependencies:
    enrichment.py:     Status classification and event enrichment
    parquet_writer.py: Parquet schema definition and S3 writer
    service_page.py:   Inline HTML/JS for the headless service
"""

import json
import logging
import os
import base64

from enrichment import enrich_event
from parquet_writer import group_by_partition, write_parquet_to_s3
from service_page import build_service_html

# ──────────────────────────────────────────────
# LOGGING CONFIGURATION
# ──────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

# Structured log format for CloudWatch
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
    ))
    logger.addHandler(handler)


# ──────────────────────────────────────────────
# SDK BUNDLE (loaded once at init)
# ──────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_SDK_BUNDLE_PATH = os.path.join(_SCRIPT_DIR, 'connect-sdk-service.bundle.js')

with open(_SDK_BUNDLE_PATH, 'r', encoding='utf-8') as _f:
    _SDK_BUNDLE_JS = _f.read()

SERVICE_HTML = build_service_html(_SDK_BUNDLE_JS)
logger.info("Service HTML built: %d bytes (SDK bundle: %d bytes)", len(SERVICE_HTML), len(_SDK_BUNDLE_JS))


# ──────────────────────────────────────────────
# RESPONSE HELPERS
# ──────────────────────────────────────────────
def json_response(status_code, body):
    """Build a JSON response for the Function URL."""
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body),
    }


def html_response(html, connect_domain):
    """Build an HTML response with CSP headers for Connect embedding."""
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/html; charset=utf-8',
            'Content-Security-Policy': (
                f"frame-ancestors https://*.awsapps.com "
                f"https://{connect_domain} "
                f"https://*.my.connect.aws"
            ),
            'X-Content-Type-Options': 'nosniff',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
        },
        'body': html,
    }


# ──────────────────────────────────────────────
# MAIN HANDLER
# ──────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Lambda Function URL handler. Routes by HTTP method.
    """
    http_method = (
        event.get('requestContext', {})
             .get('http', {})
             .get('method', 'GET')
    )

    logger.info("Received %s request", http_method)

    if http_method == 'GET':
        return handle_get()
    elif http_method == 'POST':
        return handle_post(event)
    else:
        return json_response(405, {'error': 'Method not allowed'})


# ──────────────────────────────────────────────
# GET HANDLER - Serve the headless service page
# ──────────────────────────────────────────────
def handle_get():
    """
    Return the headless 3P service page with inline JavaScript.
    The page initializes the Amazon Connect SDK, subscribes to agent
    state changes, and sends batched events back to this Function URL.
    """
    connect_domain = os.environ.get(
        'CONNECT_INSTANCE_DOMAIN', '*.my.connect.aws'
    )
    logger.info("Serving headless service page (CSP domain: %s)", connect_domain)
    return html_response(SERVICE_HTML, connect_domain)


# ──────────────────────────────────────────────
# POST HANDLER - Receive events, enrich, write Parquet
# ──────────────────────────────────────────────
def handle_post(event):
    """
    Receive batched agent state change events, enrich with pre-computed
    fields, convert to Parquet, and write to S3.
    """
    try:
        payload = parse_body(event)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON body: %s", exc)
        return json_response(400, {'error': 'Invalid JSON'})

    # Acknowledge heartbeats without processing
    if payload.get('type') == 'heartbeat':
        agent = payload.get('agent_name', '?')
        session = payload.get('session_id', '?')
        logger.info("Heartbeat received: agent=%s session=%s", agent, session)
        return json_response(200, {'status': 'heartbeat_ack'})

    # Extract payload fields
    events = payload.get('events', [])
    session_id = payload.get('session_id', 'unknown')
    agent_arn = payload.get('agent_arn', '')
    agent_name = payload.get('agent_name', '')

    if not events:
        logger.info("Empty event batch from agent=%s", agent_name)
        return json_response(200, {'received': 0})

    logger.info(
        "Processing %d events: agent=%s session=%s",
        len(events), agent_name, session_id,
    )

    # Enrich each event into a query-ready record
    records = []
    for evt in events:
        try:
            record = enrich_event(evt, agent_arn, agent_name, session_id)
            records.append(record)
        except Exception as exc:
            logger.error("Failed to enrich event: %s", exc, exc_info=True)

    if not records:
        logger.warning("No records produced from %d events", len(events))
        return json_response(200, {'received': len(events), 'processed': 0})

    # Group by partition and write Parquet files
    try:
        partitions = group_by_partition(records)
        files_written = 0
        for partition_key, partition_records in partitions.items():
            write_parquet_to_s3(partition_records, partition_key)
            files_written += 1

        logger.info(
            "Completed: %d records -> %d Parquet file(s)",
            len(records), files_written,
        )

        return json_response(200, {
            'received': len(events),
            'processed': len(records),
            'files_written': files_written,
        })

    except Exception as exc:
        logger.error("Failed to write Parquet: %s", exc, exc_info=True)
        return json_response(500, {'error': 'Failed to write data'})


# ──────────────────────────────────────────────
# BODY PARSER
# ──────────────────────────────────────────────
def parse_body(event):
    """
    Parse the request body from the Function URL event.

    Handles both plain JSON and base64-encoded bodies (which the
    Function URL may produce depending on content type).
    Also handles navigator.sendBeacon() payloads.
    """
    body = event.get('body', '{}')

    if event.get('isBase64Encoded', False):
        body = base64.b64decode(body).decode('utf-8')

    return json.loads(body)
