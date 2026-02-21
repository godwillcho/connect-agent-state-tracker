"""
Parquet Writer Module

Defines the Parquet schema for agent state transition records and
handles writing enriched records to S3 with Hive-style partitioning.

Parquet files are written with Snappy compression and organized as:
  s3://bucket/prefix/year=YYYY/month=MM/day=DD/batch_<ts>_<id>.snappy.parquet
"""

import io
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')


# ──────────────────────────────────────────────
# PARQUET SCHEMA
# ──────────────────────────────────────────────
PARQUET_SCHEMA = pa.schema([
    pa.field('agent_arn',             pa.string()),
    pa.field('agent_name',            pa.string()),
    pa.field('status_name',           pa.string()),
    pa.field('status_category',       pa.string()),
    pa.field('previous_status_name',  pa.string()),
    pa.field('new_status_name',       pa.string()),
    pa.field('transition_timestamp',  pa.string()),
    pa.field('timezone',              pa.string()),
    pa.field('duration_seconds',      pa.int64()),
    pa.field('is_productive',         pa.bool_()),
    pa.field('is_contact_handling',   pa.bool_()),
    pa.field('date_str',              pa.string()),
    pa.field('hour_of_day',           pa.int32()),
    pa.field('day_of_week',           pa.string()),
    pa.field('source',                pa.string()),
    pa.field('session_id',            pa.string()),
])


# ──────────────────────────────────────────────
# PARTITIONING
# ──────────────────────────────────────────────
def group_by_partition(records):
    """
    Group records by (year, month, day) partition key.

    Returns:
        dict: mapping of (year, month, day) tuples to lists of records.
    """
    partitions = {}
    for record in records:
        key = (record['year'], record['month'], record['day'])
        partitions.setdefault(key, []).append(record)
    return partitions


# ──────────────────────────────────────────────
# PARQUET WRITER
# ──────────────────────────────────────────────
def write_parquet_to_s3(records, partition_key):
    """
    Convert enriched records to a Parquet file and upload to S3
    with Hive-style partitioning.

    Args:
        records:       list of enriched record dicts
        partition_key: tuple of (year, month, day)

    The S3 key follows the pattern:
      {prefix}/year={Y}/month={MM}/day={DD}/batch_{timestamp}_{uuid}.snappy.parquet
    """
    year, month, day = partition_key
    bucket = os.environ['S3_BUCKET']
    prefix = os.environ.get('S3_PREFIX', 'agent-state-events')

    # Build columnar arrays from records
    arrays = {
        field.name: [record.get(field.name) for record in records]
        for field in PARQUET_SCHEMA
    }

    # Create typed pyarrow arrays and assemble table
    pa_arrays = [
        pa.array(arrays[field.name], type=field.type)
        for field in PARQUET_SCHEMA
    ]
    column_names = [field.name for field in PARQUET_SCHEMA]
    table = pa.table(dict(zip(column_names, pa_arrays)), schema=PARQUET_SCHEMA)

    # Write to in-memory buffer with Snappy compression
    buf = io.BytesIO()
    pq.write_table(table, buf, compression='snappy')
    buf.seek(0)

    # Build S3 key with Hive partitioning
    batch_id = uuid.uuid4().hex[:12]
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    s3_key = (
        f"{prefix}/"
        f"year={year}/"
        f"month={month:02d}/"
        f"day={day:02d}/"
        f"batch_{timestamp_str}_{batch_id}.snappy.parquet"
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buf.getvalue(),
        ContentType='application/octet-stream',
    )

    logger.info(
        "Wrote %d records to s3://%s/%s (partition: %d-%02d-%02d)",
        len(records), bucket, s3_key, year, month, day,
    )
