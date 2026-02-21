# Amazon Connect Agent State Tracker

Real-time agent state tracking for Amazon Connect. Captures every agent status transition (Available, Break, On Contact, Offline, etc.) directly from the Agent Workspace, enriches events with duration and productivity metrics, and writes Parquet files to S3 for Athena querying.

## How It Works

```
┌──────────────────────────────────────────┐
│       Amazon Connect Agent Workspace     │
│                                          │
│   ┌──────────────────────────────────┐   │
│   │  Headless 3P Service (iframe)    │   │
│   │                                  │   │
│   │  Connect SDK → onStateChanged()  │   │
│   │  Buffers events, flushes every   │   │
│   │  5 seconds via POST to Lambda    │   │
│   └──────────────┬───────────────────┘   │
│                  │                        │
└──────────────────┼────────────────────────┘
                   │  HTTPS POST (batched JSON)
                   ▼
┌──────────────────────────────────────────┐
│  Lambda Function URL                     │
│                                          │
│  GET  → Serves headless service HTML     │
│  POST → Enriches events, writes Parquet  │
│                                          │
│  Enrichment:                             │
│   • Duration (seconds in prev status)    │
│   • Status category (ROUTABLE / OFFLINE  │
│     / NOT_ROUTABLE)                      │
│   • Productivity & contact-handling flags │
│   • UTC → US/Eastern conversion          │
│   • Partition keys (year/month/day)      │
└──────────────────┬───────────────────────┘
                   │  Snappy Parquet
                   ▼
┌──────────────────────────────────────────┐
│  S3 (Hive-partitioned)                   │
│  s3://bucket/prefix/                     │
│    year=2026/month=02/day=21/            │
│      batch_20260221_143000_abc123.parquet │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│  Glue Table (partition projection)       │
│  + Athena Workgroup                      │
│                                          │
│  No crawler needed — partition           │
│  projection auto-discovers partitions    │
└──────────────────────────────────────────┘
```

## Features

- **Real-time capture** — Every agent state change is captured as it happens via the Connect SDK
- **Pre-computed analytics** — Duration, productivity flags, status categories computed at write time
- **Zero-crawler** — Glue partition projection eliminates the need for a Glue Crawler
- **US/Eastern timestamps** — All times converted from UTC to Eastern (EST/EDT) with timezone indicator
- **Batched writes** — Events buffered client-side (5s interval) to minimize Lambda invocations
- **Heartbeat monitoring** — 60-second heartbeat confirms active tracking sessions
- **Beacon fallback** — `navigator.sendBeacon()` fires on tab/window close to prevent data loss
- **Retry with backoff** — Exponential backoff with jitter on failed flushes
- **Environment support** — Deploy with `-dev`, `-prod`, or any custom suffix
- **One-command deploy/destroy** — `deploy.py` handles the full lifecycle

## Architecture

| Resource | Purpose |
|---|---|
| Lambda Function + Function URL | Serves headless service page (GET) and processes events (POST) |
| S3 Bucket | Stores Parquet files with lifecycle transitions (Standard-IA → Glacier → Expire) |
| Glue Database + Table | Schema definition with partition projection for Athena |
| Athena Workgroup | Dedicated query workspace with byte-scan limits |
| 3P Application (Service) | Registered headless iframe service in the Agent Workspace |
| Integration Association | Links the 3P app to the Connect instance |
| IAM Role | Lambda execution with scoped S3 write permissions |

## Parquet Schema

All fields are pre-computed — no transforms needed at query time.

| Column | Type | Description |
|---|---|---|
| `agent_arn` | string | Agent's full ARN |
| `agent_name` | string | Agent's login name (e.g., `jsmith`) |
| `status_name` | string | Status the agent was in (= `previous_status_name`) |
| `status_category` | string | `ROUTABLE`, `NOT_ROUTABLE`, or `OFFLINE` |
| `previous_status_name` | string | Status before this transition |
| `new_status_name` | string | Status after this transition |
| `transition_timestamp` | string | When the transition occurred (US/Eastern ISO 8601) |
| `timezone` | string | Timezone label (`EST` or `EDT`) |
| `duration_seconds` | bigint | Seconds spent in the previous status |
| `is_productive` | boolean | Available or contact-handling status |
| `is_contact_handling` | boolean | On Contact, ACW, Incoming, etc. |
| `date_str` | string | Date in `YYYY-MM-DD` (Eastern) |
| `hour_of_day` | int | Hour 0-23 (Eastern) |
| `day_of_week` | string | `Monday` through `Sunday` |
| `source` | string | Always `agent_workspace_service` |
| `session_id` | string | Unique ID per browser session |

**Partition columns:** `year`, `month`, `day` (based on Eastern time)

## Status Classification

| Category | Statuses | Productive |
|---|---|---|
| `ROUTABLE` | Available | Yes |
| `NOT_ROUTABLE` (contact) | On Contact, Incoming, Connecting, Connected, On Hold, ACW, After Contact Work, Joined, Busy, Missed, Error | Yes |
| `NOT_ROUTABLE` (custom) | Break, Lunch, Training, *any custom status* | No |
| `OFFLINE` | Offline, Logged Out | No |

Any status created in your Connect instance is automatically captured. Unknown statuses default to `NOT_ROUTABLE`.

## Prerequisites

- AWS CLI configured with credentials
- Python 3.12+ with `boto3` installed
- Node.js (only needed to rebuild the SDK bundle)
- An Amazon Connect instance

## Quick Start

### Deploy

```bash
python deploy.py deploy
```

The script prompts for all parameters interactively. Most have sensible defaults — just press Enter to accept.

#### Deployment Parameters

| Parameter | Default | Required | Description |
|---|---|---|---|
| **AWS Region** | `us-east-1` | | AWS region where all resources are created |
| **Stack name** | `agent-state-tracker` | | Base name for the CloudFormation stack. All resource names derive from this (Lambda, Glue DB, workgroup, etc.) |
| **Environment suffix** | `dev` | | Appended to the stack name (e.g., `agent-state-tracker-dev`). Use `prod`, `staging`, or any custom label to run multiple environments side by side |
| **Connect instance domain** | — | Yes | Your Connect instance domain (e.g., `myinstance.my.connect.aws`). Used for CSP headers so the service page can load inside the Agent Workspace iframe |
| **S3 bucket name** | `softwareone-agent-state-551642657889` | | Name of the S3 bucket where Parquet files are stored. The bucket is created by the stack |
| **S3 key prefix** | `agent-state-events` | | Prefix under the bucket for Parquet files. Data is stored as `s3://bucket/prefix/year=.../month=.../day=.../` |
| **Athena results bucket** | `softwareone-athena-results-551642657889` | | S3 bucket for Athena query result files. If provided, an existing bucket is used; if empty, the stack creates one named `<stack-name>-athena-results` |
| **Athena bytes scan limit** | `2199023255552` (2 TB) | | Maximum bytes a single Athena query can scan before being rejected. Prevents accidental expensive queries. Athena charges $5/TB scanned. Set to `0` for unlimited |
| **Data retention days** | `90` | | Number of days before Parquet files are expired and deleted from S3. Files transition to Standard-IA at 30 days and Glacier at 60 days before expiration |
| **Lambda Layer ARN** | `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16` | | ARN of the AWS-managed AWSSDKPandas layer, which provides PyArrow for Parquet writing. Must match your deployment region |
| **Connect instance ARN** | — | Yes | Full ARN of your Amazon Connect instance (e.g., `arn:aws:connect:us-east-1:123456789012:instance/xxxxxxxx-...`). Used to register the 3P service and associate it with the instance |
| **Security profile selection** | *(interactive)* | | After deployment, the script lists all security profiles and prompts you to choose which ones should have access to the 3P service (comma-separated numbers) |

The deploy script will:
1. Create/update the CloudFormation stack (`agent-state-tracker-dev`)
2. Package and upload Lambda code (handler, enrichment, parquet writer, service page, SDK bundle)
3. Grant security profile access to the 3P service
4. Print a deployment summary with all resource names and ARNs

### Destroy

```bash
python deploy.py destroy
```

The destroy script will:
1. Revoke security profile access (with 30s propagation wait)
2. Empty all S3 buckets owned by the stack
3. Delete the Athena workgroup (including query history)
4. Delete the CloudFormation stack

### Rebuild SDK Bundle (optional)

Only needed if you update the Connect SDK packages.

```bash
cd sdk-build
npm install
npx esbuild entry-service.js --bundle --minify --format=iife --target=es2020 \
  --outfile=../connect-sdk-service.bundle.js
```

## S3 Lifecycle Policy

| Age | Storage Class |
|---|---|
| 0-29 days | Standard |
| 30-59 days | Standard-IA |
| 60-89 days | Glacier |
| 90+ days | Expired (deleted) |

Retention period is configurable via the `DataRetentionDays` parameter (default: 90).

## Example Athena Queries

See [`agent_state_queries.sql`](agent_state_queries.sql) for a full set of ready-to-use queries.

**Daily time-in-status per agent:**
```sql
SELECT agent_name, status_name, status_category,
       COUNT(*) AS transitions,
       SUM(duration_seconds) AS total_seconds,
       ROUND(SUM(duration_seconds) / 3600.0, 2) AS total_hours
FROM agent_state_transitions
WHERE year = 2026 AND month = 2 AND day = 21
  AND duration_seconds > 0
GROUP BY agent_name, status_name, status_category
ORDER BY agent_name, total_seconds DESC;
```

**Agent productivity breakdown:**
```sql
SELECT agent_name, date_str,
       ROUND(SUM(CASE WHEN is_productive THEN duration_seconds ELSE 0 END) / 3600.0, 2) AS productive_hours,
       ROUND(SUM(CASE WHEN NOT is_productive THEN duration_seconds ELSE 0 END) / 3600.0, 2) AS non_productive_hours,
       ROUND(100.0 * SUM(CASE WHEN is_productive THEN duration_seconds ELSE 0 END)
             / NULLIF(SUM(duration_seconds), 0), 1) AS productivity_pct
FROM agent_state_transitions
WHERE year = 2026 AND month = 2 AND duration_seconds > 0
GROUP BY agent_name, date_str
ORDER BY date_str DESC, agent_name;
```

## File Structure

```
.
├── deploy.py                        # Deployment & teardown automation
├── agent-state-cfn.yaml             # CloudFormation template (all infrastructure)
├── handler.py                       # Lambda entry point (GET/POST routing)
├── enrichment.py                    # Event enrichment & status classification
├── parquet_writer.py                # Parquet schema & S3 writer
├── service_page.py                  # Headless service HTML/JS template
├── connect-sdk-service.bundle.js    # Pre-built SDK bundle (esbuild IIFE, ~43KB)
├── agent_state_queries.sql          # Ready-to-use Athena queries
└── sdk-build/                       # SDK bundle source
    ├── entry-service.js             # esbuild entry point
    ├── package.json                 # @amazon-connect/app + @amazon-connect/contact
    └── node_modules/                # npm dependencies (not committed)
```

## Lambda Layer

This project uses the AWS-managed **AWSSDKPandas** Lambda layer for PyArrow. Find the ARN for your region:

| Region | Layer ARN |
|---|---|
| us-east-1 | `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16` |
| us-west-2 | `arn:aws:lambda:us-west-2:336392948345:layer:AWSSDKPandas-Python312:16` |
| eu-west-1 | `arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python312:16` |

Full list: [aws-sdk-pandas layers](https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html)

## Cost Estimate

### 100 agents — rough monthly estimate

Assumptions: 100 agents, 8-hour shifts, ~20 state transitions per hour per agent, 22 working days/month.

A single contact generates ~4 transitions (Available → Incoming → Connected → ACW → Available). At 4-5 contacts/hour plus occasional breaks, that's ~20 transitions/hour or **~160 transitions/agent/day**.

**Data volume:**
- ~160 transitions/day x 100 agents x 22 days = **352,000 events/month**
- Each Parquet record is ~200 bytes compressed = **~70 MB Parquet/month**

**Lambda invocations:**
- Events are batched client-side (flush every 5 seconds only when events are buffered)
- Realistically **~30,000-60,000 invocations/month** (most 5-second intervals have no events to flush)

| Resource | Calculation | Monthly Cost |
|---|---|---|
| Lambda | ~100K invocations x 512 MB x ~200ms avg | ~$0.50 |
| S3 Standard | ~100 MB Parquet storage | ~$0.01 |
| S3 Lifecycle | Standard-IA after 30 days, Glacier after 60 | Reduces cost further |
| Glue Data Catalog | 1 database + 1 table | Free tier |
| Athena queries | ~$5/TB scanned; partitioned Parquet = few MB per query | ~$0.01-$5.00 |
| CloudWatch Logs | Lambda execution logs | ~$0.50 |
| **Total** | | **~$1-$6/month** |

The main variable is Athena usage — a heavy analyst running many unfiltered queries could push costs higher, but the 2 TB byte-scan limit prevents runaway bills.

## License

MIT
