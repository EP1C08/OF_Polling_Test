# OnlyFans Message Polling System - Architecture Documentation

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Operational Modes](#operational-modes)
4. [Core Components](#core-components)
5. [Data Flow](#data-flow)
6. [Redis Data Structures](#redis-data-structures)
7. [Database Schema](#database-schema)
8. [Authentication System](#authentication-system)
9. [Checkpoint System](#checkpoint-system)
10. [Error Handling](#error-handling)
11. [Docker Deployment](#docker-deployment)
12. [Performance Characteristics](#performance-characteristics)

---

## System Overview

This is a distributed message polling system for OnlyFans creators. It fetches messages from creator-fan conversations using the `ultima-scraper-api` library and persists them to PostgreSQL via Redis queues.

### Key Features

- **Producer-Consumer Architecture**: Decoupled components communicate via Redis
- **Real-Time WebSocket Monitoring**: Instant message detection (no polling delay)
- **Incremental Fetching**: 90%+ data reduction for known fans
- **Checkpoint System**: Crash recovery and resume capability
- **Rate Limit Handling**: Automatic retry with exponential backoff
- **Heavy Fan Support**: Special handling for fans with massive message histories
- **Multi-Creator Support**: Single db_worker handles all creators efficiently

### Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.10+ (asyncio) |
| Message Queue | Redis 7 (Lists, Pub/Sub) |
| Database | PostgreSQL 14+ |
| ORM | SQLAlchemy 2.0 (async) |
| API Library | ultima-scraper-api |
| Containerization | Docker Compose |
| Proxy/Antidetect | GoLogin |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ONLYFANS API                                       │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│  producer.py  │      │  websocket_   │      │  fan_sync.py  │
│  (bulk load)  │      │  listener.py  │      │  (every 4h)   │
│               │      │  (24/7 events)│      │               │
└───────┬───────┘      └───────┬───────┘      └───────┬───────┘
        │                      │                      │
        │              ┌───────┴───────┐              │
        │              │   ROUTING     │              │
        │              │               │              │
        │              ▼               ▼              │
        │     ┌─────────────┐ ┌─────────────┐        │
        │     │ new_fans    │ │ known_fans  │        │
        │     │ _priority   │ │ _queue      │        │
        │     └──────┬──────┘ └──────┬──────┘        │
        │            │               │               │
        │            ▼               ▼               │
        │     ┌─────────────┐ ┌─────────────┐        │
        │     │ new_fan_    │ │ known_fan_  │        │
        │     │ processor   │ │ processor   │        │
        │     │ (full fetch)│ │ (increment) │        │
        │     └──────┬──────┘ └──────┬──────┘        │
        │            │               │               │
        └────────────┼───────────────┼───────────────┘
                     │               │
                     ▼               ▼
        ┌─────────────────────────────────────────────┐
        │              REDIS LISTS                    │
        │  of:{creator_id}:messages                   │
        │  of:{creator_id}:bundles                    │
        │  of:{creator_id}:bundle_items               │
        │  of:{creator_id}:fan_interactions           │
        │  of:{creator_id}:analytics                  │
        └─────────────────────┬───────────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │  db_worker.py   │
                     │  (batch insert) │
                     └────────┬────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   PostgreSQL    │
                     │   Database      │
                     └─────────────────┘
```

---

## Operational Modes

### Mode 1: Initial Bulk Collection

**Purpose**: Fetch complete message history for all fans (one-time, ~16 hours)

**Command**:
```bash
docker-compose -f docker-compose.generated.yml up redis producer-* db_worker --build -d
```

**Process**:
1. Producer authenticates with OnlyFans API
2. Loads fan list from JSON or API (with pagination fallback)
3. **Phase 1**: Process normal fans (3 concurrent, 10-min timeout)
4. **Phase 2**: Retry rate-limited fans (exponential backoff)
5. **Phase 3**: Process heavy fans (no timeout, sequential)
6. All data pushed to Redis, db_worker saves to PostgreSQL

### Mode 2: Real-Time 24/7 Monitoring

**Purpose**: Continuous message detection and incremental fetching

**Command**:
```bash
docker-compose -f docker-compose.generated.yml up redis db_worker listener-* \
    new-fan-processor-* known-fan-processor-* fan-sync-* --build -d
```

**Process**:
1. WebSocket listener detects new message events instantly
2. Checks if fan is new (no history) or known (has history)
3. Routes to appropriate queue:
   - New fan → `new_fans_priority` → full history fetch
   - Known fan → `known_fans_queue` → incremental fetch (uses cutoff_id)
4. Fan sync runs every 4 hours to detect new subscribers

---

## Core Components

### producer.py

**Purpose**: Initial bulk data collection (1 instance per creator)

**Key Features**:
- 3-phase processing (normal → rate-limited → heavy fans)
- Concurrent processing with rolling window (3 fans default)
- Checkpoint-based crash recovery
- Reauthentication every 100 fans

**Environment Variables**:
| Variable | Default | Description |
|----------|---------|-------------|
| `CREATOR_ID` | required | Creator's OnlyFans ID |
| `CREATOR_NAME` | CREATOR_ID | Display name for logs |
| `CONCURRENT_FANS` | 3 | Parallel fan processing |
| `FETCH_TIMEOUT` | 600 | Timeout per fan (seconds) |
| `REAUTH_INTERVAL` | 100 | Reauthenticate every N fans |

### db_worker.py

**Purpose**: Persist data from Redis to PostgreSQL (1 instance for all creators)

**Key Features**:
- Spawns internal worker per creator
- Batch processing (50 items per batch)
- SQLAlchemy async with connection pooling
- Handles all 5 data types (messages, bundles, items, interactions, analytics)

**Environment Variables**:
| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `REDIS_HOST` | redis | Redis hostname |
| `REDIS_PORT` | 6385 | Redis port |
| `BATCH_SIZE` | 50 | Items per batch insert |

### websocket_listener.py

**Purpose**: Real-time event detection via WebSocket (1 per creator)

**Key Features**:
- 24/7 WebSocket connection to OnlyFans
- Event-only detection (no message fetching)
- Routes fans to appropriate queue (new vs known)
- Optional timewaster detection
- 24-hour message age filter

**Environment Variables**:
| Variable | Default | Description |
|----------|---------|-------------|
| `CREATOR_ID` | required | Creator's OnlyFans ID |
| `DATABASE_URL` | required | PostgreSQL connection string |
| `MIN_MESSAGE_AGE_HOURS` | 24 | Skip messages newer than this |
| `TW_ENABLED` | true | Enable timewaster detection |

### new_fan_processor.py

**Purpose**: Fetch full message history for new fans

**Key Features**:
- Monitors `new_fans_priority` Redis queue
- Fetches complete message history (cutoff_id=None)
- Concurrent processing (configurable)
- Checkpoint tracking for crash recovery

### known_fan_processor.py

**Purpose**: Fetch incremental messages for known fans

**Key Features**:
- Monitors `known_fans_queue` Redis queue
- Uses cutoff_id for incremental fetching (90%+ less data)
- Publishes to Pub/Sub for external consumers
- Efficient for high-volume real-time processing

### fan_sync.py

**Purpose**: Detect new subscribers periodically

**Key Features**:
- Runs every 4 hours (configurable)
- Compares database fans vs conversations JSON
- Fetches full history for newly detected fans
- Handles subscribers who messaged before WebSocket was active

---

## Data Flow

### Message Processing Pipeline

```
Raw API Message
      │
      ▼
FastMessageFetcher (50-message batches, 2.5x faster)
      │
      ▼
process_messages_and_bundles()
      │
      ├─→ Messages (message_id, content, timestamp, sender)
      ├─→ Bundles (price, media_count, is_mass_message)
      ├─→ Bundle Items (media_id, type, duration)
      ├─→ Fan Interactions (purchase status, timestamp)
      └─→ Analytics (view count, conversion rate, revenue)
      │
      ▼
Sanitizer (UTF-8 validation, control char removal)
      │
      ▼
Redis LPUSH (of:{creator_id}:{data_type})
      │
      ▼
db_worker RPOP + Batch Insert
      │
      ▼
PostgreSQL (ON CONFLICT DO NOTHING/UPDATE)
```

### WebSocket Event Flow

```
OnlyFans WebSocket Event
      │
      ▼
websocket_listener.py
      │
      ├─→ Parse event (api2_chat_message)
      ├─→ Extract fan_id
      ├─→ Check message age (>24h?)
      ├─→ Check timewaster (optional)
      │
      ▼
Query database: get_cutoff_id(model_id, fan_id)
      │
      ├─→ NULL (new fan) → LPUSH new_fans_priority
      └─→ EXISTS (known) → LPUSH known_fans_queue
```

---

## Redis Data Structures

### Lists (Data Pipeline)

| Key Pattern | Purpose | Operations |
|-------------|---------|------------|
| `of:{creator_id}:messages` | Message queue | LPUSH (producer) / RPOP (consumer) |
| `of:{creator_id}:bundles` | Bundle queue | LPUSH / RPOP |
| `of:{creator_id}:bundle_items` | Media items queue | LPUSH / RPOP |
| `of:{creator_id}:fan_interactions` | Purchase records | LPUSH / RPOP |
| `of:{creator_id}:analytics` | Bundle metrics | LPUSH / RPOP |

### Queues (Event Routing)

| Key Pattern | Purpose |
|-------------|---------|
| `of:{creator_id}:new_fans_priority` | New fans needing full fetch |
| `of:{creator_id}:known_fans_queue` | Known fans needing incremental |

### Status Keys

| Key Pattern | Purpose |
|-------------|---------|
| `of:{creator_id}:producer_done` | Producer completion flag |
| `of:{creator_id}:processing_lock:{fan_id}` | Prevent duplicate processing (TTL 300s) |

### Pub/Sub Channels

| Channel Pattern | Purpose |
|-----------------|---------|
| `of:pubsub:{creator_id}:messages` | Real-time message broadcast |

---

## Database Schema

### messages_new

Primary message storage table.

```sql
CREATE TABLE messages_new (
    id SERIAL PRIMARY KEY,
    message_id TEXT UNIQUE NOT NULL,
    model_id TEXT NOT NULL,
    fan_id TEXT NOT NULL,
    sender_id TEXT,
    model_name TEXT,
    sender_username TEXT,
    message TEXT,
    message_type TEXT,
    media_id TEXT,
    media_type TEXT,
    price DECIMAL(10,2),
    is_free BOOLEAN,
    is_purchased BOOLEAN,
    is_from_me BOOLEAN,
    created_at TIMESTAMP,
    fetched_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_messages_model_id ON messages_new(model_id);
CREATE INDEX idx_messages_fan_id ON messages_new(fan_id);
CREATE INDEX idx_messages_created_at ON messages_new(created_at);
CREATE INDEX idx_messages_composite ON messages_new(model_id, fan_id, created_at);
```

### bundles_new

Media bundles and mass messages.

```sql
CREATE TABLE bundles_new (
    id SERIAL PRIMARY KEY,
    bundle_id TEXT UNIQUE NOT NULL,
    message_id TEXT,
    creator_id TEXT NOT NULL,
    creator_username TEXT,
    total_price DECIMAL(10,2),
    media_count INT,
    photo_count INT,
    video_count INT,
    audio_count INT,
    is_mass_message BOOLEAN,
    queue_id TEXT,
    name TEXT,
    description TEXT,
    created_at TIMESTAMP,
    first_seen_at TIMESTAMP DEFAULT NOW()
);
```

### bundle_items_new

Individual media items within bundles.

```sql
CREATE TABLE bundle_items_new (
    id SERIAL PRIMARY KEY,
    bundle_id TEXT REFERENCES bundles_new(bundle_id) ON DELETE CASCADE,
    media_id TEXT NOT NULL,
    media_type TEXT,
    duration INT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(bundle_id, media_id)
);
```

### bundle_fan_interactions_new

Fan purchase interactions.

```sql
CREATE TABLE bundle_fan_interactions_new (
    id SERIAL PRIMARY KEY,
    bundle_id TEXT REFERENCES bundles_new(bundle_id) ON DELETE CASCADE,
    fan_user_id TEXT NOT NULL,
    message_id TEXT,
    sent_at TIMESTAMP,
    is_purchased BOOLEAN,
    purchased_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(bundle_id, fan_user_id)
);
```

### bundle_analytics_new

Bundle performance metrics.

```sql
CREATE TABLE bundle_analytics_new (
    id SERIAL PRIMARY KEY,
    bundle_id TEXT UNIQUE REFERENCES bundles_new(bundle_id) ON DELETE CASCADE,
    api_sent_count INT,
    api_viewed_count INT,
    api_purchased_count INT,
    tracked_offers INT,
    tracked_purchases INT,
    view_rate DECIMAL(5,2),
    conversion_rate DECIMAL(5,2),
    total_revenue DECIMAL(10,2),
    net_revenue DECIMAL(10,2),
    average_time_to_purchase INT,
    best_time_of_day INT,
    best_day_of_week INT,
    last_synced TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW()
);
```

### creator_credentials

Encrypted authentication credentials.

```sql
CREATE TABLE creator_credentials (
    id SERIAL PRIMARY KEY,
    model_id TEXT UNIQUE NOT NULL,
    account_name TEXT,
    encrypted_email TEXT,
    encrypted_auth TEXT,  -- JSON: {cookie, x_bc, user_agent}
    gologin_profile_id TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW()
);
```

---

## Authentication System

### Credential Sources (Priority Order)

1. **PostgreSQL Database** (creator_credentials table)
   - Encrypted credentials stored securely
   - Includes GoLogin profile ID for antidetection

2. **auth_multi.json** (fallback)
   - Local JSON file for standalone mode
   - Supports nested and flat formats

### GoLogin Integration

GoLogin provides antidetection browser profiles to avoid OnlyFans bot detection.

```python
# Different API tokens for different creator groups
GALEN_CREATORS = ["Juno", "avabarham2", "Luna", "luna"]

def get_gologin_token(creator_name: str) -> str:
    if creator_name in GALEN_CREATORS:
        return os.getenv("GALEN_API_TOKEN")
    return os.getenv("GOLOGIN_API_TOKEN")
```

### Session Management

- Reauthentication every 100 fans (configurable)
- GoLogin profile provides fresh cookies
- Automatic session refresh on 401/403 errors

---

## Checkpoint System

### Purpose

Track processing state to enable crash recovery without duplicate processing.

### Storage

JSON files per creator: `checkpoints/{creator_name}.json`

### Structure

```json
{
  "completed": [123, 456, 789],
  "in_progress": [101],
  "heavy": [
    {"fan_id": 202, "fan_username": "heavy_user"}
  ],
  "rate_limited": [
    {"fan_id": 303, "fan_username": "limited_user", "retry_count": 1}
  ],
  "failed": [
    {"fan_id": 404, "fan_username": "failed_user", "reason": "Max retries exceeded"}
  ]
}
```

### Operations

| Method | Purpose |
|--------|---------|
| `mark_in_progress(fan_id)` | Called before processing (prevents duplicates) |
| `mark_completed(fan_id)` | Called after success |
| `mark_heavy_fan(fan_id, username)` | Fan timed out, defer to Phase 3 |
| `mark_rate_limited(fan_id, username, retry_count)` | 429 error, retry later |
| `mark_permanently_failed(fan_id, username, error)` | Max retries exceeded |

### Thread Safety

- Uses `threading.Lock()` for concurrent access
- Safe for multiple workers processing same creator

---

## Error Handling

### Rate Limiting (429 Errors)

```
Detection: 'rate limit' in error OR '429' status OR 'too many requests'

Phase 1: Mark for Phase 2 retry
Phase 2: Exponential backoff (30s → 60s → 120s)
After 3 attempts: Mark permanently failed
```

### Connection Errors

| Error Type | Handling |
|------------|----------|
| Redis connection loss | Auto-reconnect with retry |
| Database connection loss | Connection pool auto-reconnect |
| API session expired | Reauthenticate every 100 fans |

### Timeouts

| Phase | Timeout | Handling |
|-------|---------|----------|
| Phase 1 (normal) | 10 minutes | Move to heavy fan queue |
| Phase 3 (heavy) | None | Let complete naturally |

### Data Integrity

- `message_id` as unique key prevents duplicates
- `ON CONFLICT DO NOTHING` for safe upserts
- Checkpoint prevents reprocessing same fan

---

## Docker Deployment

### Generate Compose File

```bash
# Production mode (all fans)
python generate_compose.py production

# Test mode (limited fans)
python generate_compose.py test
```

### Container Structure (7 creators)

| Service | Count | Memory | Purpose |
|---------|-------|--------|---------|
| Redis | 1 | 2GB | Message queue |
| db_worker | 1 | 1.5GB | Database persistence |
| producer-* | 7 | 2GB each | Initial bulk collection |
| listener-* | 7 | 1GB each | WebSocket event detection |
| new-fan-processor-* | 7 | 1GB each | Full history fetch |
| known-fan-processor-* | 7 | 512MB each | Incremental fetch |
| fan-sync-* | 7 | 512MB each | New subscriber detection |
| gologin-keepalive | 1 | 256MB | Session keepalive |

**Total**: ~38 containers, ~15GB peak memory

### Environment Variables

Required in `.env` file:

```env
DATABASE_URL=postgresql://user:pass@host:5432/dbname
ENCRYPTION_KEY=your-fernet-encryption-key
GOLOGIN_API_TOKEN=your-gologin-api-token
GALEN_API_TOKEN=your-galen-api-token  # Optional, for specific creators
```

---

## Performance Characteristics

### Initial Bulk Collection

| Metric | Value |
|--------|-------|
| Duration | ~16 hours for 284,933 fans |
| Concurrent fans | 3 (rolling window) |
| Avg time per fan | ~4 minutes |
| Heavy fans | Can take 1+ hour each |

### Real-Time Processing

| Metric | Value |
|--------|-------|
| WebSocket detection | Instant (milliseconds) |
| New fan full fetch | 1-5 minutes |
| Known fan incremental | 10-30 seconds |
| Data reduction | 90%+ (incremental vs full) |

### Database Performance

| Operation | Latency |
|-----------|---------|
| get_cutoff_id (single fan) | <50ms |
| get_known_fans (all fans) | 20-30 seconds |
| Batch insert (50 items) | <100ms |

### Fast Message Fetcher

- Batch size: 50 messages (vs library default 20)
- Performance: 2.5x faster than standard fetcher
- Direct API calls with auto-retry on network errors

---

## Module Reference

### modules/

| Module | Purpose |
|--------|---------|
| `authentication.py` | Credential loading, GoLogin integration |
| `message_fetcher.py` | Standard message fetching |
| `fast_message_fetcher.py` | High-performance 50-batch fetcher |
| `incremental_fetcher.py` | Cutoff-based incremental fetch |
| `bundle_processor.py` | Extract bundles, items, analytics |
| `checkpoint.py` | Thread-safe progress tracking |
| `cutoff_manager.py` | Query last message_id from DB |
| `redis_producer.py` | Push to Redis Lists |
| `redis_consumer.py` | Read from Redis Lists |
| `sanitizer.py` | UTF-8 cleaning, CSV escaping |
| `conversation_loader.py` | Load fans from JSON |
| `db_credential_loader.py` | Load encrypted credentials |
| `encryption.py` | Fernet encryption/decryption |
| `logger.py` | Logging setup utility |
| `gologin_manager.py` | GoLogin profile management |
| `timewaster.py` | Spend analysis and marking |

### models/

| Module | Purpose |
|--------|---------|
| `db_models.py` | SQLAlchemy ORM models |
| `timewaster_models.py` | Timewaster detection models |

---

## Troubleshooting

### Common Issues

**Producer stuck on heavy fans**
- Check `checkpoints/{creator}.json` for heavy fan list
- Heavy fans have no timeout - let them complete
- Consider increasing memory if OOM errors occur

**WebSocket disconnects**
- Check GoLogin profile is active
- Verify credentials in database are current
- Review listener logs for specific errors

**Database connection errors**
- Verify DATABASE_URL is correct
- Check PostgreSQL max_connections limit
- Review connection pool settings

**Rate limiting**
- System handles automatically with exponential backoff
- If persistent, check if IP is blocked
- Consider rotating GoLogin profiles

### Log Locations

```
logs/
  {creator_name}/
    producer_{timestamp}.log
    db_worker_{timestamp}.log
    websocket_{timestamp}.log
    new_fan_processor_{timestamp}.log
    known_fan_processor_{timestamp}.log
    fan_sync_{timestamp}.log
```

### Monitoring Commands

```bash
# Check producer progress
docker logs -f of-producer-{creator_name}

# Check database worker
docker logs -f of-db-worker

# Check WebSocket listener
docker logs -f of-listener-{creator_name}

# Check Redis queue sizes
redis-cli -p 6385 LLEN of:{creator_id}:messages
```
