# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation

For detailed documentation, see the `docs/` directory:
- [QUICKSTART.md](docs/QUICKSTART.md) - Getting started guide
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - System architecture and data flow
- [API_REFERENCE.md](docs/API_REFERENCE.md) - Module and function reference

## Project Overview

OnlyFans message polling system built with Python that fetches messages and bundles from OnlyFans creators' conversations using the `ultima-scraper-api`. The system uses a producer-consumer architecture with Redis Lists for distributed message processing and exports data to PostgreSQL database.

## Architecture

### Producer-Consumer Model
- **Producer** ([producer.py](producer.py)): Fetches messages from OnlyFans API and pushes to Redis Lists (one instance per creator, initial bulk collection)
- **Database Worker** ([db_worker.py](db_worker.py)): Reads from Redis Lists and saves to PostgreSQL (one container handles all creators)
- **WebSocket Listener** ([websocket_listener.py](websocket_listener.py)): Real-time event detection via OnlyFans WebSocket (event-only, routes to queues)
- **New Fan Processor** ([new_fan_processor.py](new_fan_processor.py)): Fetches full message history for new fans from priority queue
- **Known Fan Processor** ([known_fan_processor.py](known_fan_processor.py)): Fetches incremental messages for known fans from queue
- **Fan Sync Worker** ([fan_sync.py](fan_sync.py)): Detects new subscribers every 4 hours
- **Standalone** ([main.py](main.py)): Single-process version that fetches and exports directly without Redis (legacy)

### Data Flow
1. Producer authenticates with OnlyFans API using credentials from `auth_multi.json`
2. Loads conversation list from JSON files in `conversations/` directory
3. Processes fans concurrently (3 at a time by default) and fetches message history
4. Messages are processed into 5 data types: messages, bundles, bundle_items, fan_interactions, analytics
5. Data is pushed to Redis Lists (namespace: `of:{creator_id}:{list_type}`)
6. Database worker reads from lists and batch inserts to PostgreSQL
7. Producer signals completion via `of:{creator_id}:producer_done` Redis key

### Checkpoint System ([modules/checkpoint.py](modules/checkpoint.py))
Thread-safe checkpoint manager persists processing state to JSON files in `checkpoints/` directory:
- **In-progress tracking**: Marks fans before processing to prevent duplicates on crash/restart
- **Heavy fan handling**: Fans that timeout (>10 min) are deferred and processed at the end without timeout
- **Rate limit retry**: Exponential backoff (30s, 60s, 120s) for rate-limited fans, processed BEFORE heavy fans
- **Permanent failures**: After 3 retries, fans are marked as permanently failed

### Key Modules
- **authentication.py**: Loads `auth_multi.json` (supports nested `accounts` array or flat list), creates `AuthDetails` objects
- **message_fetcher.py**: Fetches paginated message history, processes bundles (media bundles/mass messages)
- **fast_message_fetcher.py**: High-performance fetcher with 50-message batches (2.5x faster)
- **bundle_processor.py**: Extracts bundle metadata, media items, purchase status, analytics
- **redis_producer.py**: Pushes to Redis Lists with sanitization (LPUSH operations)
- **cutoff_manager.py**: Queries last message_id from database for incremental fetching
- **incremental_fetcher.py**: Fetches only new messages using cutoff_id (90%+ reduction)
- **conversation_loader.py**: Loads lightweight conversation data from JSON files (avoids API calls)
- **sanitizer.py**: Removes invalid UTF-8 and control characters for database compatibility
- **db_credential_loader.py**: Loads encrypted credentials from PostgreSQL

## Authentication

The `auth_multi.json` file stores OnlyFans credentials and supports two formats:

**Nested format** (preferred):
```json
{
  "accounts": [
    {
      "name": "Creator Name",
      "active": true,
      "auth": {
        "id": 12345,
        "cookie": "...",
        "x_bc": "...",
        "user_agent": "..."
      }
    }
  ]
}
```

**Flat format**:
```json
[
  {
    "id": 12345,
    "username": "creator",
    "cookie": "...",
    "x_bc": "..."
  }
]
```

The `username` or `name` field is used for logging and folder organization.

## Docker Deployment

### Generate Compose File
```bash
python generate_compose.py production
python generate_compose.py test  # For testing with limited data
```

This dynamically generates `docker-compose.generated.yml` (or `.test.yml`) based on creators in `auth_multi.json`:
- 1 Redis instance (port 6385)
- 1 producer per creator (fetches messages, pushes to Redis)
- 1 consumer per creator (reads from Redis, exports to CSV)
- **1 database worker (reads from Redis, saves to PostgreSQL for ALL creators)**
- Shared volumes: `auth_multi.json`, `conversations/`, `logs/`, `checkpoints/`, `output/`

**Database Worker Architecture:**
- Single container with 1 database connection pool
- Spawns 1 async worker per creator internally
- Each worker processes its creator's Redis streams independently
- More efficient than separate containers per creator

### Run Containers
```bash
# Initial Bulk Collection (fetch all historical data, ~16 hours)
docker-compose -f docker-compose.generated.yml up redis producer-* db_worker --build -d

# Real-time mode (24/7 WebSocket monitoring + on-demand processing)
docker-compose -f docker-compose.generated.yml up redis db_worker listener-* fan-sync-* new-fan-processor-* known-fan-processor-* --build -d

# All services
docker-compose -f docker-compose.generated.yml up --build -d
```

### Monitor Progress
```bash
# Producers (initial bulk collection)
docker logs -f of-producer-{creator_name}

# Database Worker (saves all data to PostgreSQL)
docker logs -f of-db-worker

# WebSocket Listeners (real-time message detection)
docker logs -f of-listener-{creator_name}

# Fan Sync Workers (new subscriber detection)
docker logs -f of-fan-sync-{creator_name}

# New Fan Processors (full history fetch for new fans)
docker logs -f of-new-fan-processor-{creator_name}

# Known Fan Processors (incremental fetch for known fans)
docker logs -f of-known-fan-processor-{creator_name}
```

### Configuration via Environment Variables

**Producer:**
- `CREATOR_ID`: Creator's OnlyFans ID (required)
- `CREATOR_NAME`: Display name for logs and folders
- `CONCURRENT_FANS`: Number of fans to process in parallel (default: 3)
- `FAN_DELAY`: Seconds between batches (default: 5)
- `FETCH_TIMEOUT`: Timeout per fan in seconds (default: 600 = 10 minutes)
- `REAUTH_INTERVAL`: Reauthenticate every N fans (default: 100)
- `REDIS_HOST`, `REDIS_PORT`: Redis connection (default: redis:6385)

**Database Worker:**
- `DATABASE_URL`: PostgreSQL connection string (required for db mode)
- `REDIS_HOST`, `REDIS_PORT`: Redis connection (default: redis:6385)
- `AUTH_FILE`: Path to auth_multi.json (default: /app/auth_multi.json)

## Real-Time WebSocket System

**NEW**: The system now supports 24/7 real-time message detection using OnlyFans WebSockets, eliminating the need for continuous polling.

### Architecture Components

#### 1. WebSocket Listener ([websocket_listener.py](websocket_listener.py))
- **Purpose**: 24/7 real-time event detection (EVENT-ONLY, no fetching)
- **Technology**: Uses `ultima-scraper-api`'s WebSocket support (`authed.listen()` and `authed.subscribe()`)
- **Deployment**: 1 container per creator (7 total for current setup)
- **Memory**: ~300-400MB per listener

**How it works:**
1. Connects to OnlyFans WebSocket and subscribes to event queue
2. Receives instant notification when new message arrives
3. Queries database to check if fan exists (has messages in `messages_new` table)
4. Routes fan to appropriate Redis queue:
   - NEW fan (not in DB) → `new_fans_priority` queue → new_fan_processor
   - KNOWN fan (in DB) → `known_fans_queue` → known_fan_processor

#### 1a. New Fan Processor ([new_fan_processor.py](new_fan_processor.py))
- **Purpose**: Fetch full message history for new fans
- **Queue**: `of:{creator_id}:new_fans_priority`
- **Deployment**: 1 container per creator
- **Memory**: 1GB limit (needs to fetch full histories)

**How it works:**
1. BRPOP from `new_fans_priority` queue (blocking wait)
2. Fetch FULL message history (cutoff_id=None)
3. Push to Redis lists → db_worker saves to PostgreSQL
4. Mark as completed in checkpoint

#### 1b. Known Fan Processor ([known_fan_processor.py](known_fan_processor.py))
- **Purpose**: Fetch incremental messages for known fans
- **Queue**: `of:{creator_id}:known_fans_queue`
- **Deployment**: 1 container per creator
- **Memory**: 512MB limit (lightweight incremental fetch)

**How it works:**
1. BRPOP from `known_fans_queue` (blocking wait)
2. Get cutoff_id from database (last known message_id)
3. Fetch ONLY new messages using incremental fetcher
4. Push to Redis lists → db_worker saves to PostgreSQL
5. Publish to Pub/Sub for external consumers

#### 2. Fan Sync Worker ([fan_sync.py](fan_sync.py))
- **Purpose**: Detect new subscribers every 4 hours
- **Deployment**: 1 container per creator (7 total)
- **Schedule**: Runs every 14400 seconds (4 hours)

**How it works:**
1. Queries database for all known fan IDs using SQLAlchemy
2. Loads current subscribers from `conversations/{creator_name}.json`
3. Compares sets to find new fans (all_fans - known_fans)
4. Fetches complete message history for new fans only
5. Pushes to Redis → db_worker saves to database

**Performance:**
- Database query: ~20-30 seconds for 284,933 fans (streaming)
- Set comparison: <1 second (in-memory)
- Total: ~40-45 seconds per sync cycle

#### 3. Cutoff Manager ([modules/cutoff_manager.py](modules/cutoff_manager.py))
- **Purpose**: Query last message_id from database for incremental fetching
- **Technology**: SQLAlchemy with async PostgreSQL queries
- **Methods**:
  - `get_cutoff_id(model_id, fan_id)`: Get last message_id for specific fan (<50ms)
  - `get_all_fan_cutoffs(model_id)`: Get all cutoffs for creator (20-30s streaming)
  - `get_known_fans(model_id)`: Get list of all known fan IDs

#### 4. Incremental Fetcher ([modules/incremental_fetcher.py](modules/incremental_fetcher.py))
- **Purpose**: Fetch only new messages using `cutoff_id` parameter
- **Uses**: Existing `fetch_all_messages()` function (already supports cutoff_id)
- **Benefit**: 90%+ reduction in data fetched (only new messages, not entire history)

### SQLAlchemy Models ([models/db_models.py](models/db_models.py))
Database table models for querying:
- **Message**: Main messages table (indexed on model_id, fan_id, message_id)
- **Bundle**: Media bundles and mass messages
- **BundleItem**: Individual media items within bundles
- **BundleFanInteraction**: Fan purchase interactions
- **BundleAnalytics**: Bundle performance metrics

**Features:**
- Type-safe ORM with IDE autocomplete
- Built-in connection pooling (pool_size=1-5, auto-reconnect)
- Query caching and streaming for large result sets
- Async support via `sqlalchemy[asyncio]`

### Docker Deployment (Real-Time Mode)

**Initial Setup (one-time):**
```bash
python generate_compose.py production
docker-compose -f docker-compose.generated.yml up redis producer-* db_worker --build -d
# Wait ~16 hours for initial data collection (284,933 fans)
```

**Real-Time Monitoring (24/7):**
```bash
docker-compose -f docker-compose.generated.yml up redis db_worker listener-* fan-sync-* new-fan-processor-* known-fan-processor-* --build -d
```

**Monitor Logs:**
```bash
# WebSocket listeners (real-time notifications)
docker logs -f of-listener-{creator_name}

# Fan sync workers (new subscriber detection)
docker logs -f of-fan-sync-{creator_name}

# Database worker (saves all data)
docker logs -f of-db-worker
```

### Environment Variables

**WebSocket Listener:**
- `CREATOR_ID`: Creator's OnlyFans ID (required)
- `CREATOR_NAME`: Display name for logs
- `DATABASE_URL`: PostgreSQL connection string (required)
- `REDIS_HOST`, `REDIS_PORT`: Redis connection (default: redis:6385)

**Fan Sync Worker:**
- `CREATOR_ID`: Creator's OnlyFans ID (required)
- `CREATOR_NAME`: Display name for logs
- `DATABASE_URL`: PostgreSQL connection string (required)
- `REDIS_HOST`, `REDIS_PORT`: Redis connection (default: redis:6385)
- `SYNC_INTERVAL`: Seconds between syncs (default: 14400 = 4 hours)

### Performance Comparison

| Operation | Old System (Polling) | New System (WebSocket) | Improvement |
|-----------|---------------------|------------------------|-------------|
| Initial data load | 16 hours (284,933 fans) | 16 hours (same) | Same (one-time) |
| New message detect | 16 hours (full re-scan) | INSTANT (WebSocket) | ∞ (real-time) |
| New subscriber check | N/A (manual) | 40 seconds every 4h | Automated |
| Fetch 1 fan (new msg) | Fetch ALL messages | Fetch ONLY new (cutoff_id) | 90%+ less data |
| Database query (68k fans) | Raw asyncpg: 5-8s | SQLAlchemy: 4-6s | 20% faster + type safety |

### Data Flow (Real-Time)

```
┌─────────────────────────────────────────────────────────────────┐
│ INITIAL SETUP (One-Time, ~16 hours)                             │
│ producer.py → Redis → db_worker → PostgreSQL                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ ONGOING 24/7 REAL-TIME                                          │
│                                                                 │
│ WebSocket Event                                                 │
│     │                                                           │
│     ▼                                                           │
│ websocket_listener.py (EVENT-ONLY)                              │
│     │ (check messages_new table)                                │
│     │                                                           │
│     ├─ NEW fan (cutoff_id = None)                               │
│     │  │ LPUSH to new_fans_priority                             │
│     │  ▼                                                        │
│     │  new_fan_processor.py                                     │
│     │  │ (full history fetch)                                   │
│     │  ▼                                                        │
│     │  Redis → db_worker → PostgreSQL                           │
│     │                                                           │
│     └─ KNOWN fan (cutoff_id exists)                             │
│        │ LPUSH to known_fans_queue                              │
│        ▼                                                        │
│        known_fan_processor.py                                   │
│        │ (incremental fetch)                                    │
│        ▼                                                        │
│        Redis → db_worker → PostgreSQL                           │
│        │                                                        │
│        ▼                                                        │
│        Pub/Sub publish                                          │
│                                                                 │
│ ┌─ Fan Sync (every 4h) ──────────────────────────────────┐     │
│ │   DB query (20-30s) → Compare JSON → Fetch new fans    │     │
│ └────────────────────────→ Redis → db_worker → PostgreSQL┘     │
└─────────────────────────────────────────────────────────────────┘
```

### Container Overview

For 7 creators (Ayumi, Hyunnie, Irene, Jess, Loli, Tayla):
- **1 Redis** container
- **7 Producers** (initial setup only, exit when done)
- **1 Database Worker** (handles all creators)
- **7 WebSocket Listeners** (24/7 event detection, routes to queues)
- **7 New Fan Processors** (full history fetch for new fans)
- **7 Known Fan Processors** (incremental fetch for known fans)
- **7 Fan Sync Workers** (new subscriber detection every 4 hours)
- **Total: 37 containers** (1 Redis + 1 db_worker + 7 listeners + 7 new_fan_processor + 7 known_fan_processor + 7 fan_sync + 7 producers)

**Memory Usage:**
- Redis: 2GB limit
- Database Worker: 1.5GB limit
- Each Listener: 512MB limit
- Each New Fan Processor: 1GB limit
- Each Known Fan Processor: 512MB limit
- Each Fan Sync: 512MB limit
- Total: ~15GB peak

## Common Commands

### Standalone Mode (No Docker)
```bash
# Install dependencies
pip install -r requirements.txt

# Run single-process fetch and export
python main.py
```

### Producer-Consumer Mode (Manual)
```bash
# Start Redis
redis-server --port 6385

# Run producer (one per creator)
REDIS_PORT=6385 CREATOR_ID=12345 CREATOR_NAME=CreatorName python producer.py

# Run consumer (one per creator)
REDIS_PORT=6385 CREATOR_ID=12345 CREATOR_NAME=CreatorName python consumer.py
```

### Generate Docker Compose
```bash
# Production mode (all fans)
python generate_compose.py production

# Test mode (3 fans, 10 messages each)
python generate_compose.py test
```

### Clear Checkpoints (Start Fresh)
Delete the checkpoint file for a creator:
```bash
rm checkpoints/{creator_name}.json
```

## Output Structure

CSV files are organized by creator:
```
output/
  {creator_name}/
    messages.csv
    bundles.csv
    bundle_items.csv
    bundle_fan_interactions.csv
    bundle_analytics.csv
```

Logs are organized by creator and type:
```
logs/
  {creator_name}/
    producer_{timestamp}.log
    consumer_{timestamp}.log
```

## Processing Phases

The producer processes fans in 3 phases:

1. **Normal Fans**: Concurrent processing (3 at a time) with 10-minute timeout
2. **Rate-Limited Fans**: Retry with exponential backoff (30s, 60s, 120s) - processed BEFORE heavy fans
3. **Heavy Fans**: Sequential processing with NO timeout (for fans with massive message histories)

## Important Notes

- The system fetches complete message history (all messages from latest to oldest)
- Message pagination is handled internally by `ultima-scraper-api`'s `get_messages(limit=20)`
- Checkpoints enable resume on crash/restart without re-fetching completed fans
- Consumer uses append mode for incremental CSV updates (safe for long-running producers)
- Redis Streams are trimmed after acknowledgment to prevent memory bloat
- Heavy fans are stored in checkpoints (persists across Redis restarts, unlike Redis-based queue)
- Reauthentication happens every 100 processed fans to maintain session validity

## Coding Standards

This project follows **Fandom Agency coding standards** (see `coding_standards.pdf`).

### General Principles
- Code readability is paramount - write code that explains itself
- Follow PEP 8 as the foundation, with specific overrides noted below
- Prefer refactoring complex code over adding comments

### Naming Conventions
- **Packages/Modules**: `snake_case`
- **Classes**: `PascalCase` (e.g., `CustomJpegGenerator`, not `CustomJPEGGenerator`)
- **Functions**: `snake_case`
- **Variables**: `snake_case` with meaningful names (no single characters except `i` for counters)
- **Constants**: `UPPERCASE`
- **Private variables/functions**: `_snake_case_with_leading_underscore`
- **Keywords conflicts**: Add trailing underscore (e.g., `input_`)

### Comments
- **Avoid comments** - prefer self-explanatory code
- Let user write their own comments instead

### Code Style
- **Indentation**: 4 spaces (no tabs)
- **Function length**: ~50 lines max (soft limit)
- **File length**: 0-500 lines approximately
- **Line ending**: Always end Python modules with a blank line

### Function Parameters
Use hanging indents for long parameter lists:
```python
def function_name(
    self,
    longer_variable_one_name,
    longer_variable_two_name,
):
```

### Control Flow
Avoid nested if statements - use early returns instead:
```python
# Good
if user != actual_username:
    return InvalidUsername()
if password != actual_password:
    return InvalidPassword()

session = Session(username, password)
```

### Error Handling
- Do not abuse error handling - only enclose code that can raise an exception inside try/except blocks
- As much as possible, avoid raising `Exception`, use a more focused exception class

### Type Hints
- **Required** for all function parameters and return types (unless return is None)
- Add space between variable name and type: `variable: str`
- Example: `def function_name(param: str, count: int = 10) -> int:`

### Docstrings
- **Required** for all classes and public functions
- Use triple double quotes: `"""`
- No space between opening quotes and first word
- Must end with period
- Follow reStructuredText format:
```python
"""Brief summary of what the function does.

:param param_name: Parameter description
:raises ErrorType: Error description
:return: Return description
"""
```
- Do not use `rtype`, let type hinting fulfill that purpose

### Logging
- Use proper logging, never print statements in production
- Example: `logger = logging.getLogger(__name__)`
- If the project has an internal logging library, use that instead

### Best Practices
- Eliminate duplicate code - extract to functions/modules
- Don't compare boolean variables to True/False directly
- Use meaningful variable names that explain purpose
- Keep functions focused on single responsibility
