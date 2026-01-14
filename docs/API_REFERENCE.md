# OnlyFans Message Polling System - API Reference

## Core Modules

### modules/authentication.py

Handles credential loading and OnlyFans API authentication.

#### Functions

```python
async def load_auth_credentials(auth_file: str = 'auth_multi.json') -> list
```
Load credentials from JSON file.

**Parameters:**
- `auth_file`: Path to authentication JSON file

**Returns:** List of credential dictionaries

---

```python
async def authenticate_account(auth_details: AuthDetails, logger) -> tuple
```
Authenticate with OnlyFans API.

**Parameters:**
- `auth_details`: AuthDetails object with credentials
- `logger`: Logger instance

**Returns:** Tuple of (api, authed) objects

---

```python
async def create_api_helper(
    auth_details: AuthDetails,
    logger,
    gologin_profile_id: str = None,
    gologin_api_token: str = None,
    use_gologin_credentials: bool = False
) -> tuple
```
Create authenticated API session with optional GoLogin proxy.

**Parameters:**
- `auth_details`: AuthDetails object
- `logger`: Logger instance
- `gologin_profile_id`: GoLogin profile ID for antidetection
- `gologin_api_token`: GoLogin API token
- `use_gologin_credentials`: Use fresh cookies from GoLogin

**Returns:** Tuple of (api, authed) objects

---

### modules/fast_message_fetcher.py

High-performance message fetching with 50-message batches.

#### Class: FastMessageFetcher

```python
class FastMessageFetcher:
    def __init__(self, authed, logger=None)
```

**Methods:**

```python
async def fetch_all_messages(
    self,
    user,
    cutoff_id: int = None,
    max_messages: int = None
) -> list
```
Fetch all messages for a user.

**Parameters:**
- `user`: User object to fetch messages from
- `cutoff_id`: Stop fetching when reaching this message_id (for incremental)
- `max_messages`: Maximum messages to fetch (for testing)

**Returns:** List of message objects

---

### modules/incremental_fetcher.py

Cutoff-based incremental message fetching.

#### Functions

```python
async def fetch_new_messages(
    user,
    authed,
    cutoff_id: int,
    logger=None
) -> list
```
Fetch only messages newer than cutoff_id.

**Parameters:**
- `user`: User object
- `authed`: Authenticated API object
- `cutoff_id`: Last known message_id
- `logger`: Logger instance

**Returns:** List of new message objects (90%+ less data than full fetch)

---

### modules/message_fetcher.py

Standard message fetching and processing.

#### Functions

```python
async def fetch_all_messages(user, authed, logger=None) -> list
```
Fetch all messages using library method (slower).

---

```python
async def fetch_all_messages_fast(
    user,
    authed,
    logger=None,
    cutoff_id: int = None
) -> list
```
Fetch messages using FastMessageFetcher (2.5x faster).

---

```python
async def process_messages_and_bundles(
    messages: list,
    creator_id: str,
    creator_username: str,
    fan_id: str,
    authed
) -> tuple
```
Process raw messages into structured data.

**Returns:** Tuple of:
- `messages_data`: List of message dictionaries
- `bundles_data`: List of bundle dictionaries
- `bundle_items_data`: List of media item dictionaries
- `interactions_data`: List of fan interaction dictionaries
- `analytics_data`: List of analytics dictionaries

---

### modules/checkpoint.py

Thread-safe checkpoint management for crash recovery.

#### Class: CheckpointManager

```python
class CheckpointManager:
    def __init__(self, creator_name: str, checkpoint_dir: str = 'checkpoints')
```

**Methods:**

```python
def mark_in_progress(self, fan_id: int) -> None
```
Mark fan as currently being processed.

---

```python
def mark_completed(self, fan_id: int) -> None
```
Mark fan as successfully completed.

---

```python
def mark_heavy_fan(self, fan_id: int, fan_username: str) -> None
```
Mark fan as heavy (timed out, needs Phase 3 processing).

---

```python
def mark_rate_limited(self, fan_id: int, fan_username: str, retry_count: int) -> None
```
Mark fan as rate-limited with retry count.

---

```python
def mark_permanently_failed(self, fan_id: int, fan_username: str, error: str) -> None
```
Mark fan as permanently failed (max retries exceeded).

---

```python
def is_completed(self, fan_id: int) -> bool
def is_in_progress(self, fan_id: int) -> bool
def is_heavy_fan(self, fan_id: int) -> bool
def is_rate_limited(self, fan_id: int) -> bool
```
Check fan status.

---

```python
def get_progress_stats(self, total_users: int) -> dict
```
Get processing statistics.

**Returns:**
```python
{
    'completed': int,
    'in_progress': int,
    'remaining': int,
    'progress_percent': float
}
```

---

```python
def get_heavy_fans(self) -> list
def get_rate_limited_fans(self) -> list
```
Get lists of heavy/rate-limited fans for retry processing.

---

### modules/cutoff_manager.py

Database queries for incremental fetching.

#### Class: CutoffManager

```python
class CutoffManager:
    def __init__(self, database_url: str)
```

**Methods:**

```python
async def get_cutoff_id(self, model_id: str, fan_id: str) -> int | None
```
Get last known message_id for a fan.

**Parameters:**
- `model_id`: Creator's ID
- `fan_id`: Fan's ID

**Returns:** Last message_id or None if new fan

---

```python
async def get_known_fans(self, model_id: str) -> set
```
Get set of all known fan IDs for a creator.

**Returns:** Set of fan_id integers

---

```python
async def get_all_fan_cutoffs(self, model_id: str) -> dict
```
Get cutoff_ids for all fans (streaming query).

**Returns:** Dict mapping fan_id to last message_id

---

### modules/redis_producer.py

Push data to Redis Lists.

#### Class: RedisProducer

```python
class RedisProducer:
    def __init__(self, redis_host: str = 'redis', redis_port: int = 6385)
```

**Methods:**

```python
async def connect(self) -> None
async def close(self) -> None
```
Connection management.

---

```python
async def push_messages_batch(self, creator_id: str, messages: list) -> None
async def push_bundle(self, creator_id: str, bundle: dict) -> None
async def push_bundle_items(self, creator_id: str, items: list) -> None
async def push_fan_interactions(self, creator_id: str, interactions: list) -> None
async def push_analytics(self, creator_id: str, analytics: list) -> None
```
Push data to respective Redis Lists.

---

```python
async def mark_producer_done(self, creator_id: str) -> None
```
Set completion flag for db_worker.

---

```python
async def push_to_queue(self, creator_id: str, queue_name: str, fan_id: str) -> None
```
Push fan to event queue (new_fans_priority or known_fans_queue).

---

### modules/conversation_loader.py

Load fan lists from JSON files.

#### Functions

```python
def load_conversations_from_json(
    creator_name: str,
    conversations_dir: str = 'conversations'
) -> list
```
Load conversations from JSON file.

**Parameters:**
- `creator_name`: Creator's name (matches filename)
- `conversations_dir`: Directory containing JSON files

**Returns:** List of user dictionaries with id, username, name

**Raises:** `FileNotFoundError` if JSON not found

---

```python
def create_user_object_from_json(user_data: dict) -> SimpleUser
```
Create user object compatible with API methods.

---

### modules/db_credential_loader.py

Load encrypted credentials from database.

#### Functions

```python
async def load_credentials_from_db(
    database_url: str = None,
    encryption_key: str = None,
    model_id: str = None,
    only_authenticated: bool = True
) -> list
```
Load and decrypt credentials from creator_credentials table.

**Parameters:**
- `database_url`: PostgreSQL connection string (falls back to env)
- `encryption_key`: Fernet key (falls back to env)
- `model_id`: Filter by specific creator (optional)
- `only_authenticated`: Only return active credentials

**Returns:** List of credential dictionaries

---

### modules/sanitizer.py

Data sanitization for database compatibility.

#### Functions

```python
def sanitize_text(text: str) -> str
```
Remove invalid UTF-8, control characters, zero-width chars.

---

```python
def sanitize_for_csv(text: str) -> str
```
Escape text for CSV export.

---

```python
def sanitize_dict(data: dict) -> dict
```
Recursively sanitize all string values in dictionary.

---

### modules/bundle_processor.py

Extract bundle data from messages.

#### Functions

```python
def process_bundle(message, creator_id: str, creator_username: str) -> tuple
```
Extract bundle metadata from a message.

**Returns:** Tuple of (bundle_dict, items_list, interaction_dict, analytics_dict)

---

```python
def generate_bundle_id(message) -> str
```
Generate unique bundle ID using SHA256 hash.

---

### modules/logger.py

Logging setup utility.

#### Functions

```python
def setup_logger(
    creator_name: str,
    log_type: str = 'general',
    log_dir: str = 'logs'
) -> logging.Logger
```
Create configured logger with file and console handlers.

**Parameters:**
- `creator_name`: Creator name for log directory
- `log_type`: Log type (producer, consumer, websocket, etc.)
- `log_dir`: Base log directory

**Returns:** Configured Logger instance

---

## Models

### models/db_models.py

SQLAlchemy ORM models.

#### Message

```python
class Message(Base):
    __tablename__ = 'messages_new'

    id: int                    # Primary key
    message_id: str            # Unique OnlyFans message ID
    model_id: str              # Creator ID
    fan_id: str                # Fan ID
    sender_id: str             # Who sent the message
    model_name: str            # Creator username
    sender_username: str       # Sender username
    message: str               # Message content
    message_type: str          # 'text', 'media', 'bundle'
    media_id: str              # Media file ID
    media_type: str            # 'photo', 'video', 'audio'
    price: Decimal             # Purchase price
    is_free: bool              # Free message flag
    is_purchased: bool         # Fan purchased flag
    is_from_me: bool           # Creator sent flag
    created_at: datetime       # Message creation time
    fetched_at: datetime       # When we fetched it
```

#### Bundle

```python
class Bundle(Base):
    __tablename__ = 'bundles_new'

    id: int
    bundle_id: str             # Unique bundle ID
    message_id: str            # Container message ID
    creator_id: str            # Creator ID
    creator_username: str
    total_price: Decimal       # Bundle price
    media_count: int           # Total media items
    photo_count: int
    video_count: int
    audio_count: int
    is_mass_message: bool      # Mass message vs PPM
    queue_id: str              # Promo queue ID
    name: str                  # Bundle name
    description: str           # Bundle description
    created_at: datetime
    first_seen_at: datetime
```

#### BundleItem

```python
class BundleItem(Base):
    __tablename__ = 'bundle_items_new'

    id: int
    bundle_id: str             # FK to bundles_new
    media_id: str              # Media file ID
    media_type: str            # 'photo', 'video', 'audio'
    duration: int              # Duration in seconds
    created_at: datetime
```

#### BundleFanInteraction

```python
class BundleFanInteraction(Base):
    __tablename__ = 'bundle_fan_interactions_new'

    id: int
    bundle_id: str             # FK to bundles_new
    fan_user_id: str           # Fan who interacted
    message_id: str            # Message ID
    sent_at: datetime          # When sent to fan
    is_purchased: bool         # Did fan purchase
    purchased_at: datetime     # Purchase timestamp
    created_at: datetime
```

#### BundleAnalytics

```python
class BundleAnalytics(Base):
    __tablename__ = 'bundle_analytics_new'

    id: int
    bundle_id: str             # FK to bundles_new (unique)
    api_sent_count: int        # Messages sent
    api_viewed_count: int      # Views
    api_purchased_count: int   # Purchases
    tracked_offers: int
    tracked_purchases: int
    view_rate: Decimal         # View percentage
    conversion_rate: Decimal   # Conversion percentage
    total_revenue: Decimal
    net_revenue: Decimal
    average_time_to_purchase: int
    best_time_of_day: int      # Hour (0-23)
    best_day_of_week: int      # Day (0-6)
    last_synced: datetime
    last_updated: datetime
```

#### CreatorCredential

```python
class CreatorCredential(Base):
    __tablename__ = 'creator_credentials'

    id: int
    model_id: str              # Unique creator ID
    account_name: str          # Display name
    encrypted_email: str       # Encrypted email
    encrypted_auth: str        # Encrypted {cookie, x_bc, user_agent}
    gologin_profile_id: str    # GoLogin profile ID
    created_at: datetime
    last_updated: datetime
```

---

## Producer Functions

### producer.py

#### fetch_all_chats_paginated

```python
async def fetch_all_chats_paginated(authed, logger) -> list
```
Fetch ALL chats using manual pagination (fallback when JSON missing).

**Parameters:**
- `authed`: Authenticated OnlyFans user object
- `logger`: Logger instance

**Returns:** List of user dictionaries with id, username, name

**Note:** Uses `/api2/v2/chats?limit=100&offset=X&order=recent` endpoint with `hasMore` pagination.

---

#### process_single_fan

```python
async def process_single_fan(
    fan_user,
    authed,
    checkpoint,
    producer,
    creator_id_str: str,
    creator_username: str,
    fetch_timeout: int,
    logger,
    semaphore
) -> dict
```
Process a single fan with concurrent-safe error handling.

**Returns:**
```python
{
    'status': 'success' | 'empty' | 'timeout' | 'rate_limit' | 'error',
    'fan_id': int,
    'fan_username': str,
    'elapsed': float,  # Optional
    'error': str       # Optional, for error cases
}
```

---

#### retry_rate_limited_fan

```python
async def retry_rate_limited_fan(
    fan_data: dict,
    authed,
    checkpoint,
    producer,
    creator_id_str: str,
    creator_username: str,
    fetch_timeout: int,
    logger,
    max_retries: int = 3
) -> dict
```
Retry a rate-limited fan with exponential backoff (30s, 60s, 120s).

**Returns:**
```python
{
    'status': 'success' | 'rate_limit' | 'failed',
    'fan_id': int,
    'fan_username': str
}
```

---

## Environment Variables Reference

### Required

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `ENCRYPTION_KEY` | Fernet encryption key for credentials |
| `CREATOR_ID` | Creator's OnlyFans ID (for producer/processors) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `CREATOR_NAME` | CREATOR_ID | Display name for logs |
| `REDIS_HOST` | redis | Redis hostname |
| `REDIS_PORT` | 6385 | Redis port |
| `CONCURRENT_FANS` | 3 | Parallel fan processing |
| `FETCH_TIMEOUT` | 600 | Timeout per fan (seconds) |
| `REAUTH_INTERVAL` | 100 | Reauthenticate every N fans |
| `TEST_LIMIT` | None | Limit fans for testing |
| `BATCH_SIZE` | 50 | Database batch insert size |
| `SYNC_INTERVAL` | 14400 | Fan sync interval (seconds) |
| `MIN_MESSAGE_AGE_HOURS` | 24 | Skip recent messages |
| `GOLOGIN_API_TOKEN` | None | GoLogin API token |
| `GALEN_API_TOKEN` | None | Alternative GoLogin token |
| `TW_ENABLED` | true | Enable timewaster detection |
| `TW_MAX_SPEND` | 50.0 | Max spend for timewaster |
| `TW_MIN_MESSAGES` | 50 | Min messages for timewaster |
| `TW_MAX_RPM` | 0.05 | Max revenue per message |
