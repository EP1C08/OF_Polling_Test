# WRONGTYPE Error - Root Cause & Resolution

## Problem Summary

**Error**: `WRONGTYPE Operation against a key holding the wrong kind of value`

**Location**: Redis operations in producer, websocket_listener, fan_sync, and db_worker

**Impact**: All services attempting to push/pop data from Redis fail, preventing data collection and storage

---

## Root Cause Analysis

### The Issue

Redis keys were being created with **conflicting data types**:

1. **Old System (Streams)**: Legacy consumer containers used `redis_consumer.py` which calls:
   ```python
   await self.redis.xgroup_create(stream_key, consumer_group, id='0', mkstream=True)
   ```
   - The `mkstream=True` parameter **automatically creates Redis Stream keys**
   - Stream keys: `of:{creator_id}:messages`, `of:{creator_id}:bundles`, etc.

2. **Current System (Lists)**: All active services use `redis_producer.py` which calls:
   ```python
   await self.redis.lpush(list_key, json_data)  # List operation
   await self.redis.lpop(list_key)              # List operation
   ```

3. **The Conflict**:
   - Consumer creates key `of:20661234:messages` as **Stream** type
   - Producer tries to `LPUSH` to same key → **WRONGTYPE error** (can't use List commands on Stream keys)
   - db_worker tries to `LPOP` from same key → **WRONGTYPE error**
   - websocket_listener tries to `LPUSH` to same key → **WRONGTYPE error**

### Evidence from Logs

**db_worker.log**:
```
[ERROR] [Jess] ✗ Redis error on key 'of:20661234:messages'
[ERROR] [Jess]   Key type: stream (expected: 'list')
[ERROR] [Jess]   Error: WRONGTYPE Operation against a key holding the wrong kind of value
```

**websocket_listener.log**:
```
[ERROR] ✗ Error fetching/processing messages for fan 456439940:
        WRONGTYPE Operation against a key holding the wrong kind of value
  File "/app/modules/redis_producer.py", line 85, in push_message
    list_length = await self.redis.lpush(list_key, json_data)
redis.exceptions.ResponseError: WRONGTYPE Operation against a key holding the wrong kind of value
```

**producer.log**:
```
[ERROR]   ✗ chambers: Error - WRONGTYPE Operation against a key holding the wrong kind of value
  File "/app/producer.py", line 83, in process_single_fan
    await producer.push_bundle(creator_id_str, bundle)
  File "/app/modules/redis_producer.py", line 112, in push_bundle
    return await self.redis.lpush(list_key, json_data)
redis.exceptions.ResponseError: WRONGTYPE Operation against a key holding the wrong kind of value
```

---

## System Architecture Evolution

### Old Architecture (Stream-based)
```
Producer → Redis Streams → Consumer → CSV Files
           (XADD)          (XREADGROUP)
```

**Files**:
- `modules/redis_producer.py` - Originally used Streams (now migrated to Lists)
- `modules/redis_consumer.py` - Uses Streams (legacy, no longer needed)
- `consumer.py` - Main consumer script (legacy)

### Current Architecture (List-based with Database)
```
Producer/Listener/FanSync → Redis Lists → db_worker → PostgreSQL
                            (LPUSH)       (LPOP)
```

**Files**:
- `modules/redis_producer.py` - Uses Lists (LPUSH)
- `producer.py` - Initial bulk collection
- `websocket_listener.py` - Real-time WebSocket notifications
- `fan_sync.py` - New subscriber detection
- `db_worker.py` - Multi-creator database worker (LPOP)

### Why the Migration?

**Streams → Lists**:
- **Simpler**: Lists are easier to work with (LPUSH/LPOP vs XADD/XREADGROUP)
- **No consumer groups**: Lists don't need consumer group management
- **Direct to database**: db_worker saves directly to PostgreSQL (no CSV intermediary)
- **Better fit**: Producer-consumer pattern naturally maps to List operations

---

## Resolution Steps

### 1. Remove Consumer Service from Docker Compose Generator

**File Modified**: `generate_compose.py`

**Changes**:
- **Removed** lines 190-221: Consumer service definition (used Stream-based redis_consumer.py)
- **Updated** container count: `2 + len(creators) * 4` → `2 + len(creators) * 3`
- **Removed** CSV mode usage instructions

**Before**:
```python
# Consumer service
consumer_service = f'consumer-{creator_name}'
compose_dict['services'][consumer_service] = {
    'build': {
        'context': '.',
        'dockerfile': 'Dockerfile.consumer'
    },
    ...
}
```

**After**:
```python
# Consumer service removed (no longer needed - using db_worker instead)

# WebSocket Listener service (24/7 real-time notifications)
listener_service = f'listener-{creator_name}'
```

### 2. Regenerate Docker Compose Configuration

```bash
python generate_compose.py production
```

**Output** (after fix):
```
✓ Generated docker-compose.generated.yml
  Redis: 1 container
  Creators: 9
  Producers: 9 containers (initial data collection)
  Database Worker: 1 container (handles all creators)
  WebSocket Listeners: 9 containers (24/7 real-time)
  Fan Sync Workers: 9 containers (every 4 hours)
  Total: 29 containers

Usage:
  Initial setup:  docker-compose -f docker-compose.generated.yml up redis producer-* db_worker --build -d
  Real-time mode: docker-compose -f docker-compose.generated.yml up redis db_worker listener-* fan-sync-* --build -d
```

Note: **No more consumer-\* services!**

### 3. Stop Existing Consumer Containers

If any consumer containers were running:

```bash
# Stop all consumer containers
docker ps -q --filter "name=of-consumer-" | xargs -r docker stop

# Remove consumer containers
docker ps -aq --filter "name=of-consumer-" | xargs -r docker rm
```

### 4. Flush Redis Database

Clear any Stream keys created by old consumers:

```bash
docker exec -it of-redis redis-cli -p 6385 FLUSHDB
```

**Verification**:
```bash
docker exec -it of-redis redis-cli -p 6385
127.0.0.1:6385> KEYS of:*
(empty array)
127.0.0.1:6385> exit
```

### 5. Restart Services (Without Consumers)

**For Initial Data Collection** (~16 hours):
```bash
docker-compose -f docker-compose.generated.yml up -d redis db_worker producer-*
```

**For Real-Time Mode** (24/7 ongoing):
```bash
docker-compose -f docker-compose.generated.yml up -d redis db_worker listener-* fan-sync-*
```

**DO NOT run consumer-\* services** - they are no longer in the generated file and would cause WRONGTYPE errors if manually added.

---

## Verification

### Check Running Containers

```bash
docker ps --filter "name=of-" --format "table {{.Names}}\t{{.Status}}"
```

**Expected** (Real-time mode, 9 creators):
```
NAME                      STATUS
of-redis                  Up
of-db-worker             Up
of-listener-jess         Up
of-listener-irene        Up
of-listener-ayumi        Up
... (7 more listeners)
of-fan-sync-jess         Up
of-fan-sync-irene        Up
of-fan-sync-ayumi        Up
... (7 more fan-sync)
```

**Should NOT see**: `of-consumer-*` containers

### Check Redis Key Types

```bash
docker exec -it of-redis redis-cli -p 6385
127.0.0.1:6385> KEYS of:*
127.0.0.1:6385> TYPE of:20661234:messages
list
127.0.0.1:6385> TYPE of:20661234:bundles
list
```

All keys should be type **"list"**, NOT "stream".

### Monitor Logs for Errors

```bash
# Check db_worker logs
tail -f logs/db_worker/db_worker.log | grep -i wrongtype

# Check websocket listener logs
tail -f logs/websocket_Jess/websocket.log | grep -i wrongtype

# Check producer logs (if running)
tail -f logs/Jess/producer.log | grep -i wrongtype
```

**Expected**: No WRONGTYPE errors after fix.

---

## Prevention

### Architecture Guidelines

1. **Single Data Type Per Namespace**: All services accessing `of:{creator_id}:{type}` keys must use the same Redis data type
2. **Consistent Producer Module**: All services use `modules/redis_producer.py` (List-based)
3. **No Legacy Consumers**: Never run `consumer.py` or services using `modules/redis_consumer.py`

### Code Review Checklist

When adding new services:
- ✅ Uses `redis_producer.py` for Redis operations
- ✅ Uses LPUSH/LPOP (List commands) or RPUSH/RPOP
- ❌ Does NOT use XADD/XREAD/XREADGROUP (Stream commands)
- ❌ Does NOT use `redis_consumer.py`

### Docker Compose Guidelines

- ✅ Include: redis, db_worker, producer-*, listener-*, fan-sync-*
- ❌ Exclude: consumer-* (legacy, causes conflicts)

---

## Troubleshooting

### If WRONGTYPE Errors Persist

1. **Check for rogue consumer containers**:
   ```bash
   docker ps -a | grep consumer
   ```

2. **Check Redis key types**:
   ```bash
   docker exec -it of-redis redis-cli -p 6385
   127.0.0.1:6385> KEYS of:*
   127.0.0.1:6385> TYPE of:20661234:messages
   ```

3. **If keys are still "stream" type**, delete them:
   ```bash
   docker exec -it of-redis redis-cli -p 6385 DEL "of:20661234:messages"
   docker exec -it of-redis redis-cli -p 6385 DEL "of:20661234:bundles"
   # ... etc for all stream keys
   ```

4. **Nuclear option** (if all else fails):
   ```bash
   # Stop all containers
   docker-compose -f docker-compose.generated.yml down

   # Flush Redis completely
   docker exec -it of-redis redis-cli -p 6385 FLUSHALL

   # Restart services
   docker-compose -f docker-compose.generated.yml up -d redis db_worker listener-* fan-sync-*
   ```

### If Redis Persistence is Enabled

Redis may be **loading old Stream keys from disk** on startup:

```bash
# Check if persistence is enabled
docker exec -it of-redis redis-cli -p 6385 CONFIG GET save
docker exec -it of-redis redis-cli -p 6385 CONFIG GET appendonly

# If appendonly is "yes", old data persists
# Solution: Delete RDB/AOF files
docker-compose -f docker-compose.generated.yml down
docker volume rm of_redis_data  # Deletes persistent volume
docker-compose -f docker-compose.generated.yml up -d redis
```

---

## Timeline of Events

1. **Initial System**: Used Redis Streams for producer → consumer → CSV export
2. **Migration**: Switched to Redis Lists for producer → db_worker → PostgreSQL
3. **Oversight**: Consumer service still generated in `docker-compose.yml`
4. **Conflict**: Consumers created Stream keys, producers/listeners used List commands
5. **Error**: WRONGTYPE errors across all services
6. **Fix**: Removed consumer service generation, flushed Redis, restarted services
7. **Resolution**: All services now use Lists consistently, no more conflicts

---

## Key Takeaways

1. **Redis data types are not interchangeable** - Stream commands can't operate on List keys and vice versa
2. **Legacy code can cause conflicts** - Even if not actively used, old services can interfere
3. **Docker Compose is the source of truth** - If a service is defined, it may start unexpectedly
4. **Flush Redis after architecture changes** - Old keys can persist and cause type mismatches
5. **Consumer services are obsolete** - db_worker replaces CSV consumers for database mode

---

## Related Files

### Modified
- `generate_compose.py` - Removed consumer service generation

### Active (List-based)
- `modules/redis_producer.py` - LPUSH/LPOP operations
- `producer.py` - Initial bulk collection
- `websocket_listener.py` - Real-time notifications
- `fan_sync.py` - New subscriber detection
- `db_worker.py` - Database worker

### Legacy (Stream-based, DO NOT USE)
- `modules/redis_consumer.py` - XREADGROUP operations
- `consumer.py` - CSV consumer script
- `Dockerfile.consumer` - Consumer container definition

---

## Date Resolved

**2025-12-03**

## Resolution Status

✅ **RESOLVED** - Consumer services removed from docker-compose generation, preventing Stream key creation and WRONGTYPE errors.
