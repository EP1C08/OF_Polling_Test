# OnlyFans Message Polling System - Quick Start Guide

## Prerequisites

- Python 3.10+
- Docker & Docker Compose
- PostgreSQL 14+
- Redis 7+
- GoLogin account (for antidetection)

## Setup

### 1. Clone and Install Dependencies

```bash
cd OF_Polling_Test
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` file:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/onlyfans_db

# Encryption (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
ENCRYPTION_KEY=your-fernet-key-here

# GoLogin API tokens
GOLOGIN_API_TOKEN=your-gologin-token
GALEN_API_TOKEN=your-galen-token  # Optional, for specific creators
```

### 3. Add Creator Credentials to Database

Credentials are stored in the `creator_credentials` table. Add them via your preferred method (SQL, admin panel, etc.).

Required fields:
- `model_id`: Creator's OnlyFans ID
- `account_name`: Display name
- `encrypted_auth`: Encrypted JSON with `cookie`, `x_bc`, `user_agent`
- `gologin_profile_id`: GoLogin browser profile ID

### 4. Generate Docker Compose

```bash
# Production mode
python generate_compose.py production

# Test mode (limited data)
python generate_compose.py test
```

This reads creators from the database and generates `docker-compose.generated.yml`.

## Running the System

### Initial Bulk Collection (First Run)

Fetch all historical messages (~16 hours for large accounts):

```bash
docker-compose -f docker-compose.generated.yml up redis producer-* db_worker --build -d
```

Monitor progress:
```bash
docker logs -f of-producer-{creator_name}
```

### Real-Time Mode (After Bulk Collection)

24/7 monitoring for new messages:

```bash
docker-compose -f docker-compose.generated.yml up redis db_worker \
    listener-* new-fan-processor-* known-fan-processor-* fan-sync-* --build -d
```

### All Services

```bash
docker-compose -f docker-compose.generated.yml up --build -d
```

## Directory Structure

```
OF_Polling_Test/
├── producer.py              # Bulk data collection
├── db_worker.py             # Database persistence
├── websocket_listener.py    # Real-time event detection
├── new_fan_processor.py     # Full history fetch for new fans
├── known_fan_processor.py   # Incremental fetch for known fans
├── fan_sync.py              # Periodic new subscriber detection
├── generate_compose.py      # Docker Compose generator
├── modules/                 # Core modules
│   ├── authentication.py
│   ├── fast_message_fetcher.py
│   ├── checkpoint.py
│   └── ...
├── models/                  # SQLAlchemy models
├── conversations/           # Fan list JSON files
├── checkpoints/             # Processing state files
├── logs/                    # Log files per creator
└── docs/                    # Documentation
```

## Common Commands

### Check Queue Sizes

```bash
redis-cli -p 6385 LLEN of:{creator_id}:messages
redis-cli -p 6385 LLEN of:{creator_id}:new_fans_priority
redis-cli -p 6385 LLEN of:{creator_id}:known_fans_queue
```

### Clear Checkpoints (Start Fresh)

```bash
rm checkpoints/{creator_name}.json
```

### Fetch Conversations JSON

```bash
python fetch_chats.py
```

### View Logs

```bash
# Producer logs
tail -f logs/{creator_name}/producer_*.log

# Database worker logs
tail -f logs/db_worker_*.log
```

## Troubleshooting

### Producer Not Processing Fans

1. Check checkpoint file exists: `checkpoints/{creator_name}.json`
2. Verify credentials in database are valid
3. Check GoLogin profile is active

### WebSocket Disconnecting

1. Verify GoLogin profile ID is correct
2. Check if cookies expired (re-authenticate via GoLogin)
3. Review listener logs for specific errors

### Database Connection Errors

1. Verify `DATABASE_URL` is correct
2. Check PostgreSQL is running and accessible
3. Ensure database user has required permissions

## Next Steps

- Read [ARCHITECTURE.md](ARCHITECTURE.md) for detailed system documentation
- Review [CLAUDE.md](../CLAUDE.md) for coding standards and project context
