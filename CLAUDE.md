# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OnlyFans message polling system built with Python that fetches messages and bundles from OnlyFans creators' conversations using the `ultima-scraper-api`. The system uses a producer-consumer architecture with Redis streams for distributed message processing and exports data to CSV files.

## Architecture

### Producer-Consumer Model
- **Producer** ([producer.py](producer.py)): Fetches messages from OnlyFans API and pushes to Redis Streams (one instance per creator)
- **Consumer** ([consumer.py](consumer.py)): Reads from Redis Streams and exports to CSV files (one instance per creator)
- **Standalone** ([main.py](main.py)): Single-process version that fetches and exports directly without Redis

### Data Flow
1. Producer authenticates with OnlyFans API using credentials from `auth_multi.json`
2. Loads conversation list from JSON files in `conversations/` directory
3. Processes fans concurrently (3 at a time by default) and fetches message history
4. Messages are processed into 5 data types: messages, bundles, bundle_items, fan_interactions, analytics
5. Data is pushed to Redis Streams (namespace: `of:{creator_id}:{stream_type}`)
6. Consumer reads from streams and appends to CSV files incrementally
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
- **bundle_processor.py**: Extracts bundle metadata, media items, purchase status, analytics
- **redis_producer.py**: Pushes to Redis Streams with sanitization, handles heavy fans and errors
- **redis_consumer.py**: Consumer groups for parallel processing, incremental CSV append, memory management
- **conversation_loader.py**: Loads lightweight conversation data from JSON files (avoids API calls)
- **sanitizer.py**: Removes invalid UTF-8 and control characters for CSV compatibility

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
- 1 producer + 1 consumer per creator
- Shared volumes: `auth_multi.json`, `conversations/`, `logs/`, `checkpoints/`, `output/`

### Run Containers
```bash
docker-compose -f docker-compose.generated.yml up --build -d
```

### Monitor Progress
```bash
docker logs -f of-producer-{creator_name}
docker logs -f of-consumer-{creator_name}
```

### Configuration via Environment Variables
- `CREATOR_ID`: Creator's OnlyFans ID (required)
- `CREATOR_NAME`: Display name for logs and folders
- `CONCURRENT_FANS`: Number of fans to process in parallel (default: 3)
- `FAN_DELAY`: Seconds between batches (default: 5)
- `FETCH_TIMEOUT`: Timeout per fan in seconds (default: 600 = 10 minutes)
- `REAUTH_INTERVAL`: Reauthenticate every N fans (default: 100)
- `REDIS_HOST`, `REDIS_PORT`: Redis connection (default: redis:6385)

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
CREATOR_ID=12345 CREATOR_NAME=CreatorName python producer.py

# Run consumer (one per creator)
CREATOR_ID=12345 CREATOR_NAME=CreatorName python consumer.py
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
