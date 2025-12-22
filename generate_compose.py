"""Dynamic Docker Compose Generator.

Generates docker-compose.yml based on creators in auth_multi.json.
"""

import json
import yaml
from pathlib import Path


def load_creators_from_auth(auth_file: str = 'auth_multi.json') -> list:
    """Load creator info (ID and name) from auth_multi.json.

    :param auth_file: Path to authentication JSON file
    :return: List of creator dictionaries with 'id' and 'name' keys
    """
    with open(auth_file, 'r') as f:
        auth_data = json.load(f)

    creators = []

    # Handle different auth file structures
    if isinstance(auth_data, dict) and 'accounts' in auth_data:
        # Structure: {"accounts": [...], "settings": {...}}
        accounts = auth_data['accounts']
        for account in accounts:
            # Only process active accounts
            if not account.get('active', True):
                continue

            # Get ID from nested auth object
            creator_id = None
            if 'auth' in account and 'id' in account['auth']:
                creator_id = account['auth']['id']
            elif 'id' in account:
                creator_id = account['id']

            # Get name for container naming
            creator_name = account.get('name', str(creator_id))

            if creator_id:
                creators.append({
                    'id': str(creator_id),
                    'name': creator_name
                })

    elif isinstance(auth_data, list):
        # Structure: [{...}, {...}]
        for account in auth_data:
            creator_id = account.get('id') or account.get('username')
            creator_name = account.get('name', str(creator_id))
            if creator_id:
                creators.append({
                    'id': str(creator_id),
                    'name': creator_name
                })

    elif isinstance(auth_data, dict):
        # Structure: single account dict
        creator_id = auth_data.get('id') or auth_data.get('username')
        creator_name = auth_data.get('name', str(creator_id))
        if creator_id:
            creators.append({
                'id': str(creator_id),
                'name': creator_name
            })

    return creators


def generate_docker_compose(
    creators: list,
    mode: str = 'production',
    output_file: str = None,
) -> str:
    """Generate docker-compose configuration.

    :param creators: List of creator dictionaries with 'id' and 'name' keys
    :param mode: 'production' or 'test'
    :param output_file: Output filename (auto-generated if None)
    :return: Path to generated docker-compose file
    """

    is_test = mode == 'test'
    compose_dict = {
        'version': '3.8',
        'services': {},
        'volumes': {},
        'networks': {
            'of-network' if not is_test else 'of-test-network': {
                'driver': 'bridge'
            }
        }
    }

    network_name = 'of-test-network' if is_test else 'of-network'
    redis_name = 'redis-test' if is_test else 'redis'
    container_prefix = 'of-test' if is_test else 'of'
    redis_internal_port = '6385'
    redis_port_mapping = f'{redis_internal_port}:{redis_internal_port}'

    # Redis service
    compose_dict['services'][redis_name] = {
        'image': 'redis:7-alpine',
        'container_name': f'{container_prefix}-redis',
        'ports': [redis_port_mapping],
        'volumes': [f'redis_{"test_" if is_test else ""}data:/data'],
        'command': f'redis-server --port {redis_internal_port} --appendonly yes --maxmemory 2gb --maxmemory-policy allkeys-lru',
        'restart': 'unless-stopped',
        'networks': [network_name],
        'deploy': {
            'resources': {
                'limits': {
                    'memory': '2G'
                },
                'reservations': {
                    'memory': '512M'
                }
            }
        }
    }

    compose_dict['volumes'][f'redis_{"test_" if is_test else ""}data'] = {
        'driver': 'local'
    }

    # Define db_worker service name (needed by listener/fan_sync depends_on)
    db_worker_service = 'db_worker'

    # Generate producer and consumer for each creator
    for creator in creators:
        creator_id = creator['id']
        creator_name = creator['name'].lower()  # Docker requires lowercase

        # Producer service
        producer_service = f'producer-{creator_name}'
        compose_dict['services'][producer_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.producer'
            },
            'container_name': f'{container_prefix}-producer-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'AUTH_FILE=/app/auth_multi.json',
                'DATABASE_URL=${DATABASE_URL}',
                'ENCRYPTION_KEY=${ENCRYPTION_KEY}',
                'GOLOGIN_API_TOKEN=${GOLOGIN_API_TOKEN}'
            ],
            'volumes': [
                './auth_multi.json:/app/auth_multi.json:ro',
                './conversations:/app/conversations:ro',
                './test_logs:/app/logs' if is_test else './logs:/app/logs',
                './test_checkpoints:/app/checkpoints' if is_test else './checkpoints:/app/checkpoints'
            ],
            'depends_on': [redis_name],
            'restart': 'no',  # Exit when done (don't restart)
            'networks': [network_name],
            'deploy': {
                'resources': {
                    'limits': {
                        'memory': '2G'  # Increased from 1G for 3 concurrent fans
                    },
                    'reservations': {
                        'memory': '512M'  # Increased from 256M
                    }
                }
            }
        }

        # Add test mode environment variables
        if is_test:
            compose_dict['services'][producer_service]['environment'].extend([
                'TEST_LIMIT=5',         # Only process 5 fans per creator
                'MESSAGE_LIMIT=10',
                'CONCURRENT_FANS=50',   # 10 fans at once
                'FAN_DELAY=5',          # 5s between batches
                'FETCH_TIMEOUT=600',    # 10 minute timeout
                'REAUTH_INTERVAL=100'   # Reauth every 100 fans
            ])
        else:
            # Production mode - add concurrent processing configuration
            compose_dict['services'][producer_service]['environment'].extend([
                'CONCURRENT_FANS=50',   # 10 fans at once
                'FAN_DELAY=5',          # 5s between batches
                'FETCH_TIMEOUT=600',    # 10 minute timeout
                'REAUTH_INTERVAL=100'   # Reauthenticate every 100 fans
            ])

        # WebSocket Listener service (24/7 real-time notifications + timewaster detection)
        listener_service = f'listener-{creator_name}'
        compose_dict['services'][listener_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.listener'
            },
            'container_name': f'{container_prefix}-listener-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                'DATABASE_URL=${DATABASE_URL}',
                'ENCRYPTION_KEY=${ENCRYPTION_KEY}',
                'GOLOGIN_API_TOKEN=${GOLOGIN_API_TOKEN}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'MIN_MESSAGE_AGE_HOURS=24',
                # Timewaster detection settings
                'TW_ENABLED=true',
                'TW_MAX_SPEND=50.0',
                'TW_MIN_MESSAGES=50',
                'TW_MAX_RPM=0.05',
                'TW_COLLECTION_NAME=time waster',
                'TW_DISPLAY_PREFIX=AI - Timewaster'
            ],
            'volumes': [
                './auth_multi.json:/app/auth_multi.json:ro',
                './test_logs:/app/logs' if is_test else './logs:/app/logs'
            ],
            'depends_on': [redis_name, db_worker_service],
            'restart': 'unless-stopped',  # Auto-restart on failure
            'networks': [network_name],
            'deploy': {
                'resources': {
                    'limits': {
                        'memory': '1G'  # Increased for Live API message fetching
                    },
                    'reservations': {
                        'memory': '256M'
                    }
                }
            }
        }

        # Fan Sync service (detect new subscribers every 4 hours)
        fan_sync_service = f'fan-sync-{creator_name}'
        compose_dict['services'][fan_sync_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.fan_sync'
            },
            'container_name': f'{container_prefix}-fan-sync-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                'DATABASE_URL=${DATABASE_URL}',
                'ENCRYPTION_KEY=${ENCRYPTION_KEY}',
                'GOLOGIN_API_TOKEN=${GOLOGIN_API_TOKEN}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'SYNC_INTERVAL=14400',  # 4 hours
                'MIN_MESSAGE_AGE_HOURS=24'
            ],
            'volumes': [
                './auth_multi.json:/app/auth_multi.json:ro',
                './conversations:/app/conversations:ro',
                './test_logs:/app/logs' if is_test else './logs:/app/logs'
            ],
            'depends_on': [redis_name, db_worker_service],
            'restart': 'unless-stopped',  # Auto-restart on failure
            'networks': [network_name],
            'deploy': {
                'resources': {
                    'limits': {
                        'memory': '512M'
                    },
                    'reservations': {
                        'memory': '128M'
                    }
                }
            }
        }

        # New Fan Processor service (priority queue for new fans detected by WebSocket)
        new_fan_processor_service = f'new-fan-processor-{creator_name}'
        compose_dict['services'][new_fan_processor_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.new_fan_processor'
            },
            'container_name': f'{container_prefix}-new-fan-processor-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                'DATABASE_URL=${DATABASE_URL}',
                'ENCRYPTION_KEY=${ENCRYPTION_KEY}',
                'GOLOGIN_API_TOKEN=${GOLOGIN_API_TOKEN}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'CONCURRENT_FANS=10',
                'MIN_MESSAGE_AGE_HOURS=24'
            ],
            'volumes': [
                './auth_multi.json:/app/auth_multi.json:ro',
                './test_checkpoints:/app/checkpoints' if is_test else './checkpoints:/app/checkpoints',
                './test_logs:/app/logs' if is_test else './logs:/app/logs'
            ],
            'depends_on': [redis_name, db_worker_service],
            'restart': 'unless-stopped',  # Auto-restart on failure
            'networks': [network_name],
            'deploy': {
                'resources': {
                    'limits': {
                        'memory': '1G'  # Similar to producer (needs to fetch full histories)
                    },
                    'reservations': {
                        'memory': '256M'
                    }
                }
            }
        }

        # Known Fan Processor service (queue for known fans - incremental fetch)
        known_fan_processor_service = f'known-fan-processor-{creator_name}'
        compose_dict['services'][known_fan_processor_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.known_fan_processor'
            },
            'container_name': f'{container_prefix}-known-fan-processor-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                'DATABASE_URL=${DATABASE_URL}',
                'ENCRYPTION_KEY=${ENCRYPTION_KEY}',
                'GOLOGIN_API_TOKEN=${GOLOGIN_API_TOKEN}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'CONCURRENT_FANS=10',
                'MIN_MESSAGE_AGE_HOURS=24'
            ],
            'volumes': [
                './auth_multi.json:/app/auth_multi.json:ro',
                './test_logs:/app/logs' if is_test else './logs:/app/logs'
            ],
            'depends_on': [redis_name, db_worker_service],
            'restart': 'unless-stopped',  # Auto-restart on failure
            'networks': [network_name],
            'deploy': {
                'resources': {
                    'limits': {
                        'memory': '512M'  # Lighter than new_fan_processor (incremental only)
                    },
                    'reservations': {
                        'memory': '128M'
                    }
                }
            }
        }

    # Database Worker service (multi-creator support)
    compose_dict['services'][db_worker_service] = {
        'build': {
            'context': '.',
            'dockerfile': 'Dockerfile.db_worker'
        },
        'container_name': f'{container_prefix}-db-worker',
        'environment': [
            'DATABASE_URL=${DATABASE_URL}',
            f'REDIS_HOST={redis_name}',
            f'REDIS_PORT={redis_internal_port}',
            'AUTH_FILE=/app/auth_multi.json',
            'BATCH_SIZE=50',
            'GC_INTERVAL=1000'
        ],
        'volumes': [
            './auth_multi.json:/app/auth_multi.json:ro',
            './test_output:/app/output' if is_test else './output:/app/output',
            './test_logs:/app/logs' if is_test else './logs:/app/logs'
        ],
        'depends_on': [redis_name],
        'restart': 'no',
        'networks': [network_name],
        'deploy': {
            'resources': {
                'limits': {
                    'memory': '1536M'
                },
                'reservations': {
                    'memory': '256M'
                }
            }
        }
    }

    # GoLogin Keep-Alive service (pings all profiles every 5 minutes)
    gologin_keepalive_service = 'gologin-keepalive'
    compose_dict['services'][gologin_keepalive_service] = {
        'build': {
            'context': '.',
            'dockerfile': 'Dockerfile.gologin_keepalive'
        },
        'container_name': f'{container_prefix}-gologin-keepalive',
        'environment': [
            'GOLOGIN_API_TOKEN=${GOLOGIN_API_TOKEN}',
            'DATABASE_URL=${DATABASE_URL}',
            'ENCRYPTION_KEY=${ENCRYPTION_KEY}',
            'PING_INTERVAL=300',  # 5 minutes
            'FAIL_ON_ERROR=true'  # Stop container on ping failure
        ],
        'volumes': [
            './test_logs:/app/logs' if is_test else './logs:/app/logs'
        ],
        'restart': 'no',  # Manual intervention on failure
        'networks': [network_name],
        'deploy': {
            'resources': {
                'limits': {
                    'memory': '256M'
                },
                'reservations': {
                    'memory': '64M'
                }
            }
        }
    }

    # Generate output filename
    if not output_file:
        if is_test:
            output_file = 'docker-compose.test.yml'
        else:
            output_file = 'docker-compose.generated.yml'

    # Write to file
    with open(output_file, 'w') as f:
        yaml.dump(compose_dict, f, default_flow_style=False, sort_keys=False)

    print(f"Generated {output_file}")
    print(f"  Redis: 1 container")
    print(f"  Creators: {len(creators)}")
    print(f"  Producers: {len(creators)} containers (initial data collection)")
    print(f"  Database Worker: 1 container (handles all creators)")
    print(f"  GoLogin Keep-Alive: 1 container (pings all profiles every 5 min)")
    print(f"  WebSocket Listeners: {len(creators)} containers (24/7 event detection + timewaster check)")
    print(f"  Fan Sync Workers: {len(creators)} containers (every 4 hours)")
    print(f"  New Fan Processors: {len(creators)} containers (full history fetch)")
    print(f"  Known Fan Processors: {len(creators)} containers (incremental fetch)")
    print(f"  Total: {3 + len(creators) * 5} containers")
    print(f"\nCreators: {', '.join([c['name'] for c in creators])}")
    print(f"\nUsage:")
    print(f"  Initial setup:  docker-compose -f {output_file} up redis gologin-keepalive producer-* db_worker --build -d")
    print(f"  Real-time mode: docker-compose -f {output_file} up redis gologin-keepalive db_worker listener-* fan-sync-* new-fan-processor-* known-fan-processor-* --build -d")

    return output_file


def main() -> None:
    """Main function to generate docker-compose file from command line."""
    import sys

    mode = 'production'
    auth_file = 'auth_multi.json'
    output_file = None

    if len(sys.argv) > 1:
        mode = sys.argv[1]  # 'production' or 'test'
    if len(sys.argv) > 2:
        auth_file = sys.argv[2]
    if len(sys.argv) > 3:
        output_file = sys.argv[3]

    print("=" * 60)
    print("Docker Compose Generator")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Auth file: {auth_file}")

    # Load creators
    try:
        creators = load_creators_from_auth(auth_file)
        print(f"Found {len(creators)} creator(s)\n")
    except Exception as e:
        print(f"✗ Error loading auth file: {e}")
        sys.exit(1)

    if not creators:
        print("✗ No creators found in auth file")
        sys.exit(1)

    # Generate compose file
    try:
        output = generate_docker_compose(creators, mode, output_file)
        print(f"\n✓ Success! Run with:")
        print(f"  docker-compose -f {output} up --build -d")
    except Exception as e:
        print(f"✗ Error generating compose file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
