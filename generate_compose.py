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

    # Generate producer and consumer for each creator
    for creator in creators:
        creator_id = creator['id']
        creator_name = creator['name'].lower()  # Docker requires lowercase

        # Producer service
        producer_service = f'producer-{creator_name}'
        compose_dict['services'][producer_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.test-producer' if is_test else 'Dockerfile.producer'
            },
            'container_name': f'{container_prefix}-producer-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'AUTH_FILE=/app/auth_multi.json'
            ],
            'volumes': [
                './auth_multi.json:/app/auth_multi.json:ro',
                './conversations:/app/conversations:ro',
                './logs:/app/logs',
                './checkpoints:/app/checkpoints'
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
                'TEST_LIMIT=3',
                'MESSAGE_LIMIT=10',
                'CONCURRENT_FANS=3',    # 3 fans at once
                'FAN_DELAY=5',          # 5s between batches
                'FETCH_TIMEOUT=600',    # 10 minute timeout
                'REAUTH_INTERVAL=100'   # Reauth every 100 fans
            ])
        else:
            # Production mode - add concurrent processing configuration
            compose_dict['services'][producer_service]['environment'].extend([
                'CONCURRENT_FANS=3',    # 3 fans at once
                'FAN_DELAY=5',          # 5s between batches
                'FETCH_TIMEOUT=600',    # 10 minute timeout
                'REAUTH_INTERVAL=100'   # Reauthenticate every 100 fans
            ])

        # Consumer service
        consumer_service = f'consumer-{creator_name}'
        output_path = './test_output:/app/output' if is_test else './output:/app/output'

        compose_dict['services'][consumer_service] = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile.consumer'
            },
            'container_name': f'{container_prefix}-consumer-{creator_name}',
            'environment': [
                f'CREATOR_ID={creator_id}',
                f'CREATOR_NAME={creator["name"]}',
                f'REDIS_HOST={redis_name}',
                f'REDIS_PORT={redis_internal_port}',
                'OUTPUT_DIR=/app/output'
            ],
            'volumes': [output_path, './logs:/app/logs'],
            'depends_on': [redis_name],
            'restart': 'no',  # Exit when done (don't restart)
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

    # Generate output filename
    if not output_file:
        if is_test:
            output_file = 'docker-compose.test.yml'
        else:
            output_file = 'docker-compose.generated.yml'

    # Write to file
    with open(output_file, 'w') as f:
        yaml.dump(compose_dict, f, default_flow_style=False, sort_keys=False)

    print(f"✓ Generated {output_file}")
    print(f"  Redis: 1 container")
    print(f"  Creators: {len(creators)}")
    print(f"  Producers: {len(creators)} containers")
    print(f"  Consumers: {len(creators)} containers")
    print(f"  Total: {1 + len(creators) * 2} containers")
    print(f"\nCreators: {', '.join([c['name'] for c in creators])}")

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
