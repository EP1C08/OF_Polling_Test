"""
Conversation Loader module
Loads fan conversations from JSON files instead of API calls
"""

import json
from pathlib import Path
from typing import List, Dict, Any


def load_conversations_from_json(creator_name: str, conversations_dir: str = 'conversations') -> List[Dict[str, Any]]:
    """
    Load conversations from JSON file

    Args:
        creator_name: Creator's name (matches the JSON filename)
        conversations_dir: Directory containing conversation JSON files

    Returns:
        List of user dictionaries with id, username, name, etc.
    """
    # Try to find the JSON file
    conversations_path = Path(conversations_dir)

    if not conversations_path.exists():
        raise FileNotFoundError(f"Conversations directory not found: {conversations_dir}")

    # Try different filename patterns
    possible_files = [
        conversations_path / f"conversations_{creator_name}.json",
        conversations_path / f"{creator_name}.json",
        conversations_path / f"conversations_{creator_name.lower()}.json",
    ]

    json_file = None
    for file in possible_files:
        if file.exists():
            json_file = file
            break

    if not json_file:
        raise FileNotFoundError(
            f"Conversation JSON not found for creator '{creator_name}' in {conversations_dir}"
        )

    # Load the JSON
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    users = data.get('users', [])
    total = data.get('total_conversations', len(users))

    print(f"✓ Loaded {len(users)} conversations from {json_file.name} (total: {total})")

    return users


def create_user_object_from_json(user_data: Dict[str, Any]):
    """
    Create a simple user object from JSON data
    This mimics the structure returned by get_chats()

    Args:
        user_data: Dictionary with user info from JSON

    Returns:
        Simple object with id, username, name attributes
    """
    class SimpleUser:
        def __init__(self, user_dict):
            self.id = user_dict['id']
            self.username = user_dict.get('username', f"u{self.id}")
            self.name = user_dict.get('name', self.username)

    return SimpleUser(user_data)
