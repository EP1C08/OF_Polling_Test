"""
Logging utility for producers and consumers
"""

import logging
import sys
from pathlib import Path


def setup_logger(creator_name: str, log_type: str = "producer", log_dir: str = "logs"):
    """
    Setup logging to file and console with incremental numbering

    Args:
        creator_name: Creator name for folder organization
        log_type: "producer" or "consumer"
        log_dir: Base log directory

    Returns:
        Configured logger instance
    """
    log_path = Path(log_dir) / creator_name

    # Ensure log directory exists with proper error handling
    try:
        log_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"WARNING: Could not create log directory {log_path}: {e}")
        print(f"Attempting to create base log directory only...")
        try:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e2:
            print(f"ERROR: Could not create base log directory: {e2}")
            print(f"Logs will only be sent to console")

    # Use single log file that keeps appending
    log_file = log_path / f"{log_type}.log"

    # Create logger
    logger = logging.getLogger(f"{log_type}_{creator_name}")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Custom file handler that flushes immediately for real-time logging
    class ImmediateFlushFileHandler(logging.FileHandler):
        def emit(self, record):
            super().emit(record)
            self.flush()

    # File handler with immediate flush (only if directory was created successfully)
    file_handler = None
    if log_path.exists():
        try:
            file_handler = ImmediateFlushFileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
        except Exception as e:
            print(f"WARNING: Could not create file handler for {log_file}: {e}")
            file_handler = None

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_formatter)

    # Add handlers
    if file_handler:
        logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Ensure Python doesn't buffer output
    logger.propagate = False

    if file_handler:
        logger.info(f"Logging initialized: {log_file}")
    else:
        logger.warning(f"File logging disabled, using console only (could not create {log_file})")

    return logger
