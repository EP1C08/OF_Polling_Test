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
    log_path.mkdir(parents=True, exist_ok=True)

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

    # File handler with immediate flush
    file_handler = ImmediateFlushFileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('[%(levelname)s] %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('[%(levelname)s] %(message)s')
    console_handler.setFormatter(console_formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Ensure Python doesn't buffer output
    logger.propagate = False

    logger.info(f"Logging initialized: {log_file}")

    return logger
