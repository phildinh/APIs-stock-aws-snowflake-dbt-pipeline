# ============================================================
# logger.py
# Purpose: Centralised logging for the entire pipeline
# Usage:   from utils.logger import get_logger
#          logger = get_logger(__name__)
#          logger.info("Starting ingestion")
# ============================================================

import logging
import os
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a logger that writes to both console and file.
    
    Args:
        name: Usually __name__ from the calling module
        
    Returns:
        Configured logger instance
    """

    # Get or create a logger with this name
    # If called twice with the same name, returns the existing one
    logger = logging.getLogger(name)

    # If this logger already has handlers attached, return it as-is
    # This prevents duplicate log lines if get_logger() is called multiple times
    if logger.handlers:
        return logger

    # Set the minimum level — DEBUG and above will be captured
    logger.setLevel(logging.DEBUG)

    # --------------------------------------------------------
    # Format: 2024-01-15 08:30:00 | INFO     | logger | Message
    # --------------------------------------------------------
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # --------------------------------------------------------
    # Handler 1 — Console
    # Shows INFO and above in your terminal during development
    # --------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # --------------------------------------------------------
    # Handler 2 — File
    # Writes DEBUG and above to logs/pipeline.log
    # 'a' means append — never overwrites previous runs
    # --------------------------------------------------------
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(
        filename=log_dir / "pipeline.log",
        mode="a",
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Attach both handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger