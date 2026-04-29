"""
Shared logging setup for Phase 1 scripts.

Each script gets:
  - a file handler at logs/<script_name>.log (rotating daily by date suffix)
  - a console handler at INFO+

Usage:
    from logger import get_logger
    log = get_logger(__name__)
    log.info("downloaded %s", path)
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

from config import LOG_DIR

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger that writes to both stdout and a per-script log file."""
    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured (e.g. re-import in same process).
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(_FORMAT, datefmt=_DATEFMT)

    # Console
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File - one log per script per day
    script = name.split(".")[-1]
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = Path(LOG_DIR) / f"{script}.{today}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
