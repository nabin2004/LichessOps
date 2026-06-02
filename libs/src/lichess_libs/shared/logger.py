"""Central logging configuration for the Lichess project."""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path

_LOG_FILE_NAME = "lichess.log"
_INITIALIZED = False


def _resolve_level() -> int:
    name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, name, None)
    if isinstance(level, int):
        return level
    return logging.INFO


def setup_logging() -> None:
    """Configure root logging once: rotating file + stderr. Safe to call repeatedly."""
    global _INITIALIZED
    if _INITIALIZED:
        return

    root = logging.getLogger()
    log_dir = Path(os.getenv("LOG_DIR", "logs")).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = (log_dir / _LOG_FILE_NAME).resolve()

    resolved_target = log_file
    for h in root.handlers:
        if isinstance(h, logging.handlers.RotatingFileHandler):
            existing = getattr(h, "baseFilename", None)
            if existing and Path(existing).resolve() == resolved_target:
                _INITIALIZED = True
                return

    level = _resolve_level()
    root.setLevel(level)
    formatter = logging.Formatter(
        "[ %(asctime)s ] %(levelname)s %(name)s:%(lineno)d - %(message)s"
    )

    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10_485_760,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(console_handler)
    _INITIALIZED = True


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a named logger; ensures ``setup_logging`` has run at least once."""
    setup_logging()
    return logging.getLogger(name or "lichess")

