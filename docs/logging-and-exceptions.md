# Logging and exceptions

This project uses Python’s standard `logging` library with a small wrapper in `libs.shared.logger`, and a custom `LichessException` in `libs.shared.exception` for consistent error messages and structured logging.

## Quick start

1. At application entry (e.g. `main()` or a CLI/script), call **`setup_logging()`** once so file rotation and stderr output are configured.
2. In each module, get a logger with **`get_logger(__name__)`** and use `debug`, `info`, `warning`, `error`, etc.

```python
from libs.shared import get_logger, setup_logging

def main() -> None:
    setup_logging()
    logger = get_logger(__name__)
    logger.info("Starting job")


if __name__ == "__main__":
    main()
```

If you only import `get_logger`, **`setup_logging()` runs automatically** the first time `get_logger` is called, so library-style imports still work without an explicit startup call. Calling `setup_logging()` in `main()` is recommended for apps so logging is ready before other imports log.

## `setup_logging()`

- **Purpose:** Configures the **root** logger with two handlers, once per process (idempotent).
- **Handlers:**
  - **File:** `RotatingFileHandler` writing to `{LOG_DIR}/lichess.log`, 10 MB per file, 5 backups, UTF-8.
  - **Console:** `StreamHandler` to **stderr** (typical for Docker/Kubernetes and local development).
- **Format:** `[ asctime ] LEVEL logger_name:lineno - message`
- Safe to call multiple times; duplicate file handlers for the same path are not attached.

## `get_logger(name=None)`

- Returns a standard `logging.Logger` instance.
- If `name` is omitted, the logger name is `"lichess"`.
- **Convention:** pass **`__name__`** so log lines show the real module path.

```python
from libs.shared import get_logger

logger = get_logger(__name__)
logger.debug("Detailed diagnostics")
logger.info("Normal progress message")
logger.warning("Something odd happened")
logger.error("Operation failed", exc_info=True)  # include traceback when appropriate
```

## Environment variables

| Variable     | Default | Description |
|-------------|---------|-------------|
| `LOG_LEVEL` | `INFO`  | Logging level name, e.g. `DEBUG`, `INFO`, `WARNING`, `ERROR`. Invalid values fall back to `INFO`. |
| `LOG_DIR`   | `logs`  | Directory for `lichess.log` (created if missing). Tilde (`~`) is expanded. |

Example:

```bash
LOG_LEVEL=DEBUG LOG_DIR=/var/log/lichess uv run python main.py
```

Log files under `logs/` are gitignored; do not commit them.

## `LichessException`

Use this when you want to **re-raise** an exception with a stable, human-readable message that includes **file name**, **line number**, and the **original error**, while also emitting an **ERROR** log line with traceback (`exc_info`).

### When to use

- Inside an **`except`** block, after catching an exception you intend to wrap.
- Pass the caught exception (or a string message) as the first argument and **`sys`** as the second so traceback metadata comes from the active exception.

### API

```python
LichessException(error_message, error_details: types.ModuleType)
```

- **`error_message`:** Usually the caught exception instance `e`, or a string describing the failure.
- **`error_details`:** Pass **`sys`** so the wrapper reads `sys.exc_info()` for line and filename. If there is no traceback (for example, raised outside an `except` block), line and file are recorded as `"unknown"`.

### Recommended pattern

Always use **exception chaining** so the original traceback is preserved:

```python
import sys
from libs.shared import get_logger, LichessException

logger = get_logger(__name__)

try:
    risky_operation()
except Exception as e:
    logger.debug("Context before wrap: %s", e)
    raise LichessException(e, sys) from e
```

### What gets logged

Constructing `LichessException` calls `logger.error` on the shared module logger with `exc_info=True`, so the **current exception context** is logged when you are still inside the `except` block.

### String representation

`str(exception)` (and printed tracebacks) include:

`Error occurred in python script name [<path>] line number [<n>] error message [<message>]`

## Importing `LichessException`

The package re-exports symbols from `libs.shared` for convenience:

```python
from libs.shared import LichessException, get_logger, setup_logging
```

`LichessException` is loaded **lazily** when first accessed, which keeps `python -m libs.shared.exception` free of duplicate-import warnings. Prefer `from libs.shared import …` for clarity.

## Demo module

To see logging and `LichessException` end-to-end:

```bash
uv run python -m libs.shared.exception
```

This writes to stderr and to `{LOG_DIR}/lichess.log` and then raises `LichessException` (process exits with a non-zero status).

## See also

- Python logging how-to: [Logging HOWTO](https://docs.python.org/3/howto/logging.html)
