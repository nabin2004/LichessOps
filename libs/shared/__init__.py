from .logger import get_logger, setup_logging

__all__ = ["LichessException", "get_logger", "setup_logging"]


def __getattr__(name: str):
    if name == "LichessException":
        from .exception import LichessException

        return LichessException
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
