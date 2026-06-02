from .artifact_manager import get_artifact_path, get_run_dir
from .config_loader import load_config
from .logger import get_logger, setup_logging
from .s3 import (
    download_file,
    is_minio_backend,
    object_exists,
    processed_bucket_name,
    raw_bucket_name,
    s3_client,
    s3_endpoint,
    s3_uri,
    storage_backend,
    upload_file,
)

__all__ = [
    "LichessException",
    "download_file",
    "get_artifact_path",
    "get_logger",
    "get_run_dir",
    "is_minio_backend",
    "load_config",
    "object_exists",
    "processed_bucket_name",
    "raw_bucket_name",
    "s3_client",
    "s3_endpoint",
    "s3_uri",
    "setup_logging",
    "storage_backend",
    "upload_file",
]


def __getattr__(name: str):
    if name == "LichessException":
        from .exception import LichessException

        return LichessException
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

