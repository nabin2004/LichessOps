# lichess-libs

Shared helpers for the Lichess monorepo.

## Import namespace

- Distribution name: `lichess-libs`
- Python imports: `lichess_libs.*`

Most packages import utilities from `lichess_libs.shared`, for example:

```python
from lichess_libs.shared import get_artifact_path, get_logger, load_config
```

## What’s inside

- `lichess_libs.shared.config_loader`: load and deep-merge YAML config (`config/` + `packages/<component>/configs/`)
- `lichess_libs.shared.artifact_manager`: safe artifact paths and run directories (under `artifacts/` or `ARTIFACT_DIR`)
- `lichess_libs.shared.s3`: MinIO/S3 helpers for upload/download
- `lichess_libs.shared.storage_config`: config-derived storage paths and S3 URI helpers
- `lichess_libs.shared.logger`: consistent log formatting and rotation
- `lichess_libs.shared.exception`: `LichessException` wrapper with file/line context
