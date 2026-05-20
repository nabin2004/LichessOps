# Config loading

This project loads YAML configuration from the repository root and optionally merges in package-specific overrides using [`libs.shared.config_loader`](../libs/shared/config_loader.py).

## Quick start

```python
from libs.shared import load_config

cfg = load_config("lichess_data")
level = cfg.get("logging", {}).get("level", "INFO")
```

`component_name` must match the folder name under `packages/` (for example `lichess_data`, `lichess_serving`, `lichess_models`, `lichess_features`).

## `load_config(component_name, config_name="default")`

- **`component_name`:** Package directory name under `packages/`; used to resolve `packages/<component_name>/configs/<config_name>.yaml`.
- **`config_name`:** Base name of the YAML file (without `.yaml`). Default is `"default"`.
- **Returns:** A plain `dict` built from YAML. Types are whatever `yaml.safe_load` produces (nested dicts, lists, scalars).

### Merge order

1. **Global (base):** `{repo}/config/{config_name}.yaml`
2. **Component (overrides):** `{repo}/packages/{component_name}/configs/{config_name}.yaml`

Keys from the component file **deep-merge** into the global config: nested dicts are merged recursively; when a key conflicts at a leaf, the **component value wins**. Replacing a scalar or list in the component file replaces that key entirely (no deep merge into lists).

### Example: deep merge

Global `config/default.yaml`:

```yaml
training:
  epochs: 100
  optimizer: adam
```

Component `packages/lichess_data/configs/default.yaml`:

```yaml
training:
  lr: 0.01
```

Merged result for `load_config("lichess_data")`:

```python
{
    "training": {
        "epochs": 100,
        "optimizer": "adam",
        "lr": 0.01,
    }
}
```

## Config file locations

| Layer     | Path pattern |
|-----------|----------------|
| Global    | `config/<config_name>.yaml` |
| Component | `packages/<component_name>/configs/<config_name>.yaml` |

The repo root is detected by walking up from [`config_loader.py`](../libs/shared/config_loader.py) until a `pyproject.toml` is found.

## Missing or empty files

- If a path does not exist, that layer is skipped. You can have **only** global config, **only** component config, or both.
- An empty file or a YAML document that parses as `null` is treated as an empty dict `{}` for that layer.

## Errors

Invalid YAML raises **`ValueError`** with a message that includes the file path, for example: `Invalid YAML in /path/to/config/default.yaml: ...`.

## Debugging

At **`LOG_LEVEL=DEBUG`**, the loader logs which config paths were loaded or skipped (see [Logging and exceptions](./logging-and-exceptions.md)).

## See also

- [Logging and exceptions](./logging-and-exceptions.md)
- PyYAML [`safe_load`](https://pyyaml.org/wiki/PyYAMLDocumentation) (official documentation)
