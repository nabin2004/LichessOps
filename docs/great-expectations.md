# Great Expectations validation

This document describes how developers run and extend Great Expectations (GE) checks in the `lichess_data` pipeline.

## Overview

GE validation runs after extraction and preprocessing to verify basic data quality. The current implementation is intentionally minimal:

- Processed Parquet: column presence, non-null checks for key fields, row count > 0.
- Preprocessed features (full history): column presence, non-null checks for key derived fields, row count > 0.
- Preprocessed train/test: column presence, non-null checks for key derived fields, row count > 0.

The checks are defined and executed in the `lichess_data.validate` package. The current runner uses an **ephemeral** GE context (no checked-in context directory required).

## Where it lives

- Runner: `packages/lichess_data/src/lichess_data/validate/ge_runner.py`
- CLI entrypoint: `packages/lichess_data/src/lichess_data/cli.py`
- Default config: `packages/lichess_data/configs/default.yaml`

## CLI usage

Validate both stages for a month:

```bash
lichess-data validate-ge --month 2013-01 --stage all --strict
```

Processed only:

```bash
lichess-data validate-ge --month 2013-01 --stage processed --strict
```

Preprocessed only:

```bash
lichess-data validate-ge --month 2013-01 --stage preprocessed --strict
```

Features only (full history before split):

```bash
lichess-data validate-ge --month 2013-01 --stage features --strict
```

If you want to target a specific path:

```bash
lichess-data validate-ge --stage processed --input /path/to/2013-01.parquet
lichess-data validate-ge --stage preprocessed --input-dir /path/to/2013-01/
```

`--strict` exits with a non-zero status on validation failure (useful for CI and Airflow).

## What is validated

### Processed Parquet

The processed dataset should include the core fields emitted by the PGN parser:

- Columns required: `result`, `white`, `black`, `utc_date`, `utc_time`
- Each required column must be non-null
- Table row count must be at least 1

### Preprocessed features (full history)

The features dataset written by preprocessing (before Feast split) should include the minimum derived fields used in downstream models:

- Columns required: `result_label`, `white_elo`, `black_elo`, `utc_datetime`
- Each required column must be non-null
- Table row count must be at least 1

### Preprocessed train/test

The preprocessed dataset should include the minimum derived fields used in downstream models:

- Columns required: `result_label`, `white_elo`, `black_elo`, `utc_datetime`
- Each required column must be non-null
- Table row count must be at least 1

If a file is missing or a required column is absent, validation fails.

## Configuration

Defaults live in `packages/lichess_data/configs/default.yaml`:

```yaml
great_expectations: {}
```

The current implementation runs GE checks in-process and returns a JSON-like result object; you can extend required columns or add new expectations directly in `ge_runner.py`.

## Airflow integration

The monthly Airflow DAG runs GE checks after preprocessing (and after checksum validation). See:

- `services/airflow/dags/lichess_monthly_ingestion.py`

To skip GE checks in a trigger, set `run_validation=false` in the DAG params.

## Extending expectations

Add new expectations in `ge_runner.py` by expanding the `*_REQUIRED_COLUMNS` lists or adding new validators in `_apply_processed_expectations()` and `_apply_preprocessed_expectations()`.

If you introduce stricter checks (value bounds, allowed categories, null ratios), update this document to reflect the new rules and consider adding configuration knobs in `configs/default.yaml`.
