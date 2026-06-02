FROM apache/airflow:3.1.6

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow/project

COPY --chown=airflow:airflow pyproject.toml uv.lock /opt/airflow/project/
COPY --chown=airflow:airflow libs/ /opt/airflow/project/libs/
COPY --chown=airflow:airflow packages/ /opt/airflow/project/packages/

RUN python -m pip install --no-cache-dir uv \
    && uv pip install --no-cache-dir \
        /opt/airflow/project/libs \
        /opt/airflow/project/packages/lichess_data \
        /opt/airflow/project/packages/lichess_models \
    && uv pip install --no-cache-dir --no-deps feast==0.46.0 \
    && uv pip install --no-cache-dir --no-deps \
        /opt/airflow/project/packages/lichess_features \
    && uv pip install --no-cache-dir \
        "click>=7,<9" "colorama>=0.3.9" "dask>=2024.1.0" "dill~=0.3.0" "protobuf>=4.24.0" \
        "Jinja2>=2,<4" jsonschema mmh3 "pydantic>=2" "pygments>=2.12,<3" \
        "PyYAML>=5.4,<7" requests "SQLAlchemy>1" "tabulate>=0.8,<1" \
        "tenacity>=7,<9" "toml>=0.10,<1" "tqdm>=4,<5" "typeguard>=4" \
        "fastapi>=0.68" "uvicorn[standard]>=0.14,<1" gunicorn uvicorn-worker \
        prometheus_client psutil "bigtree>=0.19.2" pyjwt