FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/lichess/project

COPY pyproject.toml uv.lock /opt/lichess/project/
COPY libs/ /opt/lichess/project/libs/
COPY packages/ /opt/lichess/project/packages/

RUN python -m pip install --no-cache-dir uv \
    && uv pip install --system --no-cache-dir \
        /opt/lichess/project/libs \
        /opt/lichess/project/packages/lichess_models \
        pymysql sqlalchemy \
    && printf 'pandas>=3.0.3\npyarrow>=24.0.0\n' > /tmp/uv-overrides.txt \
    && uv pip install --system --no-cache-dir --overrides /tmp/uv-overrides.txt mlflow==3.12.0 \
    && uv pip install --system --no-cache-dir \
        /opt/lichess/project/packages/lichess_serving

EXPOSE 8082

CMD ["lichess-serving", "--host", "0.0.0.0", "--port", "8082"]
