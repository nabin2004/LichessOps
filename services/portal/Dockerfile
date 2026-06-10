FROM python:3.12-slim-bookworm

ARG USER_ID=1000
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -g "${USER_ID}" appuser \
    && useradd -lm -u "${USER_ID}" -g "${USER_ID}" appuser

WORKDIR /app
COPY src/app.py /app/app.py
RUN pip install --no-cache-dir \
    "requests>=2.32,<3" \
    "streamlit>=1.40,<2" \
    && chown -R appuser:appuser /app

USER appuser
ENV STREAMLIT_SERVER_HEADLESS=true
EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0"]
