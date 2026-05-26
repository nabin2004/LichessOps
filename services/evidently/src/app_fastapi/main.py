"""Minimal FastAPI stub for Compose healthchecks — extend with Evidently reports as needed."""

from fastapi import FastAPI

app = FastAPI(title="lichess evidently stub")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "evidently-api-stub"}
