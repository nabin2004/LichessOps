"""Unified Lichess MLOps portal — predict, monitoring, and service links."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import requests
import streamlit as st

st.set_page_config(page_title="Lichess MLOps Portal", layout="wide", page_icon="♟️")

SERVING_URL = os.environ.get("SERVING_URL", "http://localhost:8082").rstrip("/")
EVIDENTLY_URL = os.environ.get("EVIDENTLY_URL", "http://localhost:5001").rstrip("/")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")
MLFLOW_URL = os.environ.get("MLFLOW_URL", "http://localhost:5000").rstrip("/")
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
MINIO_CONSOLE_URL = os.environ.get("MINIO_CONSOLE_URL", "http://localhost:9001").rstrip("/")
SPARK_URL = os.environ.get("SPARK_URL", "http://localhost:8081").rstrip("/")
EVIDENTLY_STREAMLIT_URL = os.environ.get("EVIDENTLY_STREAMLIT_URL", "http://localhost:8501").rstrip("/")
REPORTS_DIR = Path(os.environ.get("EVIDENTLY_REPORTS_DIR", "services/evidently/reports"))

GRAFANA_SERVING_DASH = f"{GRAFANA_URL}/d/lichess-serving/lichess-serving"
GRAFANA_SYSTEM_DASH = f"{GRAFANA_URL}/d/lichess-system/lichess-system"


def localhost_url(url: str) -> str:
    """Replace hostname with 'localhost' while preserving scheme, port and path."""
    if not url:
        return url
    parsed = urlparse(url)
    # Keep the original port if present, otherwise use default for scheme
    netloc = parsed.netloc
    if ":" in netloc:
        host, port = netloc.split(":", 1)
        new_netloc = f"localhost:{port}"
    else:
        new_netloc = "localhost"
    return urlunparse(parsed._replace(netloc=new_netloc))


def probe(url: str, timeout: float = 3.0) -> bool:
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code < 500
    except requests.RequestException:
        return False


def api_post(base: str, endpoint: str, payload: dict, timeout: int = 300) -> tuple[bool, dict]:
    try:
        response = requests.post(f"{base}{endpoint}", json=payload, timeout=timeout)
        return response.ok, response.json() if response.content else {}
    except requests.RequestException as exc:
        return False, {"detail": str(exc)}


def show_report(report_name: str) -> None:
    report_path = REPORTS_DIR / report_name
    if not report_name.endswith(".html"):
        report_path = REPORTS_DIR / f"{report_name}.html"
    if report_path.is_file():
        st.components.v1.html(report_path.read_text(encoding="utf-8"), height=900, scrolling=True)
    else:
        st.link_button("Open report in browser", f"{EVIDENTLY_URL}/reports/{report_name}")


with st.sidebar:
    st.title("♟️ Lichess Portal")
    st.caption("Unified MLOps UI")
    serving_ok = probe(f"{SERVING_URL}/health")
    evidently_ok = probe(f"{EVIDENTLY_URL}/health")
    if serving_ok:
        st.success("Serving online")
    else:
        st.error("Serving offline")
    if evidently_ok:
        st.success("Evidently online")
    else:
        st.warning("Evidently offline")

tabs = st.tabs(["Predict", "Monitoring", "Services", "Grafana"])

with tabs[0]:
    st.header("Game outcome prediction")
    st.caption(f"Calls `{localhost_url(SERVING_URL)}/predict`")

    c1, c2 = st.columns(2)
    with c1:
        player_elo = st.number_input("Player Elo", min_value=400, max_value=3500, value=1800)
        player_color = st.selectbox("Player color", ["white", "black"])
        eco = st.text_input("ECO code", value="B20")
        opening_family = st.text_input("Opening family", value="Sicilian Defense")
        time_control = st.text_input("Time control", value="Blitz")
    with c2:
        opponent_elo = st.number_input("Opponent Elo", min_value=400, max_value=3500, value=1700)
        player_eco_score = st.slider("Player ECO score", 0.0, 1.0, 0.5)
        player_h2h_win_rate = st.slider("H2H win rate", 0.0, 1.0, 0.5)
        opening_population_win_rate = st.slider("Opening population win rate", 0.0, 1.0, 0.5)

    if st.button("Predict", type="primary"):
        payload = {
            "player_elo": player_elo,
            "opponent_elo": opponent_elo,
            "player_color": player_color,
            "eco": eco,
            "opening_family": opening_family or None,
            "time_control": time_control,
            "player_eco_score": player_eco_score,
            "player_h2h_win_rate": player_h2h_win_rate,
            "opening_population_win_rate": opening_population_win_rate,
        }
        try:
            response = requests.post(f"{SERVING_URL}/predict", json=payload, timeout=30)
            if response.ok:
                result = response.json()
                outcome = result.get("predicted_outcome", "?")
                label = {"1": "Win", "0": "Loss", "½": "Draw"}.get(outcome, outcome)
                st.success(f"Predicted outcome: **{label}** ({outcome})")
                probs = result.get("probabilities", {})
                if probs:
                    st.bar_chart(
                        {
                            "lose": probs.get("lose", 0),
                            "win": probs.get("win", 0),
                            "draw": probs.get("draw", 0),
                        }
                    )
                if result.get("recommended_opening_score") is not None:
                    st.metric("Recommended opening score", f"{result['recommended_opening_score']:.3f}")
            else:
                st.error(response.text)
        except requests.RequestException as exc:
            st.error(f"Request failed: {exc}")

with tabs[1]:
    st.header("Model monitoring")
    st.caption(f"Evidently API at `{localhost_url(EVIDENTLY_URL)}` — ColumnStore-backed drift and quality.")

    ref_month = st.text_input("Reference month (YYYY-MM)", value="2013-01", key="mon_ref")
    cur_month = st.text_input("Current month (YYYY-MM)", value="2013-01", key="mon_cur")
    sample_size = st.slider("Sample size", 100, 10_000, 5000, key="mon_sample")

    mon_c1, mon_c2, mon_c3 = st.columns(3)
    if mon_c1.button("Drift report", use_container_width=True):
        with st.spinner("Generating drift report…"):
            ok, payload = api_post(
                EVIDENTLY_URL,
                "/reports/drift",
                {
                    "data_source": "columnstore",
                    "reference_month": ref_month,
                    "current_month": cur_month,
                    "sample_size": sample_size,
                },
            )
        if ok:
            st.success(f"Report: {payload.get('report_name')}")
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Failed"))

    if mon_c2.button("Data quality", use_container_width=True):
        with st.spinner("Running data quality check…"):
            ok, payload = api_post(
                EVIDENTLY_URL,
                "/reports/data-quality",
                {
                    "data_source": "columnstore",
                    "reference_month": ref_month,
                    "current_month": cur_month,
                    "sample_size": sample_size,
                },
            )
        if ok:
            st.success(f"Report: {payload.get('report_name')}")
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Failed"))

    if mon_c3.button("Classification", use_container_width=True):
        with st.spinner("Computing classification metrics…"):
            ok, payload = api_post(
                EVIDENTLY_URL,
                "/reports/classification-performance",
                {
                    "data_source": "columnstore",
                    "reference_month": ref_month,
                    "current_month": cur_month,
                    "sample_size": sample_size,
                },
            )
        if ok:
            cols = st.columns(4)
            for col, key in zip(cols, ["accuracy", "precision", "recall", "f1"], strict=False):
                if key in payload:
                    col.metric(key.capitalize(), payload[key])
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Failed"))

with tabs[2]:
    st.header("Service links")
    services = [
        ("Lichess Portal", "http://localhost:8502", f"{SERVING_URL}/health".replace("/health", "")),
        ("Serving API", f"{SERVING_URL}/docs", f"{SERVING_URL}/health"),
        ("Serving health", f"{SERVING_URL}/health", f"{SERVING_URL}/health"),
        ("Grafana", GRAFANA_URL, f"{GRAFANA_URL}/api/health"),
        ("Grafana Serving dashboard", GRAFANA_SERVING_DASH, f"{GRAFANA_URL}/api/health"),
        ("Prometheus", f"{PROMETHEUS_URL}/targets", f"{PROMETHEUS_URL}/-/healthy"),
        ("MLflow", MLFLOW_URL, f"{MLFLOW_URL}/health"),
        ("Airflow", AIRFLOW_URL, f"{AIRFLOW_URL}/api/v2/monitor/health"),
        ("Evidently API", f"{EVIDENTLY_URL}/docs", f"{EVIDENTLY_URL}/health"),
        ("Evidently Streamlit", EVIDENTLY_STREAMLIT_URL, f"{EVIDENTLY_STREAMLIT_URL}/_stcore/health"),
        ("MinIO Console", MINIO_CONSOLE_URL, f"{MINIO_CONSOLE_URL}/"),
        ("Spark Master", SPARK_URL, SPARK_URL),
    ]

    for name, link, health_url in services:
        col_a, col_b = st.columns([3, 1])
        with col_a:
            display_link = localhost_url(link)
            st.markdown(f"**{name}** — [{display_link}]({display_link})")
        with col_b:
            if probe(health_url):
                st.success("UP")
            else:
                st.error("DOWN")

with tabs[3]:
    st.header("Grafana dashboards")
    st.caption("Metrics populate after predictions are served (pipeline seeds sample requests).")
    st.link_button(
        "Open Lichess Serving dashboard",
        localhost_url(GRAFANA_SERVING_DASH),
        use_container_width=True,
    )
    st.link_button(
        "Open Lichess System dashboard",
        localhost_url(GRAFANA_SYSTEM_DASH),
        use_container_width=True,
    )
    st.markdown(
        f"- **Serving metrics**: model loaded, request rate, latency — scraped from `{localhost_url(SERVING_URL)}/metrics`\n"
        f"- **System metrics**: CPU, memory, disk — from node-exporter\n"
        f"- **Prometheus targets**: [{localhost_url(PROMETHEUS_URL)}/targets]({localhost_url(PROMETHEUS_URL)}/targets)"
    )