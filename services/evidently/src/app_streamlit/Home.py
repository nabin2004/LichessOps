"""Streamlit UI for Evidently drift reports."""

from __future__ import annotations

import os
from pathlib import Path

import requests
import streamlit as st

st.set_page_config(page_title="Lichess Evidently", layout="wide")

host = os.environ.get("FASTAPI_HOST", "evidently-api")
port = os.environ.get("FASTAPI_PORT", "5000")
base_url = f"http://{host}:{port}"
reports_dir = Path(os.environ.get("EVIDENTLY_REPORTS_DIR", "/app/reports"))

st.title("Evidently drift reports")
st.caption(f"API: {base_url}")

col1, col2 = st.columns(2)
with col1:
    reference_path = st.text_input("Reference data path", value="reference.parquet")
with col2:
    current_path = st.text_input("Current data path", value="current.parquet")

if st.button("Generate drift report", type="primary"):
    with st.spinner("Running Evidently DataDriftPreset..."):
        response = requests.post(
            f"{base_url}/reports/drift",
            json={
                "reference_path": reference_path,
                "current_path": current_path,
            },
            timeout=300,
        )
    if response.ok:
        payload = response.json()
        st.success(
            f"Saved `{payload['report_name']}.html` "
            f"({payload['reference_rows']} ref / {payload['current_rows']} cur rows)"
        )
    else:
        st.error(response.text)

st.divider()
st.subheader("Saved reports")

try:
    listed = requests.get(f"{base_url}/reports", timeout=10).json().get("reports", [])
except requests.RequestException as exc:
    st.warning(f"Could not reach API: {exc}")
    listed = sorted(path.name for path in reports_dir.glob("*.html"))

if not listed:
    st.info(
        "No reports yet. Place `reference.parquet` and `current.parquet` under "
        "`services/evidently/data/`, then click **Generate drift report**."
    )
else:
    selected = st.selectbox("Report", listed)
    if selected:
        report_path = reports_dir / selected
        if report_path.is_file():
            st.components.v1.html(
                report_path.read_text(encoding="utf-8"),
                height=900,
                scrolling=True,
            )
        else:
            st.link_button("Open report", f"{base_url}/reports/{selected}")
