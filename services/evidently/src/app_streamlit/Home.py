"""Minimal Streamlit dashboard — extend to call evidently-api."""

import os

import streamlit as st

st.set_page_config(page_title="lichess evidently", layout="wide")
host = os.environ.get("FASTAPI_HOST", "evidently-api")
port = os.environ.get("FASTAPI_PORT", "5000")
st.title("Evidently (stub)")
st.caption(f"Backend: http://{host}:{port}")
