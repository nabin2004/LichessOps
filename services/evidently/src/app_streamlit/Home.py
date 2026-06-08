"""Streamlit UI for Lichess Evidently monitoring dashboard."""
from __future__ import annotations

import json
import os
from pathlib import Path

import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Lichess MLOps Monitor", layout="wide", page_icon="♟️")

host = os.environ.get("FASTAPI_HOST", "evidently-api")
port = os.environ.get("FASTAPI_PORT", "5000")
BASE_URL = f"http://{host}:{port}"
REPORTS_DIR = Path(os.environ.get("EVIDENTLY_REPORTS_DIR", "/app/reports"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_post(endpoint: str, payload: dict, timeout: int = 300) -> tuple[bool, dict]:
    try:
        r = requests.post(f"{BASE_URL}{endpoint}", json=payload, timeout=timeout)
        return r.ok, r.json() if r.content else {}
    except requests.RequestException as e:
        return False, {"detail": str(e)}


def api_get(endpoint: str, timeout: int = 10) -> tuple[bool, dict]:
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", timeout=timeout)
        return r.ok, r.json() if r.content else {}
    except requests.RequestException as e:
        return False, {"detail": str(e)}


def show_report(report_name: str) -> None:
    """Render report inline if local file exists, else link to API."""
    report_path = REPORTS_DIR / report_name
    if not report_name.endswith(".html"):
        report_path = REPORTS_DIR / f"{report_name}.html"
    if report_path.is_file():
        st.components.v1.html(report_path.read_text(encoding="utf-8"), height=900, scrolling=True)
    else:
        st.link_button("Open report in browser", f"{BASE_URL}/reports/{report_name}")


def data_inputs(key_prefix: str) -> tuple[str, str]:
    c1, c2 = st.columns(2)
    ref = c1.text_input("Reference data path", value="reference.parquet", key=f"{key_prefix}_ref")
    cur = c2.text_input("Current data path", value="current.parquet", key=f"{key_prefix}_cur")
    return ref, cur


def classification_inputs(key_prefix: str) -> tuple[str, str, str, str]:
    ref, cur = data_inputs(key_prefix)
    c1, c2 = st.columns(2)
    target = c1.text_input("Target column", value="target", key=f"{key_prefix}_target")
    pred = c2.text_input("Prediction column", value="prediction", key=f"{key_prefix}_pred")
    return ref, cur, target, pred


# ---------------------------------------------------------------------------
# Sidebar – health + summary
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("♟️ Lichess MLOps")
    st.caption(f"API: `{BASE_URL}`")
    st.divider()

    ok, health = api_get("/health")
    if ok:
        st.success("API healthy")
    else:
        st.error(f"API unreachable — {health.get('detail', '')}")

    st.divider()
    st.subheader("Model health summary")
    if st.button("Refresh summary", use_container_width=True):
        ok, summary = api_get("/reports/summary")
        if ok:
            st.metric("Total reports", summary.get("total_reports", "—"))
            latest = summary.get("latest_report")
            if latest:
                st.caption(f"Latest: `{latest}`")
            pl = summary.get("prediction_log", {})
            if pl:
                st.metric("Predictions logged", pl.get("total_logged", 0))
                dist = pl.get("last_1000_distribution", {})
                if dist:
                    st.json(dist)
        else:
            st.warning(summary.get("detail", "Could not fetch summary"))

# ---------------------------------------------------------------------------
# Page tabs
# ---------------------------------------------------------------------------

tabs = st.tabs([
    "📊 Feature Drift",
    "🧹 Data Quality",
    "🎯 Target Drift",
    "🔮 Prediction Drift",
    "📈 Classification",
    "🔬 Slice Performance",
    "🛡️ Schema Validation",
    "📋 Prediction Logs",
    "🚨 Alerts",
    "📁 All Reports",
])

# ── 1. Feature Drift ────────────────────────────────────────────────────────
with tabs[0]:
    st.header("Feature Drift")
    st.caption("Detects distribution shift across all input features.")
    ref, cur = data_inputs("drift")
    sample = st.slider("Sample size", 100, 10_000, 5_000, key="drift_sample")
    if st.button("Generate drift report", type="primary", key="btn_drift"):
        with st.spinner("Running DataDriftPreset…"):
            ok, payload = api_post("/reports/drift", {
                "reference_path": ref, "current_path": cur, "sample_size": sample
            })
        if ok:
            st.success(
                f"Report `{payload['report_name']}` — "
                f"{payload['reference_rows']} ref / {payload['current_rows']} cur rows"
            )
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Error"))

# ── 2. Data Quality ─────────────────────────────────────────────────────────
with tabs[1]:
    st.header("Data Quality")
    st.caption("Missing values, duplicates, constant columns, dtype mismatches.")
    ref, cur = data_inputs("dq")
    if st.button("Run data quality check", type="primary", key="btn_dq"):
        with st.spinner("Checking data quality…"):
            ok, payload = api_post("/reports/data-quality", {
                "reference_path": ref, "current_path": cur
            })
        if ok:
            c1, c2, c3 = st.columns(3)
            c1.metric("Missing ratio", f"{payload['missing_ratio']:.2%}")
            c2.metric("Duplicate rows", payload["duplicate_rows"])
            c3.metric("Constant columns", len(payload["constant_columns"]))

            if payload["constant_columns"]:
                st.warning(f"Constant columns: {payload['constant_columns']}")
            if payload["schema_mismatches"]:
                st.warning(f"Dtype mismatches: {payload['schema_mismatches']}")
            else:
                st.success("No schema mismatches detected.")
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Error"))

# ── 3. Target Drift ──────────────────────────────────────────────────────────
with tabs[2]:
    st.header("Target Drift")
    st.caption("Monitors shift in the label distribution (win / loss / draw).")
    ref, cur, target, pred = classification_inputs("td")
    if st.button("Run target drift", type="primary", key="btn_td"):
        with st.spinner("Comparing target distributions…"):
            ok, payload = api_post("/reports/target-drift", {
                "reference_path": ref, "current_path": cur,
                "target_col": target, "prediction_col": pred,
            })
        if ok:
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Reference distribution")
                st.json(payload["reference_distribution"])
            with c2:
                st.subheader("Current distribution")
                st.json(payload["current_distribution"])
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Error"))

# ── 4. Prediction Drift ──────────────────────────────────────────────────────
with tabs[3]:
    st.header("Prediction Drift")
    st.caption("Tracks output distribution and confidence shifts over time.")
    ref, cur, target, pred = classification_inputs("pd")
    if st.button("Run prediction drift", type="primary", key="btn_pd"):
        with st.spinner("Comparing prediction distributions…"):
            ok, payload = api_post("/reports/prediction-drift", {
                "reference_path": ref, "current_path": cur,
                "target_col": target, "prediction_col": pred,
            })
        if ok:
            c1, c2 = st.columns(2)
            c1.metric("Reference mean prediction", payload["reference_mean_prediction"])
            c2.metric("Current mean prediction", payload["current_mean_prediction"])
            st.subheader("Distributions")
            cc1, cc2 = st.columns(2)
            with cc1:
                st.caption("Reference")
                st.json(payload["reference_distribution"])
            with cc2:
                st.caption("Current")
                st.json(payload["current_distribution"])
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Error"))

# ── 5. Classification Performance ───────────────────────────────────────────
with tabs[4]:
    st.header("Classification Performance")
    st.caption("Accuracy, precision, recall, F1 on current data vs reference.")
    ref, cur, target, pred = classification_inputs("cp")
    if st.button("Run classification report", type="primary", key="btn_cp"):
        with st.spinner("Computing classification metrics…"):
            ok, payload = api_post("/reports/classification-performance", {
                "reference_path": ref, "current_path": cur,
                "target_col": target, "prediction_col": pred,
            })
        if ok:
            cols = st.columns(4)
            for col, key in zip(cols, ["accuracy", "precision", "recall", "f1"]):
                val = payload.get(key)
                if val is not None:
                    col.metric(key.capitalize(), f"{val:.4f}")
            if "note" in payload:
                st.info(payload["note"])
            show_report(payload["report_name"])
        else:
            st.error(payload.get("detail", "Error"))

# ── 6. Slice Performance ─────────────────────────────────────────────────────
with tabs[5]:
    st.header("Slice Performance")
    st.caption("Per-segment accuracy/F1 breakdown — by game type, ELO range, etc.")
    data_path = st.text_input("Data path", value="current.parquet", key="slice_data")
    c1, c2 = st.columns(2)
    target_col = c1.text_input("Target column", value="target", key="slice_target")
    pred_col = c2.text_input("Prediction column", value="prediction", key="slice_pred")
    slice_cols_raw = st.text_input(
        "Slice columns (comma-separated)", value="game_type", key="slice_cols"
    )
    if st.button("Run slice analysis", type="primary", key="btn_slice"):
        slice_cols = [s.strip() for s in slice_cols_raw.split(",") if s.strip()]
        with st.spinner("Computing per-slice metrics…"):
            ok, payload = api_post("/reports/performance-slices", {
                "data_path": data_path,
                "target_col": target_col,
                "prediction_col": pred_col,
                "slice_cols": slice_cols,
            })
        if ok:
            for slice_col, groups in payload.get("slices", {}).items():
                st.subheader(f"Sliced by `{slice_col}`")
                rows = [{"value": k, **v} for k, v in groups.items()]
                st.dataframe(rows, use_container_width=True)
        else:
            st.error(payload.get("detail", "Error"))

# ── 7. Schema Validation ─────────────────────────────────────────────────────
with tabs[6]:
    st.header("Schema Validation")
    st.caption("Missing columns, new unseen categories, dtype mismatches.")
    ref, cur = data_inputs("schema")
    if st.button("Validate schema", type="primary", key="btn_schema"):
        with st.spinner("Validating…"):
            ok, payload = api_post("/reports/schema-validation", {
                "reference_path": ref, "current_path": cur
            })
        if ok:
            if payload["valid"]:
                st.success("✅ Schema is valid — no issues detected.")
            else:
                st.error("❌ Schema issues found.")

            if payload["missing_columns_in_current"]:
                st.warning(f"Missing in current: `{payload['missing_columns_in_current']}`")
            if payload["new_columns_in_current"]:
                st.info(f"New columns in current: `{payload['new_columns_in_current']}`")
            if payload["dtype_mismatches"]:
                st.warning("Dtype mismatches:")
                st.json(payload["dtype_mismatches"])
            if payload["new_unseen_categories"]:
                st.warning("New unseen categories:")
                st.json(payload["new_unseen_categories"])
        else:
            st.error(payload.get("detail", "Error"))

# ── 8. Prediction Logs ───────────────────────────────────────────────────────
with tabs[7]:
    st.header("Prediction Logs")
    st.caption("Log individual predictions and inspect the log.")

    with st.expander("Log a prediction"):
        lc1, lc2, lc3 = st.columns(3)
        player_elo = lc1.number_input("Player ELO", value=1500, key="log_pelo")
        opp_elo = lc2.number_input("Opponent ELO", value=1600, key="log_oelo")
        game_type = lc3.selectbox("Game type", ["blitz", "rapid", "bullet", "classical"], key="log_gt")
        pred_val = st.selectbox("Prediction", [0, 1, 2], format_func=lambda x: {0: "Loss", 1: "Win", 2: "Draw"}[x], key="log_pred")
        if st.button("Log prediction", key="btn_log"):
            ok, payload = api_post("/monitor/prediction-logs", {
                "player_elo": player_elo,
                "opponent_elo": opp_elo,
                "game_type": game_type,
                "prediction": pred_val,
            })
            if ok:
                st.success(f"Logged to `{payload['log_file']}`")
            else:
                st.error(payload.get("detail", "Error"))

    st.divider()
    limit = st.slider("Show last N entries", 10, 500, 100, key="log_limit")
    if st.button("Fetch logs", key="btn_fetch_logs"):
        ok, payload = api_get(f"/monitor/prediction-logs?limit={limit}")
        if ok:
            logs = payload.get("logs", [])
            st.caption(f"Total logged: {payload.get('total', '?')} | Showing: {len(logs)}")
            if logs:
                st.dataframe(logs, use_container_width=True)
            else:
                st.info("No logs yet.")
        else:
            st.error(payload.get("detail", "Error"))

# ── 9. Alerts ────────────────────────────────────────────────────────────────
with tabs[8]:
    st.header("Alert Evaluation")
    st.caption("Evaluate thresholds and surface severity-tagged alerts.")

    ac1, ac2, ac3 = st.columns(3)
    drift_score = ac1.number_input("Drift score", min_value=0.0, max_value=1.0, value=0.0, step=0.01, key="alert_drift")
    accuracy = ac2.number_input("Accuracy", min_value=0.0, max_value=1.0, value=0.0, step=0.01, key="alert_acc")
    missing_ratio = ac3.number_input("Missing ratio", min_value=0.0, max_value=1.0, value=0.0, step=0.01, key="alert_missing")

    with st.expander("Custom thresholds"):
        tc1, tc2, tc3 = st.columns(3)
        drift_thresh = tc1.number_input("Drift threshold", value=0.5, step=0.05, key="thresh_drift")
        acc_thresh = tc2.number_input("Accuracy threshold", value=0.65, step=0.05, key="thresh_acc")
        missing_thresh = tc3.number_input("Missing threshold", value=0.05, step=0.01, key="thresh_missing")

    if st.button("Evaluate alerts", type="primary", key="btn_alerts"):
        ok, payload = api_post("/alerts/evaluate", {
            "drift_score": drift_score,
            "accuracy": accuracy,
            "missing_ratio": missing_ratio,
            "drift_threshold": drift_thresh,
            "accuracy_threshold": acc_thresh,
            "missing_threshold": missing_thresh,
        })
        if ok:
            alerts = payload.get("alerts", [])
            count = payload.get("alert_count", 0)
            if count == 0:
                st.success("✅ No alerts — all metrics within thresholds.")
            else:
                st.error(f"🚨 {count} alert(s) triggered")
                for alert in alerts:
                    severity = alert["severity"]
                    icon = "🔴" if severity == "high" else "🟡"
                    st.warning(f"{icon} **{alert['type'].upper()}** ({severity}): {alert['message']}")
        else:
            st.error(payload.get("detail", "Error"))

# ── 10. All Reports ──────────────────────────────────────────────────────────
with tabs[9]:
    st.header("All Saved Reports")

    ok, data = api_get("/reports")
    if not ok:
        listed = sorted(p.name for p in REPORTS_DIR.glob("*.html") if p.is_file())
        st.caption("(Falling back to local file scan — API unreachable)")
    else:
        listed = data.get("reports", [])

    if not listed:
        st.info(
            "No reports yet. Run any report from the tabs above.\n\n"
            "Place `reference.parquet` and `current.parquet` under `services/evidently/data/`."
        )
    else:
        selected = st.selectbox("Select report", listed, key="report_select")
        if selected:
            st.caption(f"Viewing: `{selected}`")
            show_report(selected)
            st.link_button(
                "⬇️ Download",
                f"{BASE_URL}/reports/{selected}/download",
                use_container_width=False,
            )