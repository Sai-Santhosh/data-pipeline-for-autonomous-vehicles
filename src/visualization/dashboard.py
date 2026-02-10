"""
Real-time Fleet Data Dashboard — Self-Driving metrics and insights.

Displays vehicle telemetry, alerts, interventions, miles-per-intervention,
and perception summaries. Aligned with fleet data visualization use cases.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium

from config import load_config
from src.metrics.queries import (
    latest_telemetry,
    alerts_summary,
    interventions_per_vehicle,
    miles_per_intervention,
    perception_summary,
)

# Ensure DB name exists (use postgres if fleet_data not created yet)
def _ensure_config():
    cfg = load_config()
    # Allow running against postgres if fleet_data not created
    return cfg

st.set_page_config(page_title="Fleet Data — Self-Driving Metrics", layout="wide")
cfg = _ensure_config()
refresh_sec = cfg.get("streamlit", {}).get("refresh_seconds", 5)

st.title("Fleet Data — Self-Driving Performance Dashboard")
st.caption("Real-time metrics from fleet telemetry, perception events, and driving events")

vehicle_options = ["All"] + [str(i) for i in range(1, 11)]
selected_vehicle = st.sidebar.selectbox("Vehicle", options=vehicle_options)
hours = st.sidebar.slider("Time window (hours)", 1, 168, 24)
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
if auto_refresh:
    st.sidebar.caption(f"Refreshing every {refresh_sec}s")

try:
    vehicle_id = None if selected_vehicle == "All" else int(selected_vehicle)
    telemetry = latest_telemetry(vehicle_id=vehicle_id)
    alerts = alerts_summary(vehicle_id=vehicle_id, limit=20)
    interventions = interventions_per_vehicle(vehicle_id=vehicle_id, hours=hours)
    mpi = miles_per_intervention(hours=hours)
    perception = perception_summary(hours=hours, vehicle_id=vehicle_id)
except Exception as e:
    st.error("Database connection failed. Ensure TimescaleDB is up and schema is applied. " + str(e))
    st.stop()

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Vehicles with data", len(telemetry) if not telemetry.empty else 0)
with col2:
    st.metric("Alerts (latest)", len(alerts) if not alerts.empty else 0)
with col3:
    total_interventions = int(interventions["event_count"].sum()) if not interventions.empty else 0
    st.metric("Interventions / disengagements", total_interventions)
with col4:
    if not mpi.empty and "km_per_intervention" in mpi.columns:
        avg_mpi = mpi["km_per_intervention"].dropna().mean()
        st.metric("Avg km per intervention", f"{avg_mpi:.1f}" if pd.notna(avg_mpi) else "—")
    else:
        st.metric("Avg km per intervention", "—")

# --- Telemetry: speed, battery, trip ---
st.subheader("Latest telemetry")
if not telemetry.empty:
    t = telemetry.iloc[0]
    c1, c2, c3 = st.columns(3)
    with c1:
        speed = float(t.get("current_speed_kmh", 0))
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=speed,
            title={"text": "Speed (km/h)"},
            gauge={"axis": {"range": [0, 120]}, "threshold": {"line": {"color": "red"}, "value": 65}},
        ))
        fig.update_layout(height=200, margin=dict(l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        battery = float(t.get("battery_level_pct", 0))
        color = "#4CAF50" if battery > 50 else "#FF9800" if battery > 20 else "#F44336"
        st.markdown(
            f'<div style="background:#eee;border-radius:8px;padding:12px;">'
            f'<strong>Battery</strong><br/><span style="font-size:24px;color:{color}">{battery:.1f}%</span></div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"**Trip**  \n📍 {t.get('start_location', '—')} → 🎯 {t.get('destination', '—')}"
        )
    # Map
    lat, lon = t.get("latitude"), t.get("longitude")
    if pd.notna(lat) and pd.notna(lon):
        m = folium.Map(location=[float(lat), float(lon)], zoom_start=12, tiles="OpenStreetMap")
        folium.Marker([float(lat), float(lon)], popup=f"Vehicle {t.get('vehicle_id', '?')}").add_to(m)
        st_folium(m, width=None, height=350)
else:
    st.info("No telemetry yet. Run the producer and consumer to stream data.")

# --- Alerts ---
st.subheader("Latest alerts")
if not alerts.empty:
    for _, row in alerts.head(10).iterrows():
        icon = "🚨" if "Speed" in str(row.get("alert_type", "")) else "⚠️" if "Collision" in str(row.get("alert_type", "")) else "🔋"
        st.markdown(
            f"{icon} **{row['alert_type']}** (V{row['vehicle_id']}) — {row['alert_message']}"
        )
else:
    st.info("No alerts.")

# --- Self-Driving metrics: interventions & miles per intervention ---
st.subheader("Self-Driving metrics (last %s h)" % hours)
col_a, col_b = st.columns(2)
with col_a:
    if not interventions.empty:
        fig = px.bar(interventions, x="vehicle_id", y="event_count", color="event_type", barmode="group", title="Events per vehicle")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("No driving events in window.")
with col_b:
    if not mpi.empty:
        st.dataframe(mpi.style.format({"km_driven": "{:.1f}", "km_per_intervention": "{:.1f}"}), use_container_width=True)
    else:
        st.write("No miles-per-intervention data.")

# --- Perception summary ---
st.subheader("Perception events (object classes)")
if not perception.empty:
    fig = px.bar(perception, x="object_class", y="detection_count", color="vehicle_id", barmode="stack", title="Detections by class")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No perception events in window.")

if auto_refresh:
    import time
    time.sleep(refresh_sec)
    st.rerun()
else:
    if st.sidebar.button("Refresh"):
        st.rerun()
