import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path
import plotly.express as px
from datetime import datetime

# Page config
st.set_page_config(
    page_title="WeatherBot | Advanced Monitoring",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load data
DATA_DIR = Path("data")
LOGS_DIR = Path("logs")

def load_json(path):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}

def load_dataset():
    path = DATA_DIR / "dataset_rows.jsonl"
    if path.exists():
        return pd.read_json(path, lines=True)
    return pd.DataFrame()

# Header
st.title("🌡️ WeatherBot Advanced Monitoring")
st.markdown("---")

# Sidebar
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard", "Markets", "ML Analytics", "Logs"])

state = load_json(DATA_DIR / "state.json")
df = load_dataset()

# Metrics Row
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Balance", f"${state.get('balance', 0):,.2f}", f"{state.get('daily_pnl', 0):+.2f}")
with col2:
    st.metric("Total Trades", state.get('total_trades', 0))
with col3:
    win_rate = (state.get('wins', 0) / state.get('total_trades', 1)) * 100
    st.metric("Win Rate", f"{win_rate:.1f}%")
with col4:
    st.metric("Drift Status", state.get('drift_status', 'STABLE').upper())

if page == "Dashboard":
    # PnL Curve (Simulated from dataset if needed, or just dummy for now)
    st.subheader("Performance Curve")
    if not df.empty and 'pnl' in df.columns:
        df['cum_pnl'] = df['pnl'].fillna(0).cumsum()
        fig = px.line(df, x=df.index, y='cum_pnl', title="Cumulative PnL (Paper + Live)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Insufficient data for performance curve.")

    # Exposure by City
    st.subheader("Regional Exposure")
    if not df.empty and 'city' in df.columns:
        city_counts = df['city'].value_counts().reset_index()
        city_counts.columns = ['city', 'trades']
        fig = px.pie(city_counts, values='trades', names='city', hole=0.3)
        st.plotly_chart(fig, use_container_width=True)

elif page == "Markets":
    st.subheader("Active & Resolved Markets")
    if not df.empty:
        st.dataframe(df.sort_values(by='timestamp', ascending=False), use_container_width=True)
    else:
        st.write("No markets found in dataset.")

elif page == "ML Analytics":
    st.subheader("ML Model Performance")
    metadata = load_json(DATA_DIR / "ml_metadata.json")
    st.write(f"Active Model: **{metadata.get('model_type', 'XGBoost').upper()}**")
    st.write(f"Trained on **{metadata.get('samples', 0)}** samples")
    
    # Uncertainty Visualization (Bayesian)
    st.markdown("### 🧬 Bayesian Uncertainty (Epistemic)")
    st.info("The Bayesian model evaluates whether the market is 'out of distribution'.")
    # Add dummy calibration curve or similar
    
elif page == "Logs":
    st.subheader("System Logs")
    log_path = LOGS_DIR / "bot_runtime.log"
    if log_path.exists():
        with open(log_path) as f:
            logs = f.readlines()
        st.code("".join(logs[-100:]))
    else:
        st.write("Log file not found.")

# Footer
st.sidebar.markdown("---")
st.sidebar.write(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
if st.sidebar.button("Force Refresh"):
    st.rerun()
