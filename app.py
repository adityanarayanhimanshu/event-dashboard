import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

st.set_page_config(page_title="Intraday Strategy Tracker", layout="wide")
st.title("📊 Intraday Strategy Performance Tracker")
st.caption("Auto-refreshes every 5 min • Top strategies by win rate & PnL")

@st.cache_resource
def get_engine():
    engine = create_engine(st.secrets["NEON_URL"])
    # Auto-create tables if they don't exist
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS strategy_performance (
                date DATE PRIMARY KEY,
                prob_th FLOAT,
                rank_th FLOAT,
                target_pct FLOAT,
                risk_pct FLOAT,
                win_rate FLOAT,
                total_trades INT,
                pnl FLOAT
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                stock TEXT,
                prob FLOAT,
                entry_time TIMESTAMP,
                exit_time TIMESTAMP,
                status TEXT,
                pnl FLOAT
            );
        """))
    return engine

engine = get_engine()

# Top 5 today
st.subheader(f"🏆 Top 5 Strategies - Today ({date.today()})")
try:
    today = date.today()
    daily_df = pd.read_sql(f"""
        SELECT * FROM strategy_performance 
        WHERE date = '{today}' 
        ORDER BY pnl DESC
    """, engine)
    if not daily_df.empty:
        st.dataframe(daily_df.head(5).style.highlight_max(axis=0, color="lightgreen"), use_container_width=True)
    else:
        st.info("Strategy performance for today will appear after 1:30 PM")
except:
    st.info("Waiting for first calculation...")

# All-time top 5
st.subheader("🏆 All-time Top 5 Strategies")
try:
    all_time = pd.read_sql("SELECT * FROM strategy_performance ORDER BY pnl DESC LIMIT 5", engine)
    st.dataframe(all_time, use_container_width=True)
except:
    st.info("No historical data yet")

# Latest signals
st.subheader("📡 Latest Live Signals")
try:
    latest = pd.read_sql('SELECT "Datetime", "Stock", "Pred", "Return", "TargetHit" FROM events ORDER BY "Datetime" DESC LIMIT 10', engine)
    st.dataframe(latest, use_container_width=True)
except:
    st.info("No events data yet")

st.caption("✅ Updater is saving candles till 3:30 PM IST")
