import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime
import joblib

st.set_page_config(page_title="Intraday Strategy Tracker", layout="wide")
st.title("📊 Intraday Strategy Performance Tracker")
st.caption("Auto-refreshes every 5 min • Top strategies by win rate & PnL • Updater runs till 3:30 PM IST")

@st.cache_resource
def get_engine():
    engine = create_engine(st.secrets["NEON_URL"])
    # Auto-create tables if missing
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

# Load model (must be uploaded to GitHub as intraday_quant_model.pkl)
@st.cache_resource
def load_model():
    return joblib.load("intraday_quant_model.pkl")

model = load_model()

# ====================== LATEST LIVE SIGNALS ======================
st.subheader("📡 Latest Live Signals")
latest = pd.read_sql('SELECT * FROM events ORDER BY "Datetime" DESC LIMIT 10', engine)
st.dataframe(latest[['Datetime', 'Stock', 'Pred', 'Return', 'TargetHit']], use_container_width=True)

# ====================== TOP 5 STRATEGIES ======================
st.subheader(f"🏆 Top 5 Strategies - Today ({date.today()})")
try:
    daily_df = pd.read_sql(f"SELECT * FROM strategy_performance WHERE date = '{date.today()}' ORDER BY pnl DESC", engine)
    if not daily_df.empty:
        st.dataframe(daily_df.head(5).style.highlight_max(axis=0, color="lightgreen"), use_container_width=True)
    else:
        st.info("Waiting for first calculation (after 1:30 PM)")
except:
    st.info("Waiting for first calculation...")

st.subheader("🏆 All-time Top 5 Strategies")
try:
    all_time = pd.read_sql("SELECT * FROM strategy_performance ORDER BY pnl DESC LIMIT 5", engine)
    st.dataframe(all_time, use_container_width=True)
except:
    st.info("No historical data yet")

# ====================== PAPER TRADING TRACKER ======================
st.subheader("📝 Paper Trading Tracker (Persistent)")
trades = pd.read_sql("SELECT * FROM trades ORDER BY entry_time DESC", engine)
st.dataframe(trades, use_container_width=True)

# Simple entry example (expand later)
if st.button("Example: Enter Long on latest signal"):
    if not latest.empty:
        stock = latest.iloc[0]['Stock']
        prob = latest.iloc[0]['Pred']
        new_trade = pd.DataFrame([{"stock": stock, "prob": prob, "entry_time": datetime.now(), "status": "Open", "pnl": 0}])
        new_trade.to_sql('trades', engine, if_exists='append', index=False)
        st.success(f"Entered Long {stock}")

st.caption("✅ Your updater is running till 3:30 PM IST • Data & strategies update automatically")
