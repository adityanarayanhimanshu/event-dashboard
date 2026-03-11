import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime

st.set_page_config(page_title="Intraday Strategy Tracker", layout="wide")
st.title("📊 Intraday Strategy Performance Tracker")
st.caption("Auto-refreshes every 5 min • Top strategies by win rate & PnL • Updater runs till 3:30 PM IST")

@st.cache_resource
def get_engine():
    engine = create_engine(st.secrets["NEON_URL"])
    # Force create tables (safe - IF NOT EXISTS)
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
st.subheader("📝 Paper Trading Tracker")
try:
    trades = pd.read_sql("SELECT * FROM trades ORDER BY entry_time DESC", engine)
    st.dataframe(trades, use_container_width=True)
except:
    st.info("Paper trading tracker ready — first trade will create the table")

if st.button("Example: Enter Long on latest signal"):
    if not latest.empty:
        stock = latest.iloc[0]['Stock']
        prob = latest.iloc[0].get('Pred', 0.0)
        new_trade = pd.DataFrame([{"stock": stock, "prob": prob, "entry_time": datetime.now(), "status": "Open", "pnl": 0}])
        new_trade.to_sql('trades', engine, if_exists='append', index=False)
        st.success(f"Entered Long {stock}")

st.caption("✅ Your system is now live and updating automatically!")
