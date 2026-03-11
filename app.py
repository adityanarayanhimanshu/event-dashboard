import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import date

st.set_page_config(page_title="Intraday Strategy Tracker", layout="wide")
st.title("📊 Live Intraday Strategy Performance Tracker")
st.caption("Auto-refreshes every 5 min • Top strategies by win rate & PnL")

@st.cache_resource
def get_engine():
    return create_engine(st.secrets["NEON_URL"])

engine = get_engine()

# Top 5 strategies today
today = date.today()
daily_df = pd.read_sql(f"""
    SELECT * FROM strategy_performance 
    WHERE date = '{today}' 
    ORDER BY pnl DESC
""", engine)

st.subheader(f"🏆 Top 5 Strategies - Today ({today})")
if not daily_df.empty:
    st.dataframe(daily_df.head(5).style.highlight_max(axis=0, color="lightgreen"), use_container_width=True)
else:
    st.info("Strategy calculation will appear after 1:30 PM today")

# All-time top 5
all_time = pd.read_sql("""
    SELECT * FROM strategy_performance 
    ORDER BY pnl DESC LIMIT 5
""", engine)
st.subheader("🏆 All-time Top 5 Strategies")
st.dataframe(all_time, use_container_width=True)

# Latest signals (your original style)
st.subheader("📡 Latest Live Signals")
latest = pd.read_sql('SELECT * FROM events ORDER BY "Datetime" DESC LIMIT 10', engine)
st.dataframe(latest[['Datetime', 'Stock', 'Pred', 'Return', 'TargetHit']], use_container_width=True)

st.caption("✅ Updater is saving candles till 3:30 PM IST • Data grows automatically")