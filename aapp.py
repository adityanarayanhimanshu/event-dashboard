import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import joblib
import plotly.express as px

# ====================== FORCE RUN OUTSIDE MARKET HOURS ======================
FORCE_RUN = True  # ← Change to False when ready for real market hours

# ====================== IST DATE & TIME ======================
def get_ist_now():
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now

ist_now = get_ist_now()
ist_today = ist_now.date()

# ====================== BEAUTIFUL POWER BI / DARK THEME ======================
st.set_page_config(page_title="Intraday Quant Dashboard", layout="wide", page_icon="📈")
st.markdown("""
<style>
    .main {background-color: #0b0e16; color: #e0e0e0;}
    h1 {color: #00d4a8; font-size: 2.8rem; text-align: center;}
    .stMetric {background-color: #1a2233; border-radius: 10px; padding: 18px; border: 1px solid #00d4a8; box-shadow: 0 4px 12px rgba(0,0,0,0.3);}
    .stMetric label {color: #a0c0ff;}
    .stMetric .stMetricValue {color: #ffffff; font-size: 1.5rem;}
    .stButton>button {background-color: #00d4a8; color: black; border-radius: 8px; font-weight: bold; border: none;}
    .stButton>button:hover {background-color: #00b894;}
    .stSlider {color: #00d4a8;}
    .dataframe {background-color: #1a2233; color: #e0e0e0;}
    .dataframe th {background-color: #00d4a8; color: black;}
    .dataframe tr:nth-child(even) {background-color: #202b3d;}
</style>
""", unsafe_allow_html=True)

st.title("🚀 INTRADAY QUANT DASHBOARD")
st.caption(f"Auto-refreshes every 5 min • Auto exit + PnL • Using IST ({ist_today}) • Force run: {'ON' if FORCE_RUN else 'OFF'}")

# ====================== AUTOMATIC TABLE CREATION ======================
engine = create_engine(st.secrets["NEON_URL"])
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS strategy_performance (
            date DATE PRIMARY KEY, prob_th FLOAT, rank_th FLOAT, 
            target_pct FLOAT, risk_pct FLOAT, win_rate FLOAT, 
            total_trades INT, pnl FLOAT
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY, stock TEXT, prob FLOAT, 
            entry_time TIMESTAMP, exit_time TIMESTAMP, 
            status TEXT, pnl FLOAT
        );
    """))

# ====================== LOAD MODEL & DATA ======================
@st.cache_resource
def load_model():
    try:
        return joblib.load("intraday_quant_model.pkl")
    except:
        return None

model = load_model()
latest = pd.read_sql('SELECT * FROM events ORDER BY "Datetime" DESC LIMIT 50', engine)

# ====================== SIDEBAR FILTERS ======================
st.sidebar.header("🎛️ Trading Filters")
min_prob = st.sidebar.slider("Min Probability", 0.0, 1.0, 0.65, 0.01)
min_rank = st.sidebar.slider("Min Rank", 0.0, 1.0, 0.65, 0.01)
bps = st.sidebar.slider("Cost (bps)", 0, 20, 8)
target_pct = st.sidebar.slider("Target %", 0.1, 2.0, 0.5, 0.1)
risk_pct = st.sidebar.slider("Risk %", 0.1, 1.0, 0.3, 0.1)

# ====================== TABS ======================
tab1, tab2, tab3, tab4 = st.tabs(["📡 Live Signals", "🏆 Strategies", "📝 Paper Trading", "📈 Charts"])

with tab1:
    st.subheader("All Stocks - Descending by Pred")
    full_signals = latest.sort_values("Pred", ascending=False).reset_index(drop=True)
    st.dataframe(full_signals[['Datetime', 'Stock', 'Pred', 'Return', 'TargetHit']], width='stretch')

    col_long, col_short = st.columns(2)
    with col_long:
        st.subheader("Top 5 Longs (High Pred)")
        longs = full_signals.head(5)
        st.dataframe(longs[['Stock', 'Pred']], width='stretch')

    with col_short:
        st.subheader("Top 5 Shorts (Low Pred)")
        shorts = full_signals.tail(5)
        st.dataframe(shorts[['Stock', 'Pred']], width='stretch')

with tab2:
    st.subheader(f"🏆 Top 5 Strategies - Today (IST: {ist_today})")
    try:
        daily = pd.read_sql(f"SELECT * FROM strategy_performance WHERE date = '{ist_today}' ORDER BY pnl DESC", engine)
        if not daily.empty:
            st.dataframe(daily.head(5).style.highlight_max(axis=0, color="#00cc96"), width='stretch')
        else:
            st.info("No strategy data for today yet (updater runs after 1:30 PM IST)")
    except:
        st.info("Waiting for first calculation after 1:30 PM IST")

    st.subheader("🏆 All-time Top 5 Strategies")
    try:
        all_time = pd.read_sql("SELECT * FROM strategy_performance ORDER BY pnl DESC LIMIT 5", engine)
        st.dataframe(all_time, width='stretch')
    except:
        st.info("No historical data yet")

with tab3:
    st.subheader("📝 Paper Trading Tracker")
    trades = pd.read_sql("SELECT * FROM trades ORDER BY entry_time DESC", engine)
    st.dataframe(trades, width='stretch')

    if st.button("🔍 Scan & Enter Qualifying Longs"):
        candidates = latest[(latest['Pred'] >= min_prob)]
        for _, row in candidates.iterrows():
            new_trade = pd.DataFrame([{"stock": row['Stock'], "prob": row['Pred'], "entry_time": datetime.now(), "status": "Open", "pnl": 0}])
            new_trade.to_sql('trades', engine, if_exists='append', index=False)
        st.success(f"✅ Entered {len(candidates)} qualifying longs")

    open_trades = trades[trades['status'] == 'Open']
    for idx, trade in open_trades.iterrows():
        col1, col2, col3 = st.columns([3,1,1])
        with col1:
            st.write(f"**{trade['stock']}** | Prob {trade['prob']:.1%} | Entered {trade['entry_time'].strftime('%H:%M')}")
        with col2:
            if st.button("Exit Manual", key=f"manual_{idx}"):
                trades.loc[idx, 'status'] = "Exited Manual"
                trades.loc[idx, 'exit_time'] = datetime.now()
                trades.loc[idx, 'pnl'] = 0
                trades.to_sql('trades', engine, if_exists='replace', index=False)
                st.success("Manual exit saved")
        with col3:
            current = latest[latest['Stock'] == trade['stock']]
            if not current.empty:
                curr_return = current.iloc[0]['Return']
                if curr_return >= target_pct:
                    trades.loc[idx, 'status'] = "Target Hit"
                    trades.loc[idx, 'exit_time'] = datetime.now()
                    trades.loc[idx, 'pnl'] = round((curr_return * 1000) - (bps * 10), 2)
                    trades.to_sql('trades', engine, if_exists='replace', index=False)
                    st.success(f"🎯 Target Hit! PnL: {trades.loc[idx, 'pnl']}")
                elif curr_return <= -risk_pct:
                    trades.loc[idx, 'status'] = "Risk Hit"
                    trades.loc[idx, 'exit_time'] = datetime.now()
                    trades.loc[idx, 'pnl'] = round((curr_return * 1000) - (bps * 10), 2)
                    trades.to_sql('trades', engine, if_exists='replace', index=False)
                    st.error(f"🛑 Risk Hit! PnL: {trades.loc[idx, 'pnl']}")

with tab4:
    st.subheader("📈 Charts")
    if not latest.empty:
        fig = px.line(latest, x="Datetime", y="Pred", title="Model Prediction Trend", markers=True, color_discrete_sequence=["#00cc96"])
        st.plotly_chart(fig, width='stretch')

st.caption("✅ Dashboard fully loaded • All PnL saved automatically • Historical tracking ready")
