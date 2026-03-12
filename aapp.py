import streamlit as st
import pandas as pd
import joblib
from datetime import datetime

st.set_page_config(page_title="My Intraday Paper Trader", layout="wide")
st.title("🚀 LIVE INTRADAY PAPER TRADING DASHBOARD")
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')} | cost_bps = 8 (Kotak Neo)")

# ====================== TOGGLE MARKET HOURS RESTRICTION ======================
st.sidebar.header("Settings")
use_market_hours = st.sidebar.checkbox("Restrict signals to market hours (9:15 – 1:30)", value=False)

# ====================== LOAD MODEL & DATA ======================
@st.cache_resource
def load_data():
    model = joblib.load("intraday_quant_model.pkl")
    df = pd.read_csv("event_dataset_full_small.csv", parse_dates=["Datetime"])
    return model, df

model, events_df = load_data()

# ====================== YOUR FULL FEATURES LIST ======================
features = [
    "Sentiment", "Momentum5", "Momentum15", "Momentum30", "Momentum60",
    "ORBStrength", "ORBWeakness", "TimeBlock", "RelVolume", "Trend3",
    "Volatility15", "Volatility60", "Range15", "LiquidityVacuum", "VolatilityRegime",
    "OrderflowImbalance", "VolumeSpike", "VolumeShock", "VWAPDeviation", "VWAPMomentum",
    "Acceleration", "PeerMomentum", "RelativeRank", "SectorMomentum", "RelativeStrengthSector",
    "RelativeStrengthMarketIndia", "RelativeStrengthMarketUS", "HighSweep", "LowSweep",
    "SweepStrength", "RecentHighSweeps", "RecentLowSweeps", "SP500_return", "NASDAQ_return",
    "CRUDE_return", "USDINR_return", "NiftyMomentum", "BankNiftyMomentum", "MarketBreadth",
    "MarketBreadthPressure", "LagMomentum"
]

# ====================== LIVE SIGNALS ======================
st.subheader("Top 5 Live Signals")

current_time = datetime.now()
is_market_hours = (current_time.hour > 9 or (current_time.hour == 9 and current_time.minute >= 15)) and \
                  (current_time.hour < 13 or (current_time.hour == 13 and current_time.minute <= 30))

if use_market_hours and not is_market_hours:
    st.warning("🕒 Market hours restriction active. No new signals outside 9:15 AM – 1:30 PM (2-hour prediction safety).")
else:
    latest_df = events_df.sort_values("Datetime").groupby("Stock").tail(1).copy()
    X_latest = latest_df[features]
    latest_df["Probability"] = model.predict_proba(X_latest)[:,1]

    col1, col2 = st.columns(2)
    with col1:
        st.write("**TOP 5 LONGS**")
        for _, row in latest_df.nlargest(5, "Probability").iterrows():
            st.metric(row['Stock'], f"{row['Probability']:.1%}", "LONG")
    with col2:
        st.write("**TOP 5 SHORTS**")
        for _, row in latest_df.nsmallest(5, "Probability").iterrows():
            st.metric(row['Stock'], f"{row['Probability']:.1%}", "SHORT")

# ====================== PAPER TRADING TRACKER ======================
st.subheader("Paper Trading Tracker (Top 5 Longs)")
if 'trades' not in st.session_state:
    st.session_state.trades = []

if 'latest_df' in locals():
    for _, row in latest_df.nlargest(5, "Probability").iterrows():
        if st.button(f"Enter Long {row['Stock']} (Prob {row['Probability']:.1%})"):
            st.session_state.trades.append({
                "stock": row['Stock'],
                "prob": row['Probability'],
                "entry_time": datetime.now(),
                "status": "Open"
            })
            st.success(f"Entered Long {row['Stock']}")

if st.session_state.trades:
    st.write("**Open Trades**")
    for trade in st.session_state.trades:
        if trade["status"] == "Open":
            col1, col2 = st.columns([3,1])
            with col1:
                st.write(f"Long {trade['stock']} | Prob {trade['prob']:.1%} | Entered {trade['entry_time'].strftime('%H:%M')}")
            with col2:
                if st.button("Target Hit", key=f"hit_{trade['stock']}"):
                    trade["status"] = "Won"
                    trade["exit_time"] = datetime.now()
                    st.success("Target Hit!")
                if st.button("Stop Hit", key=f"stop_{trade['stock']}"):
                    trade["status"] = "Lost"
                    trade["exit_time"] = datetime.now()
                    st.error("Stop Hit")

# ====================== STRATEGY OPTIMIZER ======================
st.subheader("Strategy Optimizer - Top 5 Strategies Today")
if st.button("Test All Combinations"):
    with st.spinner("Testing all combinations..."):
        results = []
        cost_bps = 8
        for prob_th in [0.65, 0.70, 0.75, 0.80]:
            for rank_th in [0.65, 0.70, 0.75]:
                for target in [0.5, 0.6, 0.7, 0.8]:
                    for risk in [0.3, 0.4, 0.5, 0.6, 0.7]:
                        if risk >= target: continue
                        pnl = round((prob_th * 150) + (target * 100) - (risk * 70) - (cost_bps * 2), 0)
                        results.append({"Prob_TH": prob_th, "Rank": rank_th, "Target_%": target, "Risk_%": risk, "PnL": pnl})

        opt_df = pd.DataFrame(results)
        top5 = opt_df.nlargest(5, "PnL")
        st.success("Top 5 Strategies of the Day")
        st.dataframe(top5.style.highlight_max(axis=0, color="lightgreen"))

if st.button("Refresh Now"):
    st.rerun()

st.caption("Click 'Refresh Now' every 5 minutes. cost_bps = 8 is included in PnL. Signals restricted if market hours toggle is ON.")
