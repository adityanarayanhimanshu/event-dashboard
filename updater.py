import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import sys
import time
import glob

# ==================== TEST MODE (keep True for now) ====================
TEST_MODE = False   # Change to False when ready for real hours
# =================================================================

print("🚀 Starting Intraday Updater (GitHub Actions)")

# ====================== CONNECTION ======================
CONNECTION_STRING = os.getenv("NEON_URL")
if not CONNECTION_STRING:
    print("❌ ERROR: NEON_URL secret missing!")
    sys.exit(1)

engine = create_engine(CONNECTION_STRING)
print("✅ Connected to Neon")

# ====================== IST TIME ======================
def get_ist_now():
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now

ist_now = get_ist_now()
print(f"🕒 IST: {ist_now.strftime('%Y-%m-%d %H:%M:%S')}")

weekday = ist_now.weekday()
ist_hour = ist_now.hour
ist_minute = ist_now.minute

is_market_open = TEST_MODE or (
    weekday < 5 and 
    (9 <= ist_hour < 15 or (ist_hour == 15 and ist_minute <= 30))
)

print(f"Market Open: {'YES (TEST MODE)' if TEST_MODE else 'YES' if is_market_open else 'NO'}")

if not is_market_open:
    print("Skipping - outside market hours")
    sys.exit(0)

print("Proceeding with update...")

# ====================== YOUR REAL DATA IMPORT & PROCESSING ======================
# (copied & adapted from your notebook)

DATA_PATH = "data/stocks"
os.makedirs(DATA_PATH, exist_ok=True)

# Your stocks dict (from notebook)
stocks = {
    "RELIANCE": 2885,
    "TCS": 11536,
    "HDFCBANK": 1333,
    # ... add all your stocks here (I shortened for message, paste your full dict)
}

new_frames = []

for stock, scrip in stocks.items():
    try:
        file = f"{DATA_PATH}/{stock}.parquet"
        start_date = "2025-01-01"  # adjust if needed

        if os.path.exists(file):
            old = pd.read_parquet(file)
            last_time = pd.to_datetime(old["Datetime"]).max()
            start_date = (last_time + timedelta(minutes=5)).strftime("%Y-%m-%d")
            print(f"{stock} updating from {start_date}")

        # Fetch from 5paisa (your real API call - replace with your client)
        data = client.historical_data(  # ← your 5paisa client here
            Exch="N", ExchangeSegment="C", ScripCode=scrip,
            time="5m", From=start_date, To=date.today().strftime("%Y-%m-%d")
        )

        if data is None or len(data) == 0:
            print(f"No new data for {stock}")
            continue

        data = pd.DataFrame(data)
        data["Stock"] = stock
        data["Datetime"] = pd.to_datetime(data["Datetime"])

        # Save locally (your function)
        if os.path.exists(file):
            old = pd.read_parquet(file)
            data = pd.concat([old, data]).drop_duplicates(subset=["Datetime"])
        data.to_parquet(file, index=False)
        print(f"{stock} saved locally")

        new_frames.append(data)

    except Exception as e:
        print(f"Error fetching {stock}: {e}")

if not new_frames:
    print("No new data from any stock. Skipping.")
    sys.exit(0)

df_new = pd.concat(new_frames, ignore_index=True)

# Append to Neon events table
df_new.to_sql('events', engine, if_exists='append', index=False, method='multi', chunksize=5000)
print(f"✅ Added {len(df_new)} new rows to events table")

# ====================== DAILY STRATEGY CALCULATION ======================
today = ist_now.date()
print(f"Checking strategy performance for {today}...")

already_done = pd.read_sql(f"SELECT 1 FROM strategy_performance WHERE date = '{today}' LIMIT 1", engine)

if len(already_done) == 0:
    print("Calculating today's performance...")
    df_today = pd.read_sql(f"SELECT * FROM events WHERE DATE(\"Datetime\") = '{today}'", engine)
    
    if not df_today.empty:
        results = []
        for prob_th in [0.65, 0.70, 0.75, 0.80]:
            for rank_th in [0.65, 0.70, 0.75]:
                for target in [0.5, 0.6, 0.7, 0.8]:
                    for risk in [0.3, 0.4, 0.5, 0.6, 0.7]:
                        if risk >= target: continue
                        signals = df_today[df_today['Pred'] >= prob_th]
                        win_rate = (signals['Return'] > 0).mean() if len(signals) > 0 else 0
                        pnl = round(signals['Return'].sum() * 1000, 0) if len(signals) > 0 else 0
                        
                        results.append({
                            "date": today,
                            "prob_th": prob_th,
                            "rank_th": rank_th,
                            "target_pct": target,
                            "risk_pct": risk,
                            "win_rate": win_rate,
                            "total_trades": len(signals),
                            "pnl": pnl
                        })
        
        pd.DataFrame(results).to_sql('strategy_performance', engine, if_exists='append', index=False)
        print(f"✅ Saved {len(results)} strategy combinations for today")
    else:
        print("No data for today yet")

print("🎉 Updater finished successfully!")
