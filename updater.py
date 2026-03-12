import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ====================== CONFIG ======================
CONNECTION_STRING = os.getenv("NEON_URL")
if not CONNECTION_STRING:
    print("❌ NEON_URL secret not found!")
    sys.exit(1)

# ====================== IST TIME & MARKET HOURS ======================
def get_ist_now():
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now

def is_market_open():
    ist = get_ist_now()
    if ist.weekday() >= 5:  # Saturday or Sunday
        return False
    hour = ist.hour
    minute = ist.minute
    # 9:15 AM to 3:30 PM IST
    if (hour > 9 or (hour == 9 and minute >= 15)) and (hour < 15 or (hour == 15 and minute <= 30)):
        return True
    return False

# ====================== CONNECTION ======================
engine = create_engine(CONNECTION_STRING, pool_pre_ping=True)

print(f"🕒 IST Time: {get_ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📈 Market Open: {is_market_open()}")

if not is_market_open():
    print("⏸️  Outside market hours. Skipping...")
    sys.exit(0)

# ====================== YOUR NEW DATA CODE GOES HERE ======================
# Replace this section with your actual code that generates new candle rows
print("🔄 Fetching new candle data...")

# EXAMPLE PLACEHOLDER (replace with your real code):
# new_data = pd.read_csv("new_events.csv")   # or API call, calculation, etc.
# new_data['Datetime'] = pd.to_datetime(new_data['Datetime'])

# For testing, we use dummy data (remove this later)
new_data = pd.DataFrame({
    'Datetime': [get_ist_now()],
    'Stock': ['NIFTY'],
    'Pred': [0.65],
    'Return': [0.002],
    'TargetHit': [0]
})

print(f"📊 Adding {len(new_data)} new rows...")

# ====================== SAVE TO NEON ======================
new_data.to_sql('events', engine, if_exists='append', index=False, method='multi', chunksize=5000)
print("✅ New candles saved to Neon")

# ====================== DAILY STRATEGY CALCULATION (after 1:30 PM) ======================
ist = get_ist_now()
if ist.hour >= 13 and ist.minute >= 30:   # after 1:30 PM IST
    print("📈 Running daily strategy performance calculation...")
    
    # Your full strategy grid calculation goes here
    # Example placeholder:
    results = []
    for prob_th in [0.65, 0.70, 0.75, 0.80]:
        for rank_th in [0.65, 0.70, 0.75]:
            for target in [0.5, 0.6, 0.7]:
                for risk in [0.3, 0.4, 0.5]:
                    if risk >= target: continue
                    pnl = round((prob_th * 120) + (target * 80) - (risk * 60), 0)
                    results.append({
                        "date": ist.date(),
                        "prob_th": prob_th,
                        "rank_th": rank_th,
                        "target_pct": target,
                        "risk_pct": risk,
                        "win_rate": 0.65,
                        "total_trades": 50,
                        "pnl": pnl
                    })

    perf_df = pd.DataFrame(results)
    perf_df.to_sql('strategy_performance', engine, if_exists='append', index=False)
    print(f"✅ Strategy performance saved for {ist.date()}")

print("🎉 Updater finished successfully!")
