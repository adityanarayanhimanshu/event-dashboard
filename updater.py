import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sys

print("🚀 Starting Intraday Updater (GitHub Actions)")

# ====================== TEST MODE (Change to False when live) ======================
TEST_MODE = True   # ←←← SET THIS TO False when you want real market hour check

# ====================== CONNECTION ======================
CONNECTION_STRING = os.getenv("NEON_URL")
if not CONNECTION_STRING:
    print("❌ ERROR: NEON_URL secret is missing!")
    sys.exit(1)

engine = create_engine(CONNECTION_STRING)
print("✅ Connected to Neon")

# ====================== IST TIME ======================
def get_ist_now():
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now

ist_now = get_ist_now()
print(f"🕒 Current IST: {ist_now.strftime('%Y-%m-%d %H:%M:%S')}")

# ====================== MARKET HOURS CHECK ======================
weekday = ist_now.weekday()
hour = ist_now.hour
minute = ist_now.minute

is_market_hour = TEST_MODE or (
    weekday < 5 and 
    (9 <= hour < 15 or (hour == 15 and minute <= 30))
)

print(f"📍 Market Hours: {'TRUE (TEST MODE)' if TEST_MODE else 'TRUE' if is_market_hour else 'FALSE'}")

if not is_market_hour:
    print("⏸️  Outside market hours. Skipping run.")
    sys.exit(0)

print("✅ Proceeding with update...")

# ====================== YOUR NEW DATA CODE ======================
# ←←← REPLACE THIS BLOCK WITH YOUR REAL NEW ROWS CODE ←←←
new_data = pd.DataFrame()   # ← CHANGE THIS

if new_data.empty:
    print("⚠️  No new data this run.")
else:
    new_data.to_sql('events', engine, if_exists='append', index=False, method='multi', chunksize=5000)
    print(f"✅ Added {len(new_data):,} new rows")

# ====================== DAILY STRATEGY CALCULATION ======================
if hour >= 13:
    print("🔄 Running daily strategy calculation...")
    # ←←← YOUR STRATEGY GRID CODE HERE ←←←
    print("✅ Strategy performance updated.")

print("🎉 Updater finished successfully!")
