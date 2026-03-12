import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import sys
import time
import glob
import pyotp
import fivepaisa

# ====================== 5PAISA CREDENTIALS (from your config.py) ======================
# ←←← PASTE YOUR REAL VALUES HERE FROM config.py ←←←
cred = {
    "APP_NAME": "intranews",
    "APP_SOURCE": "27380",
    "USER_ID": "4hQTZM7gPF",
    "PASSWORD": "Z81nxyyBOU",
    "USER_KEY": "mHedJXEhIP1SNWeYxkwjoIqqHcEI4X",
    "ENCRYPTION_KEY": "KJVar07soPNXdGngon7AU0L1W7INBC"
}

client_code = "574598849"
mpin = "850762"
totp_key = "GU3TINJZHA2DSXZVKBDUWRKZ"

# ====================== 5PAISA CLIENT INIT ======================
from py5paisa import FivePaisaClient
import pyotp

client = FivePaisaClient(cred=cred)
totp = pyotp.TOTP(totp_key)
totp_code = totp.now()

response = client.get_totp_session(
    client_code=client_code,
    totp=totp_code,
    mpin=mpin
)
print("5paisa login response:", response)

if not response.get("RequestToken"):
    print("❌ 5paisa login failed!")
    sys.exit(1)

print("5paisa client logged in successfully")

# ====================== CONNECTION TO NEON ======================
CONNECTION_STRING = os.getenv("NEON_URL")
if not CONNECTION_STRING:
    print("❌ ERROR: NEON_URL secret is missing in GitHub Actions!")
    sys.exit(1)

engine = create_engine(CONNECTION_STRING)
print("Connected to Neon")

# ====================== IST TIME ======================
ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
print(f"IST: {ist_now.strftime('%Y-%m-%d %H:%M:%S')}")

weekday = ist_now.weekday()
hour = ist_now.hour
minute = ist_now.minute

# Enforce real market hours (no TEST_MODE anymore)
if weekday >= 5 or not (9 <= hour < 15 or (hour == 15 and minute <= 30)):
    print(f"Outside market hours (9:15–15:30 IST, weekdays) — current IST: {ist_now.strftime('%H:%M')}. Skipping.")
    sys.exit(0)

print("Inside market hours — proceeding with update...")

# ====================== DATA PATH & STOCKS ======================
DATA_PATH = "data/stocks"
os.makedirs(DATA_PATH, exist_ok=True)

stocks = {
    "RELIANCE": 2885,
    "TCS": 11536,
    "HDFCBANK": 1333,
    "INFY": 1594,
    "ICICIBANK": 4963,
    "HINDUNILVR": 1394,
    "ITC": 1660,
    "SBIN": 3045,
    "BHARTIARTL": 10604,
    "KOTAKBANK": 1922,
    "LT": 11483,
    "AXISBANK": 5900,
    "ASIANPAINT": 236,
    "MARUTI": 10999,
    "TITAN": 3506,
    "BAJFINANCE": 317,
    "BAJAJFINSV": 16675,
    "HCLTECH": 7229,
    "WIPRO": 3787,
    "ULTRACEMCO": 11532,
    "ONGC": 2475,
    "TATASTEEL": 3499,
    "JSWSTEEL": 11723,
    "HINDALCO": 1363,
    "COALINDIA": 20374,
    "NTPC": 11630,
    "POWERGRID": 14977,
    "ADANIENT": 25,
    "ADANIPORTS": 15083,
    "ADANIGREEN": 3563,
    "GRASIM": 1232,
    "DIVISLAB": 10940,
    "DRREDDY": 881,
    "SUNPHARMA": 3351,
    "CIPLA": 694,
    "APOLLOHOSP": 157,
    "MAXHEALTH": 22377,
    "TORNTPHARM": 3518,
    "ALKEM": 11703,
    "ZYDUSLIFE": 7929,
    "TECHM": 13538,
    "LTIM": 17818,
    "PERSISTENT": 18365,
    "MPHASIS": 4503,
    "COFORGE": 11543,
    "NESTLEIND": 17963,
    "BRITANNIA": 547,
    "DABUR": 772,
    "GODREJCP": 10099,
    "COLPAL": 15141,
    "MARICO": 4067,
    "ICICIPRULI": 18652,
    "SBILIFE": 21808,
    "HDFCLIFE": 467,
    "BAJAJHLDNG": 7806,
    "DLF": 14732,
    "LODHA": 24948,
    "OBEROIRLTY": 20242,
    "INDIGO": 11195,
    "IRCTC": 13611,
    "ZOMATO": 5097,
    "PAYTM": 3045,
    "SIEMENS": 3150,
    "ABB": 13,
    "BHEL": 438,
    "BEL": 383,
    "HAL": 2303,
    "PAGEIND": 14413,
    "TRENT": 1964,
    "NYKAA": 6545,
    "VOLTAS": 3718,
    "GAIL": 4717,
    "PETRONET": 11351,
    "IGL": 11262,
    "MGL": 17534,
    "SRF": 3273,
    "PIIND": 24184,
    "DEEPAKNTR": 19943,
    "AARTIIND": 21238,
    "INDUSTOWER": 29135,
    "TATACOMM": 3721,
    "NAUKRI": 13751,
    "POLYCAB": 9590
}

# ====================== FETCH & APPEND NEW DATA ======================
new_frames = []

for stock, scrip in stocks.items():
    try:
        file = f"data/stocks/{stock}.parquet"
        start_date = "2025-01-01"  # fallback

        if os.path.exists(file):
            old = pd.read_parquet(file)
            last_time = pd.to_datetime(old["Datetime"]).max()
            start_date = (last_time + timedelta(minutes=5)).strftime("%Y-%m-%d")
            print(f"{stock} updating from {start_date}")

        data = client.historical_data(
            Exch="N",
            ExchangeSegment="C",
            ScripCode=scrip,
            time="5m",
            From=start_date,
            To=date.today().strftime("%Y-%m-%d")
        )

        if data is None or len(data) == 0:
            print(f"No new data for {stock}")
            continue

        data = pd.DataFrame(data)
        data["Stock"] = stock
        data["Datetime"] = pd.to_datetime(data["Datetime"])

        if os.path.exists(file):
            old = pd.read_parquet(file)
            data = pd.concat([old, data]).drop_duplicates(subset=["Datetime"])

        data.to_parquet(file, index=False)
        print(f"{stock} saved locally")

        new_frames.append(data)

    except Exception as e:
        print(f"Error {stock}: {e}")

if not new_frames:
    print("No new data from any stock. Skipping append.")
else:
    df_new = pd.concat(new_frames, ignore_index=True)
    df_new.to_sql('events', engine, if_exists='append', index=False, method='multi', chunksize=5000)
    print(f"Added {len(df_new)} new rows to events table")

# ====================== DAILY STRATEGY CALCULATION ======================
if hour >= 13:
    today = date.today()
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
            print(f"Saved {len(results)} strategy combinations for today")
        else:
            print("No data for today yet")
    else:
        print("Strategy performance already calculated today")

print("🎉 Updater finished successfully!")
