import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import pyotp
from py5paisa import FivePaisaClient

# ====================== IMPORT CONFIG ======================
from config import cred, client_code, mpin, totp_key

# ====================== LOGIN ======================
print("Starting 5Paisa login...")

client = FivePaisaClient(cred=cred)

totp = pyotp.TOTP(totp_key)
totp_code = totp.now()

try:
    response = client.get_totp_session(
        client_code=client_code,
        totp=totp_code,
        pin=mpin
    )
except Exception as e:
    print("Login exception:", e)
    sys.exit(1)

print("Login response:", response)

if not response or "RequestToken" not in response:
    print("Login failed")
    sys.exit(1)

print("5Paisa login successful")

# ====================== CONNECT DATABASE ======================
CONNECTION_STRING = os.getenv("NEON_URL")

if not CONNECTION_STRING:
    print("NEON_URL secret missing")
    sys.exit(1)

engine = create_engine(CONNECTION_STRING)
print("Connected to Neon DB")

# ====================== IST TIME ======================
ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
print("IST time:", ist_now)

weekday = ist_now.weekday()
hour = ist_now.hour
minute = ist_now.minute

# market hours 9:15–15:30
if weekday >= 5:
    print("Weekend. Skipping.")
    sys.exit(0)

if (hour < 9) or (hour == 9 and minute < 15) or (hour > 15) or (hour == 15 and minute > 30):
    print("Outside market hours. Skipping.")
    sys.exit(0)

print("Market open. Updating data.")

# ====================== DATA PATH ======================
DATA_PATH = "data/stocks"
os.makedirs(DATA_PATH, exist_ok=True)

# ====================== STOCK LIST ======================
stocks = {
    "RELIANCE":2885,
    "TCS":11536,
    "HDFCBANK":1333,
    "INFY":1594,
    "ICICIBANK":4963,
    "HINDUNILVR":1394,
    "ITC":1660,
    "SBIN":3045,
    "BHARTIARTL":10604,
    "KOTAKBANK":1922,
    "LT":11483,
    "AXISBANK":5900
}

new_frames = []

# ====================== FETCH DATA ======================
for stock, scrip in stocks.items():

    try:

        file = f"{DATA_PATH}/{stock}.parquet"
        start_date = "2025-01-01"

        if os.path.exists(file):
            old = pd.read_parquet(file)
            last_time = pd.to_datetime(old["Datetime"]).max()
            start_date = (last_time + timedelta(minutes=5)).strftime("%Y-%m-%d")

        print(stock, "fetching from", start_date)

        data = client.historical_data(
            Exch="N",
            ExchangeSegment="C",
            ScripCode=scrip,
            time="5m",
            From=start_date,
            To=date.today().strftime("%Y-%m-%d")
        )

        if not data:
            print("No new data:", stock)
            continue

        df = pd.DataFrame(data)

        df["Stock"] = stock
        df["Datetime"] = pd.to_datetime(df["Datetime"])

        if os.path.exists(file):
            old = pd.read_parquet(file)
            df = pd.concat([old, df]).drop_duplicates(subset=["Datetime"])

        df.to_parquet(file,index=False)

        new_frames.append(df)

        print(stock,"saved")

    except Exception as e:
        print("Error:",stock,e)

# ====================== SAVE TO DATABASE ======================
if new_frames:

    df_new = pd.concat(new_frames,ignore_index=True)

    df_new.to_sql(
        "events",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000
    )

    print("Added rows:",len(df_new))

else:

    print("No new data")

# ====================== STRATEGY CALCULATION ======================
if ist_now.hour >= 13:

    today = date.today()

    check = pd.read_sql(
        f"SELECT 1 FROM strategy_performance WHERE date='{today}' LIMIT 1",
        engine
    )

    if len(check)==0:

        print("Calculating strategy performance")

        df_today = pd.read_sql(
            f'SELECT * FROM events WHERE DATE("Datetime") = \'{today}\'',
            engine
        )

        if not df_today.empty:

            results = []

            for prob in [0.65,0.7,0.75,0.8]:

                signals = df_today[df_today["Pred"]>=prob]

                win_rate = (signals["Return"]>0).mean()
                pnl = signals["Return"].sum()*1000

                results.append({
                    "date":today,
                    "prob_th":prob,
                    "rank_th":0.65,
                    "target_pct":0.5,
                    "risk_pct":0.3,
                    "win_rate":win_rate,
                    "total_trades":len(signals),
                    "pnl":round(pnl,2)
                })

            pd.DataFrame(results).to_sql(
                "strategy_performance",
                engine,
                if_exists="append",
                index=False
            )

            print("Strategy results saved")

print("Updater finished successfully")
