import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import pyotp
from py5paisa import FivePaisaClient
import joblib
import numpy as np

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

# Login already successful if response exists
if response is None:
    print("Login failed")
    sys.exit(1)

print("5Paisa login successful")

# ====================== CONNECT DATABASE ======================
CONNECTION_STRING = os.getenv("NEON_URL")

print("Login response:", response)

if response is None:
    print("Login failed")
    sys.exit(1)

print("5Paisa login successful")

engine = create_engine(CONNECTION_STRING)
print("Connected to Neon DB")
########################################################
model = joblib.load("intraday_quant_model.pkl")
print("Model loaded")
features = [

"Sentiment",
"Momentum5",
"Momentum15",
"Momentum30",
"Momentum60",

"ORBStrength",
"ORBWeakness",

"TimeBlock",
"RelVolume",

"Trend3",

"Volatility15",
"Volatility60",
"Range15",

"LiquidityVacuum",
"VolatilityRegime",
"OrderflowImbalance", 

"VolumeSpike",
"VolumeShock",
"VWAPDeviation",
"VWAPMomentum",
"Acceleration",

"PeerMomentum",
"RelativeRank",
    
"SectorMomentum",   
"RelativeStrengthSector",

"RelativeStrengthMarketIndia",
"RelativeStrengthMarketUS",

"HighSweep",
"LowSweep",
"SweepStrength",
"RecentHighSweeps",
"RecentLowSweeps",

"SP500_return",
"NASDAQ_return",
"CRUDE_return",
"USDINR_return",

"NiftyMomentum",
"BankNiftyMomentum",



"MarketBreadth",
"MarketBreadthPressure",
"LagMomentum",

]
# ====================== IST TIME ======================
ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
print("IST time:", ist_now)

# ====================== MARKET TIME FILTER ======================
FORCE_RUN = True   # set True if you want to force run anytime
market_open = (9, 15)
market_close = (15, 30)

current_time = (ist_now.hour, ist_now.minute)

is_weekend = ist_now.weekday() >= 5
before_open = current_time < market_open
after_close = current_time > market_close

if not FORCE_RUN:
    if is_weekend:
        print("Weekend — skipping update")
        sys.exit(0)

    if before_open or after_close:
        print("Outside market hours — skipping update")
        sys.exit(0)

print("Market open or FORCE_RUN enabled — proceeding...")

# market hours 9:15–15:30

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
"AXISBANK":5900,
"ASIANPAINT":236,
"MARUTI":10999,
"TITAN":3506,
"BAJFINANCE":317,
"BAJAJFINSV":16675,
"HCLTECH":7229,
"WIPRO":3787,
"ULTRACEMCO":11532,

"ONGC":2475,
"TATASTEEL":3499,
"JSWSTEEL":11723,
"HINDALCO":1363,
"COALINDIA":20374,
"NTPC":11630,
"POWERGRID":14977,
"ADANIENT":25,
"ADANIPORTS":15083,
"ADANIGREEN":3563,

"GRASIM":1232,
"DIVISLAB":10940,
"DRREDDY":881,
"SUNPHARMA":3351,
"CIPLA":694,
"APOLLOHOSP":157,
"MAXHEALTH":22377,
"TORNTPHARM":3518,
"ALKEM":11703,
"ZYDUSLIFE":7929,

"TECHM":13538,
"LTIM":17818,
"PERSISTENT":18365,
"MPHASIS":4503,
"COFORGE":11543,

"NESTLEIND":17963,
"BRITANNIA":547,
"DABUR":772,
"GODREJCP":10099,
"COLPAL":15141,
"MARICO":4067,

"ICICIPRULI":18652,
"SBILIFE":21808,
"HDFCLIFE":467,
"BAJAJHLDNG":7806,

"DLF":14732,
"LODHA":24948,
"OBEROIRLTY":20242,

"INDIGO":11195,
"IRCTC":13611,
"ZOMATO":5097,
"PAYTM":3045,

"SIEMENS":3150,
"ABB":13,
"BHEL":438,
"BEL":383,
"HAL":2303,

"PAGEIND":14413,
"TRENT":1964,
"NYKAA":6545,
"VOLTAS":3718,

"GAIL":4717,
"PETRONET":11351,
"IGL":11262,
"MGL":17534,

"SRF":3273,
"PIIND":24184,
"DEEPAKNTR":19943,
"AARTIIND":21238,

"INDUSTOWER":29135,
"TATACOMM":3721,
"NAUKRI":13751,
"POLYCAB":9590
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
        
            # fetch slightly earlier to avoid missing candles
            start_date = (last_time - timedelta(minutes=5)).strftime("%Y-%m-%d")
        
        print(stock, "fetching from", start_date)

        data = client.historical_data(
            Exch="N",
            ExchangeSegment="C",
            ScripCode=scrip,
            time="5m",
            From=start_date,
            To=(date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        )
        print(stock, "rows received:", 0 if data is None else len(data))
        if data is None:
            print("No response:", stock)
            continue
        
        print(stock, "rows received:", len(data))
        
        df = pd.DataFrame(data)
        
        if df.empty:
            print("No new data:", stock)
            continue

        df = pd.DataFrame(data)

        df["Stock"] = stock
        df["Datetime"] = pd.to_datetime(df["Datetime"])

        if os.path.exists(file):
            old = pd.read_parquet(file)
            df = pd.concat([old, df]).drop_duplicates(subset=["Stock","Datetime"])

        df.to_parquet(file,index=False)

        new_frames.append(df)

        print(stock,"saved")

    except Exception as e:
        print("Error:",stock,e)

# ====================== SAVE TO DATABASE ======================
if new_frames:

    df_new = pd.concat(new_frames, ignore_index=True)

    # Run predictions
    try:
        X = df_new[features]
        # ====================== MODEL PREDICTIONS ======================

        preds = model.predict_proba(X)[:,1]
        
        # Raw prediction
        df_new["Pred_raw"] = preds
        
        # Smooth prediction
        df_new["Pred"] = (
            df_new.sort_values("Datetime")
            .groupby("Stock")["Pred_raw"]
            .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )
        
        # ====================== TIME OF DAY ADJUSTMENT ======================
        
        if ist_now.hour < 10:
            df_new["Pred"] *= 0.95
        
        elif ist_now.hour > 14:
            df_new["Pred"] *= 1.05
    except Exception as e:
        print("Prediction failed:", e)
        df_new["Pred"] = None
    
    # Save once to database
    df_new.to_sql(
        "events",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000
    )

    print("Added rows:", len(df_new))

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








