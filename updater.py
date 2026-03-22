import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import pyotp
from py5paisa import FivePaisaClient
import joblib
import numpy as np

from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
# ====================== IMPORT CONFIG ======================
from config import cred, client_code, mpin, totp_key

# ====================== LOGIN ======================
print("ing 5Paisa login...")

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

engine = create_engine(CONNECTION_STRING,pool_pre_ping=True)
print("Connected to Neon DB")
########################################################
model = joblib.load("intraday_quant_model.pkl")
# ================= SENTIMENT MODEL =================

tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
sent_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model=sent_model,
    tokenizer=tokenizer
)
# ===== DEBUG START =====
print("MODEL FEATURES:")
print(model.feature_names_in_)

print("LIVE FEATURES:")

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
        last_time = pd.read_sql(
            'SELECT MAX("Datetime") FROM events WHERE "Stock" = %s',
            engine,
            params=(stock,)
        ).iloc[0,0]
        
        if last_time is None:
            start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        else:
            start_date = last_time.strftime("%Y-%m-%d")
        print(stock, "fetching from", start_date)
        data = client.historical_data(
            Exch="N",
            ExchangeSegment="C",
            ScripCode=scrip,
            time="5m",
            From=start_date,
            To=(date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        )
        print(stock, "RAW API RESPONSE TYPE:", type(data))
        print(stock, "RAW API RESPONSE:", str(data)[:200])

        if data is None:
            print(stock, "API returned NONE")
            continue


        if isinstance(data, str):

            if data.strip() == "":
                print(stock, "API returned BLANK STRING")
                continue

        if isinstance(data, dict):

            if "data" not in data:
                print(stock, "API response missing 'data' key:", data)
                continue
        df = pd.DataFrame(data)   
        
        if df.empty:
            print(stock, "API returned empty response")
            continue

        print(stock, "rows received:", len(df))
        
        # Ensure stock column
        df["Stock"] = stock
        
        # Ensure datetime
        df["Datetime"] = pd.to_datetime(df["Datetime"])

        #if last_time is not None:
            #df = df[df["Datetime"] > last_time]
        if df.empty:
            print(stock, "no new candles")
            continue

        print(stock, "new rows:", len(df))    
        new_frames.append(df)

    except Exception as e:
        print("Error:",stock,e)

# ====================== SAVE TO DATABASE ======================
# ==================== SAVE TO DATABASE ====================

if new_frames:
    
    df_new = pd.concat(new_frames, ignore_index=True)

    # ================= LOAD RECENT HISTORY =================

    history = pd.read_sql(
        """
        SELECT *
        FROM events
        WHERE "Datetime" > NOW() - INTERVAL '10 days'
        """,
        engine
    )

    if not history.empty:
        history["Datetime"] = pd.to_datetime(history["Datetime"])
        df_all = pd.concat([history, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    df_all = df_all.sort_values(["Stock","Datetime"]).reset_index(drop=True)

    df_all["Sentiment"] = 0.0
    df_all = df_all.sort_values("Datetime")
    macro_cols = [
        "SP500_return",
        "NASDAQ_return",
        "CRUDE_return",
        "USDINR_return"
    ]
    
    index_cols = [
        "NiftyMomentum",
        "BankNiftyMomentum"
    ]
    
    for col in macro_cols + index_cols:
        if col not in df_all.columns:
            df_all[col] = np.nan
    # ================= MACRO + INDEX DATA =================

    import yfinance as yf
    
    # ================= MACRO FEATURES =================
    
    macro_tickers = {
        "SP500_return": "^GSPC",
        "NASDAQ_return": "^IXIC",
        "CRUDE_return": "CL=F",
        "USDINR_return": "USDINR=X"
    }
    
    for name, ticker in macro_tickers.items():

        start = df_all["Datetime"].min().strftime("%Y-%m-%d")
        #start = ("2026-03-10")
        data = pd.DataFrame()

        for attempt in range(3):
            data = yf.download(
                ticker,
                start=start,
                interval="5m",
                progress=False
            )
            
            if not data.empty:
                break
    
        if data.empty:
            print(f"{name} missing from Yahoo → skipping (will ffill later)")
            continue
    
        # Fix multi-index columns
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
    
        data = data.reset_index()
    
        data["Datetime"] = (
            pd.to_datetime(data["Datetime"], utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )
        
        # 🔥 ADD THIS
        data = data.sort_values("Datetime")
    
        data["Datetime"] = data["Datetime"].dt.floor("5min")
    
        data[name] = data["Close"].pct_change()
        
       
        
        data[name] = data[name].replace([np.inf, -np.inf], 0).fillna(0)

        # ================= SAFE STEP 3 =================
        if name in data.columns and not data.empty:
        
            latest_yahoo_time = data["Datetime"].max()
            latest_df_time = df_all["Datetime"].max()
        
            if latest_yahoo_time < latest_df_time:
                print(f"{name}: Yahoo lag detected → extending last value")
        
                last_value = data[name].dropna()
                if last_value.empty:
                    print(f"❌ {name}: No valid data → skipping extension")
                    continue
                
                last_value = last_value.iloc[-1]
        
                future_times = pd.date_range(
                    start=latest_yahoo_time + pd.Timedelta(minutes=5),
                    end=latest_df_time,
                    freq="5min"
                )
        
                future_df = pd.DataFrame({"Datetime": future_times})
                future_df[name] = last_value
        
                data = pd.concat([data, future_df], ignore_index=True)
        
        else:
            print(f"{name}: skipped extension (no data)")
        # ==============================================

        # DROP OLD COLUMN BEFORE MERGE (VERY IMPORTANT)
        if name in df_all.columns:
            df_all = df_all.drop(columns=[name])
        
        # ===== FIX: DAILY MERGE =====

        # Convert yahoo to daily
        data["Date"] = data["Datetime"].dt.date
        data = data.sort_values("Datetime")
        data = data.groupby("Date").last().reset_index()
        
        # Ensure df_all has Date
        df_all["Date"] = pd.to_datetime(df_all["Datetime"]).dt.date
        
        # Merge
        df_all = df_all.merge(
            data[["Date", name]],
            on="Date",
            how="left"
        )
        
        # Fill values
        df_all[name] = df_all[name].ffill().fillna(0)
        
        
    
    # ================= INDEX FEATURES =================
    
    index_tickers = {
        "NiftyMomentum": "^NSEI",
        "BankNiftyMomentum": "^NSEBANK"
    }
    
    for name, ticker in index_tickers.items():

        start = df_all["Datetime"].min().strftime("%Y-%m-%d")
        #start = ("2026-03-10")
        data = pd.DataFrame()

        for attempt in range(3):
            data = yf.download(
                ticker,
                start=start,
                interval="5m",
                progress=False
            )
            
            if not data.empty:
                break

        if data.empty:
            print(f"{name} missing from Yahoo → skipping (will ffill later)")
            continue
    
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
    
        data = data.reset_index()
    
        data["Datetime"] = (
            pd.to_datetime(data["Datetime"], utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )
        
        # 🔥 ADD THIS
        data = data.sort_values("Datetime")
    
        data["Datetime"] = data["Datetime"].dt.floor("5min")
    
        data[name] = data["Close"].pct_change()
        
       
        
        data[name] = data[name].replace([np.inf, -np.inf], 0).fillna(0)
        
        if name in data.columns and not data.empty:

            latest_yahoo_time = data["Datetime"].max()
            latest_df_time = df_all["Datetime"].max()
        
            if latest_yahoo_time < latest_df_time:
                print(f"{name}: Yahoo lag detected → extending last value")
        
                last_value = data[name].dropna()
                if last_value.empty:
                    print(f"❌ {name}: No valid data → skipping extension")
                    continue
                
                last_value = last_value.iloc[-1]
        
                future_times = pd.date_range(
                    start=latest_yahoo_time + pd.Timedelta(minutes=5),
                    end=latest_df_time,
                    freq="5min"
                )
        
                future_df = pd.DataFrame({"Datetime": future_times})
                future_df[name] = last_value
        
                data = pd.concat([data, future_df], ignore_index=True)

        # DROP OLD COLUMN BEFORE MERGE (VERY IMPORTANT)
        if name in df_all.columns:
            df_all = df_all.drop(columns=[name])
        
        
        data["Date"] = data["Datetime"].dt.date
        data = data.sort_values("Datetime")
        data = data.groupby("Date").last().reset_index()
        
        df_all["Date"] = pd.to_datetime(df_all["Datetime"]).dt.date
        
        df_all = df_all.merge(
            data[["Date", name]],
            on="Date",
            how="left"
        )
        
        df_all[name] = df_all[name].ffill().fillna(0)

    if "Date" in df_all.columns:
        df_all.drop(columns=["Date"], inplace=True)
    # ================= FORCE ALL MARKET COLUMNS =================

    required_cols = [
        "SP500_return",
        "NASDAQ_return",
        "CRUDE_return",
        "USDINR_return",
        "NiftyMomentum",
        "BankNiftyMomentum"
    ]
    
    for col in required_cols:
        if col not in df_all.columns:
            print(f"FORCE ADD: {col}")
            df_all[col] = 0
    # ================= HANDLE YAHOO DELAYS =================
    
    existing_cols = [c for c in macro_cols + index_cols if c in df_all.columns]
    df_all[existing_cols] = df_all[existing_cols].ffill()
    df_all[existing_cols] = df_all[existing_cols].fillna(0)
    df_all[existing_cols] = df_all[existing_cols].astype(float)
    # ================= LIVE NEWS SENTIMENT =================

    try:

        table_check = pd.read_sql(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name='news'
            """,
            engine
        )
    
        if not table_check.empty:
    
            news = pd.read_sql(
                """
                SELECT Datetime, Headline
                FROM news
                WHERE Datetime > NOW() - INTERVAL '1 day'
                """,
                engine
            )
    
            if not news.empty:
                news = news.copy()
            
                valid_mask = news["Headline"].notna()
                texts = news.loc[valid_mask, "Headline"].astype(str).str[:512].tolist()
            
                if texts:
                    results = sentiment_pipeline(texts, batch_size=16)
                    news.loc[valid_mask, "sent"] = [r["score"] for r in results]
            
                news["sent"] = news["sent"].fillna(0)
            
            else:
                news["sent"] = 0
            
            # ✅ MOVE THIS OUTSIDE if-else
            news["Datetime"] = pd.to_datetime(news["Datetime"])
            
            news_sent = news.groupby(
                news["Datetime"].dt.floor("5min")
            )["sent"].mean().reset_index()
            
            
            
            df_all = df_all.merge(
                news_sent,
                on="Datetime",
                how="left"
            )
            
            df_all["Sentiment"] = df_all["sent"].fillna(0)
            
            df_all.drop(columns=["sent"], inplace=True)
    
    except Exception as e:
        print("Sentiment error:", e)
    # ================= FEATURE ENGINEERING =================

    #df_new = df_new.sort_values(["Stock","Datetime"])
    
    # Basic returns
    df_all["Return"] = df_all.groupby("Stock")["Close"].pct_change()
    
    # Momentum
    df_all["Momentum5"] = df_all.groupby("Stock")["Close"].pct_change(5)
    df_all["Momentum15"] = df_all.groupby("Stock")["Close"].pct_change(15)
    df_all["Momentum30"] = df_all.groupby("Stock")["Close"].pct_change(30)
    df_all["Momentum60"] = df_all.groupby("Stock")["Close"].pct_change(60)
    
    # Trend
    df_all["Trend3"] = df_all.groupby("Stock")["Close"].pct_change(3)
    
    # Volatility
    df_all["Volatility15"] = (
        df_all.groupby("Stock")["Close"]
        .pct_change()
        .rolling(15)
        .std()
    )
    
    df_all["Volatility60"] = (
        df_all.groupby("Stock")["Close"]
        .pct_change()
        .rolling(60)
        .std()
    )
    
    # Range
    df_all["Range15"] = (
        df_all.groupby("Stock")["High"]
        .rolling(15).max().reset_index(level=0,drop=True)
        -
        df_all.groupby("Stock")["Low"]
        .rolling(15).min().reset_index(level=0,drop=True)
    ) / df_all["Close"]
    
    # Liquidity vacuum
    df_all["LiquidityVacuum"] = (
        df_all["Range15"] /
        (df_all.groupby("Stock")["Volume"]
         .rolling(20).mean()
         .reset_index(level=0,drop=True) + 1e-6)
    )
    
    # Volatility regime
    df_all["VolatilityRegime"] = (
        df_all["Volatility15"] /
        (df_all["Volatility60"] + 1e-6)
    )
    
    # Orderflow imbalance
    df_all["BuyPressure"] = (
        (df_all["Close"] - df_all["Low"]) /
        (df_all["High"] - df_all["Low"] + 1e-6)
    )
    
    df_all["SellPressure"] = (
        (df_all["High"] - df_all["Close"]) /
        (df_all["High"] - df_all["Low"] + 1e-6)
    )
    
    df_all["OrderflowImbalance"] = (
        df_all["BuyPressure"] - df_all["SellPressure"]
    )
    
    # Volume spike
    df_all["VolumeSpike"] = (
        df_all["Volume"] /
        df_all.groupby("Stock")["Volume"]
        .rolling(15).mean()
        .reset_index(level=0,drop=True)
    )
    
    df_all["VolumeShock"] = (
        df_all["Volume"] /
        (df_all.groupby("Stock")["Volume"]
         .rolling(30).mean()
         .reset_index(level=0,drop=True) + 1e-6)
    )
    
    # VWAP
    df_all["VWAP"] = (
        (df_all["Close"] * df_all["Volume"])
        .groupby(df_all["Stock"])
        .cumsum()
        /
        df_all["Volume"]
        .groupby(df_all["Stock"])
        .cumsum()
    )
    
    df_all["VWAPDeviation"] = (
        (df_all["Close"] - df_all["VWAP"]) /
        df_all["VWAP"]
    )
    
    df_all["VWAPMomentum"] = (
        df_all["VWAPDeviation"] -
        df_all.groupby("Stock")["VWAPDeviation"].shift(3)
    )
    
    # Acceleration
    df_all["Acceleration"] = (
        df_all.groupby("Stock")["Close"].pct_change(5) -
        df_all.groupby("Stock")["Close"].pct_change(15)
    )
    
    # Relative volume
    df_all["RelVolume"] = (
        df_all["Volume"] /
        df_all.groupby("Stock")["Volume"]
        .rolling(50).mean()
        .reset_index(level=0,drop=True)
    )
    
    # Time feature
    df_all["Hour"] = df_all["Datetime"].dt.hour
    df_all["Minute"] = df_all["Datetime"].dt.minute
    df_all["TimeBlock"] = df_all["Hour"]*60 + df_all["Minute"]
    # ---------------- ORB calculation ----------------
    
    # ================= ORB FEATURES (FINAL FIX) =================

    df_all = df_all.sort_values(["Stock","Datetime"])
    
    
    
    # safe calculations
    df_all["MarketOpen"] = (
        df_all["Datetime"].dt.time <= pd.to_datetime("09:45").time()
    )
    
    open_high = (
        df_all[df_all["MarketOpen"]]
        .groupby(["Stock", df_all["Datetime"].dt.date])["High"]
        .transform("max")
    )
    
    open_low = (
        df_all[df_all["MarketOpen"]]
        .groupby(["Stock", df_all["Datetime"].dt.date])["Low"]
        .transform("min")
    )
    
    df_all["ORBStrength"] = (df_all["Close"] - open_high) / open_high
    df_all["ORBWeakness"] = (df_all["Close"] - open_low) / open_low
    # Market breadth proxy
    df_all["UpStock"] = (df_all["Return"] > 0).astype(int)
    
    df_all["MarketBreadth"] = (
        df_all.groupby("Datetime")["UpStock"]
        .transform("mean")
    )
    
    df_all["MarketBreadthPressure"] = (
        df_all.groupby("Datetime")["Close"]
        .transform(lambda x: (x.pct_change() > 0).mean())
    )
    
    # Lag momentum
    df_all["LagMomentum"] = (
        df_all.groupby("Stock")["Close"]
        .pct_change(3)
        .shift(2)
    )

    # Relative rank
    df_all["RelativeRank"] = (
        df_all.groupby("Datetime")["Close"]
        .pct_change(15)
        .rank(pct=True)
    )
    
    # ======================================================
    # ================= LIQUIDITY SWEEP FEATURES =================

    lookback = 20
    
    df_all["RecentHigh"] = (
        df_all.groupby("Stock")["High"]
        .transform(lambda x: x.rolling(lookback).max())
    )
    
    df_all["RecentLow"] = (
        df_all.groupby("Stock")["Low"]
        .transform(lambda x: x.rolling(lookback).min())
    )
    
    df_all["HighSweep"] = (
        (df_all["High"] > df_all["RecentHigh"].shift(1)) &
        (df_all["Close"] < df_all["RecentHigh"].shift(1))
    ).astype(int)
    
    df_all["LowSweep"] = (
        (df_all["Low"] < df_all["RecentLow"].shift(1)) &
        (df_all["Close"] > df_all["RecentLow"].shift(1))
    ).astype(int)
    
    df_all["SweepStrength"] = (
        (df_all["High"] - df_all["Low"]) /
        df_all["Close"]
    )
    
    df_all["RecentHighSweeps"] = (
        df_all.groupby("Stock")["HighSweep"]
        .transform(lambda x: x.rolling(10).sum())
    )
    
    df_all["RecentLowSweeps"] = (
        df_all.groupby("Stock")["LowSweep"]
        .transform(lambda x: x.rolling(10).sum())
    )

    # ================= EVENT TRIGGER (MATCH TRAINING) =================

    df_all["VolAvg20"] = (
        df_all.groupby("Stock")["Volume"]
        .rolling(20)
        .mean()
        .reset_index(level=0, drop=True)
    )
    
    df_all["VolumeEvent"] = (
        df_all["Volume"] > df_all["VolAvg20"] * 1.5
    ).astype(int)
    
    df_all["MomentumEvent"] = (
        df_all.groupby("Stock")["Close"]
        .pct_change(10)
        .abs() > 0.003
    ).astype(int)
    
    df_all["SweepEvent"] = (
        (df_all["HighSweep"] == 1) |
        (df_all["LowSweep"] == 1)
    ).astype(int)
    
    df_all["EventTrigger"] = (
        (df_all["VolumeEvent"] == 1) |
        (df_all["MomentumEvent"] == 1) |
        (df_all["SweepEvent"] == 1)
    ).astype(int)
    # ================= SECTOR FEATURES =================

    sector_map = {
    
    "TCS":"IT","INFY":"IT","HCLTECH":"IT","WIPRO":"IT","TECHM":"IT",
    
    "HDFCBANK":"BANK","ICICIBANK":"BANK","SBIN":"BANK","AXISBANK":"BANK","KOTAKBANK":"BANK",
    
    "BAJFINANCE":"FINANCE","BAJAJFINSV":"FINANCE",
    
    "RELIANCE":"ENERGY","ONGC":"ENERGY","GAIL":"ENERGY",
    
    "TATASTEEL":"METAL","JSWSTEEL":"METAL","HINDALCO":"METAL",
    
    "SUNPHARMA":"PHARMA","DRREDDY":"PHARMA","CIPLA":"PHARMA",
    
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG",
    
    "LT":"INFRA",
    "MARUTI":"AUTO",
    "BHARTIARTL":"TELECOM"
    
    }
    
    df_all["Sector"] = df_all["Stock"].map(sector_map)
    
    df_all["SectorMomentum"] = (
        df_all.groupby(["Sector","Datetime"])["Return"]
        .transform("mean")
    )
    
    df_all["RelativeStrengthSector"] = (
        df_all["Return"] - df_all["SectorMomentum"]
    )
    # ================= CROSS SECTIONAL FEATURES =================

    returns10 = df_all.groupby("Stock")["Close"].pct_change(10)
    
    df_all["PeerMomentum"] = (
        returns10.groupby(df_all["Datetime"]).transform("mean")
    )
    

    df_all["RelativeStrengthMarketIndia"] = (
    df_all["Return"] - df_all["NiftyMomentum"]
    )
    
    df_all["RelativeStrengthMarketUS"] = (
        df_all["Return"] - df_all.get("SP500_return",0)
    )

    
    # CLEANUP
    # ======================================================
    
    df_all = df_all.replace([np.inf,-np.inf],np.nan)

    # Only forward fill market data
    market_cols = [
        "NiftyMomentum", "BankNiftyMomentum",
        "SP500_return", "NASDAQ_return", "CRUDE_return", "USDINR_return"
    ]
    
    df_all[market_cols] = df_all[market_cols].ffill()
    print("LIVE FEATURES:")
    print(df_all.columns.tolist())
    # Ensure model features exist
    for f in model.feature_names_in_:
        if f not in df_all.columns:
            df_all[f] = 0

    df_all[model.feature_names_in_] = df_all[model.feature_names_in_].fillna(0)
    # ================= CLEAN DUPLICATE COLUMNS =================

    drop_cols = [c for c in df_all.columns if c.endswith("_x") or c.endswith("_y")]
    
    if drop_cols:
        print("Dropping cols:", drop_cols[:10])
    
    df_all.drop(columns=drop_cols, inplace=True, errors="ignore")
    df_all = df_all.replace([np.inf, -np.inf], 0)
    print("Columns after cleanup:", len(df_all.columns))
    print("Final market columns check:")
    print(df_all[required_cols].isna().sum())
    # ================= MODEL PREDICTIONS =================

    try:

        drop_cols = [c for c in df_all.columns if c.endswith("_x") or c.endswith("_y")]
        print("Dropped cols:", drop_cols[:5])
        df_all = df_all.drop(columns=drop_cols, errors="ignore")

        print("Model expects:", len(model.feature_names_in_))
        print("Columns available:", len(df_all.columns))

        # ================= FIX STARTS HERE =================
        # keep only event rows (same as training)
        df_all = df_all[df_all["EventTrigger"] == 1]
        X = df_all[model.feature_names_in_].copy()
    
        # 🔥 CRITICAL FIX
        X = X.apply(pd.to_numeric, errors='coerce')
        X = X.ffill().fillna(0)
    
        # 🔍 DEBUG (VERY IMPORTANT)
        print("Shape of X:", X.shape)
        print("Any NaN:", X.isna().sum().sum())
        print("Dtypes:\n", X.dtypes.value_counts())
        print("Sample:\n", X.head())
    
        # ================= FIX ENDS HERE =================
        
        df_all["Pred"] = model.predict_proba(X)[:,1]
        # Keep only newly added rows properly
        new_times = df_new[["Stock", "Datetime"]].drop_duplicates()
        
        df_new = df_all.merge(
            new_times,
            on=["Stock", "Datetime"],
            how="inner"
        )

    except Exception as e:

        print("Prediction error:", e)

        df_new["Pred"] = 0

    # ================= DUPLICATE PROTECTION =================

    df_new = df_new[df_new["Datetime"].notna()]

    df_new = df_new.drop_duplicates(subset=["Stock","Datetime"])

    df_new = df_new.reset_index(drop=True)
    df_new = df_new.replace([np.inf, -np.inf], 0)
    df_new = df_new.fillna(0)
    if df_new.empty:
        print("⚠️ df_new empty after filtering — skipping insert")
        sys.exit(0)
    # ================= MATCH DATABASE SCHEMA =================

    table_columns = pd.read_sql("SELECT * FROM events LIMIT 1", engine).columns

    df_new = df_new.loc[:, df_new.columns.intersection(table_columns)]

    # ================= KEEP ONLY NEW ROWS =================

    latest_db = pd.read_sql(
    """
    SELECT "Stock","Datetime"
    FROM events
    WHERE "Datetime" > NOW() - INTERVAL '10 day'
    """,
    engine
    )
    
    if not latest_db.empty:
    
        df_new = df_new.merge(
            latest_db,
            on=["Stock","Datetime"],
            how="left",
            indicator=True
        )
    
        df_new = df_new[df_new["_merge"]=="left_only"]
    
        df_new = df_new.drop(columns="_merge")
    

    # ================= BOOLEAN COLUMN FIX =================

    bool_cols = [
        "ORBWeakness",
        "MarketRegime_MeanReversion",
        "MarketRegime_Neutral",
        "MarketRegime_Panic",
        "MarketRegime_VolatilityBreakout",
        "MarketOpen"
    ]
    if "TargetHit" in df_new.columns:
        df_new["TargetHit"] = df_new["TargetHit"].fillna(0).astype(int)
    for col in bool_cols:
        if col in df_new.columns:
            df_new[col] = df_new[col].astype(bool)
    # ================= SAVE =================

    df_new.to_sql(
        "events",
        engine,
        if_exists="append",
        index=False,
        chunksize=500
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















