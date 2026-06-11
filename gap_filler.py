"""
Gap Filler Script
=================
Detects and fills missing 5-min candles in the last 30 days.
Run this ONCE manually from Jupyter to fix all historical gaps.
Safe to run multiple times — uses on_conflict_do_nothing.
"""

import os
import sys
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import Table, MetaData
from datetime import datetime, timedelta, date
import pyotp
from py5paisa import FivePaisaClient
import joblib
import warnings
warnings.filterwarnings("ignore")

from config import cred, client_code, mpin, totp_key

# ====================== CONFIG ======================
LOOKBACK_DAYS = 30         # How far back to check for gaps
MARKET_START  = "09:15"    # First candle of the day
MARKET_END    = "15:55"    # Last candle of the day
CANDLE_FREQ   = "5min"     # 5-minute candles

def log(msg):
    ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    print(f"[{ist.strftime('%H:%M:%S')}] {msg}", flush=True)

# ====================== LOGIN ======================
log("Logging in to 5Paisa...")
client = None
response = None
for attempt in range(3):
    try:
        client = FivePaisaClient(cred=cred)
        totp_code = pyotp.TOTP(totp_key).now()
        response = client.get_totp_session(
            client_code=client_code,
            totp=totp_code,
            pin=mpin
        )
        if response is not None:
            log(f"Login successful")
            break
        time.sleep(5)
    except Exception as e:
        log(f"Login attempt {attempt+1} failed: {e}")
        time.sleep(5)

if client is None or response is None:
    log("Login failed — exiting")
    sys.exit(1)

# ====================== DB ======================
CONNECTION_STRING = os.getenv("NEON_URL")
engine = create_engine(CONNECTION_STRING, pool_pre_ping=True, pool_timeout=30)
log("Connected to NeonDB")

# ====================== STOCKS ======================
stocks = {
    "RELIANCE":2885, "TCS":11536, "HDFCBANK":1333, "INFY":1594,
    "ICICIBANK":4963, "HINDUNILVR":1394, "ITC":1660, "SBIN":3045,
    "BHARTIARTL":10604, "KOTAKBANK":1922, "LT":11483, "AXISBANK":5900,
    "ASIANPAINT":236, "MARUTI":10999, "TITAN":3506, "BAJFINANCE":317,
    "BAJAJFINSV":16675, "HCLTECH":7229, "WIPRO":3787, "ULTRACEMCO":11532,
    "ONGC":2475, "TATASTEEL":3499, "JSWSTEEL":11723, "HINDALCO":1363,
    "COALINDIA":20374, "NTPC":11630, "POWERGRID":14977, "ADANIENT":25,
    "ADANIPORTS":15083, "ADANIGREEN":3563, "GRASIM":1232, "DIVISLAB":10940,
    "DRREDDY":881, "SUNPHARMA":3351, "CIPLA":694, "APOLLOHOSP":157,
    "MAXHEALTH":22377, "TORNTPHARM":3518, "ALKEM":11703, "ZYDUSLIFE":7929,
    "TECHM":13538, "LTIM":17818, "PERSISTENT":18365, "MPHASIS":4503,
    "COFORGE":11543, "NESTLEIND":17963, "BRITANNIA":547, "DABUR":772,
    "GODREJCP":10099, "COLPAL":15141, "MARICO":4067, "ICICIPRULI":18652,
    "SBILIFE":21808, "HDFCLIFE":467, "BAJAJHLDNG":7806, "DLF":14732,
    "LODHA":24948, "OBEROIRLTY":20242, "INDIGO":11195, "IRCTC":13611,
    "ZOMATO":5097, "PAYTM":4854, "SIEMENS":3150, "ABB":13,
    "BHEL":438, "BEL":383, "HAL":2303, "PAGEIND":14413,
    "TRENT":1964, "NYKAA":6545, "VOLTAS":3718, "GAIL":4717,
    "PETRONET":11351, "IGL":11262, "MGL":17534, "SRF":3273,
    "PIIND":24184, "DEEPAKNTR":19943, "AARTIIND":21238,
    "INDUSTOWER":29135, "TATACOMM":3721, "NAUKRI":13751, "POLYCAB":9590,
}

ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)

# ====================== STEP 1: FIND GAPS ======================
log("=" * 60)
log("STEP 1 — Scanning for gaps in last 30 days...")
log("=" * 60)

# Get all trading days in last 30 days (Mon-Fri only)
start_scan = (ist_now - timedelta(days=LOOKBACK_DAYS)).date()
end_scan    = ist_now.date()

all_weekdays = pd.bdate_range(start=start_scan, end=end_scan).date.tolist()
# Remove today if market not yet closed
if ist_now.hour < 16:
    all_weekdays = [d for d in all_weekdays if d < ist_now.date()]

log(f"Trading days to check: {len(all_weekdays)} ({all_weekdays[0]} to {all_weekdays[-1]})")

# Get what we have in DB per stock per day
log("Querying DB for existing candles...")
existing = pd.read_sql(
    f"""
    SELECT "Stock", 
           DATE("Datetime") as trade_date,
           COUNT(*) as candle_count,
           MIN("Datetime"::time) as first_candle,
           MAX("Datetime"::time) as last_candle
    FROM events
    WHERE "Datetime" >= '{start_scan}'
    GROUP BY "Stock", DATE("Datetime")
    """,
    engine
)

existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
log(f"DB has {len(existing)} stock-day combinations")

# Expected candles per full day: 09:15 to 15:55 = 75 candles
EXPECTED_CANDLES = 75
MIN_CANDLES      = 12   # If less than this, definitely a gap day

# Find gap days per stock
gaps = []
for stock in stocks.keys():
    stock_data = existing[existing["Stock"] == stock]
    existing_dates = set(stock_data["trade_date"].tolist())

    for trade_date in all_weekdays:
        if trade_date not in existing_dates:
            # Entire day missing
            gaps.append({
                "stock": stock,
                "date": trade_date,
                "type": "FULL_DAY_MISSING",
                "have": 0,
                "expected": EXPECTED_CANDLES
            })
        else:
            row = stock_data[stock_data["trade_date"] == trade_date].iloc[0]
            candle_count = row["candle_count"]
            last_candle  = str(row["last_candle"])[:5]  # HH:MM

            if candle_count < MIN_CANDLES:
                gaps.append({
                    "stock": stock,
                    "date": trade_date,
                    "type": "ALMOST_EMPTY",
                    "have": candle_count,
                    "expected": EXPECTED_CANDLES
                })
            elif last_candle < "14:00":
                # Last candle is before 2pm — missing afternoon data
                gaps.append({
                    "stock": stock,
                    "date": trade_date,
                    "type": "MISSING_AFTERNOON",
                    "have": candle_count,
                    "expected": EXPECTED_CANDLES,
                    "last_candle": last_candle
                })

gap_df = pd.DataFrame(gaps) if gaps else pd.DataFrame()

if gap_df.empty:
    log("✅ No gaps found — DB is complete!")
    sys.exit(0)

# Summarize gaps
log(f"\n{'='*60}")
log(f"GAP SUMMARY")
log(f"{'='*60}")
log(f"Total gap instances: {len(gap_df)}")
log(f"Unique gap dates: {gap_df['date'].nunique()}")
log(f"Unique affected stocks: {gap_df['stock'].nunique()}")
log(f"\nBy type:")
log(gap_df["type"].value_counts().to_string())
log(f"\nBy date:")
log(gap_df.groupby("date").size().to_string())

# Group gaps by date — fetch all stocks for a gap date at once
gap_dates = sorted(gap_df["date"].unique())
log(f"\nGap dates to backfill: {gap_dates}")

# ====================== STEP 2: FETCH MISSING DATA ======================
log(f"\n{'='*60}")
log(f"STEP 2 — Fetching missing data from 5Paisa...")
log(f"{'='*60}")

all_new_frames = []

for gap_date in gap_dates:
    date_str   = gap_date.strftime("%Y-%m-%d")
    next_date  = (gap_date + timedelta(days=1)).strftime("%Y-%m-%d")
    start_time = f"{date_str} 09:15:00"

    # Stocks that have gaps on this date
    gap_stocks_today = gap_df[gap_df["date"] == gap_date]["stock"].tolist()
    log(f"\nDate {date_str}: fetching {len(gap_stocks_today)} stocks")

    day_frames = []
    failed     = []

    for stock in gap_stocks_today:
        scrip = stocks[stock]
        for attempt in range(3):
            try:
                data = client.historical_data(
                    Exch="N",
                    ExchangeSegment="C",
                    ScripCode=scrip,
                    time="5m",
                    From=start_time,
                    To=next_date
                )

                if data is None or len(data) == 0:
                    break

                df = pd.DataFrame(data)
                if df.empty:
                    break

                df["Stock"]    = stock
                df["Datetime"] = pd.to_datetime(df["Datetime"])
                df = df[df["Datetime"] >= pd.to_datetime(start_time)]
                df = df[df["Datetime"].dt.date == gap_date]

                if not df.empty:
                    day_frames.append(df)
                    log(f"  {stock}: {len(df)} candles")
                break

            except Exception as e:
                log(f"  {stock} attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(2)
                else:
                    failed.append(stock)

    if failed:
        log(f"  Failed stocks on {date_str}: {failed}")

    if day_frames:
        all_new_frames.extend(day_frames)

    # Rate limit protection between dates
    time.sleep(2)

if not all_new_frames:
    log("No new data fetched — nothing to insert")
    sys.exit(0)

df_gap = pd.concat(all_new_frames, ignore_index=True)
log(f"\nTotal new candles fetched: {len(df_gap)}")

# ====================== STEP 3: COMPUTE FEATURES ======================
log(f"\n{'='*60}")
log(f"STEP 3 — Computing features...")
log(f"{'='*60}")

# Load model
model = joblib.load("intraday_quant_model.pkl")
log(f"Model loaded — expects {len(model.feature_names_in_)} features")

# Load history for rolling feature warmup
log("Loading 30-day history for feature warmup...")
history = pd.read_sql(
    'SELECT * FROM events WHERE "Datetime" > NOW() - INTERVAL \'30 days\'',
    engine
)
history["Datetime"] = pd.to_datetime(history["Datetime"])
log(f"History rows: {len(history)}")

# Combine history + new gap data
df_all = pd.concat([history, df_gap], ignore_index=True)
df_all = df_all.sort_values(["Stock", "Datetime"]).drop_duplicates(
    subset=["Stock", "Datetime"]
).reset_index(drop=True)
log(f"Combined rows: {len(df_all)}")

# Init columns
df_all["Sentiment"] = 0.0
df_all["Date"]      = pd.to_datetime(df_all["Datetime"]).dt.date

for col in ["SP500_return","NASDAQ_return","CRUDE_return","USDINR_return",
            "NiftyMomentum","BankNiftyMomentum"]:
    if col not in df_all.columns:
        df_all[col] = 0.0

# ====================== YAHOO FEATURES ======================
import yfinance as yf

def fetch_yahoo_safe(ticker, name, start, periods=1, interval="5m"):
    try:
        data = pd.DataFrame()
        for attempt in range(3):
            data = yf.download(ticker, start=start, interval=interval, progress=False)
            if not data.empty:
                break
            time.sleep(3)

        if data.empty:
            log(f"{name}: Yahoo empty")
            return None

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        data = data.reset_index()

        # Safe datetime column detection
        if "Datetime" not in data.columns:
            dt_col = None
            for col in data.columns:
                try:
                    sample = pd.to_datetime(data[col].iloc[0], utc=True)
                    if sample.year >= 2020 and (sample.hour != 0 or sample.minute != 0):
                        dt_col = col
                        break
                except:
                    continue
            if dt_col is None:
                log(f"{name}: No datetime column found")
                return None
            data = data.rename(columns={dt_col: "Datetime"})

        data["Datetime"] = (
            pd.to_datetime(data["Datetime"], utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
            .dt.floor("5min")
        )
        data = data.sort_values("Datetime")
        data[name] = data["Close"].pct_change(periods).replace([np.inf,-np.inf],0).fillna(0)

        # Validate non-zero
        non_zero = data[name][data[name] != 0]
        if non_zero.empty:
            log(f"{name}: All zeros — skipping")
            return None

        log(f"{name}: fetched ok")
        return data

    except Exception as e:
        log(f"{name}: {e}")
        return None

# Use gap date range for Yahoo
yahoo_start = min(gap_dates) - timedelta(days=2)
yahoo_start_str = yahoo_start.strftime("%Y-%m-%d")

macro_tickers = {
    "SP500_return":  ("^GSPC",  1),
    "NASDAQ_return": ("^IXIC",  1),
    "CRUDE_return":  ("CL=F",   1),
    "USDINR_return": ("USDINR=X", 1),
}

for name, (ticker, periods) in macro_tickers.items():
    data = fetch_yahoo_safe(ticker, name, yahoo_start_str, periods=periods)
    if name in df_all.columns:
        df_all = df_all.drop(columns=[name])
    if data is not None:
        data["Date"] = data["Datetime"].dt.date
        data = data.sort_values("Datetime").groupby("Date").last().reset_index()
        df_all = df_all.merge(data[["Date", name]], on="Date", how="left")
        df_all[name] = df_all[name].ffill().fillna(0)

index_tickers = {
    "NiftyMomentum":     ("^NSEI",    10),
    "BankNiftyMomentum": ("^NSEBANK", 10),
}

for name, (ticker, periods) in index_tickers.items():
    data = fetch_yahoo_safe(ticker, name, yahoo_start_str, periods=periods)
    if name in df_all.columns:
        df_all = df_all.drop(columns=[name])
    if data is not None:
        # Floor both sides before 5-min merge
        data["Datetime"] = data["Datetime"].dt.floor("5min")
        df_all["Datetime_floor"] = pd.to_datetime(df_all["Datetime"]).dt.floor("5min")
        merged = df_all.merge(
            data[["Datetime", name]],
            left_on="Datetime_floor",
            right_on="Datetime",
            how="left",
            suffixes=("","_yahoo")
        )
        df_all[name] = merged[name].values
        df_all[name] = df_all[name].ffill().fillna(0)
        df_all.drop(columns=["Datetime_floor"], inplace=True, errors="ignore")

# ====================== FEATURE ENGINEERING ======================
log("Computing features...")
df_all = df_all.sort_values(["Stock","Datetime"]).reset_index(drop=True)

# Returns
df_all["Return"]     = df_all.groupby("Stock")["Close"].pct_change()
df_all["Momentum5"]  = df_all.groupby("Stock")["Close"].pct_change(5)
df_all["Momentum15"] = df_all.groupby("Stock")["Close"].pct_change(15)
df_all["Momentum30"] = df_all.groupby("Stock")["Close"].pct_change(30)
df_all["Momentum60"] = df_all.groupby("Stock")["Close"].pct_change(60)
df_all["Trend3"]     = df_all.groupby("Stock")["Close"].pct_change(3)

# Volatility
df_all["Volatility15"] = df_all.groupby("Stock")["Close"].pct_change().rolling(15).std()
df_all["Volatility60"] = df_all.groupby("Stock")["Close"].pct_change().rolling(60).std()

# Range
df_all["Range15"] = (
    df_all.groupby("Stock")["High"].rolling(15).max().reset_index(level=0, drop=True)
    - df_all.groupby("Stock")["Low"].rolling(15).min().reset_index(level=0, drop=True)
) / df_all["Close"]

# Liquidity vacuum
df_all["LiquidityVacuum"] = (
    df_all["Range15"] /
    (df_all.groupby("Stock")["Volume"].rolling(20).mean().reset_index(level=0, drop=True) + 1e-6)
)

# Volatility regime
df_all["VolatilityRegime"] = df_all["Volatility15"] / (df_all["Volatility60"] + 1e-6)

# Orderflow
df_all["BuyPressure"]        = (df_all["Close"] - df_all["Low"])  / (df_all["High"] - df_all["Low"] + 1e-6)
df_all["SellPressure"]       = (df_all["High"]  - df_all["Close"]) / (df_all["High"] - df_all["Low"] + 1e-6)
df_all["OrderflowImbalance"] = df_all["BuyPressure"] - df_all["SellPressure"]

# Volume
df_all["VolumeSpike"] = (
    df_all["Volume"] /
    df_all.groupby("Stock")["Volume"].rolling(15).mean().reset_index(level=0, drop=True)
)
df_all["VolumeShock"] = (
    df_all["Volume"] /
    (df_all.groupby("Stock")["Volume"].rolling(30).mean().reset_index(level=0, drop=True) + 1e-6)
)

# VWAP daily reset
df_all["VWAP"] = (
    (df_all["Close"] * df_all["Volume"])
    .groupby([df_all["Stock"], df_all["Datetime"].dt.date]).cumsum()
    / df_all["Volume"]
    .groupby([df_all["Stock"], df_all["Datetime"].dt.date]).cumsum()
)
df_all["VWAPDeviation"] = (df_all["Close"] - df_all["VWAP"]) / df_all["VWAP"]
df_all["VWAPMomentum"]  = df_all["VWAPDeviation"] - df_all.groupby("Stock")["VWAPDeviation"].shift(3)

df_all["Acceleration"] = (
    df_all.groupby("Stock")["Close"].pct_change(5) -
    df_all.groupby("Stock")["Close"].pct_change(15)
)
df_all["RelVolume"] = (
    df_all["Volume"] /
    df_all.groupby("Stock")["Volume"].rolling(50).mean().reset_index(level=0, drop=True)
)
df_all["TimeBlock"] = df_all["Datetime"].dt.hour * 60 + df_all["Datetime"].dt.minute

# ORB fixed
df_all["TradeDate"]  = df_all["Datetime"].dt.date
df_all["MarketOpen"] = df_all["Datetime"].dt.time <= pd.to_datetime("09:45").time()
orb = (
    df_all[df_all["MarketOpen"]]
    .groupby(["Stock","TradeDate"])
    .agg(orb_high=("High","max"), orb_low=("Low","min"))
    .reset_index()
)
df_all = df_all.merge(orb, on=["Stock","TradeDate"], how="left")
df_all["ORBStrength"] = (df_all["Close"] - df_all["orb_high"]) / df_all["orb_high"]
df_all["ORBWeakness"] = (df_all["Close"] - df_all["orb_low"])  / df_all["orb_low"]
df_all.drop(columns=["orb_high","orb_low"], inplace=True)

# Market breadth
df_all["UpStock"]               = (df_all["Return"] > 0).astype(int)
df_all["MarketBreadth"]         = df_all.groupby("Datetime")["UpStock"].transform("mean")
df_all["MarketBreadthPressure"] = df_all["MarketBreadth"]
df_all["LagMomentum"]           = df_all.groupby("Stock")["Close"].pct_change(3).shift(2)

# RelativeRank fixed
returns_15             = df_all.groupby("Stock")["Close"].pct_change(15)
df_all["RelativeRank"] = returns_15.groupby(df_all["Datetime"]).rank(pct=True)

# Sweeps
lookback = 20
df_all["RecentHigh"] = df_all.groupby("Stock")["High"].transform(
    lambda x: x.rolling(lookback, min_periods=lookback).max())
df_all["RecentLow"] = df_all.groupby("Stock")["Low"].transform(
    lambda x: x.rolling(lookback, min_periods=lookback).min())
df_all["HighSweep"] = (
    (df_all["High"]  > df_all["RecentHigh"].shift(1)) &
    (df_all["Close"] < df_all["RecentHigh"].shift(1))
).astype(int)
df_all["LowSweep"] = (
    (df_all["Low"]   < df_all["RecentLow"].shift(1)) &
    (df_all["Close"] > df_all["RecentLow"].shift(1))
).astype(int)
df_all["SweepStrength"]    = (df_all["High"] - df_all["Low"]) / df_all["Close"]
df_all["RecentHighSweeps"] = df_all.groupby("Stock")["HighSweep"].transform(lambda x: x.rolling(10).sum())
df_all["RecentLowSweeps"]  = df_all.groupby("Stock")["LowSweep"].transform(lambda x: x.rolling(10).sum())

# EventTrigger — fixed with daily reset for MomentumEvent
df_all["VolAvg20"]    = df_all.groupby("Stock")["Volume"].rolling(20).mean().reset_index(level=0, drop=True)
df_all["VolumeEvent"] = (df_all["Volume"] > df_all["VolAvg20"] * 1.5).astype(int)
df_all["MomentumEvent"] = (
    df_all.groupby(["Stock", df_all["Datetime"].dt.date])["Close"]
    .pct_change(10).abs().gt(0.003).astype(int)
)
df_all["SweepEvent"]   = ((df_all["HighSweep"]==1) | (df_all["LowSweep"]==1)).astype(int)
df_all["EventTrigger"] = (
    (df_all["VolumeEvent"]==1) |
    (df_all["MomentumEvent"]==1) |
    (df_all["SweepEvent"]==1)
).astype(int)

# Sector
sector_map = {
    "TCS":"IT","INFY":"IT","HCLTECH":"IT","WIPRO":"IT","TECHM":"IT",
    "LTIM":"IT","PERSISTENT":"IT","MPHASIS":"IT","COFORGE":"IT",
    "HDFCBANK":"BANK","ICICIBANK":"BANK","SBIN":"BANK","AXISBANK":"BANK","KOTAKBANK":"BANK",
    "BAJFINANCE":"FINANCE","BAJAJFINSV":"FINANCE","ICICIPRULI":"FINANCE",
    "SBILIFE":"FINANCE","HDFCLIFE":"FINANCE","BAJAJHLDNG":"FINANCE",
    "RELIANCE":"ENERGY","ONGC":"ENERGY","GAIL":"ENERGY","PETRONET":"ENERGY",
    "IGL":"ENERGY","MGL":"ENERGY","COALINDIA":"ENERGY","ADANIGREEN":"ENERGY",
    "TATASTEEL":"METAL","JSWSTEEL":"METAL","HINDALCO":"METAL",
    "SUNPHARMA":"PHARMA","DRREDDY":"PHARMA","CIPLA":"PHARMA",
    "DIVISLAB":"PHARMA","TORNTPHARM":"PHARMA","ALKEM":"PHARMA",
    "APOLLOHOSP":"PHARMA","MAXHEALTH":"PHARMA","ZYDUSLIFE":"PHARMA",
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG",
    "BRITANNIA":"FMCG","DABUR":"FMCG","GODREJCP":"FMCG","COLPAL":"FMCG","MARICO":"FMCG",
    "LT":"INFRA","NTPC":"INFRA","POWERGRID":"INFRA","BHEL":"INFRA",
    "SIEMENS":"INFRA","ABB":"INFRA","POLYCAB":"INFRA",
    "MARUTI":"AUTO","ADANIENT":"CONGLOMERATE","ADANIPORTS":"CONGLOMERATE",
    "GRASIM":"DIVERSIFIED","ULTRACEMCO":"CEMENT",
    "BHARTIARTL":"TELECOM","INDUSTOWER":"TELECOM","TATACOMM":"TELECOM",
    "ASIANPAINT":"PAINTS","TITAN":"CONSUMER","TRENT":"CONSUMER",
    "NYKAA":"CONSUMER","PAGEIND":"CONSUMER","VOLTAS":"CONSUMER",
    "DLF":"REALTY","LODHA":"REALTY","OBEROIRLTY":"REALTY",
    "INDIGO":"AVIATION","IRCTC":"TRAVEL","ZOMATO":"CONSUMER",
    "PAYTM":"FINTECH","NAUKRI":"TECH","BEL":"DEFENCE","HAL":"DEFENCE",
    "SRF":"CHEMICAL","PIIND":"CHEMICAL","DEEPAKNTR":"CHEMICAL","AARTIIND":"CHEMICAL",
}
df_all["Sector"]                 = df_all["Stock"].map(sector_map).fillna("OTHER")
df_all["SectorMomentum"]         = df_all.groupby(["Sector","Datetime"])["Return"].transform("mean")
df_all["RelativeStrengthSector"] = df_all["Return"] - df_all["SectorMomentum"]

# Cross sectional
returns10                             = df_all.groupby("Stock")["Close"].pct_change(10)
df_all["PeerMomentum"]                = returns10.groupby(df_all["Datetime"]).transform("mean")
df_all["RelativeStrengthMarketIndia"] = df_all["Return"] - df_all["NiftyMomentum"]
df_all["RelativeStrengthMarketUS"]    = df_all["Return"] - df_all["SP500_return"]

# Cleanup
df_all = df_all.replace([np.inf, -np.inf], np.nan)
market_cols = ["NiftyMomentum","BankNiftyMomentum","SP500_return","NASDAQ_return","CRUDE_return","USDINR_return"]
df_all[market_cols] = df_all[market_cols].ffill().fillna(0)
drop_cols = [c for c in df_all.columns if c.endswith("_x") or c.endswith("_y")]
df_all.drop(columns=drop_cols, inplace=True, errors="ignore")

for f in model.feature_names_in_:
    if f not in df_all.columns:
        df_all[f] = 0
df_all[model.feature_names_in_] = df_all[model.feature_names_in_].fillna(0)

# ====================== MODEL PREDICTION ======================
log("Running predictions on gap data...")
try:
    df_event = df_all[df_all["EventTrigger"] == 1].copy()
    X = df_event[model.feature_names_in_].apply(pd.to_numeric, errors="coerce").ffill().fillna(0)
    df_event["Pred"] = model.predict_proba(X)[:, 1]

    # Only keep the gap candles (not existing history)
    gap_datetimes = set(zip(df_gap["Stock"], df_gap["Datetime"]))
    mask = df_event.apply(lambda r: (r["Stock"], r["Datetime"]) in gap_datetimes, axis=1)
    df_insert = df_event[mask].copy()
    log(f"Gap rows with predictions: {len(df_insert)}")
except Exception as e:
    log(f"Prediction error: {e}")
    # Fall back to inserting without Pred
    df_insert = df_gap.copy()
    df_insert["Pred"] = 0

# ====================== STEP 4: INSERT ======================
log(f"\n{'='*60}")
log(f"STEP 4 — Inserting gap data into DB...")
log(f"{'='*60}")

# Match DB schema
try:
    table_columns = pd.read_sql("SELECT * FROM events LIMIT 1", engine).columns
    df_insert = df_insert.loc[:, df_insert.columns.intersection(table_columns)]
except Exception as e:
    log(f"Schema match error: {e}")

# Clean
df_insert = df_insert[df_insert["Datetime"].notna()]
df_insert = df_insert.drop_duplicates(subset=["Stock","Datetime"]).reset_index(drop=True)
df_insert = df_insert.replace([np.inf, -np.inf], 0).fillna(0)

if df_insert.empty:
    log("Nothing to insert after filtering")
    sys.exit(0)

log(f"Inserting {len(df_insert)} rows...")

# on_conflict_do_nothing — safe to run multiple times
try:
    meta = MetaData()
    meta.reflect(bind=engine)
    events_table = meta.tables["events"]

    inserted = 0
    skipped  = 0

    with engine.connect() as conn:
        for i in range(0, len(df_insert), 200):
            chunk = df_insert.iloc[i:i+200].to_dict(orient="records")
            stmt  = pg_insert(events_table).values(chunk)
            stmt  = stmt.on_conflict_do_nothing(
                index_elements=["Stock", "Datetime"]
            )
            result = conn.execute(stmt)
            conn.commit()
            inserted += result.rowcount
            skipped  += len(chunk) - result.rowcount

    log(f"✅ Inserted: {inserted} | Skipped (already existed): {skipped}")

except Exception as e:
    log(f"Insert error: {e}")

# ====================== STEP 5: VERIFY ======================
log(f"\n{'='*60}")
log(f"STEP 5 — Verifying gaps are filled...")
log(f"{'='*60}")

verify = pd.read_sql(
    f"""
    SELECT "Stock",
           DATE("Datetime") as trade_date,
           COUNT(*) as candle_count,
           MAX("Datetime"::time) as last_candle
    FROM events
    WHERE "Datetime" >= '{start_scan}'
    GROUP BY "Stock", DATE("Datetime")
    """,
    engine
)
verify["trade_date"] = pd.to_datetime(verify["trade_date"]).dt.date

still_missing = []
for _, row in gap_df.iterrows():
    match = verify[
        (verify["Stock"] == row["stock"]) &
        (verify["trade_date"] == row["date"])
    ]
    if match.empty or match.iloc[0]["candle_count"] < MIN_CANDLES:
        still_missing.append(row)

if still_missing:
    log(f"⚠️  Still missing after fill: {len(still_missing)} instances")
    for g in still_missing[:10]:
        log(f"   {g['stock']} {g['date']}")
else:
    log(f"✅ All gaps successfully filled!")

log(f"\nGap filler complete.")
