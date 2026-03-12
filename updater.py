import pandas as pd
from sqlalchemy import create_engine
from datetime import date
import time
from datetime import datetime, timezone, timedelta
import sys

# ==================== TEST MODE (for easy testing) ====================
TEST_MODE = True   # ← Change to False after testing
# =================================================================

CONNECTION_STRING = "NEON_URL"

engine = create_engine(CONNECTION_STRING)

utc_now = datetime.now(timezone.utc)
ist_now = utc_now + timedelta(hours=5, minutes=30)

if ist_now.weekday() >= 5 and not TEST_MODE:
    print("Weekend - skipping updater")
    sys.exit(0)

ist_hour = ist_now.hour
ist_minute = ist_now.minute

# Run task until 3:30 PM IST
if not TEST_MODE and not (9 <= ist_hour < 15 or (ist_hour == 15 and ist_minute <= 30)):
    print(f"Outside market hours (9:15 AM - 3:30 PM IST) - current IST: {ist_now.strftime('%H:%M')} - skipping")
    sys.exit(0)

print(f"🔄 Running 5-min updater at IST {ist_now.strftime('%H:%M')}...")

# Strategy performance — only until 1:30 PM (once per day)
if TEST_MODE or (ist_hour < 13 or (ist_hour == 13 and ist_minute <= 30)):
    today = date.today()
    already_done = pd.read_sql(f"SELECT 1 FROM strategy_performance WHERE date = '{today}' LIMIT 1", engine)

    if len(already_done) == 0:
        print("📊 Calculating today's strategy performance...")
        df_today = pd.read_sql(f"SELECT * FROM events WHERE DATE(\"Datetime\") = '{today}'", engine)
        
        if len(df_today) > 0:
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


print(f"✅ Updater finished at IST {ist_now.strftime('%H:%M')}")
