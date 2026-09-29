"""
AI Trade Advisor
────────────────
Drop-in page for the Alpha Terminal Streamlit app.
Analyzes each signal using Claude + live news + historical performance.
Shows GREEN / YELLOW / RED with reasoning.
Monitors open trades and alerts on adverse moves.

Setup:
  1. pip install anthropic  (add to requirements.txt)
  2. Add ANTHROPIC_API_KEY to Streamlit secrets
  3. Import and call render_ai_advisor(engine, TODAY, MARKET_OPEN, get_trades, pnl)
"""

import json
import re
import time
import uuid
import anthropic
import streamlit as st
import pandas as pd
import feedparser
from datetime import datetime, timedelta, date
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed


# ── NSE ticker map (Yahoo uses .NS suffix) ───────────────────
NSE_SUFFIX = ".NS"

SECTOR_MAP = {
    "TCS":"IT","INFY":"IT","HCLTECH":"IT","WIPRO":"IT","TECHM":"IT",
    "LTIM":"IT","PERSISTENT":"IT","MPHASIS":"IT","COFORGE":"IT",
    "HDFCBANK":"Banking","ICICIBANK":"Banking","SBIN":"Banking",
    "AXISBANK":"Banking","KOTAKBANK":"Banking",
    "BAJFINANCE":"Finance","BAJAJFINSV":"Finance",
    "RELIANCE":"Energy","ONGC":"Energy","GAIL":"Energy",
    "NTPC":"Power","POWERGRID":"Power","ADANIGREEN":"Renewables",
    "TATASTEEL":"Metals","JSWSTEEL":"Metals","HINDALCO":"Metals",
    "SUNPHARMA":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma",
    "DIVISLAB":"Pharma","APOLLOHOSP":"Healthcare",
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG",
    "MARUTI":"Auto","LT":"Infra","SIEMENS":"Capital Goods",
    "HAL":"Defence","BEL":"Defence",
    "DLF":"Realty","LODHA":"Realty","OBEROIRLTY":"Realty",
    "ZOMATO":"Consumer Tech","PAYTM":"Fintech","NAUKRI":"Tech",
    "INDIGO":"Aviation","IRCTC":"Travel",
    "SRF":"Chemicals","DEEPAKNTR":"Chemicals","AARTIIND":"Chemicals",
}


# ── Point-in-time news fetcher ───────────────────────────────
# News is tied to the signal timestamp, not simply the calendar date.
# For a 09:15 signal, the relevant window starts at the previous
# NSE trading day's 15:30 close and ends at the signal timestamp.

def _to_ist(ts=None) -> pd.Timestamp:
    if ts is None:
        return pd.Timestamp.now(tz="Asia/Kolkata")
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("Asia/Kolkata")
    return t.tz_convert("Asia/Kolkata")


def _previous_nse_close(cutoff: pd.Timestamp) -> pd.Timestamp:
    d = cutoff.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return pd.Timestamp(f"{d} 15:30:00", tz="Asia/Kolkata")


def _parse_feed_time(entry):
    # feedparser's published_parsed is a UTC struct_time when available.
    try:
        if entry.get("published_parsed"):
            import calendar
            return pd.Timestamp(calendar.timegm(entry.published_parsed), unit="s", tz="UTC").tz_convert("Asia/Kolkata")
    except Exception:
        pass
    try:
        raw = entry.get("published") or entry.get("updated")
        if raw:
            return _to_ist(pd.to_datetime(raw, utc=True))
    except Exception:
        pass
    return None


@st.cache_data(ttl=300)
def fetch_news(stock: str, analysis_time=None) -> list[str]:
    """Fetch only news that existed inside the point-in-time window.

    Window: previous NSE trading-day 15:30 -> analysis_time.
    This captures overnight/pre-open news for a 09:15 signal and prevents
    later headlines from being fed into an earlier signal analysis.
    """
    cutoff = _to_ist(analysis_time)
    window_start = _previous_nse_close(cutoff)
    headlines = []

    # Google News supports date-bounded search queries. We also apply an
    # exact timestamp filter locally because the date operators are only
    # day-granular and RSS results can contain older/newer items.
    after_day = window_start.strftime("%Y-%m-%d")
    before_day = (cutoff + timedelta(days=1)).strftime("%Y-%m-%d")
    query = (
        f'"{stock}" AND (stock OR NSE OR share) '
        f'after:{after_day} before:{before_day}'
    )
    rss_url = (
        "https://news.google.com/rss/search"
        f"?q={query.replace(' ', '+')}"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    )

    try:
        feed = feedparser.parse(rss_url)
        candidates = []
        for entry in feed.entries:
            title = entry.get("title", "")
            published_ts = _parse_feed_time(entry)
            if not title or published_ts is None:
                continue
            if window_start <= published_ts <= cutoff:
                candidates.append((published_ts, title))

        # Newest first; keep the context compact for Claude.
        candidates.sort(key=lambda x: x[0], reverse=True)
        for published_ts, title in candidates[:10]:
            headlines.append(
                f"[{stock}] {title} | {published_ts.strftime('%Y-%m-%d %H:%M IST')}"
            )
    except Exception:
        pass

    if headlines:
        return headlines

    return [
        f"No timestamped news found for {stock} between "
        f"{window_start.strftime('%Y-%m-%d %H:%M IST')} and "
        f"{cutoff.strftime('%Y-%m-%d %H:%M IST')}"
    ]


# ── Historical performance from DB ──────────────────────────
@st.cache_data(ttl=600)
def fetch_stock_history(stock: str, side: str, _engine, analysis_time=None) -> str:
    try:
        cutoff_date = None

        if analysis_time is not None:
            cutoff_date = pd.to_datetime(analysis_time).date()

            history_date_filter = f"""
                AND (e."Datetime" AT TIME ZONE 'Asia/Kolkata')::date
                    >= ('{cutoff_date}'::date - INTERVAL '90 days')::date
                AND (e."Datetime" AT TIME ZONE 'Asia/Kolkata')::date
                    < '{cutoff_date}'::date
            """
        else:
            history_date_filter = """
                AND (e."Datetime" AT TIME ZONE 'Asia/Kolkata')::date
                    >= (CURRENT_DATE - INTERVAL '90 days')::date
            """
        sql = f"""
        WITH p AS (SELECT 0.62 AS pred_th, 0.65 AS rr_th, 0.00 AS nifty_th),
        sig AS (
            SELECT e."Stock", e."Datetime" AS entry_time,
                   e."Close" AS entry_price, e."Pred", e."RelativeRank",
                   CASE
                     WHEN e."Pred">=0.63 AND e."RelativeRank">=p.rr_th
                      AND e."NiftyMomentum">=p.nifty_th AND e."Momentum5">0.002
                      AND e."Datetime" AT TIME ZONE 'Asia/Kolkata'<=(DATE(e."Datetime")+'09:45:00'::time)::timestamp
                      AND e."LiquidityVacuum">0 AND e."Trend3">0
                      AND e."VolumeShock">1.1 AND e."Momentum60">-0.003
                      AND e."Stock" NOT IN ('DLF','HCLTECH','PIIND','COALINDIA','PAYTM','TATACOMM')
                     THEN 'LONG'
                     WHEN e."Pred"<=(1-p.pred_th) AND e."RelativeRank"<=(1-p.rr_th)
                      AND e."NiftyMomentum"<=-p.nifty_th AND e."RelativeRank">0.05
                      AND e."NiftyMomentum"<-0.002 AND e."Momentum5"<0
                      AND e."VolumeShock">0.7 AND e."RelativeRank"<0.31
                      AND e."Stock" NOT IN ('DLF','COALINDIA','PIIND','NAUKRI','WIPRO','GAIL','TATASTEEL','HDFCLIFE')
                     THEN 'SHORT'
                   END AS side
            FROM events e CROSS JOIN p
            WHERE e."Stock"='{stock}'
              {history_date_filter}
              AND e."Datetime" AT TIME ZONE 'Asia/Kolkata'>=(DATE(e."Datetime")+'09:15:00'::time)::timestamp
              AND e."Datetime" AT TIME ZONE 'Asia/Kolkata'<=(DATE(e."Datetime")+'10:15:00'::time)::timestamp
        ),
        fp AS (
            SELECT s.entry_time, s.side, s.entry_price,
                   e."Datetime" AS ft,
                   CASE WHEN s.side='LONG' THEN (e."Close"-s.entry_price)/s.entry_price
                        ELSE (s.entry_price-e."Close")/s.entry_price END AS fr
            FROM sig s JOIN events e ON s."Stock"=e."Stock"
              AND e."Datetime">=s.entry_time
              AND e."Datetime"<=(DATE(s.entry_time)+'15:05:00'::time)::timestamp
            WHERE s.side='{side}'
        ),
        threshold_events AS (
            SELECT DISTINCT ON (entry_time)
                   entry_time, fr,
                   CASE
                       WHEN fr >= 0.008 THEN 'TARGET'
                       WHEN fr <= -0.018 THEN 'STOP'
                   END AS outcome
            FROM fp
            WHERE fr >= 0.008 OR fr <= -0.018
            ORDER BY entry_time, ft
        ),
        ex AS (SELECT DISTINCT ON(entry_time) entry_time,fr FROM fp ORDER BY entry_time,ft DESC)
        SELECT DATE(sig.entry_time) as dt,
               CASE WHEN te.outcome='TARGET' THEN 0.008
                    WHEN te.outcome='STOP' THEN -0.018
                    ELSE COALESCE(ex.fr,0) END AS ret,
               COALESCE(te.outcome, 'EOD') AS outcome
        FROM sig
        LEFT JOIN threshold_events te ON sig.entry_time=te.entry_time
        LEFT JOIN ex ON sig.entry_time=ex.entry_time
        WHERE sig.side='{side}'
        ORDER BY sig.entry_time DESC LIMIT 15
        """
        hist = pd.read_sql(sql, _engine)
        if hist.empty:
            return f"No historical {side} trades for {stock} in last 90 days."

        wins      = (hist['ret'] > 0).sum()
        tgt_hits  = (hist['outcome'] == 'TARGET').sum()
        stops     = (hist['outcome'] == 'STOP').sum()
        n         = len(hist)
        win_rate  = wins / n if n else 0
        tgt_rate  = tgt_hits / n if n else 0
        avg_ret   = hist['ret'].mean() * 100
        total_ret = hist['ret'].sum() * 100

        # Simple status label (same spirit as your Stock Ledger)
        if n < 3:
            status = "THIN DATA"
        elif win_rate >= 0.70 and total_ret > 0:
            status = "KEEP"
        elif stops >= 3 or (win_rate < 0.50 and total_ret < 0):
            status = "BLACKLIST?"
        else:
            status = "WATCH"

        lines = [
            f"STOCK EDGE CARD — {stock} | {side}",
            f"Trades: {n} | Win Rate: {win_rate*100:.0f}% | Target Hit: {tgt_rate*100:.0f}% | Stops: {stops}",
            f"Avg Return: {avg_ret:+.3f}% | Total Return (sample): {total_ret:+.2f}%",
            f"Status: {status}",
            "",
            "Recent trades:",
        ]
        for _, r in hist.head(8).iterrows():
            icon = "✓" if r['ret'] > 0 else "✗"
            lines.append(f"  {icon} {r['dt']} → {r['ret']*100:+.2f}% ({r['outcome']})")
        return "\n".join(lines)

    except Exception as e:
        return f"Could not fetch history: {e}"


# ── Claude API call ──────────────────────────────────────────
@st.cache_data(ttl=180)   # cache 3 mins per signal
def analyze_signal(
    stock: str, side: str,
    pred: float, rr: float, nifty_mom: float,
    vol_shock: float, mom5: float,
    history_text: str, news_headlines: list[str],
    current_pnl: float | None = None,
    analysis_time=None,
    historical_mode: bool = False,
    enable_search: bool = True
) -> dict:

    api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _fallback("ANTHROPIC_API_KEY not set in Streamlit secrets.")

    ai = anthropic.Anthropic(api_key=api_key)
    news_text = "\n".join(f"• {h}" for h in news_headlines)
    sector    = SECTOR_MAP.get(stock, "unknown sector")

    point_in_time_signal = analysis_time is not None and current_pnl is None

    if historical_mode and analysis_time is not None:
        cutoff_text = _to_ist(analysis_time).strftime("%Y-%m-%d %H:%M:%S")
        research_block = f"""
NEWS RESEARCH MODE: HISTORICAL POINT-IN-TIME
Trade timestamp / information cutoff: {cutoff_text} Asia/Kolkata.

All supplied historical performance data and supplied headlines are cutoff-filtered.
Use ONLY information that was publicly available on or before the cutoff.
Do not use or infer any price movement, trade outcome, news, announcement, or market information that occurred after the cutoff.
Treat the cutoff as a strict information boundary.
"""
    elif point_in_time_signal:
        cutoff_text = _to_ist(analysis_time).strftime("%Y-%m-%d %H:%M:%S")
        research_block = f"""
NEWS RESEARCH MODE: POINT-IN-TIME LIVE SIGNAL
Information cutoff: {cutoff_text} Asia/Kolkata.

This is the information set that should have been available when the signal fired.
Use the supplied timestamped news only up to the cutoff. Do NOT use later headlines,
announcements, market moves, or other information published after the cutoff.
If web search is available, restrict searches to information published on or before the cutoff.
"""
    else:
        cutoff_text = "CURRENT"
        research_block = """
NEWS RESEARCH MODE: CURRENT MONITOR
Search the web for the latest available news and developments affecting this stock, its sector, and the broader NSE/Nifty market.
Prioritize recent company-specific and market-moving information.
"""

    monitor_block = ""
    if current_pnl is not None:
        status = "WINNING" if current_pnl > 0 else "LOSING"
        monitor_block = (
            f"\nACTIVE TRADE STATUS:\n"
            f"Current P&L: {current_pnl*100:+.3f}% — trade is {status}.\n"
            f"Decide whether to HOLD, WAIT, or EXIT based on market shift."
        )

    search_instruction = (
        "Use Claude web search to independently research relevant current/historical news before deciding."
        if enable_search else
        "Do NOT use web search. Base the analysis only on the supplied signal, historical performance, and supplied headlines."
    )

    prompt = f"""You are a sharp intraday trading advisor for NSE India.

SIGNAL
Stock : {stock}  ({sector})
Side  : {side}
Pred  : {pred:.3f}  (LONG threshold ≥0.63 | SHORT ≤0.38)
Rel Rank : {rr:.3f}
Nifty Mom: {nifty_mom:.4f}
Vol Shock: {vol_shock:.2f}×
5-min Mom: {mom5*100:+.3f}%
{monitor_block}

HISTORICAL PERFORMANCE
{history_text}

{research_block}

HEADLINES FROM GOOGLE NEWS (supporting input; verify important items with web search)
{news_text}

{search_instruction}
News is supporting evidence only; do not invent technical evidence or override the quantitative signal without a clear reason.

Reply with ONLY one complete, valid JSON object.
Do not use markdown or code fences.
Do not write anything before or after the JSON.
Do not truncate any string or field.
Make sure the JSON ends with the final closing brace.
{{
  "signal": "GREEN" | "YELLOW" | "RED",
  "confidence": <integer 0-100>,
  "target_pct": <float, default 0.008 for +0.8%>,
  "stop_loss_pct": <float, default -0.018 for -1.8%>,
  "suggested_position_size": "FULL" | "HALF" | "SKIP",
  "headline": "<one sharp sentence>",
  "reason": "<2-3 sentences: signal quality + news + history>",
  "risk_factors": ["<risk1>", "<risk2>"],
  "news_sentiment": "POSITIVE" | "NEUTRAL" | "NEGATIVE",
  "action": "<TAKE TRADE | WAIT FOR CONFIRMATION | SKIP | EXIT NOW>"
}}

Signal logic:
GREEN  — take the trade, conditions aligned
YELLOW — uncertain, wait or reduce size
RED    — avoid, or exit immediately if already in trade

STOCK EDGE RULES (use the HISTORICAL PERFORMANCE card above):
1. Stock historical edge is the most important factor after signal quality.
2. Status KEEP + solid signal (Pred/RR/VolumeShock aligned) → prefer GREEN / TAKE TRADE / FULL.
3. Status WATCH → prefer YELLOW / HALF or WAIT FOR CONFIRMATION unless signal is very strong.
4. Status BLACKLIST? or poor win rate / many stops → prefer RED / SKIP even if Pred looks good.
5. Status THIN DATA → be cautious; do not size FULL unless signal + news are both strong.
6. Never override a chronically weak stock history only because Pred is high.
7. In reason, briefly mention the stock's historical edge (win rate / status)."""

    try:
        request_kwargs = {
            "model": "claude-opus-5-5",
            "max_tokens": 2048,
            "messages": [{"role": "user", "content": prompt}],
        }
        # Web search is allowed for a genuinely live monitor and for a signal
        # that is effectively being analyzed at the moment it fired. For a
        # past signal, do not let a modern web result contaminate the point-in-
        # time analysis; the timestamp-filtered RSS news is the source of truth.
        web_search_allowed = bool(enable_search)
        if point_in_time_signal and analysis_time is not None:
            cutoff = _to_ist(analysis_time)
            now_ist = _to_ist()
            web_search_allowed = web_search_allowed and abs((now_ist - cutoff).total_seconds()) <= 60
        if historical_mode:
            web_search_allowed = False

        if web_search_allowed:
            request_kwargs["tools"] = [{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 3
            }]

        resp = ai.messages.create(**request_kwargs)
        text_blocks = [
            block.text for block in resp.content
            if getattr(block, "type", "") == "text" and getattr(block, "text", "")
        ]
        raw = text_blocks[-1].strip() if text_blocks else ""
        # strip markdown fences if present
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()

        # Claude can occasionally add text before/after the JSON payload.
        # Extract the JSON object before parsing so that harmless extra text
        # does not turn a valid analysis into a fallback/error.
        json_match = re.search(r"\{.*\}", raw, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))
        return json.loads(raw)
    except Exception as e:
        return _fallback(str(e))


def _fallback(reason: str) -> dict:
    return {
        "signal": "YELLOW",
        "confidence": 50,
        "target_pct": 0.008,
        "stop_loss_pct": -0.018,
        "suggested_position_size": "SKIP",
        "headline": "AI analysis unavailable",
        "reason": reason,
        "risk_factors": ["Manual review needed"],
        "news_sentiment": "NEUTRAL",
        "action": "USE OWN JUDGMENT",
    }



# ── Persistent AI analysis logging ──────────────────────────

def _ensure_ai_log_table(engine):
    """Create the persistent AI-analysis history table if it does not exist."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ai_advisor_log (
                    id BIGSERIAL PRIMARY KEY,
                    analysis_type VARCHAR(40) NOT NULL,
                    analysis_key TEXT NOT NULL,
                    analysis_time TIMESTAMPTZ,
                    signal_time TIMESTAMPTZ,
                    stock VARCHAR(40),
                    side VARCHAR(10),
                    entry_price DOUBLE PRECISION,
                    current_pnl DOUBLE PRECISION,
                    pred DOUBLE PRECISION,
                    relative_rank DOUBLE PRECISION,
                    nifty_momentum DOUBLE PRECISION,
                    volume_shock DOUBLE PRECISION,
                    momentum5 DOUBLE PRECISION,
                    ai_signal VARCHAR(20),
                    confidence INTEGER,
                    target_pct DOUBLE PRECISION,
                    stop_loss_pct DOUBLE PRECISION,
                    suggested_position_size VARCHAR(20),
                    headline TEXT,
                    reason TEXT,
                    risk_factors TEXT,
                    news_sentiment VARCHAR(20),
                    action TEXT,
                    historical_mode BOOLEAN DEFAULT FALSE,
                    enable_search BOOLEAN DEFAULT FALSE,
                    model VARCHAR(100),
                    news_headlines TEXT,
                    history_text TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            # Morning analyses must be idempotent across Streamlit refreshes.
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_ai_advisor_morning_key
                ON ai_advisor_log (analysis_key)
                WHERE analysis_type = 'MORNING_SIGNAL'
            """))
        return True
    except Exception as e:
        st.warning(f"AI log table unavailable: {e}")
        return False


def _log_ai_result(
    engine, analysis_type: str, analysis_key: str, row: pd.Series,
    analysis: dict, analysis_time=None, current_pnl=None,
    historical_mode=False, enable_search=False, news_headlines=None,
    history_text=""
):
    """Persist one AI result. Manual analyses are intentionally logged per click."""
    try:
        signal_time = row.get("entry_time")
        analysis_ts = _to_ist(analysis_time if analysis_time is not None else None).to_pydatetime()
        signal_ts = _to_ist(signal_time).to_pydatetime() if pd.notna(signal_time) else None
        risks = analysis.get("risk_factors", [])
        if isinstance(risks, str):
            risks = [risks]

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ai_advisor_log (
                    analysis_type, analysis_key, analysis_time, signal_time,
                    stock, side, entry_price, current_pnl, pred, relative_rank,
                    nifty_momentum, volume_shock, momentum5, ai_signal, confidence,
                    target_pct, stop_loss_pct, suggested_position_size, headline,
                    reason, risk_factors, news_sentiment, action, historical_mode,
                    enable_search, model, news_headlines, history_text
                ) VALUES (
                    :analysis_type, :analysis_key, :analysis_time, :signal_time,
                    :stock, :side, :entry_price, :current_pnl, :pred, :relative_rank,
                    :nifty_momentum, :volume_shock, :momentum5, :ai_signal, :confidence,
                    :target_pct, :stop_loss_pct, :suggested_position_size, :headline,
                    :reason, :risk_factors, :news_sentiment, :action, :historical_mode,
                    :enable_search, :model, :news_headlines, :history_text
                )
            """), {
                "analysis_type": analysis_type,
                "analysis_key": analysis_key,
                "analysis_time": analysis_ts,
                "signal_time": signal_ts,
                "stock": str(row.get("Stock", "")),
                "side": str(row.get("side", "")),
                "entry_price": float(row.get("entry_price", 0) or 0),
                "current_pnl": current_pnl,
                "pred": float(row.get("Pred", 0) or 0),
                "relative_rank": float(row.get("RelativeRank", 0) or 0),
                "nifty_momentum": float(row.get("NiftyMomentum", 0) or 0),
                "volume_shock": float(row.get("VolumeShock", 1) or 1),
                "momentum5": float(row.get("Momentum5", 0) or 0),
                "ai_signal": str(analysis.get("signal", "YELLOW")),
                "confidence": int(float(analysis.get("confidence", 50) or 50)),
                "target_pct": float(analysis.get("target_pct", 0.008) or 0.008),
                "stop_loss_pct": float(analysis.get("stop_loss_pct", -0.018) or -0.018),
                "suggested_position_size": str(analysis.get("suggested_position_size", "SKIP")),
                "headline": str(analysis.get("headline", "")),
                "reason": str(analysis.get("reason", "")),
                "risk_factors": json.dumps(risks, ensure_ascii=False),
                "news_sentiment": str(analysis.get("news_sentiment", "NEUTRAL")),
                "action": str(analysis.get("action", "")),
                "historical_mode": bool(historical_mode),
                "enable_search": bool(enable_search),
                "model": "claude-opus-5-5",
                "news_headlines": json.dumps(news_headlines or [], ensure_ascii=False),
                "history_text": str(history_text or ""),
            })
        return True
    except Exception as e:
        # Do not hide a successful AI result just because logging failed.
        st.warning(f"AI result could not be logged: {e}")
        return False


def _get_logged_analysis(engine, analysis_key: str):
    """Load the most recent saved analysis for a persistent morning key."""
    try:
        q = text("""
            SELECT ai_signal, confidence, target_pct, stop_loss_pct,
                   suggested_position_size, headline, reason, risk_factors,
                   news_sentiment, action
            FROM ai_advisor_log
            WHERE analysis_key = :key AND analysis_type = 'MORNING_SIGNAL'
            ORDER BY created_at DESC
            LIMIT 1
        """)
        with engine.connect() as conn:
            r = conn.execute(q, {"key": analysis_key}).mappings().first()
        if not r:
            return None
        risks = r.get("risk_factors") or "[]"
        try:
            risks = json.loads(risks)
        except Exception:
            risks = [str(risks)]
        return {
            "signal": r.get("ai_signal", "YELLOW"),
            "confidence": r.get("confidence", 50),
            "target_pct": r.get("target_pct", 0.008),
            "stop_loss_pct": r.get("stop_loss_pct", -0.018),
            "suggested_position_size": r.get("suggested_position_size", "SKIP"),
            "headline": r.get("headline", ""),
            "reason": r.get("reason", ""),
            "risk_factors": risks,
            "news_sentiment": r.get("news_sentiment", "NEUTRAL"),
            "action": r.get("action", ""),
        }
    except Exception:
        return None


# ── Signal card UI ───────────────────────────────────────────
def render_signal_card(row: pd.Series, analysis: dict, current_pnl: float | None = None):
    sig   = str(analysis.get("signal", "YELLOW")).upper()
    try:
        conf = max(0, min(100, int(float(analysis.get("confidence", 50)))))
    except (TypeError, ValueError):
        conf = 50
    head  = analysis.get("headline", "")
    reason= analysis.get("reason", "")
    risks = analysis.get("risk_factors", [])
    if isinstance(risks, str):
        risks = [risks]
    news_s= analysis.get("news_sentiment", "NEUTRAL")
    action= analysis.get("action", "")
    side  = str(row.get("side", ""))

    # Trade-management metrics returned by Claude
    try:
        tgt_pct = float(analysis.get("target_pct", 0.008)) * 100
    except (TypeError, ValueError):
        tgt_pct = 0.8
    try:
        sl_pct = float(analysis.get("stop_loss_pct", -0.018)) * 100
    except (TypeError, ValueError):
        sl_pct = -1.8
    pos_size = str(analysis.get("suggested_position_size", "SKIP")).upper()
    if pos_size not in {"FULL", "HALF", "SKIP"}:
        pos_size = "SKIP"

    color_map = {
        "GREEN":  ("#00e87a", "#001a0d", "🟢"),
        "YELLOW": ("#f5a623", "#1a1200", "🟡"),
        "RED":    ("#ff2d55", "#1a0008", "🔴"),
    }
    ac, bg, icon = color_map.get(sig, color_map["YELLOW"])

    # Confidence bar
    bar_filled = "█" * (conf // 10)
    bar_empty  = "░" * (10 - conf // 10)

    # News badge
    ns_color = {"POSITIVE": "#00e87a", "NEGATIVE": "#ff2d55", "NEUTRAL": "#7b92b2"}
    ns_c = ns_color.get(news_s, "#7b92b2")

    # P&L block for monitored trades
    pnl_block = ""
    if current_pnl is not None:
        pc = "#00e87a" if current_pnl >= 0 else "#ff2d55"
        pnl_block = f"""
        <div style="margin-top:10px;padding:8px 12px;background:rgba(0,0,0,0.3);
                    border-radius:6px;font-family:'IBM Plex Mono',monospace">
            Live P&amp;L &nbsp;
            <span style="color:{pc};font-size:16px;font-weight:700">
                {current_pnl*100:+.3f}%
            </span>
        </div>"""

    risks_html = "".join(
        f'<span style="background:rgba(245,166,35,0.12);color:#f5a623;'
        f'padding:2px 8px;border-radius:4px;font-size:11px;margin-right:4px">'
        f'⚠ {r}</span>'
        for r in risks
    )

    side_color = "#00e87a" if side == "LONG" else "#ff2d55"
    ts = pd.to_datetime(row.get("entry_time", "")).strftime("%H:%M") if pd.notna(row.get("entry_time","")) else ""
    pred_v = float(row.get("Pred", 0))
    rr_v   = float(row.get("RelativeRank", 0))

    metrics_html = f"""
    <div style="display:flex;gap:12px;margin-top:10px;padding:8px 12px;
                background:rgba(255,255,255,0.03);border-radius:6px;
                font-family:'IBM Plex Mono',monospace;font-size:12px;flex-wrap:wrap">
        <div>Target: <span style="color:#00e87a;font-weight:600">+{tgt_pct:.2f}%</span></div>
        <div>Stop Loss: <span style="color:#ff2d55;font-weight:600">{sl_pct:.2f}%</span></div>
        <div>Size: <span style="color:#f5a623;font-weight:600">{pos_size}</span></div>
    </div>
    """

    st.markdown(f"""
    <div style="background:{bg};border:1px solid {ac};border-left:4px solid {ac};
                border-radius:10px;padding:16px 20px;margin-bottom:12px">

      <!-- Header row -->
      <div style="display:flex;justify-content:space-between;align-items:flex-start">
        <div>
          <span style="font-family:'IBM Plex Mono',monospace;font-size:18px;
                       font-weight:700;color:#fff">{icon} {row.get('Stock','')}</span>
          <span style="background:rgba({'0,232,122' if side=='LONG' else '255,45,85'},.15);
                       color:{side_color};padding:2px 8px;border-radius:4px;
                       font-size:11px;font-weight:600;margin-left:8px">{side}</span>
          <div style="font-size:11px;color:#7b92b2;margin-top:3px">
            {ts} &nbsp;·&nbsp; Pred {pred_v:.3f} &nbsp;·&nbsp; RR {rr_v:.2f}
          </div>
        </div>
        <div style="text-align:right">
          <div style="font-size:22px;font-weight:700;color:{ac}">{sig}</div>
          <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:#7b92b2">
            {bar_filled}{bar_empty} {conf}%
          </div>
        </div>
      </div>

      <!-- Headline -->
      <div style="margin-top:12px;font-size:14px;font-weight:600;color:#fff">
        {head}
      </div>

      <!-- Reason -->
      <div style="margin-top:6px;font-size:13px;color:#c8daf0;line-height:1.6">
        {reason}
      </div>

      {metrics_html}

      {pnl_block}

      <!-- Risk + news row -->
      <div style="margin-top:12px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
        <div>{risks_html}</div>
        <div style="display:flex;gap:8px;align-items:center">
          <span style="font-size:11px;color:{ns_c}">News: {news_s}</span>
          <span style="background:{ac};color:#000;padding:3px 12px;border-radius:6px;
                       font-size:12px;font-weight:700">{action}</span>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ── Main page renderer ───────────────────────────────────────
def render_ai_advisor(engine, TODAY, MARKET_OPEN, get_trades, pnl_fn):
    st.markdown("# AI Trade Advisor")
    st.markdown(
        '<div style="color:#7b92b2;font-size:13px;margin-bottom:20px">'
        'Morning signals are analyzed once automatically. Historical analyses and '
        'open-position threshold analyses run only when you click Analyze. '
        'Every completed AI analysis is saved to the persistent AI log.</div>',
        unsafe_allow_html=True
    )

    enable_search = st.sidebar.checkbox(
        "Enable Claude Web Search", value=True, key="ai_enable_search"
    )
    if "ai_analysis_cache" not in st.session_state:
        st.session_state.ai_analysis_cache = {}

    _ensure_ai_log_table(engine)

    col1, col2 = st.columns([2, 1])
    with col1:
        view_date = st.date_input("Date", value=TODAY, max_value=TODAY, key="ai_date")
    with col2:
        st.caption("Automatic: today's morning signals only")

    df = get_trades(str(view_date), str(view_date))
    if df.empty:
        st.info(f"No signals on {view_date}.")
        return

    def analysis_cache_key(row, mode="signal"):
        return (
            f"{view_date}_{row.get('Stock','')}_{row.get('entry_time','')}"
            f"_{mode}_search_{enable_search}_model_opus55_newswindow_v4"
        )

    live_prices = {}
    try:
        lq = pd.read_sql('SELECT "Stock","Close" as lp FROM live_quotes', engine)
        live_prices = dict(zip(lq["Stock"], lq["lp"]))
    except Exception:
        pass

    # ── 1. Automatic morning analysis: TODAY only, once per signal ──
    # Historical dates are NEVER auto-analyzed.
    if view_date == TODAY:
        morning_rows = []
        for i, row in df.iterrows():
            try:
                et = _to_ist(row.get("entry_time"))
                if et.hour < 10 or (et.hour == 10 and et.minute <= 15):
                    morning_rows.append((i, row))
            except Exception:
                pass

        if morning_rows:
            st.markdown(
                '<div style="font-size:10px;color:#7b92b2;text-transform:uppercase;'
                'letter-spacing:.12em;border-top:1px solid #1a3050;'
                'padding-top:14px;margin-top:10px">Morning Signal AI</div>',
                unsafe_allow_html=True
            )
            for i, row in morning_rows:
                key = (
                    f"{view_date}|{row.get('Stock','')}|{row.get('entry_time','')}|"
                    f"{row.get('side','')}|MORNING_SIGNAL"
                )
                cache_key = analysis_cache_key(row, mode="morning")
                a = st.session_state.ai_analysis_cache.get(cache_key)

                if a is None:
                    a = _get_logged_analysis(engine, key)
                    if a is not None:
                        st.session_state.ai_analysis_cache[cache_key] = a

                if a is None:
                    with st.spinner(f"AI analyzing morning signal: {row['Stock']}..."):
                        news = fetch_news(str(row["Stock"]), analysis_time=row.get("entry_time"))
                        hist = fetch_stock_history(
                            str(row["Stock"]), str(row["side"]), engine,
                            analysis_time=row.get("entry_time")
                        )
                        a = analyze_signal(
                            stock=str(row["Stock"]), side=str(row["side"]),
                            pred=float(row.get("Pred", 0)),
                            rr=float(row.get("RelativeRank", 0)),
                            nifty_mom=float(row.get("NiftyMomentum", 0)),
                            vol_shock=float(row.get("VolumeShock", 1)),
                            mom5=float(row.get("Momentum5", 0)),
                            history_text=hist, news_headlines=news,
                            current_pnl=None,
                            analysis_time=row.get("entry_time"),
                            historical_mode=False,
                            enable_search=enable_search,
                        )
                        # Permanent dedupe happens through the unique morning key.
                        _log_ai_result(
                            engine, "MORNING_SIGNAL", key, row, a,
                            analysis_time=row.get("entry_time"),
                            historical_mode=False,
                            enable_search=enable_search,
                            news_headlines=news, history_text=hist,
                        )
                        st.session_state.ai_analysis_cache[cache_key] = a

                render_signal_card(row, a)

    # ── 2. Historical signal analysis is manual ─────────
    # Today's signals are handled by the automatic Morning Signal section above.
    if view_date < TODAY:
        st.markdown(
            '<div style="font-size:10px;color:#7b92b2;text-transform:uppercase;'
            'letter-spacing:.12em;border-top:1px solid #1a3050;'
            'padding-top:14px;margin-top:20px">Signal History — Manual Analysis</div>',
            unsafe_allow_html=True
        )

        for i, row in df.iterrows():
            is_historical = True
            cache_key = analysis_cache_key(row, mode="signal")
            cached_a = st.session_state.ai_analysis_cache.get(cache_key)

            if cached_a is not None:
                render_signal_card(row, cached_a)
                continue

            with st.container():
                c1, c2 = st.columns([3, 1])
                with c1:
                    side_col = "#00e87a" if row["side"] == "LONG" else "#ff2d55"
                    st.markdown(
                        f'<div style="background:#0a1628;border:1px solid #1a3050;'
                        f'border-radius:8px;padding:12px 16px">'
                        f'<span style="font-family:\'IBM Plex Mono\',monospace;'
                        f'font-size:15px;font-weight:600;color:#fff">{row["Stock"]}</span>'
                        f'<span style="color:{side_col};margin-left:8px;font-size:12px">'
                        f'{row["side"]}</span>'
                        f'<span style="color:#7b92b2;font-size:12px;margin-left:8px">'
                        f'Pred {float(row.get("Pred",0)):.3f}</span></div>',
                        unsafe_allow_html=True
                    )
                with c2:
                    if st.button("Analyze", key=f"btn_signal_{view_date}_{i}"):
                        with st.spinner(f"Analyzing {row['Stock']}..."):
                            news = fetch_news(
                                str(row["Stock"]), analysis_time=row.get("entry_time")
                            )
                            hist = fetch_stock_history(
                                str(row["Stock"]), str(row["side"]), engine,
                                analysis_time=row.get("entry_time")
                            )
                            a = analyze_signal(
                                stock=str(row["Stock"]), side=str(row["side"]),
                                pred=float(row.get("Pred",0)),
                                rr=float(row.get("RelativeRank",0)),
                                nifty_mom=float(row.get("NiftyMomentum",0)),
                                vol_shock=float(row.get("VolumeShock",1)),
                                mom5=float(row.get("Momentum5",0)),
                                history_text=hist, news_headlines=news,
                                analysis_time=row.get("entry_time"),
                                historical_mode=is_historical,
                                enable_search=False if is_historical else enable_search,
                            )
                            analysis_key = (
                                f"{view_date}|{row.get('Stock','')}|{row.get('entry_time','')}|"
                                f"{row.get('side','')}|SIGNAL_MANUAL"
                            )
                            _log_ai_result(
                                engine, "HISTORICAL_SIGNAL" if is_historical else "SIGNAL_MANUAL",
                                analysis_key + "|" + str(uuid.uuid4()), row, a,
                                analysis_time=pd.Timestamp.now(tz="Asia/Kolkata"),
                                historical_mode=is_historical,
                                enable_search=False if is_historical else enable_search,
                                news_headlines=news, history_text=hist,
                            )
                            st.session_state.ai_analysis_cache[cache_key] = a
                        st.rerun()

    # ── 3. Open-position threshold: alert only; Claude runs on click ──
    open_trades = pd.DataFrame()
    if view_date == TODAY:
        open_trades = df[
            (df.get("eod_flag", pd.Series(0, index=df.index)) == 1)
            & df["Stock"].isin(live_prices)
        ]

    if not open_trades.empty and live_prices:
        st.markdown(
            '<div style="font-size:10px;color:#7b92b2;text-transform:uppercase;'
            'letter-spacing:.12em;border-top:1px solid #1a3050;'
            'padding-top:14px;margin-top:20px">Open Positions — Live Monitor</div>',
            unsafe_allow_html=True
        )

        for i, row in open_trades.iterrows():
            ep = float(row.get("entry_price", 0))
            lp = float(live_prices.get(str(row["Stock"]), 0))
            if ep <= 0 or lp <= 0:
                continue
            cpnl = (lp - ep) / ep if row["side"] == "LONG" else (ep - lp) / ep
            threshold_hit = cpnl >= 0.008 or cpnl < -0.004

            if threshold_hit:
                threshold_name = "TARGET THRESHOLD" if cpnl >= 0.008 else "RISK THRESHOLD"
                st.warning(
                    f"{row['Stock']} {row['side']} — {threshold_name} reached "
                    f"({cpnl*100:+.3f}%). AI analysis will NOT run automatically."
                )
                if st.button(
                    "Analyse Position",
                    key=f"btn_position_{view_date}_{i}_{round(cpnl*100000)}"
                ):
                    with st.spinner(f"Analyzing current {row['Stock']} position..."):
                        now_ist = pd.Timestamp.now(tz="Asia/Kolkata")
                        news = fetch_news(str(row["Stock"]), analysis_time=now_ist)
                        hist = fetch_stock_history(
                            str(row["Stock"]), str(row["side"]), engine
                        )
                        a = analyze_signal(
                            stock=str(row["Stock"]), side=str(row["side"]),
                            pred=float(row.get("Pred",0)),
                            rr=float(row.get("RelativeRank",0)),
                            nifty_mom=float(row.get("NiftyMomentum",0)),
                            vol_shock=float(row.get("VolumeShock",1)),
                            mom5=float(row.get("Momentum5",0)),
                            history_text=hist, news_headlines=news,
                            current_pnl=cpnl,
                            analysis_time=now_ist,
                            historical_mode=False,
                            enable_search=enable_search,
                        )
                        # Every button click gets its own log record.
                        _log_ai_result(
                            engine, "THRESHOLD_ANALYSIS", str(uuid.uuid4()), row, a,
                            analysis_time=now_ist, current_pnl=cpnl,
                            historical_mode=False, enable_search=enable_search,
                            news_headlines=news, history_text=hist,
                        )
                        render_signal_card(row, a, cpnl)
            else:
                pc = "#00e87a" if cpnl >= 0 else "#f5a623"
                st.markdown(
                    f'<div style="background:#0a1628;border:1px solid #1a3050;'
                    f'border-radius:8px;padding:10px 16px;margin-bottom:8px;'
                    f'display:flex;justify-content:space-between">'
                    f'<span style="font-family:\'IBM Plex Mono\',monospace;color:#fff">'
                    f'{row["Stock"]} {row["side"]}</span>'
                    f'<span style="font-family:\'IBM Plex Mono\',monospace;color:{pc}">'
                    f'{cpnl*100:+.3f}% &nbsp; 🟢 holding</span></div>',
                    unsafe_allow_html=True
                )

    st.markdown(
        '<div style="font-size:11px;color:#7b92b2;margin-top:16px">'
        'Morning analyses are persistent and run once per signal · '
        'Historical analyses are manual · Threshold analyses run only after clicking Analyze · '
        'Every AI result is logged to ai_advisor_log</div>',
        unsafe_allow_html=True
    )
