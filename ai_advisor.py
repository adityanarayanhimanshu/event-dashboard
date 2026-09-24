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
import anthropic
import streamlit as st
import pandas as pd
import feedparser
from datetime import datetime, timedelta, date
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


# ── News fetcher ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_news(stock: str) -> list[str]:
    headlines = []

    # Single Google News RSS query for current stock news
    query = f'"{stock}" AND (stock OR NSE OR share)'
    rss_url = (
        "https://news.google.com/rss/search"
        f"?q={query.replace(' ', '+')}"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    )

    try:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries[:6]:
            title = entry.get("title", "")
            published = entry.get("published", "")
            if title:
                headlines.append(f"[{stock}] {title} | {published}")
    except Exception:
        pass

    return headlines or [f"No recent news found for {stock}"]


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
        th AS (SELECT DISTINCT ON(entry_time) entry_time,fr FROM fp WHERE fr>=0.008 ORDER BY entry_time,ft),
        sh AS (SELECT DISTINCT ON(entry_time) entry_time,fr FROM fp WHERE fr<=-0.018 ORDER BY entry_time,ft),
        ex AS (SELECT DISTINCT ON(entry_time) entry_time,fr FROM fp ORDER BY entry_time,ft DESC)
        SELECT DATE(sig.entry_time) as dt,
               CASE WHEN th.entry_time IS NOT NULL THEN 0.008
                    WHEN sh.entry_time IS NOT NULL THEN -0.018
                    ELSE COALESCE(ex.fr,0) END AS ret,
               CASE WHEN th.entry_time IS NOT NULL THEN 'TARGET'
                    WHEN sh.entry_time IS NOT NULL THEN 'STOP'
                    ELSE 'EOD' END AS outcome
        FROM sig
        LEFT JOIN th ON sig.entry_time=th.entry_time
        LEFT JOIN sh ON sig.entry_time=sh.entry_time
        LEFT JOIN ex ON sig.entry_time=ex.entry_time
        WHERE sig.side='{side}'
        ORDER BY sig.entry_time DESC LIMIT 15
        """
        hist = pd.read_sql(sql, _engine)
        if hist.empty:
            return f"No historical {side} trades for {stock} in last 90 days."

        wins      = (hist['ret'] > 0).sum()
        tgt_hits  = (hist['outcome'] == 'TARGET').sum()
        avg_ret   = hist['ret'].mean() * 100
        lines = [
            f"Last {len(hist)} {side} trades: {wins}/{len(hist)} wins, "
            f"{tgt_hits} targets hit, avg {avg_ret:+.3f}% per trade",
            ""
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

    if historical_mode and analysis_time is not None:
        cutoff_text = pd.to_datetime(analysis_time).strftime("%Y-%m-%d %H:%M:%S")
        research_block = f"""
NEWS RESEARCH MODE: HISTORICAL
Trade timestamp / information cutoff: {cutoff_text} Asia/Kolkata.

All supplied historical performance data has also been cutoff-filtered.
The historical performance section contains only completed trading days BEFORE the signal date.
Do not use or infer any price movement, trade outcome, news, announcement, or market information that occurred after the information cutoff.
Treat the cutoff as a strict information boundary.

Search the web for news and market information that was publicly available ON OR BEFORE this timestamp.
For historical web searches, include an explicit `before:YYYY-MM-DD` date restriction in search queries whenever possible.
Do NOT use later news, later announcements, or later price-moving information.
If a source was published after the cutoff, ignore it for the analysis.
"""     
    else:
        cutoff_text = "CURRENT"
        research_block = """
NEWS RESEARCH MODE: CURRENT
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
RED    — avoid, or exit immediately if already in trade"""

    try:
        request_kwargs = {
            "model": "claude-opus-5-5",
            "max_tokens": 2048,
            "messages": [{"role": "user", "content": prompt}],
        }
        if enable_search:
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
        'Claude analyzes each signal using live news, sector context, '
        'and your historical performance per stock. '
        'Refreshes every 3 minutes.</div>',
        unsafe_allow_html=True
    )

    # ── AI controls / session cache ─────────────────────────
    enable_search = st.sidebar.checkbox("Enable Claude Web Search", value=True, key="ai_enable_search")
    if "ai_analysis_cache" not in st.session_state:
        st.session_state.ai_analysis_cache = {}

    # ── Date selector ────────────────────────────────────────
    col1, col2 = st.columns([2, 1])
    with col1:
        view_date = st.date_input("Date", value=TODAY, max_value=TODAY, key="ai_date")
    with col2:
        auto_analyze = st.checkbox("Auto-analyze all", value=False)

    df = get_trades(str(view_date), str(view_date))

    if df.empty:
        st.info(f"No signals on {view_date}.")
        return

    def analysis_cache_key(row, mode="signal"):
        return (f"{view_date}_{row.get('Stock','')}_{row.get('entry_time','')}"
                f"_{mode}_search_{enable_search}_model_opus55")

    # ── Live quotes for open trade monitoring ────────────────
    live_prices = {}
    try:
        lq = pd.read_sql('SELECT "Stock","Close" as lp FROM live_quotes', engine)
        live_prices = dict(zip(lq["Stock"], lq["lp"]))
    except Exception:
        pass

    # ── Summary bar ──────────────────────────────────────────
    n_green = n_yellow = n_red = 0
    analyses: dict[int, dict] = {}

    # Pre-compute analyses if auto mode
    if auto_analyze:
        prog = st.progress(0, text="Analyzing signals in parallel...")

        def process_single_row(item):
            i, row, cache_key = item
            is_historical = view_date < TODAY
            news = [] if is_historical else fetch_news(str(row["Stock"]))
            hist = fetch_stock_history(str(row["Stock"]), str(row["side"]), engine, analysis_time=row.get("entry_time"))

            current_pnl = None
            if str(row["Stock"]) in live_prices and int(row.get("eod_flag", 0)) == 1:
                ep = float(row.get("entry_price", 0))
                lp = float(live_prices[str(row["Stock"])])
                if ep > 0:
                    current_pnl = ((lp - ep) / ep if row["side"] == "LONG"
                                   else (ep - lp) / ep)

            a = analyze_signal(
                stock=str(row["Stock"]),
                side=str(row["side"]),
                pred=float(row.get("Pred", 0)),
                rr=float(row.get("RelativeRank", 0)),
                nifty_mom=float(row.get("NiftyMomentum", 0)),
                vol_shock=float(row.get("VolumeShock", 1)),
                mom5=float(row.get("Momentum5", 0)),
                history_text=hist,
                news_headlines=news,
                current_pnl=current_pnl,
                analysis_time=row.get("entry_time"),
                historical_mode=is_historical,
                enable_search=enable_search,
            )
            return i, cache_key, a

        items = list(df.iterrows())
        work_items = []
        completed = 0

        # Resolve already-cached analyses on the main Streamlit thread.
        # Session state is deliberately not accessed from worker threads.
        for i, row in items:
            cache_key = analysis_cache_key(row)
            cached = st.session_state.ai_analysis_cache.get(cache_key)
            if cached is not None:
                analyses[i] = cached
                signal = str(cached.get("signal", "YELLOW")).upper()
                if signal == "GREEN":
                    n_green += 1
                elif signal == "RED":
                    n_red += 1
                else:
                    n_yellow += 1
                completed += 1
            else:
                work_items.append((i, row, cache_key))

        if completed:
            prog.progress(
                completed / len(items),
                text=f"Loaded {completed}/{len(items)} cached analyses..."
            )

        if work_items:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(process_single_row, item) for item in work_items]
                for future in as_completed(futures):
                    i, cache_key, a = future.result()
                    # Store results on the main Streamlit thread.
                    st.session_state.ai_analysis_cache[cache_key] = a
                    analyses[i] = a
                    signal = str(a.get("signal", "YELLOW")).upper()
                    if signal == "GREEN":
                        n_green += 1
                    elif signal == "RED":
                        n_red += 1
                    else:
                        n_yellow += 1

                    completed += 1
                    prog.progress(
                        completed / len(items),
                        text=f"Analyzed {completed}/{len(items)} signals..."
                    )

        prog.empty()

    # ── Signal summary chips ─────────────────────────────────
    if analyses:
        st.markdown(f"""
        <div style="display:flex;gap:12px;margin:16px 0 20px">
          <div style="background:rgba(0,232,122,.12);border:1px solid #00e87a;
                      border-radius:8px;padding:10px 20px;text-align:center">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:28px;
                        font-weight:700;color:#00e87a">{n_green}</div>
            <div style="font-size:11px;color:#7b92b2">GREEN</div>
          </div>
          <div style="background:rgba(245,166,35,.12);border:1px solid #f5a623;
                      border-radius:8px;padding:10px 20px;text-align:center">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:28px;
                        font-weight:700;color:#f5a623">{n_yellow}</div>
            <div style="font-size:11px;color:#7b92b2">YELLOW</div>
          </div>
          <div style="background:rgba(255,45,85,.12);border:1px solid #ff2d55;
                      border-radius:8px;padding:10px 20px;text-align:center">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:28px;
                        font-weight:700;color:#ff2d55">{n_red}</div>
            <div style="font-size:11px;color:#7b92b2">RED</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Render each trade ────────────────────────────────────
    # Sort: RED first, then YELLOW, then GREEN
    sig_order = {"RED": 0, "YELLOW": 1, "GREEN": 2}
    rows_sorted = sorted(df.iterrows(),
                         key=lambda x: sig_order.get(
                             analyses.get(x[0], {}).get("signal", "YELLOW"), 1))

    for i, row in rows_sorted:
        cached_a = st.session_state.ai_analysis_cache.get(analysis_cache_key(row))
        if auto_analyze and i in analyses:
            a = analyses[i]
            ep = float(row.get("entry_price", 0))
            lp = float(live_prices.get(str(row["Stock"]), 0))
            cpnl = None
            if ep > 0 and lp > 0 and int(row.get("eod_flag", 0)) == 1:
                cpnl = ((lp-ep)/ep if row["side"]=="LONG" else (ep-lp)/ep)
            render_signal_card(row, a, cpnl)
        elif cached_a is not None:
            # Show a previously analyzed card after Streamlit reruns.
            render_signal_card(row, cached_a)
        else:
            # Manual analyze button
            with st.container():
                c1, c2 = st.columns([3, 1])
                with c1:
                    side_col = "#00e87a" if row["side"]=="LONG" else "#ff2d55"
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
                    if st.button("Analyze", key=f"btn_{i}"):
                        with st.spinner(f"Analyzing {row['Stock']}..."):
                            is_historical = view_date < TODAY
                            news = [] if is_historical else fetch_news(str(row["Stock"]))
                            hist = fetch_stock_history(
                                str(row["Stock"]), str(row["side"]), engine, analysis_time=row.get("entry_time"))
                            cache_key = analysis_cache_key(row)
                            a = st.session_state.ai_analysis_cache.get(cache_key)
                            if a is None:
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
                                    enable_search=enable_search,
                                )
                                st.session_state.ai_analysis_cache[cache_key] = a
                            render_signal_card(row, a)

    # ── Open trade monitor (EOD trades with live price) ──────
    open_trades = pd.DataFrame()
    if view_date == TODAY:
        open_trades = df[(df.get("eod_flag", pd.Series(0, index=df.index)) == 1) &
                         df["Stock"].isin(live_prices)]

    if not open_trades.empty and live_prices:
        st.markdown(
            '<div style="font-size:10px;color:#7b92b2;text-transform:uppercase;'
            'letter-spacing:.12em;border-top:1px solid #1a3050;'
            'padding-top:14px;margin-top:20px">Open Positions — Live Monitor</div>',
            unsafe_allow_html=True
        )

        for _, row in open_trades.iterrows():
            ep = float(row.get("entry_price", 0))
            lp = float(live_prices.get(str(row["Stock"]), 0))
            if ep <= 0 or lp <= 0:
                continue
            cpnl = (lp-ep)/ep if row["side"]=="LONG" else (ep-lp)/ep

            # Re-analyze when the position hits the target (+0.8%)
            # or crosses the monitoring stop threshold (-0.4%).
            is_target_hit = cpnl >= 0.008
            is_stop_loss = cpnl < -0.004

            if is_target_hit or is_stop_loss:
                news = fetch_news(str(row["Stock"]))
                hist = fetch_stock_history(str(row["Stock"]), str(row["side"]), engine)
                monitor_key = analysis_cache_key(
                    row, mode=f"monitor_{round(cpnl * 1000)}"
                )
                a = st.session_state.ai_analysis_cache.get(monitor_key)
                if a is None:
                    a = analyze_signal(
                        stock=str(row["Stock"]), side=str(row["side"]),
                        pred=float(row.get("Pred",0)),
                        rr=float(row.get("RelativeRank",0)),
                        nifty_mom=float(row.get("NiftyMomentum",0)),
                        vol_shock=float(row.get("VolumeShock",1)),
                        mom5=float(row.get("Momentum5",0)),
                        history_text=hist, news_headlines=news,
                        current_pnl=cpnl,
                        analysis_time=row.get("entry_time"),
                        historical_mode=False,
                        enable_search=enable_search,
                    )
                    st.session_state.ai_analysis_cache[monitor_key] = a
                render_signal_card(row, a, cpnl)
            else:
                # Simple status card
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
        f'<div style="font-size:11px;color:#7b92b2;margin-top:16px">'
        f'Analysis cached 3 min · News cached 5 min · '
        f'Page auto-refreshes 60 s</div>',
        unsafe_allow_html=True
    )
