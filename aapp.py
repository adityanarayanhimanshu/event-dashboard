import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date, time as dtime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import calendar as cal_lib

# ══════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════
CAPITAL  = 100_000
COST_PCT = 16 / 10_000
RF_DAILY = 0.065 / 252       # 6.5% annual risk-free rate

def ist():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

NOW   = ist()
TODAY = NOW.date()
MARKET_OPEN = (
    TODAY.weekday() < 5 and
    dtime(9, 15) <= NOW.time() <= dtime(15, 30)
)

# ══════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Alpha Terminal",
    layout="wide",
    page_icon="◈",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=Inter:wght@400;500;600&display=swap');

:root {
    --bg:      #03080f;
    --surface: #0a1628;
    --raised:  #112240;
    --border:  #1a3050;
    --accent:  #00ffcc;
    --long:    #00e87a;
    --short:   #ff2d55;
    --warn:    #f5a623;
    --text:    #c8daf0;
    --muted:   #7b92b2;
    --glow:    rgba(0,255,204,0.12);
}

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background: var(--bg);
    color: var(--text);
}
.stApp { background: var(--bg); }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #060e1c;
    border-right: 1px solid var(--border);
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 4px;
    gap: 2px;
}
.stTabs [data-baseweb="tab"] {
    color: var(--muted);
    font-size: 13px;
    font-family: 'Inter', sans-serif;
    border-radius: 6px;
    padding: 6px 14px;
}
.stTabs [aria-selected="true"] {
    background: var(--raised) !important;
    color: var(--accent) !important;
}

/* ── Big P&L number ── */
.hero-pnl {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 52px;
    font-weight: 700;
    letter-spacing: -0.03em;
    line-height: 1;
}
.hero-glow-pos { color: var(--accent); text-shadow: 0 0 30px rgba(0,255,204,0.35); }
.hero-glow-neg { color: var(--short);  text-shadow: 0 0 30px rgba(255,45,85,0.35); }

/* ── Metric card ── */
.mcard {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 18px;
    height: 100%;
}
.mcard-label {
    font-size: 10px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 6px;
}
.mcard-val {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 24px;
    font-weight: 600;
    color: #fff;
}
.pos  { color: var(--long) !important; }
.neg  { color: var(--short) !important; }
.neu  { color: var(--accent) !important; }
.warn { color: var(--warn) !important; }

/* ── Trade card ── */
.tcard {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 7px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.tcard-l  { border-left-color: var(--long); }
.tcard-s  { border-left-color: var(--short); }
.tcard-st { border-left-color: var(--warn); }

.tn   { font-family:'IBM Plex Mono',monospace; font-size:15px; font-weight:600; color:#fff; }
.tm   { font-size:11px; color:var(--muted); margin-top:3px; }
.tr   { font-family:'IBM Plex Mono',monospace; font-size:20px; font-weight:600; text-align:right; }
.tp   { font-family:'IBM Plex Mono',monospace; font-size:12px; text-align:right; margin-top:2px; }

.tag  { display:inline-block; padding:2px 7px; border-radius:4px; font-size:10px;
        font-weight:600; letter-spacing:0.06em; margin-right:4px; }
.tg-l { background:rgba(0,232,122,0.12); color:var(--long); }
.tg-s { background:rgba(255,45,85,0.12);  color:var(--short); }
.tg-t { background:rgba(0,255,204,0.10); color:var(--accent); }
.tg-x { background:rgba(245,166,35,0.12); color:var(--warn); }
.tg-e { background:rgba(123,146,178,0.12); color:var(--muted); }

/* ── Section heading ── */
.sec-head {
    font-size: 11px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    border-bottom: 1px solid var(--border);
    padding-bottom: 8px;
    margin: 20px 0 14px 0;
}

/* ── Status badge ── */
.badge-open   { background:rgba(0,232,122,0.15); color:var(--long);
                padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; }
.badge-closed { background:rgba(255,45,85,0.10); color:var(--short);
                padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; }

/* ── Ratio card ── */
.ratio-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-top: 2px solid var(--accent);
    border-radius: 8px;
    padding: 16px;
    text-align: center;
}
.ratio-val { font-family:'IBM Plex Mono',monospace; font-size:32px; font-weight:700; color:var(--accent); }
.ratio-lbl { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:0.1em; margin-top:4px; }

@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.2} }
.dot-live { display:inline-block; width:7px; height:7px; border-radius:50%;
            background:var(--long); margin-right:5px; animation:pulse 1.8s infinite; }
.dot-off  { display:inline-block; width:7px; height:7px; border-radius:50%;
            background:var(--short); margin-right:5px; }
</style>
<meta http-equiv="refresh" content="60">
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════
@st.cache_resource
def get_engine():
    return create_engine(
        st.secrets["NEON_URL"],
        connect_args={"options": "-ctimezone=Asia/Kolkata"},
        pool_pre_ping=True
    )

engine = get_engine()

# ══════════════════════════════════════════════════════════════
#  SQL HELPERS
# ══════════════════════════════════════════════════════════════
_LONG_FILTERS = """
    e."Pred">=0.63 AND e."RelativeRank">=p.rr_th AND e."NiftyMomentum">=p.nifty_th
    AND e."Momentum5">0.002
    AND e."Datetime" AT TIME ZONE 'Asia/Kolkata' <= (DATE(e."Datetime")+'09:45:00'::time)::timestamp
    AND e."LiquidityVacuum">0 AND e."Trend3">0 AND e."VolumeShock">1.1 AND e."Momentum60">-0.003
    AND e."Stock" NOT IN ('DLF','HCLTECH','PIIND','COALINDIA','PAYTM','TATACOMM')"""

_SHORT_FILTERS = """
    e."Pred"<=(1-p.pred_th) AND e."RelativeRank"<=(1-p.rr_th)
    AND e."NiftyMomentum"<=-p.nifty_th AND e."RelativeRank">0.05
    AND e."NiftyMomentum"<-0.002 AND e."Momentum5"<0 AND e."Momentum15"<0
    AND e."Momentum30"<0 AND e."VolumeShock">0.7 AND e."Momentum60"<0.005
    AND e."RelativeRank"<0.31 AND e."Trend3"<0.01
    AND e."Stock" NOT IN ('DLF','COALINDIA','PIIND','NAUKRI','WIPRO','GAIL','TATASTEEL','HDFCLIFE')"""

def _core_sql(start: str, end: str) -> str:
    return f"""
WITH p AS (SELECT 0.62 AS pred_th, 0.65 AS rr_th, 0.00 AS nifty_th),
sig AS (
    SELECT e."Stock", e."Datetime" AS entry_time, DATE(e."Datetime") AS trade_date,
           e."Close" AS entry_price, e."Pred", e."RelativeRank", e."NiftyMomentum",
           CASE WHEN {_LONG_FILTERS} THEN 'LONG'
                WHEN {_SHORT_FILTERS} THEN 'SHORT' END AS side
    FROM events e CROSS JOIN p
    WHERE DATE(e."Datetime") BETWEEN '{start}' AND '{end}'
      AND ({_LONG_FILTERS} OR {_SHORT_FILTERS})
      AND e."Datetime" AT TIME ZONE 'Asia/Kolkata' >= (DATE(e."Datetime")+'09:25:00'::time)::timestamp
      AND e."Datetime" AT TIME ZONE 'Asia/Kolkata' <= (DATE(e."Datetime")+'10:15:00'::time)::timestamp
),
fp AS (
    SELECT s."Stock", s.trade_date, s.entry_time, s.side, s.entry_price, s."Pred", s."RelativeRank",
           e."Datetime" AS ft,
           CASE WHEN s.side='LONG' THEN (e."Close"-s.entry_price)/s.entry_price
                ELSE (s.entry_price-e."Close")/s.entry_price END AS fr,
           EXTRACT(EPOCH FROM (e."Datetime"-s.entry_time))/60 AS mins
    FROM sig s JOIN events e ON s."Stock"=e."Stock"
      AND e."Datetime">=s.entry_time AND e."Datetime"<=(DATE(s.entry_time)+'15:05:00'::time)::timestamp
),
th AS (SELECT DISTINCT ON ("Stock",entry_time) "Stock",entry_time,mins FROM fp WHERE fr>=0.008  ORDER BY "Stock",entry_time,ft),
sh AS (SELECT DISTINCT ON ("Stock",entry_time) "Stock",entry_time,mins FROM fp WHERE fr<=-0.018 ORDER BY "Stock",entry_time,ft),
ex AS (SELECT DISTINCT ON ("Stock",entry_time) "Stock",entry_time,fr FROM fp ORDER BY "Stock",entry_time,ft DESC)
SELECT s."Stock", s.trade_date, s.entry_time, s.side, s.entry_price, s."Pred", s."RelativeRank",
       ex.fr AS eod_return,
       CASE WHEN th.mins IS NOT NULL AND sh.mins IS NOT NULL
              THEN CASE WHEN th.mins<sh.mins THEN 0.008 ELSE -0.018 END
            WHEN th.mins IS NOT NULL THEN 0.008
            WHEN sh.mins IS NOT NULL THEN -0.018
            ELSE COALESCE(ex.fr,0) END AS trade_return,
       CASE WHEN th.mins IS NOT NULL THEN 1 ELSE 0 END AS target_hit_flag,
       CASE WHEN sh.mins IS NOT NULL THEN 1 ELSE 0 END AS stop_hit_flag,
       CASE WHEN th.mins IS NULL AND sh.mins IS NULL THEN 1 ELSE 0 END AS eod_flag
FROM sig s
LEFT JOIN th ON s."Stock"=th."Stock" AND s.entry_time=th.entry_time
LEFT JOIN sh ON s."Stock"=sh."Stock" AND s.entry_time=sh.entry_time
LEFT JOIN ex ON s."Stock"=ex."Stock" AND s.entry_time=ex.entry_time
ORDER BY s.entry_time"""

@st.cache_data(ttl=60)
def get_trades(start: str, end: str) -> pd.DataFrame:
    try:
        return pd.read_sql(_core_sql(start, end), engine)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_radar() -> pd.DataFrame:
    """Latest Pred + feature values for all stocks today during signal window."""
    sql = f"""
    SELECT DISTINCT ON ("Stock")
        "Stock", "Datetime", "Pred", "RelativeRank", "NiftyMomentum",
        "VolumeShock", "Trend3", "Momentum5", "LiquidityVacuum", "Momentum60"
    FROM events
    WHERE DATE("Datetime") = CURRENT_DATE
      AND "Datetime" AT TIME ZONE 'Asia/Kolkata' >= (CURRENT_DATE+'09:20:00'::time)::timestamp
    ORDER BY "Stock", "Datetime" DESC
    """
    try:
        return pd.read_sql(sql, engine)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_daily_moves(start: str, end: str) -> pd.DataFrame:
    sql = f"""
    WITH r AS (
        SELECT "Stock", DATE("Datetime") AS dt, "Close", "High", "Low",
               ROW_NUMBER() OVER (PARTITION BY "Stock",DATE("Datetime") ORDER BY "Datetime") rn_a,
               ROW_NUMBER() OVER (PARTITION BY "Stock",DATE("Datetime") ORDER BY "Datetime" DESC) rn_d
        FROM events WHERE DATE("Datetime") BETWEEN '{start}' AND '{end}'
    )
    SELECT "Stock", dt AS trade_date,
           MAX(CASE WHEN rn_a=1 THEN "Close" END) AS open_p,
           MAX(CASE WHEN rn_d=1 THEN "Close" END) AS close_p,
           MAX("High") AS hi, MIN("Low") AS lo
    FROM r GROUP BY "Stock", dt
    """
    try:
        df = pd.read_sql(sql, engine)
        df['daily_ret']   = (df['close_p'] - df['open_p']) / df['open_p']
        df['day_range']   = (df['hi'] - df['lo']) / df['open_p']
        return df
    except:
        return pd.DataFrame()

# ══════════════════════════════════════════════════════════════
#  CALCULATION HELPERS
# ══════════════════════════════════════════════════════════════
def pnl(ret: float) -> float:
    return CAPITAL * (ret - COST_PCT)

def summarize(df: pd.DataFrame) -> dict:
    if df.empty: return {}
    pnls = [pnl(r) for r in df['trade_return']]
    return {
        'trades':   len(df),
        'total':    sum(pnls),
        'ret_pct':  df['trade_return'].sum() * 100,
        'tgt_rate': df['target_hit_flag'].mean() * 100,
        'win_rate': (df['trade_return'] > 0).mean() * 100,
        'stops':    int(df['stop_hit_flag'].sum()),
        'avg_pct':  df['trade_return'].mean() * 100,
    }

def risk_ratios(df: pd.DataFrame) -> dict:
    if len(df) < 5: return {}
    df = df.copy()
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    df['pnl_val'] = df['trade_return'].apply(pnl)
    daily = df.groupby(df['entry_time'].dt.date)['pnl_val'].sum()
    dr = daily / CAPITAL
    mean_dr, std_dr = dr.mean(), dr.std()
    neg_dr = dr[dr < 0].std()
    sharpe  = (mean_dr - RF_DAILY) / std_dr * np.sqrt(252) if std_dr > 0 else 0
    sortino = (mean_dr - RF_DAILY) / neg_dr * np.sqrt(252) if neg_dr > 0 else 0
    cum     = df.sort_values('entry_time')['pnl_val'].cumsum()
    peak    = cum.cummax()
    dd_pct  = (cum - peak) / (CAPITAL + peak.abs()) * 100
    max_dd  = abs(dd_pct.min())
    ann_ret = mean_dr * 252 * 100
    calmar  = ann_ret / max_dd if max_dd > 0 else 0
    return {
        'sharpe': round(sharpe, 2),
        'sortino': round(sortino, 2),
        'calmar': round(calmar, 2),
        'max_dd_pct': round(max_dd, 2),
        'ann_ret': round(ann_ret, 1),
    }

def stock_classify(row: pd.Series) -> tuple:
    if row['trades'] < 3:     return '⚪', 'Thin data',        'var(--muted)'
    if row['win_rate'] >= 0.70 and row['total'] > 0:  return '🟢', 'Keep',              'var(--long)'
    if row['stops'] >= 2:     return '🔴', 'Blacklist?',       'var(--short)'
    if row['win_rate'] >= 0.50 and row['total'] > -5000: return '🟡', 'Watch',           'var(--warn)'
    return '🔴', 'Blacklist?', 'var(--short)'

fmt_inr = lambda v: f"{'+'if v>=0 else ''}₹{v:,.0f}"
fmt_pct = lambda v: f"{'+'if v>=0 else ''}{v:.2f}%"
pc      = lambda v: 'pos' if v >= 0 else 'neg'

def mcard(label: str, val: str, cls: str = 'neu') -> str:
    return (f'<div class="mcard"><div class="mcard-label">{label}</div>'
            f'<div class="mcard-val {cls}">{val}</div></div>')

def tcard(row, dm=None, dr=None) -> str:
    side  = str(row.get('side', ''))
    ret   = float(row['trade_return'])
    tgt   = int(row.get('target_hit_flag', 0))
    stp   = int(row.get('stop_hit_flag', 0))
    cls   = 'tcard-l' if side=='LONG' else ('tcard-st' if stp else 'tcard-s')
    s_tag = f'<span class="tag tg-l">LONG</span>' if side=='LONG' else '<span class="tag tg-s">SHORT</span>'
    o_tag = ('<span class="tag tg-t">TARGET ✓</span>' if tgt else
             '<span class="tag tg-x">STOP ✗</span>'  if stp else
             '<span class="tag tg-e">EOD</span>')
    rc = 'var(--long)' if ret >= 0 else 'var(--short)'
    p  = pnl(ret)
    ts = pd.to_datetime(row['entry_time']).strftime('%H:%M')
    ds = pd.to_datetime(row['trade_date']).strftime('%d %b')
    mv = (f' · Day <span style="color:{"var(--long)"if dm>=0 else "var(--short)"}">{dm*100:+.2f}%</span>'
          f' Range {dr*100:.2f}%' if dm is not None else '')
    return (f'<div class="tcard {cls}">'
            f'<div><div class="tn">{row["Stock"]}</div>'
            f'<div class="tm">{ds} · {ts} · Pred {float(row.get("Pred",0)):.3f}'
            f' · RR {float(row.get("RelativeRank",0)):.2f}{mv}</div>'
            f'<div style="margin-top:6px">{s_tag}{o_tag}</div></div>'
            f'<div><div class="tr" style="color:{rc}">{ret*100:+.2f}%</div>'
            f'<div class="tp" style="color:{rc}">{fmt_inr(p)}</div></div></div>')

CHART_LAYOUT = dict(
    paper_bgcolor='#03080f', plot_bgcolor='#060e1c',
    font=dict(family='Inter', color='#7b92b2', size=12),
    margin=dict(l=0, r=0, t=12, b=0),
    xaxis=dict(showgrid=False, color='#7b92b2', zeroline=False),
    yaxis=dict(showgrid=True, gridcolor='#112240', color='#7b92b2', zeroline=False),
)

# ══════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════
dot_html = '<span class="dot-live"></span>' if MARKET_OPEN else '<span class="dot-off"></span>'
status   = 'Market Open' if MARKET_OPEN else 'Market Closed'

with st.sidebar:
    st.markdown(f"""
    <div style="padding:18px 0 22px">
      <div style="font-family:'IBM Plex Mono',monospace;font-size:18px;font-weight:700;
                  color:#fff;letter-spacing:-0.02em">◈ Alpha Terminal</div>
      <div style="font-size:11px;color:var(--muted);margin-top:6px">
          {dot_html}{status} · {TODAY.strftime('%d %b %Y')}</div>
      <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;
                  color:var(--accent);margin-top:2px">{NOW.strftime('%H:%M:%S')} IST</div>
    </div>""", unsafe_allow_html=True)

    page = st.radio("", [
        "⚡  Live Feed",
        "📊  Results",
        "🔬  Analytics",
        "🏆  Stock Ledger",
        "📈  Portfolio",
    ], label_visibility="collapsed")

    st.markdown(f"""
    <div style="border-top:1px solid var(--border);margin-top:14px;padding-top:14px;
                font-size:11px;color:var(--muted);line-height:2">
      Capital &emsp; <span style="color:var(--text);font-family:'IBM Plex Mono',monospace">₹1,00,000</span><br>
      Cost &emsp;&emsp; <span style="color:var(--text);font-family:'IBM Plex Mono',monospace">16 bps</span><br>
      Refresh &emsp; <span style="color:var(--text);font-family:'IBM Plex Mono',monospace">60 s</span>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  DATE HELPERS
# ══════════════════════════════════════════════════════════════
ws       = TODAY - timedelta(days=TODAY.weekday())
lw_s     = ws - timedelta(7)
lw_e     = ws - timedelta(1)
lm_first = TODAY.replace(day=1)
lm_last  = (lm_first - timedelta(1))
lm_first = lm_last.replace(day=1)

PERIODS = {
    "Today":      (str(TODAY), str(TODAY)),
    "Yesterday":  (str(TODAY-timedelta(1)), str(TODAY-timedelta(1))),
    "This Week":  (str(ws), str(TODAY)),
    "Last Week":  (str(lw_s), str(lw_e)),
    "This Month": (str(TODAY.replace(day=1)), str(TODAY)),
    "Last Month": (str(lm_first), str(lm_last)),
}

def render_metrics(m: dict, ncols: int = 6):
    if not m:
        st.info("No trades in this period.")
        return
    cols = st.columns(ncols)
    data = [
        ("P&L",        fmt_inr(m['total']),           pc(m['total'])),
        ("Return",     fmt_pct(m['ret_pct']),          pc(m['ret_pct'])),
        ("Trades",     str(m['trades']),               'neu'),
        ("Target %",   f"{m['tgt_rate']:.0f}%",       'neu'),
        ("Win %",      f"{m['win_rate']:.0f}%",        'neu'),
        ("Stops",      str(m['stops']),                'warn' if m['stops'] else 'neu'),
    ]
    for col, (lbl, val, cls) in zip(cols, data):
        with col: st.markdown(mcard(lbl, val, cls), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  PAGE 1 — LIVE FEED
# ══════════════════════════════════════════════════════════════
if page == "⚡  Live Feed":
    st.markdown("# Live Feed")

    today_df = get_trades(str(TODAY), str(TODAY))
    m = summarize(today_df)

    # Hero P&L
    total = m.get('total', 0)
    cls   = 'hero-glow-pos' if total >= 0 else 'hero-glow-neg'
    st.markdown(
        f'<div class="hero-pnl {cls}">{fmt_inr(total)}</div>'
        f'<div style="font-size:13px;color:var(--muted);margin:6px 0 20px">'
        f'Today · {m.get("trades", 0)} signals · {fmt_pct(m.get("ret_pct", 0))} return</div>',
        unsafe_allow_html=True
    )

    render_metrics(m)

    # ── Signal Radar ────────────────────────────────────────────
    st.markdown('<div class="sec-head">Signal Radar — All Stocks</div>', unsafe_allow_html=True)

    radar = get_radar()
    if radar.empty:
        st.info("No candles yet today. Radar activates after 09:20 on weekdays.")
    else:
        # LONG proximity: count filters passing
        def long_prox(r):
            score = 0
            if float(r.get('Pred', 0))           >= 0.63:   score += 1
            if float(r.get('RelativeRank',
