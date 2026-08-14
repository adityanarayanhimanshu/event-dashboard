import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date, time as dtime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ══════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════
CAPITAL  = 100_000
COST_PCT = 8 / 10_000
RF_DAILY = 0.065 / 252

def ist():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

NOW   = ist()
TODAY = NOW.date()
MARKET_OPEN = (TODAY.weekday() < 5 and dtime(9,15) <= NOW.time() <= dtime(15,30))

# ══════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════════════
st.set_page_config(page_title="Alpha Terminal", layout="wide",
                   page_icon="◈", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=Inter:wght@400;500;600&display=swap');
:root{--bg:#03080f;--sf:#0a1628;--rz:#112240;--bd:#1a3050;
      --ac:#00ffcc;--lg:#00e87a;--sh:#ff2d55;--wn:#f5a623;
      --tx:#c8daf0;--mu:#7b92b2;--glow:rgba(0,255,204,0.13);}
html,body,[class*="css"]{font-family:'Inter',sans-serif;background:var(--bg);color:var(--tx);}
.stApp{background:var(--bg);}
[data-testid="stSidebar"]{background:#060e1c;border-right:1px solid var(--bd);}
.stTabs [data-baseweb="tab-list"]{background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:4px;gap:2px;}
.stTabs [data-baseweb="tab"]{color:var(--mu);font-size:13px;border-radius:6px;padding:6px 14px;}
.stTabs [aria-selected="true"]{background:var(--rz)!important;color:var(--ac)!important;}
.hero{font-family:'IBM Plex Mono',monospace;font-size:54px;font-weight:700;letter-spacing:-0.03em;line-height:1;}
.gpos{color:var(--ac);text-shadow:0 0 32px rgba(0,255,204,0.3);}
.gneg{color:var(--sh);text-shadow:0 0 32px rgba(255,45,85,0.3);}
.mc{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:14px 18px;height:100%;}
.ml{font-size:10px;color:var(--mu);text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;}
.mv{font-family:'IBM Plex Mono',monospace;font-size:24px;font-weight:600;color:#fff;}
.pos{color:var(--lg)!important;} .neg{color:var(--sh)!important;}
.neu{color:var(--ac)!important;} .wrn{color:var(--wn)!important;}
.tc{background:var(--sf);border:1px solid var(--bd);border-left:3px solid;border-radius:8px;
    padding:12px 16px;margin-bottom:7px;display:flex;justify-content:space-between;align-items:center;}
.tcl{border-left-color:var(--lg);} .tcs{border-left-color:var(--sh);} .tcx{border-left-color:var(--wn);}
.tn{font-family:'IBM Plex Mono',monospace;font-size:15px;font-weight:600;color:#fff;}
.tm{font-size:11px;color:var(--mu);margin-top:3px;}
.tr{font-family:'IBM Plex Mono',monospace;font-size:20px;font-weight:600;text-align:right;}
.tp{font-family:'IBM Plex Mono',monospace;font-size:12px;text-align:right;margin-top:2px;}
.tg{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:.06em;margin-right:4px;}
.tgl{background:rgba(0,232,122,.12);color:var(--lg);}
.tgs{background:rgba(255,45,85,.12);color:var(--sh);}
.tgt{background:rgba(0,255,204,.10);color:var(--ac);}
.tgx{background:rgba(245,166,35,.12);color:var(--wn);}
.tge{background:rgba(123,146,178,.12);color:var(--mu);}
.sh{font-size:10px;color:var(--mu);text-transform:uppercase;letter-spacing:.12em;
    border-bottom:1px solid var(--bd);padding-bottom:8px;margin:20px 0 14px 0;}
.rc{background:var(--sf);border:1px solid var(--bd);border-top:2px solid var(--ac);
    border-radius:8px;padding:16px;text-align:center;}
.rv{font-family:'IBM Plex Mono',monospace;font-size:30px;font-weight:700;}
.rl{font-size:10px;color:var(--mu);text-transform:uppercase;letter-spacing:.1em;margin-top:4px;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
.dlive{display:inline-block;width:7px;height:7px;border-radius:50%;
       background:var(--lg);margin-right:5px;animation:pulse 1.8s infinite;}
.doff{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--sh);margin-right:5px;}
</style>
<meta http-equiv="refresh" content="60">
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["NEON_URL"],
                         connect_args={"options":"-ctimezone=Asia/Kolkata"},
                         pool_pre_ping=True)
engine = get_engine()

# ══════════════════════════════════════════════════════════════
#  SQL
# ══════════════════════════════════════════════════════════════
def _core_sql(s, e):
    LF = """e."Pred">=0.63 AND e."RelativeRank">=p.rr_th AND e."NiftyMomentum">=p.nifty_th
    AND e."Momentum5">0.002
    AND e."Datetime" AT TIME ZONE 'Asia/Kolkata'<=(DATE(e."Datetime")+'09:45:00'::time)::timestamp
    AND e."LiquidityVacuum">0 AND e."Trend3">0 AND e."VolumeShock">1.1 AND e."Momentum60">-0.003
    AND e."Stock" NOT IN ('DLF','HCLTECH','PIIND','COALINDIA','PAYTM','TATACOMM')"""
    SF = """e."Pred"<=(1-p.pred_th) AND e."RelativeRank"<=(1-p.rr_th)
    AND e."NiftyMomentum"<=-p.nifty_th AND e."RelativeRank">0.05
    AND e."NiftyMomentum"<-0.002 AND e."Momentum5"<0 AND e."Momentum15"<0
    AND e."Momentum30"<0 AND e."VolumeShock">0.7 AND e."Momentum60"<0.005
    AND e."RelativeRank"<0.31 AND e."Trend3"<0.01
    AND e."Stock" NOT IN ('DLF','COALINDIA','PIIND','NAUKRI','WIPRO','GAIL','TATASTEEL','HDFCLIFE')"""
    return f"""
WITH p AS (SELECT 0.62 AS pred_th,0.65 AS rr_th,0.00 AS nifty_th),
sig AS (
    SELECT e."Stock",e."Datetime" AS entry_time,DATE(e."Datetime") AS trade_date,
           e."Close" AS entry_price,e."Pred",e."RelativeRank",e."NiftyMomentum",
           CASE WHEN {LF} THEN 'LONG' WHEN {SF} THEN 'SHORT' END AS side
    FROM events e CROSS JOIN p
    WHERE DATE(e."Datetime") BETWEEN '{s}' AND '{e}'
      AND ({LF} OR {SF})
      AND e."Datetime" AT TIME ZONE 'Asia/Kolkata'>=(DATE(e."Datetime")+'09:15:00'::time)::timestamp
      AND e."Datetime" AT TIME ZONE 'Asia/Kolkata'<=(DATE(e."Datetime")+'10:15:00'::time)::timestamp
),
fp AS (
    SELECT s."Stock",s.trade_date,s.entry_time,s.side,s.entry_price,s."Pred",s."RelativeRank",
           e."Datetime" AS ft,
           CASE WHEN s.side='LONG' THEN (e."Close"-s.entry_price)/s.entry_price
                ELSE (s.entry_price-e."Close")/s.entry_price END AS fr,
           EXTRACT(EPOCH FROM (e."Datetime"-s.entry_time))/60 AS mins
    FROM sig s JOIN events e ON s."Stock"=e."Stock"
      AND e."Datetime">=s.entry_time
      AND e."Datetime"<=(DATE(s.entry_time)+'15:05:00'::time)::timestamp
),
th AS (SELECT DISTINCT ON("Stock",entry_time) "Stock",entry_time,mins FROM fp WHERE fr>=0.008  ORDER BY "Stock",entry_time,ft),
sh AS (SELECT DISTINCT ON("Stock",entry_time) "Stock",entry_time,mins FROM fp WHERE fr<=-0.018 ORDER BY "Stock",entry_time,ft),
ex AS (SELECT DISTINCT ON("Stock",entry_time) "Stock",entry_time,fr            FROM fp          ORDER BY "Stock",entry_time,ft DESC)
SELECT s."Stock",s.trade_date,s.entry_time,s.side,s.entry_price,s."Pred",s."RelativeRank",s."NiftyMomentum",
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
def get_trades(s,e):
    try:    return pd.read_sql(_core_sql(s,e), engine)
    except Exception as ex: st.error(f"DB error: {ex}"); return pd.DataFrame()

@st.cache_data(ttl=60)
def get_radar():
    sql="""SELECT DISTINCT ON ("Stock") "Stock","Datetime","Pred","RelativeRank",
           "NiftyMomentum","VolumeShock","Trend3","Momentum5","LiquidityVacuum","Momentum60"
           FROM events WHERE DATE("Datetime")=CURRENT_DATE
           AND "Datetime" AT TIME ZONE 'Asia/Kolkata'>=(CURRENT_DATE+'09:15:00'::time)::timestamp
           ORDER BY "Stock","Datetime" DESC"""
    try:    return pd.read_sql(sql, engine)
    except: return pd.DataFrame()

@st.cache_data(ttl=300)
def get_moves(s,e):
    sql=f"""WITH r AS (
        SELECT "Stock",DATE("Datetime") dt,"Close","High","Low",
               ROW_NUMBER() OVER(PARTITION BY "Stock",DATE("Datetime") ORDER BY "Datetime") ra,
               ROW_NUMBER() OVER(PARTITION BY "Stock",DATE("Datetime") ORDER BY "Datetime" DESC) rd
        FROM events WHERE DATE("Datetime") BETWEEN '{s}' AND '{e}'
    )
    SELECT "Stock",dt AS trade_date,
           MAX(CASE WHEN ra=1 THEN "Close" END) op,
           MAX(CASE WHEN rd=1 THEN "Close" END) cl,
           MAX("High") hi,MIN("Low") lo
    FROM r GROUP BY "Stock",dt"""
    try:
        df=pd.read_sql(sql,engine)
        df['dr']=(df['cl']-df['op'])/df['op']
        df['rng']=(df['hi']-df['lo'])/df['op']
        return df
    except: return pd.DataFrame()

# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def pnl(r):   return CAPITAL*(r-COST_PCT)
def fi(v):    return f"{'+'if v>=0 else ''}₹{v:,.0f}"
def fp2(v):   return f"{'+'if v>=0 else ''}{v:.2f}%"
def pc(v):    return 'pos' if v>=0 else 'neg'

def summ(df):
    if df.empty: return {}
    pnls=[pnl(r) for r in df['trade_return']]
    return dict(trades=len(df),total=sum(pnls),ret=df['trade_return'].sum()*100,
                tgt=df['target_hit_flag'].mean()*100,win=(df['trade_return']>0).mean()*100,
                stops=int(df['stop_hit_flag'].sum()),avg=df['trade_return'].mean()*100)

def ratios(df):
    if len(df)<5: return {}
    df=df.copy(); df['entry_time']=pd.to_datetime(df['entry_time'])
    df['pv']=df['trade_return'].apply(pnl)
    dr=(df.groupby(df['entry_time'].dt.date)['pv'].sum()/CAPITAL)
    mn,sd=dr.mean(),dr.std()
    nsd=dr[dr<0].std()
    sh=(mn-RF_DAILY)/sd*np.sqrt(252) if sd>0 else 0
    so=(mn-RF_DAILY)/nsd*np.sqrt(252) if nsd>0 else 0
    cm_v=df['pv'].cumsum(); pk=cm_v.cummax()
    dd=abs(((cm_v-pk)/(CAPITAL+pk.abs())*100).min())
    ar=mn*252*100
    ca=ar/dd if dd>0 else 0
    return dict(sh=round(sh,2),so=round(so,2),ca=round(ca,2),
                dd=round(dd,2),ar=round(ar,1))

def classify(row):
    t=row['trades']; w=row['win']; p=row['total']; s=row['stops']
    if t<3:         return '⚪','Thin data',    '#7b92b2'
    if w>=.70 and p>0: return '🟢','Keep',      '#00e87a'
    if s>=2:        return '🔴','Blacklist?',   '#ff2d55'
    if w>=.50 and p>-5000: return '🟡','Watch', '#f5a623'
    return '🔴','Blacklist?','#ff2d55'

def mcard(lbl,val,cls='neu'):
    return f'<div class="mc"><div class="ml">{lbl}</div><div class="mv {cls}">{val}</div></div>'

def render_m(m,n=6):
    if not m: st.info("No trades."); return
    cols=st.columns(n)
    data=[("P&L",fi(m['total']),pc(m['total'])),("Return",fp2(m['ret']),pc(m['ret'])),
          ("Trades",str(m['trades']),'neu'),("Target %",f"{m['tgt']:.0f}%",'neu'),
          ("Win %",f"{m['win']:.0f}%",'neu'),("Stops",str(m['stops']),'wrn' if m['stops'] else 'neu')]
    for col,(l,v,c) in zip(cols,data):
        with col: st.markdown(mcard(l,v,c),unsafe_allow_html=True)

def tcard(row,dm=None,dr=None):
    sd=str(row.get('side','')); r=float(row['trade_return'])
    tg=int(row.get('target_hit_flag',0)); st2=int(row.get('stop_hit_flag',0))
    cc='tcl' if sd=='LONG' else ('tcx' if st2 else 'tcs')
    sg=f'<span class="tg tgl">LONG</span>' if sd=='LONG' else '<span class="tg tgs">SHORT</span>'
    og=('<span class="tg tgt">TARGET ✓</span>' if tg else
        '<span class="tg tgx">STOP ✗</span>'   if st2 else
        '<span class="tg tge">EOD</span>')
    rc='var(--lg)' if r>=0 else 'var(--sh)'
    p2=pnl(r)
    ts=pd.to_datetime(row['entry_time']).strftime('%H:%M')
    ds=pd.to_datetime(row['trade_date']).strftime('%d %b')
    mv=''
    if dm is not None:
        mc2='var(--lg)' if dm>=0 else 'var(--sh)'
        mv=f' · Day <span style="color:{mc2}">{dm*100:+.2f}%</span> Rng {dr*100:.2f}%' if dr else f' · Day <span style="color:{mc2}">{dm*100:+.2f}%</span>'
    return (f'<div class="tc {cc}"><div>'
            f'<div class="tn">{row["Stock"]}</div>'
            f'<div class="tm">{ds} · {ts} · Pred {float(row.get("Pred",0)):.3f} · RR {float(row.get("RelativeRank",0)):.2f}{mv}</div>'
            f'<div style="margin-top:6px">{sg}{og}</div></div>'
            f'<div><div class="tr" style="color:{rc}">{r*100:+.2f}%</div>'
            f'<div class="tp" style="color:{rc}">{fi(p2)}</div></div></div>')

CL=dict(paper_bgcolor='#03080f',plot_bgcolor='#060e1c',
        font=dict(family='Inter',color='#7b92b2',size=12),
        margin=dict(l=0,r=0,t=12,b=0),
        xaxis=dict(showgrid=False,color='#7b92b2',zeroline=False),
        yaxis=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',zeroline=False))

# ══════════════════════════════════════════════════════════════
#  DATES
# ══════════════════════════════════════════════════════════════
ws=TODAY-timedelta(days=TODAY.weekday())
lws=ws-timedelta(7); lwe=ws-timedelta(1)
lmf=TODAY.replace(day=1); lml=(lmf-timedelta(1)); lmf=lml.replace(day=1)
PERIODS={"Today":(str(TODAY),str(TODAY)),
          "Yesterday":(str(TODAY-timedelta(1)),str(TODAY-timedelta(1))),
          "This Week":(str(ws),str(TODAY)),
          "Last Week":(str(lws),str(lwe)),
          "This Month":(str(TODAY.replace(day=1)),str(TODAY)),
          "Last Month":(str(lmf),str(lml))}

# ══════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════
dot='<span class="dlive"></span>' if MARKET_OPEN else '<span class="doff"></span>'
st_txt='Market Open' if MARKET_OPEN else 'Market Closed'
with st.sidebar:
    st.markdown(f"""<div style="padding:18px 0 22px">
      <div style="font-family:'IBM Plex Mono',monospace;font-size:18px;font-weight:700;color:#fff">◈ Alpha Terminal</div>
      <div style="font-size:11px;color:var(--mu);margin-top:5px">{dot}{st_txt} · {TODAY.strftime('%d %b %Y')}</div>
      <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--ac);margin-top:2px">{NOW.strftime('%H:%M:%S')} IST</div>
    </div>""",unsafe_allow_html=True)
    page=st.radio("",["⚡  Live Feed","📊  Results","🔬  Analytics","🏆  Stock Ledger","📈  Portfolio"],
                  label_visibility="collapsed")
    st.markdown(f"""<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px;
                font-size:11px;color:var(--mu);line-height:2">
      Capital &emsp;<span style="color:var(--tx);font-family:'IBM Plex Mono',monospace">₹1,00,000</span><br>
      Cost &emsp;&emsp;<span style="color:var(--tx);font-family:'IBM Plex Mono',monospace">8 bps</span><br>
      Refresh &emsp;<span style="color:var(--tx);font-family:'IBM Plex Mono',monospace">60 s</span>
    </div>""",unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  PAGE 1 — LIVE FEED
# ══════════════════════════════════════════════════════════════
if page=="⚡  Live Feed":
    st.markdown("# Live Feed")
    df=get_trades(str(TODAY),str(TODAY)); m=summ(df)
    tot=m.get('total',0); cls='gpos' if tot>=0 else 'gneg'
    st.markdown(f'<div class="hero {cls}">{fi(tot)}</div>'
                f'<div style="font-size:13px;color:var(--mu);margin:6px 0 20px">'
                f'Today · {m.get("trades",0)} signals · {fp2(m.get("ret",0))} return</div>',
                unsafe_allow_html=True)
    render_m(m)

    st.markdown('<div class="sh">Signal Radar — All Stocks Today</div>',unsafe_allow_html=True)
    rd=get_radar()
    if rd.empty:
        st.info("Radar activates after 09:20 on weekdays.")
    else:
        def lp(r):
            s=0
            if float(r.get('Pred',0))>=0.63:          s+=1
            if float(r.get('RelativeRank',0))>=0.65:   s+=1
            if float(r.get('NiftyMomentum',0))>=0.0:   s+=1
            if float(r.get('Momentum5',0))>0.002:       s+=1
            if float(r.get('LiquidityVacuum',0))>0:     s+=1
            if float(r.get('Trend3',0))>0:              s+=1
            if float(r.get('VolumeShock',0))>1.1:       s+=1
            if float(r.get('Momentum60',0))>-0.003:     s+=1
            return s/8
        def sp(r):
            s=0
            if float(r.get('Pred',0))<=0.38:            s+=1
            if float(r.get('RelativeRank',0))<=0.35:    s+=1
            if float(r.get('NiftyMomentum',0))<-0.002:  s+=1
            if float(r.get('Momentum5',0))<0:            s+=1
            if float(r.get('VolumeShock',0))>0.7:        s+=1
            if float(r.get('Trend3',0))<0.01:            s+=1
            return s/6
        rd['ls']=rd.apply(lp,axis=1); rd['ss']=rd.apply(sp,axis=1)
        rd['ms']=rd[['ls','ss']].max(axis=1)
        rd['vs']=rd['VolumeShock'].clip(0,4).fillna(1)
        clrs=rd['ms'].apply(lambda v:f'rgba(0,232,122,{v:.2f})' if v>=.5 else f'rgba(255,45,85,{max(v,.1):.2f})')
        fig_r=go.Figure(go.Scatter(
            x=rd['Pred'],y=rd['RelativeRank'],mode='markers+text',
            text=rd['Stock'],textposition='top center',
            textfont=dict(size=8,color='#7b92b2'),
            marker=dict(size=rd['vs']*10+4,color=list(clrs),
                        line=dict(color='rgba(255,255,255,0.08)',width=1)),
            customdata=list(zip(rd['ms'].round(2),rd['VolumeShock'].round(2),rd['NiftyMomentum'].round(4))),
            hovertemplate=('<b>%{text}</b><br>Pred:%{x:.3f} RR:%{y:.3f}<br>'
                           'Proximity:%{customdata[0]:.0%}<br>'
                           'VolShock:%{customdata[1]:.2f}<br>'
                           'NiftyMom:%{customdata[2]:.4f}<extra></extra>')
        ))
        fig_r.add_vline(x=0.63,line_dash="dot",line_color="rgba(0,232,122,0.4)",line_width=1)
        fig_r.add_vline(x=0.38,line_dash="dot",line_color="rgba(255,45,85,0.4)",line_width=1)
        fig_r.add_hline(y=0.65,line_dash="dot",line_color="rgba(0,232,122,0.4)",line_width=1)
        fig_r.add_hline(y=0.35,line_dash="dot",line_color="rgba(255,45,85,0.4)",line_width=1)
        fig_r.update_layout(
            paper_bgcolor='#03080f',plot_bgcolor='#060e1c',
            font=dict(family='Inter',color='#7b92b2',size=12),
            margin=dict(l=0,r=0,t=12,b=0),height=420,
            xaxis=dict(showgrid=False,color='#7b92b2',zeroline=False,title="Pred Score",range=[0.2,0.9]),
            yaxis=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',zeroline=False,title="Relative Rank",range=[0,1])
        )
        st.plotly_chart(fig_r,use_container_width=True)
        st.caption("Bubble size = VolumeShock · Green = nearing LONG threshold · Red = nearing SHORT threshold · Dotted lines = signal thresholds")

    if not df.empty:
        st.markdown('<div class="sh">Today\'s Signals</div>',unsafe_allow_html=True)
        mv=get_moves(str(TODAY),str(TODAY))
        if not mv.empty:
            mv['trade_date']=pd.to_datetime(mv['trade_date']).dt.date
            df['tdd']=pd.to_datetime(df['trade_date']).dt.date
            df=df.merge(mv[['Stock','trade_date','dr','rng']],
                        left_on=['Stock','tdd'],right_on=['Stock','trade_date'],how='left')
        srt=st.selectbox("Sort",["Return ↓","Return ↑","Entry Time"],key="ls")
        if srt=="Return ↓":   df=df.sort_values('trade_return',ascending=False)
        elif srt=="Return ↑": df=df.sort_values('trade_return')
        else:                  df=df.sort_values('entry_time')
        html=""
        for _,row in df.iterrows():
            dm=float(row['dr'])  if 'dr'  in row and pd.notna(row.get('dr'))  else None
            dr2=float(row['rng']) if 'rng' in row and pd.notna(row.get('rng')) else None
            html+=tcard(row,dm,dr2)
        st.markdown(html,unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  PAGE 2 — RESULTS
# ══════════════════════════════════════════════════════════════
elif page=="📊  Results":
    st.markdown("# Period Results")
    tabs=st.tabs(list(PERIODS.keys()))
    for tab,(pn,(s,e)) in zip(tabs,PERIODS.items()):
        with tab:
            df=get_trades(s,e)
            if df.empty: st.info(f"No trades · {s} → {e}"); continue
            m=summ(df); render_m(m)
            st.markdown(f'<div style="font-size:11px;color:var(--mu);margin:12px 0 16px">{s} → {e}</div>',unsafe_allow_html=True)
            c1,c2=st.columns(2)
            for col,sl in [(c1,'LONG'),(c2,'SHORT')]:
                with col:
                    sd=df[df['side']==sl]
                    if len(sd):
                        sp2=sum(pnl(r) for r in sd['trade_return'])
                        tc2='tgl' if sl=='LONG' else 'tgs'
                        col_s='var(--lg)' if sp2>=0 else 'var(--sh)'
                        st.markdown(f'<div class="mc" style="padding:12px 16px">'
                                    f'<span class="tg {tc2}">{sl}</span>'
                                    f'<span style="font-family:\'IBM Plex Mono\',monospace;font-size:13px;color:#fff;margin-left:6px">'
                                    f'{len(sd)}t · {sd["target_hit_flag"].mean()*100:.0f}% tgt · {(sd["trade_return"]>0).mean()*100:.0f}% win</span>'
                                    f'<span style="font-family:\'IBM Plex Mono\',monospace;font-size:13px;margin-left:10px;color:{col_s}">{fi(sp2)}</span>'
                                    f'</div>',unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="mc" style="padding:12px 16px">'
                                    f'<span class="tg {"tgl" if sl=="LONG" else "tgs"}">{sl}</span>'
                                    f'<span style="font-size:13px;color:var(--mu)">No trades</span></div>',
                                    unsafe_allow_html=True)
            dday=df.groupby('trade_date').agg(
                Trades=('trade_return','count'),
                Return=('trade_return',lambda x:round(x.sum()*100,2)),
                PnL=('trade_return',lambda x:round(sum(pnl(r) for r in x))),
                Target=('target_hit_flag','sum'),
                Wins=('trade_return',lambda x:int((x>0).sum())),
                Stops=('stop_hit_flag','sum'),
            ).reset_index().rename(columns={'trade_date':'Date'})
            dday['Return']=dday['Return'].apply(lambda x:f"{x:+.2f}%")
            dday['PnL']=dday['PnL'].apply(fi)
            st.dataframe(dday,use_container_width=True,hide_index=True)
            with st.expander("Full trade list"):
                d=df[['Stock','trade_date','entry_time','side','trade_return',
                      'target_hit_flag','stop_hit_flag','eod_return','Pred','RelativeRank']].copy()
                d['Ret%']=(d['trade_return']*100).round(3)
                d['PnL (₹)']=d['trade_return'].apply(lambda x:round(pnl(x)))
                d['EOD%']=(d['eod_return']*100).round(3)
                d['Pred']=d['Pred'].round(3); d['RR']=d['RelativeRank'].round(3)
                st.dataframe(d.rename(columns={'trade_date':'Date','entry_time':'Entry',
                                               'side':'Side','target_hit_flag':'Tgt','stop_hit_flag':'Stp'})
                              [['Stock','Date','Entry','Side','Ret%','PnL (₹)','Tgt','Stp','EOD%','Pred','RR']],
                             use_container_width=True,hide_index=True)

# ══════════════════════════════════════════════════════════════
#  PAGE 3 — ANALYTICS
# ══════════════════════════════════════════════════════════════
elif page=="🔬  Analytics":
    st.markdown("# Analytics")
    adf=get_trades("2026-03-10",str(TODAY))
    if adf.empty: st.info("Not enough data.")
    else:
        adf['entry_time']=pd.to_datetime(adf['entry_time'])
        adf['time_slot']=adf['entry_time'].dt.strftime('%H:%M')
        adf['dow']=adf['entry_time'].dt.day_name()
        adf['pv']=adf['trade_return'].apply(pnl)
        t1,t2,t3=st.tabs(["Entry Time","Distribution","Regime"])

        with t1:
            st.markdown('<div class="sh">Avg Return % · Entry Time × Side</div>',unsafe_allow_html=True)
            pv=adf.groupby(['side','time_slot'])['trade_return'].mean().unstack('side')*100
            pv=pv.fillna(0).sort_index()
            sides=[c for c in ['LONG','SHORT'] if c in pv.columns]
            times=list(pv.index)
            zv=[[pv.loc[t,s] if s in pv.columns else 0 for s in sides] for t in times]
            fig_h=go.Figure(go.Heatmap(z=zv,x=sides,y=times,
                colorscale=[[0,'#ff2d55'],[0.5,'#112240'],[1,'#00ffcc']],zmid=0,
                text=[[f"{v:+.2f}%" for v in row] for row in zv],
                texttemplate="%{text}",showscale=True))
            fig_h.update_layout(**CL,height=360)
            st.plotly_chart(fig_h,use_container_width=True)

            st.markdown('<div class="sh">Trade Count by Entry Time</div>',unsafe_allow_html=True)
            tc=adf.groupby(['time_slot','side']).size().unstack('side').fillna(0)
            fig_tc=go.Figure()
            for s,c in [('LONG','#00e87a'),('SHORT','#ff2d55')]:
                if s in tc.columns:
                    fig_tc.add_trace(go.Bar(x=list(tc.index),y=tc[s],name=s,marker_color=c,opacity=0.75))
            fig_tc.update_layout(**CL,height=240,barmode='group',showlegend=True,
                                 legend=dict(bgcolor='#0a1628',bordercolor='#112240'))
            st.plotly_chart(fig_tc,use_container_width=True)

        with t2:
            c1,c2=st.columns(2)
            with c1:
                st.markdown('<div class="sh">Return Distribution</div>',unsafe_allow_html=True)
                rets=adf['trade_return']*100
                fig_hi=go.Figure()
                fig_hi.add_trace(go.Histogram(x=rets,nbinsx=30,marker_color='#00ffcc',opacity=0.7))
                fig_hi.add_vline(x=0.8,line_dash="dot",line_color='rgba(0,232,122,0.5)')
                fig_hi.add_vline(x=-1.8,line_dash="dot",line_color='rgba(255,45,85,0.5)')
                fig_hi.update_layout(**CL,height=280,showlegend=False,
                                     xaxis_title="Return %",yaxis_title="Count")
                st.plotly_chart(fig_hi,use_container_width=True)
            with c2:
                st.markdown('<div class="sh">P&L by Day of Week</div>',unsafe_allow_html=True)
                dow_o=['Monday','Tuesday','Wednesday','Thursday','Friday']
                dp=adf.groupby('dow')['pv'].sum().reindex(dow_o)
                bc=['#00e87a' if v>=0 else '#ff2d55' for v in dp]
                fig_d=go.Figure(go.Bar(x=dp.index,y=dp.values,marker_color=bc,opacity=0.8,
                    text=[fi(v) for v in dp],textposition='outside',
                    textfont=dict(size=9,family='IBM Plex Mono',color='#7b92b2')))
                fig_d.update_layout(**CL,height=280,showlegend=False)
                st.plotly_chart(fig_d,use_container_width=True)

        with t3:
            adf['regime']=adf['NiftyMomentum'].apply(
                lambda x:'Bullish' if float(x)>0.002 else ('Bearish' if float(x)<-0.002 else 'Neutral'))
            rg=adf.groupby('regime').agg(
                Days=('trade_date','nunique'),Trades=('trade_return','count'),
                Total=('pv','sum'),TgtRate=('target_hit_flag','mean'),
                WinRate=('trade_return',lambda x:(x>0).mean()),Stops=('stop_hit_flag','sum'),
            ).reset_index()
            rg['P&L']=rg['Total'].apply(fi)
            rg['TgtRate']=(rg['TgtRate']*100).apply(lambda x:f"{x:.0f}%")
            rg['WinRate']=(rg['WinRate']*100).apply(lambda x:f"{x:.0f}%")
            st.dataframe(rg[['regime','Days','Trades','P&L','TgtRate','WinRate','Stops']]
                           .rename(columns={'regime':'Regime'}),
                         use_container_width=True,hide_index=True)

# ══════════════════════════════════════════════════════════════
#  PAGE 4 — STOCK LEDGER
# ══════════════════════════════════════════════════════════════
elif page=="🏆  Stock Ledger":
    st.markdown("# Stock Ledger")
    adf=get_trades("2026-03-10",str(TODAY))
    if adf.empty: st.info("No data.")
    else:
        adf['pv']=adf['trade_return'].apply(pnl)
        sdf=adf.groupby(['Stock','side']).agg(
            trades=('trade_return','count'),total=('pv','sum'),
            win=('trade_return',lambda x:(x>0).mean()),
            tgt=('target_hit_flag','mean'),stops=('stop_hit_flag','sum'),
            avg=('trade_return','mean'),
        ).reset_index()
        sdf['status'],sdf['label'],sdf['color']=zip(*sdf.apply(classify,axis=1))
        sc=st.selectbox("Sort by",["Total P&L","Win Rate","Trades","Stops"])
        cm={"Total P&L":"total","Win Rate":"win","Trades":"trades","Stops":"stops"}
        sdf=sdf.sort_values(cm[sc],ascending=False)
        fs=st.radio("Side",["All","LONG","SHORT"],horizontal=True)
        if fs!="All": sdf=sdf[sdf['side']==fs]
        for _,row in sdf.iterrows():
            pv2=float(row['total']); pc2='var(--lg)' if pv2>=0 else 'var(--sh)'
            sc2='tgl' if row['side']=='LONG' else 'tgs'
            tc3='tcl' if row['side']=='LONG' else 'tcs'
            st.markdown(f"""<div class="tc {tc3}" style="border-left-color:{row['color']}">
              <div>
                <div class="tn">{row['status']} {row['Stock']}
                  <span class="tg {sc2}" style="margin-left:6px">{row['side']}</span>
                  <span style="font-size:11px;color:{row['color']};margin-left:4px">{row['label']}</span>
                </div>
                <div class="tm" style="margin-top:5px">
                  {int(row['trades'])} trades &ensp;
                  Target {row['tgt']*100:.0f}% &ensp;
                  Win {row['win']*100:.0f}% &ensp;
                  Avg {row['avg']*100:+.3f}% &ensp;
                  Stops {int(row['stops'])}
                </div>
              </div>
              <div><div class="tr" style="color:{pc2}">{fi(pv2)}</div></div>
            </div>""",unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  PAGE 5 — PORTFOLIO
# ══════════════════════════════════════════════════════════════
elif page=="📈  Portfolio":
    st.markdown("# Portfolio")
    adf=get_trades("2026-03-10",str(TODAY))
    if adf.empty: st.info("No trade history.")
    else:
        adf=adf.sort_values('entry_time').reset_index(drop=True)
        adf['entry_time']=pd.to_datetime(adf['entry_time'])
        adf['pv']=adf['trade_return'].apply(pnl)
        adf['cum']=adf['pv'].cumsum()
        pk=adf['cum'].cummax(); adf['dd']=(adf['cum']-pk)/(CAPITAL+pk.abs())*100
        tot=adf['cum'].iloc[-1]; mdd=abs(adf['dd'].min())
        rt=ratios(adf)

        c1,c2,c3,c4,c5=st.columns(5)
        rdata=[(c1,"Sharpe",rt.get('sh',0),True),(c2,"Sortino",rt.get('so',0),True),
               (c3,"Calmar",rt.get('ca',0),True),(c4,"Ann Ret %",rt.get('ar',0),True),
               (c5,"Max DD %",mdd,False)]
        for col,lbl,val,is_pos_good in rdata:
            with col:
                v=f"{val:.2f}" if lbl not in ['Ann Ret %','Max DD %'] else f"{val:.1f}%"
                vc='var(--ac)' if (is_pos_good and val>0) else ('var(--sh)' if not is_pos_good else 'var(--mu)')
                st.markdown(f'<div class="rc"><div class="rv" style="color:{vc}">{v}</div>'
                            f'<div class="rl">{lbl}</div></div>',unsafe_allow_html=True)

        # Equity + Drawdown
        st.markdown('<div class="sh">Equity Curve + Drawdown</div>',unsafe_allow_html=True)
        fig=make_subplots(rows=2,cols=1,row_heights=[0.68,0.32],
                          shared_xaxes=True,vertical_spacing=0.04)
        dc=['#00e87a' if v>=0 else '#ff2d55' for v in adf['pv']]
        fig.add_trace(go.Scatter(x=adf['entry_time'],y=adf['cum'],
            fill='tozeroy',fillcolor='rgba(0,255,204,0.05)',
            line=dict(color='#00ffcc',width=2),name='Cumulative P&L',
            hovertemplate='%{x|%d %b %Y}<br>₹%{y:,.0f}<extra></extra>'),row=1,col=1)
        fig.add_trace(go.Scatter(x=adf['entry_time'],y=adf['cum'],
            mode='markers',marker=dict(size=5,color=dc,opacity=0.5),name='Trades',
            customdata=list(zip(adf['Stock'],adf['pv'])),
            hovertemplate='%{customdata[0]}<br>₹%{customdata[1]:,.0f}<extra></extra>'),row=1,col=1)
        fig.add_trace(go.Scatter(x=adf['entry_time'],y=adf['dd'],
            fill='tozeroy',fillcolor='rgba(255,45,85,0.12)',
            line=dict(color='#ff2d55',width=1.5),name='Drawdown %',
            hovertemplate='DD: %{y:.2f}%<extra></extra>'),row=2,col=1)
        fig.add_hline(y=0,line_dash="dot",line_color='#1a3050',row=1,col=1)
        fig.update_layout(
            paper_bgcolor='#03080f',plot_bgcolor='#060e1c',
            font=dict(family='Inter',color='#7b92b2',size=12),
            margin=dict(l=0,r=0,t=12,b=0),
            height=500,
            xaxis=dict(showgrid=False,color='#7b92b2',zeroline=False),
            yaxis=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',zeroline=False,
                       tickprefix='₹',tickformat=',.0f'),
            xaxis2=dict(showgrid=False,color='#7b92b2',zeroline=False),
            yaxis2=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',
                        ticksuffix='%',zeroline=False),
            legend=dict(bgcolor='#0a1628',bordercolor='#112240',font=dict(color='#7b92b2')),
            hovermode='x unified')
        st.plotly_chart(fig,use_container_width=True)

        # Rolling 20-trade win rate
        st.markdown('<div class="sh">Rolling 20-Trade Win Rate</div>',unsafe_allow_html=True)
        adf['rwr']=(adf['trade_return']>0).rolling(20).mean()*100
        fig_w=go.Figure(go.Scatter(x=adf['entry_time'],y=adf['rwr'],
            line=dict(color='#00ffcc',width=2),fill='tozeroy',
            fillcolor='rgba(0,255,204,0.04)',
            hovertemplate='%{x|%d %b}<br>Win Rate: %{y:.1f}%<extra></extra>'))
        fig_w.add_hline(y=50,line_dash="dot",line_color='#7b92b2',line_width=1)
        fig_w.update_layout(
            paper_bgcolor='#03080f',plot_bgcolor='#060e1c',
            font=dict(family='Inter',color='#7b92b2',size=12),
            margin=dict(l=0,r=0,t=12,b=0),height=200,showlegend=False,
            xaxis=dict(showgrid=False,color='#7b92b2',zeroline=False),
            yaxis=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',
                       zeroline=False,ticksuffix='%',range=[0,100]))
        st.plotly_chart(fig_w,use_container_width=True)

        # Monthly heatmap
        st.markdown('<div class="sh">Monthly P&L Heatmap</div>',unsafe_allow_html=True)
        adf['yr']=adf['entry_time'].dt.year; adf['mo']=adf['entry_time'].dt.month
        months_l=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        years_l=sorted(adf['yr'].unique())
        mp=adf.groupby(['yr','mo'])['pv'].sum()
        zz=[[float(mp.get((y,m),0)) for m in range(1,13)] for y in years_l]
        tz=[[fi(float(mp.get((y,m),0))) if mp.get((y,m),0)!=0 else '' for m in range(1,13)] for y in years_l]
        fig_hm=go.Figure(go.Heatmap(z=zz,x=months_l,y=[str(y) for y in years_l],
            colorscale=[[0,'#ff2d55'],[0.5,'#0a1628'],[1,'#00ffcc']],zmid=0,
            text=tz,texttemplate="%{text}",textfont=dict(size=10,family='IBM Plex Mono'),
            showscale=True,hovertemplate='%{y} %{x}<br>%{text}<extra></extra>'))
        fig_hm.update_layout(
            paper_bgcolor='#03080f',plot_bgcolor='#060e1c',
            font=dict(family='Inter',color='#7b92b2',size=12),
            margin=dict(l=0,r=0,t=12,b=0),
            height=max(120,len(years_l)*60+60),
            xaxis=dict(showgrid=False,color='#7b92b2',zeroline=False,side='top'),
            yaxis=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',zeroline=False))
        st.plotly_chart(fig_hm,use_container_width=True)

        # Daily bars
        st.markdown('<div class="sh">Daily P&L</div>',unsafe_allow_html=True)
        dp2=adf.groupby(adf['entry_time'].dt.date)['pv'].sum().reset_index()
        dp2.columns=['date','d']
        fig_db=go.Figure(go.Bar(x=dp2['date'],y=dp2['d'],
            marker_color=['#00e87a' if v>=0 else '#ff2d55' for v in dp2['d']],
            opacity=0.8,hovertemplate='%{x}<br>₹%{y:,.0f}<extra></extra>'))
        fig_db.add_hline(y=0,line_color='#1a3050')
        fig_db.update_layout(
            paper_bgcolor='#03080f',plot_bgcolor='#060e1c',
            font=dict(family='Inter',color='#7b92b2',size=12),
            margin=dict(l=0,r=0,t=12,b=0),height=220,showlegend=False,
            xaxis=dict(showgrid=False,color='#7b92b2',zeroline=False),
            yaxis=dict(showgrid=True,gridcolor='#112240',color='#7b92b2',
                       zeroline=False,tickprefix='₹',tickformat=',.0f'))
        st.plotly_chart(fig_db,use_container_width=True)
        
