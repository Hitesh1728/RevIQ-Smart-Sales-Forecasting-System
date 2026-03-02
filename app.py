"""
AI Sales Forecasting System — Streamlit Frontend
Multi-file upload · AutoML · Forecast · Dashboard
"""

import streamlit as st
import requests
import os
import time
import webbrowser
import tempfile
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import threading

st.set_page_config(
    page_title="RevIQ — Smart Sales Forecasting System",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

API_BASE = os.getenv("BACKEND_URL", "http://localhost:8000")

# ─── CSS ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  

  :root {
    --bg:       #020202;
    --bg1:      #0a0a0a;
    --bg2:      #111111;
    --bg3:      #181818;
    --border:   #1f1f1f;
    --border2:  #2a2a2a;
    --text:     #ffffff;
    --muted:    #888888;
    --muted2:   #aaaaaa;
    --accent:   #c8ff00;
    --accent2:  #00ffcc;
    --red:      #ff4455;
    --mono:     'Times New Roman', Times, serif;
    --sans:     'Times New Roman', Times, serif;
  }

  html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"],
  .main, .block-container {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: var(--sans) !important;
  }

  .block-container { padding-top: 1.5rem !important; max-width: 1400px !important; }

  [data-testid="stSidebar"] {
    background-color: var(--bg1) !important;
    border-right: 1px solid var(--border) !important;
  }
  [data-testid="stSidebar"] * { color: var(--text) !important; }

  p, span, label, div, h1, h2, h3, h4, h5, h6,
  .stMarkdown, .stText { color: var(--text) !important; }

  /* File uploader */
  [data-testid="stFileUploader"] {
    background: var(--bg1) !important;
    border: 1px dashed var(--border2) !important;
    border-radius: 10px !important;
    transition: border-color 0.2s !important;
  }
  [data-testid="stFileUploader"]:hover {
    border-color: var(--accent) !important;
  }
  [data-testid="stFileUploadDropzone"] {
    background: transparent !important;
    color: var(--text) !important;
  }

  /* All buttons */
  .stButton > button {
    background: var(--bg2) !important;
    color: var(--text) !important;
    border: 1px solid var(--border2) !important;
    border-radius: 6px !important;
    font-family: var(--mono) !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.5px !important;
    transition: all 0.15s !important;
    padding: 0.45rem 1rem !important;
  }
  .stButton > button:hover {
    background: var(--bg3) !important;
    border-color: #ffffff !important;
    transform: translateY(-1px) !important;
  }
  [data-testid="baseButton-primary"] {
    background: var(--accent) !important;
    color: #000 !important;
    border: none !important;
    font-weight: 700 !important;
  }
  [data-testid="baseButton-primary"]:hover {
    background: #b8ef00 !important;
    transform: translateY(-1px) !important;
  }

  /* Download button */
  [data-testid="stDownloadButton"] > button {
    background: var(--bg2) !important;
    color: var(--text) !important;
    border: 1px solid var(--border2) !important;
    border-radius: 6px !important;
    font-family: var(--mono) !important;
    font-size: 0.78rem !important;
    transition: all 0.15s !important;
  }
  [data-testid="stDownloadButton"] > button:hover {
    border-color: var(--accent) !important;
    color: var(--accent) !important;
  }

  /* Metrics */
  div[data-testid="metric-container"] {
    background: var(--bg1) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 14px 16px !important;
  }
  div[data-testid="metric-container"] label {
    font-family: var(--mono) !important;
    font-size: 0.7rem !important;
    color: var(--muted) !important;
    text-transform: uppercase !important;
    letter-spacing: 1.5px !important;
  }
  div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: var(--mono) !important;
    font-size: 1.3rem !important;
    font-weight: 700 !important;
    color: var(--text) !important;
  }

  /* Dataframe */
  [data-testid="stDataFrame"] { border: 1px solid var(--border) !important; border-radius: 8px !important; }

  /* Alerts */
  [data-testid="stAlert"] {
    background: var(--bg1) !important;
    border: 1px solid var(--border2) !important;
    border-radius: 8px !important;
  }
  [data-testid="stAlert"] * { color: var(--text) !important; }

  /* Progress */
  [data-testid="stProgressBar"] > div > div {
    background: var(--accent) !important;
  }

  /* Tabs */
  [data-testid="stTabs"] [role="tablist"] {
    border-bottom: 1px solid var(--border) !important;
    gap: 4px !important;
  }
  [data-testid="stTabs"] button {
    font-family: var(--mono) !important;
    font-size: 0.78rem !important;
    color: var(--muted) !important;
    background: transparent !important;
    border: none !important;
    padding: 6px 14px !important;
    border-radius: 4px 4px 0 0 !important;
    letter-spacing: 0.5px !important;
  }
  [data-testid="stTabs"] button[aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom: 2px solid var(--accent) !important;
  }
  [data-baseweb="tab-highlight"] { background: var(--accent) !important; }

  /* Expander */
  [data-testid="stExpander"] {
    background: var(--bg1) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
  }

  /* Selectbox / inputs */
  [data-testid="stSelectbox"] > div,
  [data-testid="stTextInput"] > div > div {
    background: var(--bg2) !important;
    border-color: var(--border2) !important;
    border-radius: 6px !important;
    color: var(--text) !important;
  }

  /* Spinner */
  [data-testid="stSpinner"] * { color: var(--accent) !important; }

  /* Custom classes */
  .section-header {
    font-family: var(--mono);
    font-size: 0.7rem;
    font-weight: 700;
    color: var(--muted) !important;
    text-transform: uppercase;
    letter-spacing: 3px;
    border-bottom: 1px solid var(--border);
    padding-bottom: 8px;
    margin: 24px 0 16px;
  }

  .hero-wrap {
    background: var(--bg1);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px 28px;
    margin-bottom: 24px;
    position: relative;
    overflow: hidden;
  }
  .hero-wrap::before {
    content: '';
    position: absolute;
    top: -40px; right: -40px;
    width: 180px; height: 180px;
    background: radial-gradient(circle, rgba(200,255,0,0.06) 0%, transparent 70%);
    border-radius: 50%;
  }
  .hero-title {
    font-family: var(--mono);
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--text) !important;
    letter-spacing: -0.5px;
    margin: 0 0 4px;
  }
  .hero-sub {
    font-family: var(--mono);
    font-size: 0.75rem;
    color: var(--muted) !important;
    margin: 0;
    letter-spacing: 1px;
  }
  .hero-accent { color: var(--accent) !important; }

  .stat-card {
    background: var(--bg1);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px 16px;
    margin-bottom: 8px;
  }
  .stat-card-label {
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 2px;
    margin-bottom: 4px;
  }
  .stat-card-value {
    font-family: var(--mono);
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--text);
  }
  .stat-card-value.accent { color: var(--accent); }

  .file-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: var(--bg2);
    border: 1px solid var(--border2);
    border-radius: 20px;
    padding: 4px 12px;
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--muted2);
    margin: 3px;
  }
  .file-badge .dot {
    width: 6px; height: 6px;
    background: var(--accent);
    border-radius: 50%;
    display: inline-block;
  }

  .info-box {
    background: var(--bg1);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    border-radius: 6px;
    padding: 12px 16px;
    font-family: var(--mono);
    font-size: 0.8rem;
    color: #ffffff !important;
    line-height: 1.8;
  }

  .kpi-accent { border-left: 3px solid var(--accent) !important; }
  .kpi-green  { border-left: 3px solid var(--accent2) !important; }
  .kpi-red    { border-left: 3px solid var(--red) !important; }

  hr { border-color: var(--border) !important; margin: 16px 0 !important; }
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }
</style>
""", unsafe_allow_html=True)


# ─── Session State ────────────────────────────────────────────────────────────
def init_session():
    defaults = {
        'session_id':    None,
        'session_ids':   {},
        'results':       None,
        'all_results':   {},
        'analysis_done': False,
        'dashboard_html':None,
        'active_file':   None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()


# ─── API Helpers ──────────────────────────────────────────────────────────────
def check_api() -> bool:
    try:
        return requests.get(f"{API_BASE}/health", timeout=3).status_code == 200
    except Exception:
        return False

def upload_and_analyze(file_bytes: bytes, filename: str) -> dict:
    r = requests.post(
        f"{API_BASE}/api/upload-and-analyze",
        files={"file": (filename, file_bytes, "application/octet-stream")},
        timeout=600
    )
    if r.status_code == 200:
        return r.json()
    raise Exception(r.json().get("detail", r.text))

def get_dashboard_html(session_id: str) -> str:
    r = requests.get(f"{API_BASE}/api/dashboard/{session_id}", timeout=30)
    if r.status_code == 200:
        return r.text
    raise Exception(f"Dashboard error: {r.text}")

def get_download_url(session_id: str, file_type: str) -> str:
    return f"{API_BASE}/api/download/{session_id}/{file_type}"

def reset_state():
    for k in ['session_id','session_ids','results','all_results',
              'analysis_done','dashboard_html','active_file']:
        st.session_state[k] = {} if k in ('session_ids','all_results') \
            else (False if k == 'analysis_done' else None)


# ─── Chart helpers ────────────────────────────────────────────────────────────
CHART_LAYOUT = dict(
    plot_bgcolor='#0a0a0a', paper_bgcolor='#020202',
    font=dict(color='#ffffff', size=10, family='Times New Roman'),
    margin=dict(l=50, r=20, t=36, b=40),
    xaxis=dict(gridcolor='#111', showgrid=True, zeroline=False,
               linecolor='#1f1f1f', tickfont=dict(color='#ffffff', family='Times New Roman')),
    yaxis=dict(gridcolor='#111', showgrid=True, zeroline=False,
               linecolor='#1f1f1f', tickfont=dict(color='#ffffff', family='Times New Roman')),
    legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor='#1f1f1f',
                font=dict(color='#ffffff', family='Times New Roman')),
)

def chart_layout(**kwargs):
    layout = CHART_LAYOUT.copy()
    layout.update(kwargs)
    return layout


def plot_main_chart(results: dict) -> go.Figure:
    history  = results.get('history', [])
    forecast = results.get('forecast', [])
    if not history:
        return go.Figure()

    dates     = [h.get('date', f'T{i}')    for i, h in enumerate(history)]
    actual    = [h.get('actual', 0)         for h in history]
    predicted = [h.get('predicted', 0)      for h in history]
    f_dates   = [f.get('date', f'F{i}')    for i, f in enumerate(forecast)]
    f_vals    = [f.get('predicted', 0)      for f in forecast]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=actual, name='Actual',
        line=dict(color='#e8e8e8', width=1.5),
        fill='tozeroy', fillcolor='rgba(232,232,232,0.04)'
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=predicted, name='Predicted',
        line=dict(color='#444', width=1.5, dash='dot')
    ))
    if f_dates:
        fig.add_trace(go.Scatter(
            x=f_dates, y=f_vals, name='12M Forecast',
            line=dict(color='#c8ff00', width=2),
            fill='tozeroy', fillcolor='rgba(200,255,0,0.05)'
        ))
    fig.update_layout(**chart_layout(height=340,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, x=0,
                    bgcolor='rgba(0,0,0,0)', font=dict(color='#888'))))
    return fig


def plot_forecast_bar(results: dict) -> go.Figure:
    forecast = results.get('forecast', [])
    avg      = results.get('summary', {}).get('avg_sales', 0)
    if not forecast:
        return go.Figure()
    dates  = [f.get('date', f'M{i+1}')   for i, f in enumerate(forecast)]
    values = [f.get('predicted', 0)       for f in forecast]
    colors = ['#c8ff00' if v >= avg else '#ff4455' for v in values]

    fig = go.Figure(go.Bar(
        x=dates, y=values, marker_color=colors,
        text=[f'{v:,.0f}' for v in values], textposition='outside',
        textfont=dict(size=9, color='#ffffff')
    ))
    if avg > 0:
        fig.add_hline(y=avg, line_dash='dash', line_color='#2a2a2a',
                      annotation_text=f'avg {avg:,.0f}',
                      annotation_font=dict(color='#ffffff', size=9))
    fig.update_layout(**chart_layout(height=280,
        xaxis=dict(gridcolor='#111', tickangle=-45, tickfont=dict(color='#555', size=9))))
    return fig


def plot_model_comparison(results: dict) -> go.Figure:
    mc   = results.get('model_comparison', {})
    best = results.get('metrics', {}).get('best_model', '')
    if not mc:
        return go.Figure()
    valid  = [(n, v) for n, v in mc.items() if isinstance(v, dict)]
    names  = [n for n, _ in valid]
    accs   = [max(0, 100 - v.get('mape', 100)) for _, v in valid]
    colors = ['#c8ff00' if n == best else '#1f1f1f' for n in names]
    borders= ['#c8ff00' if n == best else '#2a2a2a' for n in names]

    fig = go.Figure(go.Bar(
        x=[n.upper() for n in names], y=accs,
        marker=dict(color=colors, line=dict(color=borders, width=1)),
        text=[f'{a:.1f}%' for a in accs], textposition='outside',
        textfont=dict(size=9, color='#ffffff')
    ))
    fig.update_layout(**chart_layout(height=260,
        yaxis=dict(gridcolor='#111', range=[0, 108], showgrid=True,
                   zeroline=False, tickfont=dict(color='#555'))))
    return fig


def plot_feature_importance(results: dict) -> go.Figure:
    fi = results.get('feature_importance', {})
    if not fi:
        return go.Figure()
    items = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:10]
    names = [x[0] for x in items]
    vals  = [x[1] for x in items]

    fig = go.Figure(go.Bar(
        x=vals, y=names, orientation='h',
        marker=dict(color=vals,
                    colorscale=[[0,'#1a1a1a'],[0.5,'#555'],[1,'#c8ff00']],
                    showscale=False),
        text=[f'{v:.4f}' for v in vals], textposition='outside',
        textfont=dict(size=9, color='#ffffff')
    ))
    fig.update_layout(**chart_layout(height=300,
        margin=dict(l=130, r=60, t=36, b=40),
        yaxis=dict(autorange='reversed', gridcolor='#111',
                   tickfont=dict(color='#ffffff', size=9, family='Times New Roman'))))
    return fig


def plot_residuals(results: dict) -> go.Figure:
    history = results.get('history', [])
    if not history:
        return go.Figure()
    actual    = np.array([h.get('actual', 0)    for h in history])
    predicted = np.array([h.get('predicted', 0) for h in history])
    residuals = actual - predicted

    fig = go.Figure(go.Scatter(
        x=predicted.tolist(), y=residuals.tolist(), mode='markers',
        marker=dict(color='#2a2a2a', size=5,
                    line=dict(color='#444', width=0.5))
    ))
    fig.add_hline(y=0, line_color='#c8ff00', line_dash='dash', line_width=1)
    fig.update_layout(**chart_layout(height=260,
        xaxis=dict(gridcolor='#111', title=dict(text='Predicted', font=dict(color='#555', size=10))),
        yaxis=dict(gridcolor='#111', title=dict(text='Residual',  font=dict(color='#555', size=10)))))
    return fig


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    # ── Hero ──────────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="hero-wrap">
      <div class="hero-title">⚡ RevIQ <span class="hero-accent">Smart Sales Forecasting System</span></div>
      <p class="hero-sub">upload · automl · forecast · export — up to 20 files at once</p>
    </div>
    """, unsafe_allow_html=True)

    if not check_api():
        st.error("⚠️  Backend offline — run: `cd sales_forecasting && python start.py`")
        st.stop()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("""
        <div style="font-family:'IBM Plex Mono',monospace;font-size:0.7rem;
             color:#555;text-transform:uppercase;letter-spacing:2px;
             padding-bottom:8px;border-bottom:1px solid #1f1f1f;margin-bottom:12px">
          ⚡ RevIQ
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="info-box">
          <b style="color:#888">Formats</b><br>
          CSV · Excel · JSON<br>Parquet · TSV<br><br>
          <b style="color:#888">Models</b><br>
          XGBoost · LightGBM<br>Random Forest<br>
          Gradient Boosting<br>Extra Trees · Ridge
        </div>
        """, unsafe_allow_html=True)

        if st.session_state.analysis_done:
            st.markdown("<br>", unsafe_allow_html=True)
            n = len(st.session_state.all_results)
            st.success(f"✅  {n} file{'s' if n>1 else ''} analysed")

            if len(st.session_state.all_results) > 1:
                st.markdown('<div class="section-header">Switch Dataset</div>',
                            unsafe_allow_html=True)
                for fname in st.session_state.all_results:
                    is_active = fname == st.session_state.active_file
                    prefix = "▶ " if is_active else "   "
                    if st.button(f"{prefix}{fname}", key=f"sw_{fname}",
                                 use_container_width=True):
                        st.session_state.active_file = fname
                        st.session_state.results     = st.session_state.all_results[fname]
                        st.session_state.session_id  = st.session_state.session_ids.get(fname)
                        st.rerun()

            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("↩  New Analysis", use_container_width=True):
                reset_state()
                st.rerun()

    # ── Upload ────────────────────────────────────────────────────────────────
    if not st.session_state.analysis_done:
        st.markdown('<div class="section-header">Upload Data</div>',
                    unsafe_allow_html=True)

        col_up, col_info = st.columns([3, 1])
        with col_up:
            uploaded_files = st.file_uploader(
                "Drop files here or click to browse",
                type=['csv','xlsx','xls','json','parquet','tsv','txt'],
                accept_multiple_files=True,
                help="Hold Ctrl to select multiple files — up to 20"
            )

        with col_info:
            st.markdown("""
            <div class="info-box">
              Each file needs:<br>
              · a date column<br>
              · a numeric target<br>
              · 10+ rows<br><br>
              Select up to<br>
              <b style="color:#c8ff00">20 files</b> at once
            </div>
            """, unsafe_allow_html=True)

        if uploaded_files:
            if len(uploaded_files) > 20:
                st.warning("⚠️  Max 20 files — only first 20 will be used.")
                uploaded_files = uploaded_files[:20]

            st.markdown("<br>", unsafe_allow_html=True)
            badge_html = "".join(
                f'<span class="file-badge"><span class="dot"></span>'
                f'{f.name} <span style="color:#333">{f.size/1024:.0f}kb</span></span>'
                for f in uploaded_files
            )
            st.markdown(badge_html, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            _, col_btn, _ = st.columns([1, 2, 1])
            with col_btn:
                lbl = f"⚡  Run AutoML  —  {len(uploaded_files)} file{'s' if len(uploaded_files)>1 else ''}"
                if st.button(lbl, use_container_width=True, type="primary"):
                    _run_multi_analysis(uploaded_files)

    # ── Results ───────────────────────────────────────────────────────────────
    if st.session_state.analysis_done and st.session_state.results:
        results     = st.session_state.results
        metrics     = results.get('metrics', {})
        summary     = results.get('summary', {})
        fc_summary  = results.get('forecast_summary', {})
        profile     = results.get('profile', {})
        active_file = st.session_state.active_file or "Dataset"

        # Status bar
        c_info, c_btn = st.columns([4, 1])
        with c_info:
            st.success(
                f"✅  **{active_file}** · {profile.get('rows',0):,} rows · "
                f"target: `{profile.get('target_column','N/A')}` · "
                f"best model: `{metrics.get('best_model','N/A').upper()}` "
                f"({metrics.get('accuracy',0):.1f}%)"
            )
        with c_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("↩  New Analysis", use_container_width=True, type="primary"):
                reset_state()
                st.rerun()

        # Multi-file dataset switcher
        if len(st.session_state.all_results) > 1:
            st.markdown('<div class="section-header">All Datasets</div>',
                        unsafe_allow_html=True)
            fnames = list(st.session_state.all_results.keys())
            tabs   = st.tabs([f"📄 {n}" for n in fnames])
            for i, (tab, fname) in enumerate(zip(tabs, fnames)):
                with tab:
                    r = st.session_state.all_results[fname]
                    m = r.get('metrics', {})
                    s = r.get('summary', {})
                    c1,c2,c3,c4 = st.columns(4)
                    c1.metric("Best Model",  m.get('best_model','N/A').upper())
                    c2.metric("Accuracy",    f"{m.get('accuracy',0):.1f}%")
                    c3.metric("Total Sales", f"{s.get('total_sales',0):,.0f}")
                    c4.metric("12M Forecast",f"{r.get('forecast_summary',{}).get('total_forecast',0):,.0f}")
                    if st.button("View Full Analysis →", key=f"v_{fname}_{i}",
                                 use_container_width=True):
                        st.session_state.active_file = fname
                        st.session_state.results     = r
                        st.session_state.session_id  = st.session_state.session_ids.get(fname)
                        st.rerun()

        # ── KPI Row ───────────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Key Metrics</div>',
                    unsafe_allow_html=True)
        k1,k2,k3,k4,k5,k6,k7,k8 = st.columns(8)
        k1.metric("Total Sales",   f"{summary.get('total_sales',0):,.0f}")
        k2.metric("Avg / Period",  f"{summary.get('avg_sales',0):,.0f}")
        k3.metric("12M Forecast",  f"{fc_summary.get('total_forecast',0):,.0f}")
        k4.metric("Growth",        f"{summary.get('growth_rate',0):+.1f}%")
        k5.metric("Accuracy",      f"{metrics.get('accuracy',0):.1f}%")
        k6.metric("R² Score",      f"{metrics.get('r2',0):.3f}")
        k7.metric("MAE",           f"{metrics.get('mae',0):,.1f}")
        k8.metric("RMSE",          f"{metrics.get('rmse',0):,.1f}")

        # ── Main Chart ────────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Sales · Predicted · Forecast</div>',
                    unsafe_allow_html=True)
        st.plotly_chart(plot_main_chart(results), use_container_width=True)

        # ── Row 2 charts ──────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Forecast Breakdown · Model Comparison</div>',
                    unsafe_allow_html=True)
        ch1, ch2 = st.columns(2)
        with ch1:
            st.plotly_chart(plot_forecast_bar(results), use_container_width=True)
        with ch2:
            st.plotly_chart(plot_model_comparison(results), use_container_width=True)

        # ── Row 3 charts ──────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Feature Importance · Residuals</div>',
                    unsafe_allow_html=True)
        ch3, ch4 = st.columns(2)
        with ch3:
            st.plotly_chart(plot_feature_importance(results), use_container_width=True)
        with ch4:
            st.plotly_chart(plot_residuals(results), use_container_width=True)

        # ── Model Leaderboard ─────────────────────────────────────────────────
        st.markdown('<div class="section-header">Model Leaderboard</div>',
                    unsafe_allow_html=True)
        mc   = results.get('model_comparison', {})
        best = metrics.get('best_model', '')
        rows = []
        for name, m in mc.items():
            if not isinstance(m, dict): continue
            acc = max(0, 100 - m.get('mape', 100))
            rows.append({
                'Model':    f"⭐ {name.upper()}" if name == best else name.upper(),
                'Accuracy': f"{acc:.1f}%",
                'MAE':      f"{m.get('mae',0):.4f}",
                'RMSE':     f"{m.get('rmse',0):.4f}",
                'R²':       f"{m.get('r2',0):.4f}",
                'MAPE':     f"{m.get('mape',0):.2f}%",
                'Status':   '🏆 BEST' if name == best else '—'
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # ── 12-Month Forecast Table ───────────────────────────────────────────
        st.markdown('<div class="section-header">12-Month Forecast</div>',
                    unsafe_allow_html=True)
        forecast_data = results.get('forecast', [])
        avg_s = summary.get('avg_sales', 0)
        if forecast_data:
            fc_rows = []
            for i, f in enumerate(forecast_data):
                val  = f.get('predicted', 0)
                diff = val - avg_s
                fc_rows.append({
                    '#':       i + 1,
                    'Period':  f.get('date', f'M+{i+1}'),
                    'Forecast':f"{val:,.2f}",
                    'vs Avg':  f"{diff:+,.2f}",
                    'vs Avg %':f"{diff/max(avg_s,1)*100:+.1f}%",
                    'Trend':   '▲' if diff >= 0 else '▼'
                })
            st.dataframe(pd.DataFrame(fc_rows), use_container_width=True, hide_index=True)

        # ── Export ────────────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Export</div>',
                    unsafe_allow_html=True)

        session_id = st.session_state.session_id

        exp_col1, exp_col2, exp_col3 = st.columns([1, 1, 2])
        with exp_col1:
            if st.button("🌐  Open HTML Dashboard", use_container_width=True, type="primary"):
                _open_dashboard()
        with exp_col2:
            if st.button("↩  Back to Upload", use_container_width=True):
                reset_state()
                st.rerun()

        with exp_col3:
            dl_cols = st.columns(4)
            downloads = [
                ("html_dashboard", "⬇  HTML Dashboard",        "text/html"),
                ("pbit",           "⬇  Power BI Excel (.xlsx)", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                ("historical_csv", "⬇  Historical CSV",         "text/csv"),
                ("forecast_csv",   "⬇  Forecast CSV",           "text/csv"),
            ]
            for i, (ftype, label, mime) in enumerate(downloads):
                with dl_cols[i]:
                    try:
                        r = requests.get(get_download_url(session_id, ftype), timeout=10)
                        if r.status_code == 200:
                            fname_dl = r.headers.get(
                                'content-disposition', ftype
                            ).split('filename=')[-1].strip('"')
                            st.download_button(
                                label=label, data=r.content,
                                file_name=fname_dl, mime=mime,
                                use_container_width=True
                            )
                    except Exception:
                        pass


# ─── Multi-file Runner ────────────────────────────────────────────────────────
def _run_multi_analysis(uploaded_files):
    total    = len(uploaded_files)
    progress = st.progress(0)
    status   = st.empty()
    all_results, session_ids, errors = {}, {}, []

    for idx, f in enumerate(uploaded_files):
        status.markdown(f"**⚙️  [{idx+1}/{total}]  Analysing  `{f.name}`...**")
        holder, err_holder = [None], [None]

        def do(file=f):
            try:
                holder[0] = upload_and_analyze(file.getvalue(), file.name)
            except Exception as e:
                err_holder[0] = str(e)

        t = threading.Thread(target=do)
        t.start()

        base = int(idx / total * 100)
        nxt  = int((idx + 1) / total * 100)
        p    = base
        while t.is_alive():
            if p < nxt - 2: p += 1
            progress.progress(p)
            time.sleep(0.7)
        t.join()

        if err_holder[0]:
            errors.append(f"`{f.name}`: {err_holder[0]}")
        else:
            sid = holder[0]['session_id']
            try:
                res = requests.get(f"{API_BASE}/api/results/{sid}", timeout=30).json()
            except Exception:
                res = holder[0]
            all_results[f.name] = res
            session_ids[f.name] = sid

        progress.progress(nxt)

    progress.progress(100)

    for e in errors:
        st.error(f"❌  {e}")

    if all_results:
        first = list(all_results.keys())[0]
        st.session_state.all_results   = all_results
        st.session_state.session_ids   = session_ids
        st.session_state.active_file   = first
        st.session_state.results       = all_results[first]
        st.session_state.session_id    = session_ids[first]
        st.session_state.analysis_done = True
        status.markdown(f"**✅  {len(all_results)}/{total} file(s) done!**")
        time.sleep(0.4)
        st.rerun()
    else:
        status.markdown("**❌  All analyses failed.**")


# ─── Dashboard ────────────────────────────────────────────────────────────────
def _open_dashboard():
    sid = st.session_state.session_id
    if not sid:
        st.error("No analysis found.")
        return
    try:
        html = get_dashboard_html(sid)
        tmp  = os.path.join(tempfile.gettempdir(), f"sales_dashboard_{sid}.html")
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(html)
        webbrowser.open(f"file://{tmp}")
        st.success("✅  Dashboard opened in your browser!")
        with st.expander("👁️  Preview inline", expanded=False):
            st.components.v1.html(html, height=800, scrolling=True)
    except Exception as e:
        st.error(f"❌  {e}")


if __name__ == "__main__":
    main()
