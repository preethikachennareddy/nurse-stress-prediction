"""
Nurse Stress Prediction — Streamlit Demo Dashboard
Calls the Flask API at localhost:5000 for both models side by side.

Run:
    pip install streamlit requests pandas plotly
    streamlit run streamlit_dashboard.py
"""

import time
import random
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Nurse Stress Monitor",
    page_icon="🫀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Tighten default padding */
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: #f5f5f3;
        border-radius: 10px;
        padding: 12px 16px;
        border: 0.5px solid rgba(0,0,0,0.08);
    }

    /* Stress label badges */
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 6px;
        font-size: 13px;
        font-weight: 600;
    }
    .badge-0 { background: #E1F5EE; color: #085041; }
    .badge-1 { background: #FAEEDA; color: #633806; }
    .badge-2 { background: #FCEBEB; color: #791F1F; }

    /* Section headers */
    .section-header {
        font-size: 13px;
        font-weight: 600;
        color: #6b6b67;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 6px;
    }

    /* Nurse ID pill */
    .nurse-pill {
        display: inline-block;
        background: #E6F1FB;
        color: #0C447C;
        border-radius: 20px;
        padding: 2px 10px;
        font-size: 13px;
        font-weight: 600;
    }

    /* Comparison divider */
    .model-header {
        font-size: 14px;
        font-weight: 600;
        padding: 6px 12px;
        border-radius: 8px;
        margin-bottom: 8px;
    }
    .model-lgbm { background: #E6F1FB; color: #0C447C; }
    .model-gb   { background: #EEEDFE; color: #3C3489; }

    /* Status dot */
    .status-online  { color: #1D9E75; font-weight: 600; }
    .status-offline { color: #E24B4A; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
API_BASE   = "http://localhost:5000"
PREDICT_EP = f"{API_BASE}/model/api/predict"
STRESS_LABELS = {0: "No stress", 1: "Moderate stress", 2: "High stress"}
STRESS_COLORS = {0: "#1D9E75",   1: "#EF9F27",         2: "#E24B4A"}
BADGE_CLASS   = {0: "badge-0",   1: "badge-1",          2: "badge-2"}

# Real data anchors from workers_csv (6D.csv / BG.csv)
NURSE_PROFILES = {
    "6D": dict(hr_mean=87.6,  hr_std=13.5, temp_mean=29.9, temp_std=1.2,
               eda_mean=0.41, eda_std=0.12, x_mean=-22.6, y_mean=5.3,  z_mean=33.2),
    "BG": dict(hr_mean=88.2,  hr_std=13.7, temp_mean=33.3, temp_std=1.3,
               eda_mean=1.93, eda_std=4.3,  x_mean=-32.8, y_mean=-4.1, z_mean=31.6),
}

# ── Session state ─────────────────────────────────────────────────────────────
if "history" not in st.session_state:
    st.session_state.history = {nid: [] for nid in NURSE_PROFILES}
    # pre-populate with 30 blank rows
    for nid in NURSE_PROFILES:
        for _ in range(30):
            st.session_state.history[nid].append({
                "ts": time.time(), "hr": None, "temp": None, "eda": None,
                "x": None, "y": None, "z": None,
                "pred_lgbm": None, "pred_gb": None,
            })

if "api_ok" not in st.session_state:
    st.session_state.api_ok = False

if "tick" not in st.session_state:
    st.session_state.tick = 0

# ── Helpers ───────────────────────────────────────────────────────────────────
def jitter(mean, std, lo=None, hi=None):
    v = random.gauss(mean, std * 0.3)
    if lo is not None: v = max(lo, v)
    if hi is not None: v = min(hi, v)
    return round(v, 2)

def call_api(model: str, x, y, z, eda, hr, temp):
    """
    Call the Flask API. model param selects which model the server should use.
    Your current app.py only exposes one endpoint; extend it to accept a ?model=
    query param, or run two instances on different ports (see sidebar notes).
    """
    params = dict(x=x, y=y, z=z, eda=eda, hr=hr, temp=temp, model=model)
    resp = requests.get(PREDICT_EP, params=params, timeout=2)
    resp.raise_for_status()
    data = resp.json()
    return int(data.get("stress_level_prediction", data.get("stress_level", 0)))

def check_api():
    try:
        r = requests.get(f"{API_BASE}/model/api/predict",
                         params=dict(x=0, y=0, z=0, eda=0.5, hr=80, temp=30),
                         timeout=2)
        return r.status_code == 200
    except Exception:
        return False

def stress_badge(label):
    text = STRESS_LABELS.get(label, "–")
    cls  = BADGE_CLASS.get(label, "badge-0")
    return f'<span class="badge {cls}">{label} · {text}</span>'

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")

    refresh_rate = st.slider("Refresh interval (s)", 1, 10, 3)
    auto_refresh  = st.toggle("Auto-refresh", value=True)
    selected_nurse = st.selectbox("Focus nurse (sensors panel)", list(NURSE_PROFILES.keys()))

    st.divider()
    st.markdown("### 🔌 API status")
    if st.button("Check connection"):
        st.session_state.api_ok = check_api()

    if st.session_state.api_ok:
        st.markdown('<p class="status-online">● API online</p>', unsafe_allow_html=True)
    else:
        st.markdown('<p class="status-offline">● API offline</p>', unsafe_allow_html=True)
        st.caption("Start Flask: `python app.py`")

    st.divider()
    st.markdown("### 📝 Model routing")
    st.caption("""
Your `app.py` currently serves one model at `/model/api/predict`.

To compare both models, add a `?model=` param:

```python
@app.route('/model/api/predict')
def predict():
    model_name = request.args.get('model','lgbm')
    m = lgbm_model if model_name == 'lgbm' else gb_model
    ...
```

Or run two Flask instances:
- Port **5000** → LightGBM  
- Port **5001** → GradientBoosting
    """)

    st.divider()
    st.markdown("### 🗄️ Pipeline")
    st.caption("""
- Kafka → `stress-topic`
- Flink → `flink_events` (InfluxDB)
- Dask fallback → `source: dask-fallback`
- Grafana → `:3000`
    """)

# ── Header ────────────────────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("# 🫀 Nurse Stress Monitor")
    st.caption("Real-time physiological analysis · Flask API at `localhost:5000` · LightGBM vs GradientBoosting")
with col_h2:
    api_status = "🟢 API online" if st.session_state.api_ok else "🔴 API offline"
    st.metric("Flask API", api_status)

st.divider()

# ── Simulate + fetch predictions ──────────────────────────────────────────────
def update_readings():
    st.session_state.tick += 1
    for nid, profile in NURSE_PROFILES.items():
        x    = jitter(profile["x_mean"],    20,   -128, 127)
        y    = jitter(profile["y_mean"],    20,   -128, 127)
        z    = jitter(profile["z_mean"],    20,   -128, 127)
        hr   = jitter(profile["hr_mean"],   profile["hr_std"],   55, 155)
        temp = jitter(profile["temp_mean"], profile["temp_std"], 27, 37)
        eda  = jitter(profile["eda_mean"],  profile["eda_std"],   0, 45)

        pred_lgbm, pred_gb = None, None

        if st.session_state.api_ok:
            try:
                pred_lgbm = call_api("lgbm", x, y, z, eda, hr, temp)
            except Exception:
                pred_lgbm = None
            try:
                pred_gb = call_api("gb", x, y, z, eda, hr, temp)
            except Exception:
                pred_gb = None
        else:
            # Offline simulation so the UI stays useful
            val = {"6D": 0.3, "BG": 1.1}.get(nid, 0.5)
            val = max(0, min(2, val + random.gauss(0, 0.25)))
            pred_lgbm = int(round(val))
            pred_gb   = int(round(max(0, min(2, val + random.gauss(0, 0.3)))))

        row = dict(ts=time.time(), x=x, y=y, z=z, hr=hr, temp=temp, eda=eda,
                   pred_lgbm=pred_lgbm, pred_gb=pred_gb)
        hist = st.session_state.history[nid]
        hist.append(row)
        if len(hist) > 60:
            del hist[0]

update_readings()

# ── Top metrics ───────────────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)

all_rows   = [st.session_state.history[n][-1] for n in NURSE_PROFILES]
high_lgbm  = sum(1 for r in all_rows if r["pred_lgbm"] == 2)
high_gb    = sum(1 for r in all_rows if r["pred_gb"]   == 2)
agree      = sum(1 for r in all_rows if r["pred_lgbm"] == r["pred_gb"])

m1.metric("Active nurses",   len(NURSE_PROFILES))
m2.metric("High stress · LightGBM",  high_lgbm,  delta=None)
m3.metric("High stress · GradBoosting", high_gb,  delta=None)
m4.metric("Models agree",    f"{agree}/{len(NURSE_PROFILES)}")
m5.metric("Tick", st.session_state.tick, delta=1)

st.divider()

# ── Model comparison — side by side per nurse ─────────────────────────────────
st.markdown('<p class="section-header">Model comparison · latest prediction per nurse</p>', unsafe_allow_html=True)

for nid in NURSE_PROFILES:
    latest = st.session_state.history[nid][-1]
    nurse_col, lgbm_col, gb_col, agree_col = st.columns([1.2, 2, 2, 1])

    with nurse_col:
        st.markdown(f'<span class="nurse-pill">{nid}</span>', unsafe_allow_html=True)
        st.caption(f"HR {latest['hr']} bpm" if latest['hr'] else "–")
        st.caption(f"EDA {latest['eda']} µS" if latest['eda'] else "–")
        st.caption(f"TEMP {latest['temp']} °C" if latest['temp'] else "–")

    with lgbm_col:
        st.markdown('<div class="model-header model-lgbm">⚡ LightGBM</div>', unsafe_allow_html=True)
        if latest["pred_lgbm"] is not None:
            st.markdown(stress_badge(latest["pred_lgbm"]), unsafe_allow_html=True)
            # Mini trend
            preds = [r["pred_lgbm"] for r in st.session_state.history[nid] if r["pred_lgbm"] is not None]
            if preds:
                fig = go.Figure(go.Scatter(
                    y=preds, mode="lines",
                    line=dict(color="#378ADD", width=1.5),
                    fill="tozeroy", fillcolor="rgba(55,138,221,0.08)"
                ))
                fig.update_layout(
                    height=80, margin=dict(l=0,r=0,t=0,b=0),
                    yaxis=dict(range=[-0.1, 2.2], showgrid=False, zeroline=False,
                               tickvals=[0,1,2], ticktext=["0","1","2"]),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No data")

    with gb_col:
        st.markdown('<div class="model-header model-gb">🌲 GradientBoosting</div>', unsafe_allow_html=True)
        if latest["pred_gb"] is not None:
            st.markdown(stress_badge(latest["pred_gb"]), unsafe_allow_html=True)
            preds = [r["pred_gb"] for r in st.session_state.history[nid] if r["pred_gb"] is not None]
            if preds:
                fig = go.Figure(go.Scatter(
                    y=preds, mode="lines",
                    line=dict(color="#7F77DD", width=1.5),
                    fill="tozeroy", fillcolor="rgba(127,119,221,0.08)"
                ))
                fig.update_layout(
                    height=80, margin=dict(l=0,r=0,t=0,b=0),
                    yaxis=dict(range=[-0.1, 2.2], showgrid=False, zeroline=False,
                               tickvals=[0,1,2], ticktext=["0","1","2"]),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No data")

    with agree_col:
        if latest["pred_lgbm"] is not None and latest["pred_gb"] is not None:
            match = latest["pred_lgbm"] == latest["pred_gb"]
            st.metric("Agree", "✅ Yes" if match else "⚠️ No")
            diff = abs(latest["pred_lgbm"] - latest["pred_gb"])
            if diff:
                st.caption(f"Δ {diff} label(s)")

st.divider()

# ── Sensor detail (focused nurse) ────────────────────────────────────────────
st.markdown(f'<p class="section-header">Live sensors · Nurse {selected_nurse}</p>', unsafe_allow_html=True)

hist_df = pd.DataFrame(st.session_state.history[selected_nurse])
hist_df = hist_df.dropna(subset=["hr"])

s1, s2, s3, s4 = st.columns(4)
if not hist_df.empty:
    latest_s = hist_df.iloc[-1]
    s1.metric("HR",   f"{latest_s['hr']:.1f} bpm",   delta=f"{latest_s['hr'] - hist_df['hr'].mean():.1f}")
    s2.metric("TEMP", f"{latest_s['temp']:.1f} °C",  delta=f"{latest_s['temp'] - hist_df['temp'].mean():.2f}")
    s3.metric("EDA",  f"{latest_s['eda']:.2f} µS",   delta=f"{latest_s['eda'] - hist_df['eda'].mean():.2f}")
    s4.metric("ACC Z",f"{latest_s['z']:.1f}",         delta=f"{latest_s['z'] - hist_df['z'].mean():.1f}")

if not hist_df.empty:
    sc1, sc2 = st.columns(2)

    with sc1:
        fig_hr = px.line(hist_df, y="hr", title="Heart rate (bpm)",
                         color_discrete_sequence=["#E24B4A"])
        fig_hr.update_layout(height=220, margin=dict(l=0,r=0,t=30,b=0),
                              yaxis_title=None, xaxis_title=None,
                              showlegend=False,
                              plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        fig_hr.update_xaxes(showgrid=False, showticklabels=False)
        fig_hr.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
        st.plotly_chart(fig_hr, use_container_width=True, config={"displayModeBar": False})

    with sc2:
        fig_eda = px.line(hist_df, y="eda", title="EDA (µS)",
                          color_discrete_sequence=["#378ADD"])
        fig_eda.update_layout(height=220, margin=dict(l=0,r=0,t=30,b=0),
                               yaxis_title=None, xaxis_title=None,
                               showlegend=False,
                               plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        fig_eda.update_xaxes(showgrid=False, showticklabels=False)
        fig_eda.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
        st.plotly_chart(fig_eda, use_container_width=True, config={"displayModeBar": False})

st.divider()

# ── Disagreement log ─────────────────────────────────────────────────────────
st.markdown('<p class="section-header">Model disagreement log</p>', unsafe_allow_html=True)

disagreements = []
for nid in NURSE_PROFILES:
    for row in reversed(st.session_state.history[nid]):
        if row["pred_lgbm"] is not None and row["pred_gb"] is not None:
            if row["pred_lgbm"] != row["pred_gb"]:
                disagreements.append({
                    "Nurse": nid,
                    "LightGBM": f"{row['pred_lgbm']} · {STRESS_LABELS[row['pred_lgbm']]}",
                    "GradBoosting": f"{row['pred_gb']} · {STRESS_LABELS[row['pred_gb']]}",
                    "HR": f"{row['hr']:.1f}",
                    "EDA": f"{row['eda']:.2f}",
                    "TEMP": f"{row['temp']:.1f}",
                })
        if len(disagreements) >= 10:
            break

if disagreements:
    st.dataframe(pd.DataFrame(disagreements), use_container_width=True, hide_index=True)
else:
    st.caption("No disagreements yet — models are in agreement.")

st.divider()

# ── Manual prediction tester ──────────────────────────────────────────────────
st.markdown('<p class="section-header">Manual prediction tester</p>', unsafe_allow_html=True)
st.caption("Enter sensor values manually and compare both model predictions in real time.")

mc1, mc2, mc3 = st.columns(3)
with mc1:
    m_x    = st.number_input("ACC X",    value=-22.6, step=1.0)
    m_y    = st.number_input("ACC Y",    value=5.3,   step=1.0)
    m_z    = st.number_input("ACC Z",    value=33.2,  step=1.0)
with mc2:
    m_hr   = st.number_input("HR (bpm)", value=87.6,  step=0.5)
    m_temp = st.number_input("TEMP (°C)",value=29.9,  step=0.1)
    m_eda  = st.number_input("EDA (µS)", value=0.41,  step=0.01)
with mc3:
    st.write("")
    st.write("")
    if st.button("▶ Run prediction", use_container_width=True):
        if st.session_state.api_ok:
            try:
                r_lgbm = call_api("lgbm", m_x, m_y, m_z, m_eda, m_hr, m_temp)
                r_gb   = call_api("gb",   m_x, m_y, m_z, m_eda, m_hr, m_temp)
                st.markdown(f"**LightGBM:** {stress_badge(r_lgbm)}", unsafe_allow_html=True)
                st.markdown(f"**GradBoost:** {stress_badge(r_gb)}",  unsafe_allow_html=True)
                if r_lgbm != r_gb:
                    st.warning(f"Models disagree! LGBM={r_lgbm}, GB={r_gb}")
                else:
                    st.success(f"Both models agree: {STRESS_LABELS[r_lgbm]}")
            except Exception as e:
                st.error(f"API error: {e}")
        else:
            st.warning("API is offline. Start Flask first: `python app.py`")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
