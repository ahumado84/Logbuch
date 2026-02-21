import streamlit as st
import sqlite3
import csv
import hashlib
import io
import calendar
from datetime import datetime
from collections import defaultdict

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as rl_canvas

# ─── CONFIGURACIÓN DE PÁGINA ──────────────────────────────────────────────────
st.set_page_config(
    page_title="OP Katalog",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── PALETA & ESTILOS ─────────────────────────────────────────────────────────
C = {
    "bg":      "#0f1117", "panel":   "#161b27", "border":  "#1e2535",
    "accent":  "#4af0b0", "accent2": "#f06a4a", "accent3": "#4a8ff0",
    "yellow":  "#f0d44a", "text":    "#e2e8f0", "muted":   "#6b7899",
    "op":      "#4af0b0", "interv":  "#4a8ff0", "proz":    "#f0d44a",
}

st.markdown(f"""
<style>
  /* Fondo global */
  .stApp, [data-testid="stAppViewContainer"] {{
      background-color: {C["bg"]};
      color: {C["text"]};
  }}
  [data-testid="stSidebar"] {{
      background-color: {C["panel"]};
      border-right: 1px solid {C["border"]};
  }}
  /* Inputs */
  .stTextInput input, .stSelectbox select, .stDateInput input {{
      background-color: {C["panel"]} !important;
      color: {C["text"]} !important;
      border: 1px solid {C["border"]} !important;
      border-radius: 6px !important;
  }}
  /* Botones */
  .stButton > button {{
      background-color: {C["panel"]};
      color: {C["text"]};
      border: 1px solid {C["border"]};
      border-radius: 6px;
      transition: all 0.2s;
  }}
  .stButton > button:hover {{
      background-color: {C["accent"]};
      color: {C["bg"]};
      border-color: {C["accent"]};
  }}
  /* Métricas */
  [data-testid="metric-container"] {{
      background-color: {C["panel"]};
      border: 1px solid {C["border"]};
      border-radius: 10px;
      padding: 14px 18px;
  }}
  [data-testid="metric-container"] label {{
      color: {C["muted"]} !important;
      font-size: 11px !important;
      text-transform: uppercase;
      letter-spacing: 0.8px;
  }}
  [data-testid="metric-container"] [data-testid="stMetricValue"] {{
      color: {C["text"]} !important;
      font-size: 28px !important;
      font-weight: 800 !important;
  }}
  /* Tablas */
  .stDataFrame {{ border-radius: 10px; overflow: hidden; }}
  thead tr th {{
      background-color: {C["panel"]} !important;
      color: {C["accent"]} !important;
  }}
  tbody tr:nth-child(even) td {{ background-color: {C["panel"]} !important; }}
  tbody tr:nth-child(odd)  td {{ background-color: {C["bg"]} !important; }}
  /* Títulos de sección */
  .section-title {{
      font-size: 11px; font-weight: 700;
      text-transform: uppercase; letter-spacing: 1px;
      color: {C["muted"]}; margin-bottom: 10px;
  }}
  /* Barra de progreso custom */
  .prog-wrap {{
      background: {C["border"]}; border-radius: 6px;
      height: 10px; overflow: hidden; margin: 4px 0 12px;
  }}
  .prog-fill {{
      height: 100%; border-radius: 6px;
      transition: width 0.8s ease;
  }}
  div[data-testid="stForm"] {{
      background: {C["panel"]};
      border: 1px solid {C["border"]};
      border-radius: 12px;
      padding: 16px;
  }}
  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {{
      background-color: {C["panel"]};
      border-radius: 10px 10px 0 0;
      border-bottom: 1px solid {C["border"]};
  }}
  .stTabs [data-baseweb="tab"] {{
      color: {C["muted"]};
      font-weight: 600;
  }}
  .stTabs [aria-selected="true"] {{
      color: {C["accent"]} !important;
      border-bottom: 2px solid {C["accent"]} !important;
  }}
  hr {{ border-color: {C["border"]}; }}
</style>
""", unsafe_allow_html=True)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
ANNUAL_GOALS = {"Operation": 50, "Intervention": 30, "Prozedur": 20}
KATEGORIEN   = ["Operation", "Intervention", "Prozedur"]

EINGRIFFE = {
    "Operation": [
        "Carotis EEA/TEA", "Aortenaneurysma Rohrprothese", "Aortenaneurysma Bypass",
        "Aortobi- oder monoiliakaler Bypass", "Aortobi- oder monofemoraler Bypass",
        "Iliofemoraler Bypass", "Crossover Bypass", "Femoralis TEA",
        "Fem-pop. P1 Bypass", "Fem-pop. P3 Bypass", "Fem-cruraler Bypass",
        "P1-P3 Bypass", "Wunddebridement - VAC Wechsel",
    ],
    "Intervention": [
        "TEVAR", "FEVAR", "EVAR", "BEVAR", "Organstent",
        "Beckenstent", "Beinstent", "Thrombektomie over the wire",
    ],
    "Prozedur": [
        "ZVK-Anlage", "Drainage Thorax", "Drainage Abdomen",
        "Drainage Wunde Extremitäten", "Punktion/PE",
    ],
}

# ─── BASE DE DATOS ────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    conn = sqlite3.connect("chirurgischer_bericht.db", check_same_thread=False)
    cur  = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS users
        (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password TEXT,
         security_question TEXT, security_answer TEXT)""")
    cur.execute("PRAGMA table_info(users)")
    user_cols = [r[1] for r in cur.fetchall()]
    for col, sql in [
        ("security_question", "ALTER TABLE users ADD COLUMN security_question TEXT"),
        ("security_answer",   "ALTER TABLE users ADD COLUMN security_answer TEXT"),
    ]:
        if col not in user_cols:
            cur.execute(sql)
    cur.execute("""CREATE TABLE IF NOT EXISTS operationen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datum TEXT, datum_sort TEXT, eingriff TEXT, rolle TEXT,
        patient_id TEXT, diagnose TEXT, kategorie TEXT,
        zugang TEXT, verschlusssystem TEXT, notizen TEXT,
        username TEXT, user_id INTEGER)""")
    cur.execute("PRAGMA table_info(operationen)")
    cols = [r[1] for r in cur.fetchall()]
    for col, sql in [
        ("zugang",           "ALTER TABLE operationen ADD COLUMN zugang TEXT"),
        ("verschlusssystem", "ALTER TABLE operationen ADD COLUMN verschlusssystem TEXT"),
        ("username",         "ALTER TABLE operationen ADD COLUMN username TEXT"),
        ("datum_sort",       "ALTER TABLE operationen ADD COLUMN datum_sort TEXT"),
        ("user_id",          "ALTER TABLE operationen ADD COLUMN user_id INTEGER"),
    ]:
        if col not in cols:
            cur.execute(sql)
    conn.commit()
    return conn

def get_cur(): return get_conn().cursor()

# ─── UTILIDADES ───────────────────────────────────────────────────────────────
def hash_pw(pw):   return hashlib.sha256(pw.encode()).hexdigest()
def date_ok(d):
    try: datetime.strptime(d, "%d.%m.%Y"); return True
    except: return False
def to_sort(d):    return datetime.strptime(d, "%d.%m.%Y").strftime("%Y-%m-%d")

def reorder_ids(uname):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM operationen WHERE username=? ORDER BY id", (uname,))
    for new_id, (old_id,) in enumerate(cur.fetchall(), 1):
        cur.execute("UPDATE operationen SET user_id=? WHERE id=? AND username=?",
                    (new_id, old_id, uname))
    conn.commit()

# ─── QUERIES ──────────────────────────────────────────────────────────────────
def fetch_ops(username, is_tutor, extra="", params=()):
    cur = get_cur()
    if is_tutor:
        sql = "SELECT datum,eingriff,rolle,patient_id,kategorie,username FROM operationen"
        if extra: sql += f" WHERE {extra}"
        cur.execute(sql, params)
        cols = ["Datum","Eingriff","Rolle","Patient","Kategorie","Benutzer"]
    else:
        base = "WHERE username=?" + (f" AND ({extra})" if extra else "")
        cur.execute(
            f"SELECT user_id,datum,eingriff,rolle,patient_id,diagnose,kategorie,"
            f"zugang,verschlusssystem,notizen FROM operationen {base} ORDER BY user_id",
            (username,) + params)
        cols = ["ID","Datum","Eingriff","Rolle","Patient","Diagnose",
                "Kategorie","Zugang","Verschlusssystem","Notizen"]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

def fetch_monthly(username, year):
    cur = get_cur()
    w   = f"strftime('%Y',datum_sort)='{year}'"
    p   = ()
    if username:
        w += " AND username=?"; p = (username,)
    cur.execute(
        f"SELECT strftime('%m',datum_sort) as m, kategorie, COUNT(*) "
        f"FROM operationen WHERE {w} GROUP BY m, kategorie", p)
    data = defaultdict(lambda: defaultdict(int))
    for m, k, n in cur.fetchall():
        data[int(m)][k] = n
    return data

def fetch_totals(username):
    cur = get_cur()
    w, p = ("WHERE username=?", (username,)) if username else ("", ())
    cur.execute(f"SELECT kategorie, COUNT(*) FROM operationen {w} GROUP BY kategorie", p)
    return dict(cur.fetchall())

def fetch_roles(username):
    cur = get_cur()
    w, p = ("WHERE username=?", (username,)) if username else ("", ())
    cur.execute(f"SELECT rolle, COUNT(*) FROM operationen {w} GROUP BY rolle", p)
    return dict(cur.fetchall())

def fetch_ranking():
    cur = get_cur()
    cur.execute("""
        SELECT username, COUNT(*) as total,
            SUM(CASE WHEN kategorie='Operation'    THEN 1 ELSE 0 END),
            SUM(CASE WHEN kategorie='Intervention' THEN 1 ELSE 0 END),
            SUM(CASE WHEN kategorie='Prozedur'     THEN 1 ELSE 0 END)
        FROM operationen GROUP BY username ORDER BY total DESC""")
    return cur.fetchall()

def fetch_top_eingriffe(username):
    cur = get_cur()
    cur.execute(
        "SELECT eingriff, rolle, COUNT(*) n FROM operationen "
        "WHERE username=? GROUP BY eingriff, rolle ORDER BY n DESC LIMIT 10",
        (username,))
    return cur.fetchall()

# ─── PLOTLY HELPERS ───────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor=C["panel"], plot_bgcolor=C["bg"],
    font_color=C["text"], font_size=11,
    margin=dict(l=10, r=10, t=30, b=10),
)

# Axis style helper - apply to each figure individually
def _axis_style():
    return dict(gridcolor=C["border"], linecolor=C["border"], tickfont_color=C["muted"])

def _legend_style(**kwargs):
    base = dict(bgcolor="rgba(0,0,0,0)", font_color=C["muted"])
    base.update(kwargs)
    return base

def progress_bar_html(label, current, goal, color):
    pct      = min(current / goal, 1.0) if goal else 0
    pct_int  = int(pct * 100)
    bar_col  = C["accent"] if pct >= 1 else C["yellow"] if pct >= 0.5 else C["accent2"]
    check    = "✓" if pct >= 1 else ""
    return f"""
    <div style="margin-bottom:12px">
      <div style="display:flex;justify-content:space-between;margin-bottom:4px">
        <span style="font-size:11px;color:{C['muted']};font-weight:600">{label}</span>
        <span style="font-size:11px;color:{bar_col};font-weight:700">
          {current}/{goal} · {pct_int}% {check}
        </span>
      </div>
      <div class="prog-wrap">
        <div class="prog-fill" style="width:{pct_int}%;background:{color}"></div>
      </div>
    </div>"""

# ─── PDF EXPORT ───────────────────────────────────────────────────────────────
def make_pdf_bytes(df: pd.DataFrame, title: str) -> bytes:
    buf = io.BytesIO()
    c   = rl_canvas.Canvas(buf, pagesize=letter)
    c.setFont("Helvetica-Bold", 14); c.drawString(60, 760, title)
    c.setFont("Helvetica", 8); y = 730
    for _, row in df.iterrows():
        text = " | ".join(str(v) for v in row if v)
        while len(text) > 120:
            c.drawString(40, y, text[:120]); text = "  " + text[120:]; y -= 13
            if y < 50: c.showPage(); c.setFont("Helvetica", 8); y = 750
        c.drawString(40, y, text); y -= 18
        if y < 50: c.showPage(); c.setFont("Helvetica", 8); y = 750
    c.save(); buf.seek(0)
    return buf.read()

# ─── SESSION STATE ────────────────────────────────────────────────────────────
for key, default in [
    ("logged_in", False), ("username", ""), ("is_tutor", False),
    ("sel_year", datetime.now().year),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ══════════════════════════════════════════════════════════════════════════════
# PANTALLA DE LOGIN
# ══════════════════════════════════════════════════════════════════════════════
if not st.session_state.logged_in:
    col_l, col_c, col_r = st.columns([1, 1.2, 1])
    with col_c:
        st.markdown(f"""
        <div style="text-align:center;padding:40px 0 20px">
          <div style="font-size:48px">🏥</div>
          <h1 style="color:{C['accent']};margin:8px 0 4px;letter-spacing:-1px">OP Katalog</h1>
          <p style="color:{C['muted']};font-size:13px;margin:0">Chirurgisches Logbuch</p>
        </div>""", unsafe_allow_html=True)

        tab_login, tab_reg, tab_tutor, tab_reset = st.tabs(["Anmelden", "Registrieren", "Tutor Modus", "🔑 Passwort vergessen"])

        with tab_login:
            with st.form("form_login"):
                u = st.text_input("Benutzername")
                p = st.text_input("Passwort", type="password")
                if st.form_submit_button("Anmelden", use_container_width=True):
                    cur = get_cur()
                    cur.execute("SELECT id FROM users WHERE username=? AND password=?",
                                (u, hash_pw(p)))
                    if cur.fetchone():
                        st.session_state.update(logged_in=True, username=u, is_tutor=False)
                        st.rerun()
                    else:
                        st.error("Ungültige Anmeldedaten.")

        with tab_reg:
            with st.form("form_reg"):
                u  = st.text_input("Benutzername", key="reg_u")
                p  = st.text_input("Passwort", type="password", key="reg_p")
                sq = st.selectbox("Sicherheitsfrage", [
                    "Name des ersten Haustieres?",
                    "Geburtsstadt der Mutter?",
                    "Name der Grundschule?",
                    "Lieblingsfilm als Kind?",
                    "Name des besten Freundes als Kind?",
                ], key="reg_sq")
                sa = st.text_input("Antwort auf Sicherheitsfrage", key="reg_sa",
                                   help="Antwort wird verschlüsselt gespeichert.")
                if st.form_submit_button("Registrieren", use_container_width=True):
                    if not u or not p or not sa:
                        st.error("Alle Felder erforderlich.")
                    else:
                        try:
                            conn = get_conn()
                            conn.execute(
                                "INSERT INTO users (username,password,security_question,security_answer) "
                                "VALUES (?,?,?,?)",
                                (u, hash_pw(p), sq, hash_pw(sa.strip().lower())))
                            conn.commit()
                            st.success("Benutzer registriert. Bitte anmelden.")
                        except sqlite3.IntegrityError:
                            st.error("Benutzername existiert bereits.")

        with tab_tutor:
            with st.form("form_tutor"):
                u = st.text_input("Benutzername (optional)", key="tutor_u")
                p = st.text_input("Tutor-Passwort", type="password", key="tutor_p")
                if st.form_submit_button("Als Tutor anmelden", use_container_width=True):
                    if p == "tutor01":
                        st.session_state.update(
                            logged_in=True, username=u or "Tutor", is_tutor=True)
                        st.rerun()
                    else:
                        st.error("Ungültiges Tutor-Passwort.")

        with tab_reset:
            st.markdown(
                f"<p style='color:{C['muted']};font-size:12px;margin-bottom:12px'>"
                "Gib deinen Benutzernamen ein, beantworte die Sicherheitsfrage "
                "und vergib ein neues Passwort.</p>", unsafe_allow_html=True)

            reset_u = st.text_input("Benutzername", key="rst_u")

            if reset_u:
                cur = get_cur()
                cur.execute("SELECT security_question FROM users WHERE username=?", (reset_u,))
                row = cur.fetchone()
                if row and row[0]:
                    st.markdown(
                        f"<p style='color:{C['accent']};font-size:13px;"
                        f"font-weight:600;margin:8px 0 4px'>🔐 {row[0]}</p>",
                        unsafe_allow_html=True)
                    with st.form("form_reset"):
                        rst_ans = st.text_input("Deine Antwort", key="rst_ans")
                        rst_p1  = st.text_input("Neues Passwort", type="password", key="rst_p1")
                        rst_p2  = st.text_input("Passwort bestätigen", type="password", key="rst_p2")
                        if st.form_submit_button("Passwort zurücksetzen", use_container_width=True):
                            if not rst_ans or not rst_p1:
                                st.error("Alle Felder erforderlich.")
                            elif rst_p1 != rst_p2:
                                st.error("Passwörter stimmen nicht überein.")
                            else:
                                cur2 = get_cur()
                                cur2.execute(
                                    "SELECT id FROM users WHERE username=? AND security_answer=?",
                                    (reset_u, hash_pw(rst_ans.strip().lower())))
                                if cur2.fetchone():
                                    conn = get_conn()
                                    conn.execute(
                                        "UPDATE users SET password=? WHERE username=?",
                                        (hash_pw(rst_p1), reset_u))
                                    conn.commit()
                                    st.success("✓ Passwort erfolgreich geändert. Bitte anmelden.")
                                else:
                                    st.error("Antwort falsch. Bitte erneut versuchen.")
                elif row and not row[0]:
                    st.warning("Für diesen Benutzer ist keine Sicherheitsfrage hinterlegt. "
                               "Bitte wende dich an den Tutor (tutor01).")
                else:
                    st.info("Benutzername nicht gefunden.")
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPAL — layout único
# ══════════════════════════════════════════════════════════════════════════════
username  = st.session_state.username
is_tutor  = st.session_state.is_tutor
year      = st.session_state.sel_year

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="padding:16px 0 8px">
      <div style="font-size:22px;font-weight:800;color:{C['accent']}">🏥 OP Katalog</div>
      <div style="font-size:11px;color:{C['muted']};margin-top:2px">
        {'👁 Tutor — ' + username if is_tutor else '👤 ' + username}
      </div>
    </div>""", unsafe_allow_html=True)
    st.divider()

    # Selector de año
    st.markdown(f"<p style='color:{C['muted']};font-size:11px;font-weight:700;"
                f"text-transform:uppercase;letter-spacing:1px'>Anzeige-Jahr</p>",
                unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        if st.button("◀", key="yr_back"):
            st.session_state.sel_year -= 1; st.rerun()
    with c2:
        st.markdown(f"<div style='text-align:center;color:{C['accent']};"
                    f"font-weight:800;font-size:18px;padding-top:4px'>{year}</div>",
                    unsafe_allow_html=True)
    with c3:
        if st.button("▶", key="yr_fwd"):
            st.session_state.sel_year += 1; st.rerun()

    st.divider()
    if st.button("🚪 Abmelden", use_container_width=True):
        for k in ["logged_in", "username", "is_tutor"]:
            st.session_state[k] = False if k != "username" else ""
        st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 1: DASHBOARD (siempre visible)
# ══════════════════════════════════════════════════════════════════════════════
uname_dash = None if is_tutor else username
totals  = fetch_totals(uname_dash)
roles   = fetch_roles(uname_dash)
monthly = fetch_monthly(uname_dash, year)

st.markdown(f"<h2 style='color:{C['text']};margin:0 0 16px'>📊 Dashboard "
            f"{'— Alle Residenten' if is_tutor else '— ' + username} · {year}</h2>",
            unsafe_allow_html=True)

# ── KPI Cards ──
k1, k2, k3, k4, k5, k6 = st.columns(6)
total_n = sum(totals.values())
kpi_data = [
    (k1, total_n,                       "Gesamt"),
    (k2, roles.get("Operateur", 0),     "Operateur"),
    (k3, roles.get("Assistent", 0),     "Assistent"),
    (k4, totals.get("Operation", 0),    "Operationen"),
    (k5, totals.get("Intervention", 0), "Interventionen"),
    (k6, totals.get("Prozedur", 0),     "Prozeduren"),
]
for col, val, label in kpi_data:
    with col:
        st.metric(label, val)

st.divider()

# ── Fila: gráfico mensual + metas ──
col_chart, col_right = st.columns([2.2, 1])

with col_chart:
    m_lbls = [calendar.month_abbr[m] for m in range(1, 13)]
    fig_line = go.Figure()
    kat_colors = [C["op"], C["interv"], C["proz"]]

    for kat, col_k in zip(KATEGORIEN, kat_colors):
        vals = [monthly[m].get(kat, 0) for m in range(1, 13)]
        r, g, b = int(col_k[1:3],16), int(col_k[3:5],16), int(col_k[5:7],16)
        fig_line.add_trace(go.Scatter(
            x=m_lbls, y=vals, name=kat,
            mode="lines+markers",
            line=dict(color=col_k, width=2.5),
            marker=dict(size=6, color=col_k, line=dict(color=C["bg"], width=1.5)),
            fill="tozeroy",
            fillcolor=f"rgba({r},{g},{b},0.08)",
        ))

    cumul = []; acc = 0
    for m in range(1, 13):
        acc += sum(monthly[m].values()); cumul.append(acc)
    fig_line.add_trace(go.Scatter(
        x=m_lbls, y=cumul, name="Kumuliert",
        mode="lines", yaxis="y2",
        line=dict(color=C["yellow"], width=2, dash="dot"),
    ))
    fig_line.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text=f"Monatsverlauf {year}", font_color=C["text"], font_size=13),
        height=300,
        xaxis=_axis_style(),
        yaxis=_axis_style(),
        yaxis2=dict(overlaying="y", side="right", **_axis_style(),
                    title=dict(text="Kumuliert", font_color=C["muted"])),
        legend=_legend_style(orientation="h", y=-0.25),
    )
    st.plotly_chart(fig_line, use_container_width=True)

with col_right:
    st.markdown(f"<p class='section-title'>Jahresziele {year}</p>",
                unsafe_allow_html=True)
    for kat, col_k in zip(KATEGORIEN, kat_colors):
        html = progress_bar_html(kat, totals.get(kat, 0), ANNUAL_GOALS[kat], col_k)
        st.markdown(html, unsafe_allow_html=True)

    st.markdown(f"<p class='section-title' style='margin-top:16px'>Rolle</p>",
                unsafe_allow_html=True)
    total_r = sum(roles.values()) or 1
    st.markdown(
        progress_bar_html("Operateur", roles.get("Operateur", 0), total_r, C["accent"]) +
        progress_bar_html("Assistent", roles.get("Assistent", 0), total_r, C["accent2"]),
        unsafe_allow_html=True)

# ── Fila: ranking (tutor) o top eingriffe (residente) ──
st.divider()

if is_tutor:
    st.markdown(f"<h4 style='color:{C['text']}'>🏆 Ranking Residenten</h4>",
                unsafe_allow_html=True)
    ranking = fetch_ranking()
    if ranking:
        df_rank = pd.DataFrame(ranking,
            columns=["Benutzer","Total","Operation","Intervention","Prozedur"])
        fig_rank = go.Figure()
        for kat, col_k in zip(KATEGORIEN, kat_colors):
            fig_rank.add_trace(go.Bar(
                y=df_rank["Benutzer"], x=df_rank[kat],
                name=kat, orientation="h",
                marker_color=col_k, opacity=0.88,
            ))
        fig_rank.update_layout(
            **PLOTLY_LAYOUT,
            barmode="stack", height=250,
            title=dict(text="Eingriffe gesamt nach Benutzer",
                       font_color=C["text"], font_size=12),
            xaxis=_axis_style(),
            yaxis=_axis_style(),
            legend=_legend_style(orientation="h", y=-0.3),
        )
        col_r1, col_r2 = st.columns([1.5, 1])
        with col_r1:
            st.plotly_chart(fig_rank, use_container_width=True)
        with col_r2:
            st.dataframe(
                df_rank.style
                    .highlight_max(subset=["Total"], color=C["yellow"] + "44")
                    .format({"Total": "{}", "Operation": "{}",
                             "Intervention": "{}", "Prozedur": "{}"}),
                use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Daten vorhanden.")
else:
    st.markdown(f"<h4 style='color:{C['text']}'>📌 Top Eingriffe</h4>",
                unsafe_allow_html=True)
    rows = fetch_top_eingriffe(username)
    if rows:
        df_top = pd.DataFrame(rows, columns=["Eingriff", "Rolle", "n"])
        col_t1, col_t2 = st.columns([1.5, 1])
        with col_t1:
            fig_top = px.bar(
                df_top, x="n", y="Eingriff", color="Rolle",
                orientation="h",
                color_discrete_map={"Operateur": C["accent"], "Assistent": C["accent2"]},
            )
            fig_top.update_layout(**PLOTLY_LAYOUT, height=280,
                                  showlegend=True,
                                  xaxis=_axis_style(),
                                  yaxis=_axis_style(),
                                  legend=_legend_style(orientation="h", y=-0.3))
            st.plotly_chart(fig_top, use_container_width=True)
        with col_t2:
            st.dataframe(df_top, use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Eingriffe vorhanden.")

# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 2: NUEVA OPERACIÓN (solo residentes)
# ══════════════════════════════════════════════════════════════════════════════
if not is_tutor:
    st.divider()
    st.markdown(f"<h3 style='color:{C['text']};margin:0 0 12px'>➕ Neue Operation eintragen</h3>",
                unsafe_allow_html=True)

    with st.form("form_neue_op", clear_on_submit=True):
        fc1, fc2, fc3, fc4 = st.columns([1.2, 1.5, 1.5, 1])
        with fc1:
            datum_dt  = st.date_input("Datum *", value=datetime.today(),
                                      format="DD.MM.YYYY")
        with fc2:
            kategorie = st.selectbox("Kategorie *", KATEGORIEN)
        with fc3:
            eingriff  = st.selectbox("Eingriff *", EINGRIFFE[kategorie])
        with fc4:
            rolle     = st.selectbox("Rolle *", ["Operateur", "Assistent"])

        fc5, fc6, fc7 = st.columns(3)
        with fc5:
            patient_id = st.text_input("Patienten-ID *")
        with fc6:
            diagnose   = st.text_input("Diagnose *")
        with fc7:
            notizen    = st.text_input("Notizen")

        zugang, verschlusssystem = "", ""
        if kategorie == "Intervention":
            fi1, fi2 = st.columns(2)
            with fi1:
                zugang = st.selectbox("Zugang *", ["Punktion", "Offen"])
            with fi2:
                if zugang == "Punktion":
                    verschlusssystem = st.selectbox("Verschlusssystem *",
                                                    ["AngioSeal", "ProGlide"])

        submitted = st.form_submit_button("＋ Hinzufügen", use_container_width=True,
                                          type="primary")

    if submitted:
        datum_str = datum_dt.strftime("%d.%m.%Y")
        datum_sort = datum_dt.strftime("%Y-%m-%d")
        errors = []
        if not patient_id: errors.append("Patienten-ID fehlt")
        if not diagnose:   errors.append("Diagnose fehlt")
        if kategorie == "Intervention" and not zugang:
            errors.append("Zugang fehlt")
        if errors:
            st.error(" · ".join(errors))
        else:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("SELECT MAX(user_id) FROM operationen WHERE username=?", (username,))
            uid = (cur.fetchone()[0] or 0) + 1
            conn.execute(
                "INSERT INTO operationen (datum,datum_sort,eingriff,rolle,patient_id,"
                "diagnose,kategorie,zugang,verschlusssystem,notizen,username,user_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (datum_str, datum_sort, eingriff, rolle, patient_id,
                 diagnose, kategorie, zugang, verschlusssystem, notizen, username, uid))
            conn.commit()
            st.success(f"✓ Operation '{eingriff}' erfolgreich registriert (ID {uid}).")
            st.balloons()

# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3: LOGBUCH (siempre visible)
# ══════════════════════════════════════════════════════════════════════════════
st.divider()
st.markdown(f"<h3 style='color:{C['text']};margin:0 0 12px'>📋 Logbuch</h3>",
            unsafe_allow_html=True)

# ── Filtros ──
with st.expander("🔍 Suchen & Filtern", expanded=False):
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        filter_kat = st.selectbox("Kategorie", ["Alle"] + KATEGORIEN)
    with fc2:
        filter_rolle = st.selectbox("Rolle", ["Alle", "Operateur", "Assistent"])
    with fc3:
        if is_tutor:
            cur = get_cur()
            cur.execute("SELECT username FROM users")
            all_users = ["Alle"] + [r[0] for r in cur.fetchall()]
            filter_user = st.selectbox("Benutzer", all_users)
        else:
            filter_user = username

conditions, params = [], ()
if filter_kat != "Alle":
    conditions.append("kategorie=?"); params += (filter_kat,)
if filter_rolle != "Alle":
    conditions.append("rolle=?"); params += (filter_rolle,)
if is_tutor and filter_user != "Alle":
    conditions.append("username=?"); params += (filter_user,)

extra = " AND ".join(conditions)
df = fetch_ops(username, is_tutor, extra, params)

if df.empty:
    st.info("Keine Einträge vorhanden.")
else:
    def style_rolle(val):
        if val == "Operateur": return f"color: {C['accent']}; font-weight: bold"
        if val == "Assistent": return f"color: {C['accent2']}; font-weight: bold"
        return ""
    def style_kat(val):
        m = {"Operation": C["op"], "Intervention": C["interv"], "Prozedur": C["proz"]}
        c = m.get(val, C["text"])
        return f"color: {c}; font-weight: bold"

    styled = df.style
    if "Rolle" in df.columns:
        styled = styled.applymap(style_rolle, subset=["Rolle"])
    if "Kategorie" in df.columns:
        styled = styled.applymap(style_kat, subset=["Kategorie"])

    st.dataframe(styled, use_container_width=True, hide_index=True, height=380)
    st.caption(f"{len(df)} Einträge gefunden.")

# ── Acciones exportar / borrar ──
st.divider()
a1, a2, a3, a4 = st.columns(4)

with a1:
    if not df.empty:
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False, encoding="utf-8")
        st.download_button("📁 CSV herunterladen", csv_buf.getvalue(),
                           "logbuch.csv", "text/csv",
                           use_container_width=True)
with a2:
    if not df.empty:
        pdf_bytes = make_pdf_bytes(df, "Logbuch - Chirurgischer Bericht")
        st.download_button("📄 PDF herunterladen", pdf_bytes,
                           "logbuch.pdf", "application/pdf",
                           use_container_width=True)
with a3:
    if not is_tutor and not df.empty:
        st.markdown(f"<p style='font-size:11px;color:{C['muted']};margin-bottom:4px'>"
                    "Eintrag löschen (ID):</p>", unsafe_allow_html=True)
        del_id = st.number_input("ID", min_value=1, step=1,
                                 label_visibility="collapsed")
with a4:
    if not is_tutor and not df.empty:
        if st.button("🗑 Löschen", use_container_width=True, type="secondary"):
            conn = get_conn()
            conn.execute("DELETE FROM operationen WHERE user_id=? AND username=?",
                         (del_id, username))
            conn.commit()
            reorder_ids(username)
            st.success(f"Eintrag {del_id} gelöscht.")
            st.rerun()
