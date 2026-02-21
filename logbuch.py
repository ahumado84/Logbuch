import sqlite3
import csv
import hashlib
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import platform
import calendar
from collections import defaultdict

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ─── PALETA ───────────────────────────────────────────────────────────────────
C = {
    "bg":      "#0f1117", "panel":   "#161b27", "border":  "#1e2535",
    "accent":  "#4af0b0", "accent2": "#f06a4a", "accent3": "#4a8ff0",
    "yellow":  "#f0d44a", "text":    "#e2e8f0", "muted":   "#6b7899",
    "op":      "#4af0b0", "interv":  "#4a8ff0", "proz":    "#f0d44a",
}

ANNUAL_GOALS = {"Operation": 50, "Intervention": 30, "Prozedur": 20}
KATEGORIEN   = ["Operation", "Intervention", "Prozedur"]
KAT_COLORS   = [C["op"], C["interv"], C["proz"]]

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

# ─── UTILIDADES ───────────────────────────────────────────────────────────────

def hash_pw(pw):    return hashlib.sha256(pw.encode()).hexdigest()
def date_ok(d):
    try: datetime.strptime(d, '%d.%m.%Y'); return True
    except ValueError: return False
def to_sort(d):     return datetime.strptime(d, '%d.%m.%Y').strftime('%Y-%m-%d')

# ─── BASE DE DATOS ────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect("chirurgischer_bericht.db")
    cur  = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS users
        (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS operationen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datum TEXT, datum_sort TEXT, eingriff TEXT, rolle TEXT,
        patient_id TEXT, diagnose TEXT, kategorie TEXT,
        zugang TEXT, verschlusssystem TEXT, notizen TEXT,
        username TEXT, user_id INTEGER)''')
    cur.execute("PRAGMA table_info(operationen)")
    cols = [r[1] for r in cur.fetchall()]
    for col, sql in [
        ('zugang',          "ALTER TABLE operationen ADD COLUMN zugang TEXT"),
        ('verschlusssystem',"ALTER TABLE operationen ADD COLUMN verschlusssystem TEXT"),
        ('username',        "ALTER TABLE operationen ADD COLUMN username TEXT"),
        ('datum_sort',      "ALTER TABLE operationen ADD COLUMN datum_sort TEXT"),
    ]:
        if col not in cols: cur.execute(sql)
    if 'user_id' not in cols:
        cur.execute("ALTER TABLE operationen ADD COLUMN user_id INTEGER")
        cur.execute("SELECT DISTINCT username FROM operationen WHERE username IS NOT NULL")
        for (u,) in cur.fetchall():
            cur.execute("SELECT id FROM operationen WHERE username=? ORDER BY id", (u,))
            for i, (oid,) in enumerate(cur.fetchall(), 1):
                cur.execute("UPDATE operationen SET user_id=? WHERE id=? AND username=?", (i, oid, u))
    cur.execute("SELECT id,datum FROM operationen WHERE datum_sort IS NULL AND datum IS NOT NULL")
    for rid, d in cur.fetchall():
        try: cur.execute("UPDATE operationen SET datum_sort=? WHERE id=?", (to_sort(d), rid))
        except ValueError: pass
    conn.commit()
    return conn, cur

def reorder_ids(cur, conn, uname):
    cur.execute("SELECT user_id FROM operationen WHERE username=? ORDER BY user_id", (uname,))
    for new, (old,) in enumerate(cur.fetchall(), 1):
        cur.execute("UPDATE operationen SET user_id=? WHERE user_id=? AND username=?", (new, old, uname))
    conn.commit()

# ─── QUERIES CENTRALIZADAS ────────────────────────────────────────────────────

T_COLS  = "datum, eingriff, rolle, patient_id, kategorie, username"
U_COLS  = "user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen"
T_HEADS = ("Datum","Eingriff","Rolle","Patient","Kategorie","Benutzer")
U_HEADS = ("ID","Datum","Eingriff","Rolle","Patient","Diagnose","Kategorie","Zugang","Verschlusssystem","Notizen")

def fetch(extra="", params=()):
    if is_tutor:
        sql = f"SELECT {T_COLS} FROM operationen" + (f" WHERE {extra}" if extra else "")
        cursor.execute(sql, params)
    else:
        base = "WHERE username=?" + (f" AND ({extra})" if extra else "")
        cursor.execute(f"SELECT {U_COLS} FROM operationen {base} ORDER BY user_id",
                       (current_user,) + params)
    return cursor.fetchall()

def fmt_tutor(row):
    r = list(row); r[2] = r[2] if r[2] == "Operateur" else ""; return r

# ─── DATOS PARA DASHBOARD ─────────────────────────────────────────────────────

def dash_monthly(uname, year):
    w = f"strftime('%Y',datum_sort)='{year}'" + (" AND username=?" if uname else "")
    p = (uname,) if uname else ()
    cursor.execute(
        f"SELECT strftime('%m',datum_sort) as m, kategorie, COUNT(*) "
        f"FROM operationen WHERE {w} GROUP BY m, kategorie", p)
    data = defaultdict(lambda: defaultdict(int))
    for m, k, n in cursor.fetchall():
        data[int(m)][k] = n
    return data

def dash_totals(uname):
    w = "WHERE username=?" if uname else ""
    p = (uname,) if uname else ()
    cursor.execute(f"SELECT kategorie, COUNT(*) FROM operationen {w} GROUP BY kategorie", p)
    return dict(cursor.fetchall())

def dash_roles(uname):
    w = "WHERE username=?" if uname else ""
    p = (uname,) if uname else ()
    cursor.execute(f"SELECT rolle, COUNT(*) FROM operationen {w} GROUP BY rolle", p)
    return dict(cursor.fetchall())

def dash_ranking():
    cursor.execute("""
        SELECT username, COUNT(*) as total,
               SUM(CASE WHEN kategorie='Operation'    THEN 1 ELSE 0 END),
               SUM(CASE WHEN kategorie='Intervention' THEN 1 ELSE 0 END),
               SUM(CASE WHEN kategorie='Prozedur'     THEN 1 ELSE 0 END)
        FROM operationen GROUP BY username ORDER BY total DESC LIMIT 8""")
    return cursor.fetchall()

# ─── MATPLOTLIB HELPERS ───────────────────────────────────────────────────────

def dark_ax(fig, *axes):
    fig.patch.set_facecolor(C["panel"])
    for ax in axes:
        ax.set_facecolor(C["bg"])
        for s in ax.spines.values(): s.set_edgecolor(C["border"])
        ax.tick_params(colors=C["muted"], labelsize=7.5)
        ax.xaxis.label.set_color(C["muted"])
        ax.yaxis.label.set_color(C["muted"])
        ax.title.set_color(C["text"])

def embed(fig, master, **pack_kw):
    cv = FigureCanvasTkAgg(fig, master=master)
    cv.draw()
    cv.get_tk_widget().pack(**pack_kw)
    return cv

# ─── WIDGETS REUTILIZABLES ────────────────────────────────────────────────────

def tk_label(parent, text, fg=None, font=None, **kw):
    return tk.Label(parent, text=text, bg=C["panel"],
                    fg=fg or C["text"], font=font or ("Helvetica", 9), **kw)

def progress_canvas(parent, current, goal, color, width=180, height=13):
    pct = min(current / goal, 1.0) if goal else 0
    cv  = tk.Canvas(parent, width=width, height=height,
                    bg=C["bg"], highlightthickness=0)
    cv.create_rectangle(0, 0, width, height, fill=C["border"], width=0)
    if pct > 0:
        cv.create_rectangle(0, 0, int(width * pct), height, fill=color, width=0)
    cv.create_text(width // 2, height // 2,
                   text=f"{current}/{goal}  {int(pct*100)}%",
                   fill=C["bg"] if pct > 0.45 else C["text"],
                   font=("Helvetica", 7, "bold"))
    return cv

def kpi_card(parent, value, label, color, sub=""):
    f = tk.Frame(parent, bg=C["panel"], padx=12, pady=8,
                 highlightbackground=C["border"], highlightthickness=1)
    tk.Label(f, text=str(value), bg=C["panel"], fg=color,
             font=("Helvetica", 20, "bold")).pack()
    tk.Label(f, text=label, bg=C["panel"], fg=C["text"],
             font=("Helvetica", 8, "bold")).pack()
    if sub:
        tk.Label(f, text=sub, bg=C["panel"], fg=C["muted"],
                 font=("Helvetica", 7)).pack()
    return f

# ─── DASHBOARD (construido dentro de main_window) ─────────────────────────────

def build_dashboard(container, uname, year):
    """Borra y reconstruye el contenido del panel izquierdo del dashboard."""
    for w in container.winfo_children():
        w.destroy()

    totals  = dash_totals(uname)
    roles   = dash_roles(uname)
    monthly = dash_monthly(uname, year)
    months  = list(range(1, 13))
    m_lbls  = [calendar.month_abbr[m] for m in months]
    total_n = sum(totals.values())

    # ── KPI Row ──
    kpi_row = tk.Frame(container, bg=C["bg"])
    kpi_row.pack(fill="x", pady=(0, 10))
    kpis = [
        (total_n,                    "Gesamt",        C["text"],    ""),
        (roles.get("Operateur", 0),  "Operateur",     C["accent"],  "1. Operateur"),
        (roles.get("Assistent", 0),  "Assistent",     C["accent2"], "Assistent"),
        (totals.get("Operation", 0), "Operationen",   C["op"],      f"Ziel {ANNUAL_GOALS['Operation']}"),
        (totals.get("Intervention",0),"Interventionen",C["interv"], f"Ziel {ANNUAL_GOALS['Intervention']}"),
        (totals.get("Prozedur", 0),  "Prozeduren",    C["proz"],    f"Ziel {ANNUAL_GOALS['Prozedur']}"),
    ]
    for val, lbl, col, sub in kpis:
        card = kpi_card(kpi_row, val, lbl, col, sub)
        card.pack(side="left", expand=True, fill="both", padx=4)

    # ── Charts Row ──
    charts_row = tk.Frame(container, bg=C["bg"])
    charts_row.pack(fill="both", expand=True)

    # ── Gráfico de línea — evolución mensual ──
    line_frame = tk.Frame(charts_row, bg=C["panel"],
                          highlightbackground=C["border"], highlightthickness=1)
    line_frame.pack(side="left", fill="both", expand=True, padx=(0, 8))
    tk.Label(line_frame, text=f"Monatsverlauf {year}", bg=C["panel"],
             fg=C["text"], font=("Helvetica", 10, "bold")).pack(anchor="w", padx=12, pady=(10, 0))

    fig_line = Figure(figsize=(5.6, 2.9), dpi=95)
    ax = fig_line.add_subplot(111)
    dark_ax(fig_line, ax)

    for kat, col in zip(KATEGORIEN, KAT_COLORS):
        vals = [monthly[m].get(kat, 0) for m in months]
        ax.plot(m_lbls, vals, color=col, linewidth=2,
                marker="o", markersize=4,
                markerfacecolor=C["bg"], markeredgewidth=1.5,
                label=kat, alpha=0.9)
        # área rellena suave
        ax.fill_between(m_lbls, vals, alpha=0.06, color=col)

    # línea acumulada
    cumul = []
    acc = 0
    for m in months:
        acc += sum(monthly[m].values())
        cumul.append(acc)
    ax2 = ax.twinx()
    ax2.set_facecolor(C["bg"])
    for s in ax2.spines.values(): s.set_edgecolor(C["border"])
    ax2.tick_params(colors=C["muted"], labelsize=7)
    ax2.plot(m_lbls, cumul, color=C["yellow"], linewidth=1.5,
             linestyle="--", alpha=0.7, label="Kumuliert")
    ax2.set_ylabel("Kumuliert", fontsize=7, color=C["muted"])

    ax.set_ylabel("Eingriffe / Monat", fontsize=7)
    ax.grid(axis="y", color=C["border"], linewidth=0.5, alpha=0.7)
    ax.legend(fontsize=6.5, facecolor=C["panel"], labelcolor=C["text"],
              framealpha=0.8, loc="upper left")
    fig_line.tight_layout()
    embed(fig_line, line_frame, fill="both", expand=True, padx=6, pady=6)

    # ── Panel derecho: progreso + ranking ──
    right = tk.Frame(charts_row, bg=C["bg"])
    right.pack(side="left", fill="both", expand=False, padx=(0, 0))

    # Barras de progreso hacia meta anual
    prog_panel = tk.Frame(right, bg=C["panel"],
                          highlightbackground=C["border"], highlightthickness=1)
    prog_panel.pack(fill="x", pady=(0, 8), ipadx=12, ipady=10)
    tk.Label(prog_panel, text="Jahresziele", bg=C["panel"],
             fg=C["text"], font=("Helvetica", 10, "bold")).pack(anchor="w", padx=10, pady=(6, 8))

    for kat, col in zip(KATEGORIEN, KAT_COLORS):
        row_f = tk.Frame(prog_panel, bg=C["panel"])
        row_f.pack(fill="x", padx=10, pady=3)
        tk.Label(row_f, text=kat, bg=C["panel"], fg=C["muted"],
                 font=("Helvetica", 8), width=13, anchor="w").pack(side="left")
        pct   = min(totals.get(kat, 0) / ANNUAL_GOALS[kat], 1.0)
        pcolor = C["accent"] if pct >= 1 else C["yellow"] if pct >= 0.5 else C["accent2"]
        progress_canvas(row_f, totals.get(kat, 0), ANNUAL_GOALS[kat], col).pack(side="left")
        tk.Label(row_f, text="✓" if pct >= 1 else "", bg=C["panel"],
                 fg=C["accent"], font=("Helvetica", 10, "bold"), width=2).pack(side="left")

    # Sección Rolle
    sep = tk.Frame(prog_panel, bg=C["border"], height=1)
    sep.pack(fill="x", padx=10, pady=6)
    tk.Label(prog_panel, text="Rolle", bg=C["panel"],
             fg=C["text"], font=("Helvetica", 9, "bold")).pack(anchor="w", padx=10, pady=(0, 6))
    total_r = sum(roles.values()) or 1
    for rolle, col in [("Operateur", C["accent"]), ("Assistent", C["accent2"])]:
        row_f = tk.Frame(prog_panel, bg=C["panel"])
        row_f.pack(fill="x", padx=10, pady=3)
        tk.Label(row_f, text=rolle, bg=C["panel"], fg=C["muted"],
                 font=("Helvetica", 8), width=13, anchor="w").pack(side="left")
        progress_canvas(row_f, roles.get(rolle, 0), total_r, col).pack(side="left")

    # ── Ranking (tutor) o tabla resumen (residente) ──
    rank_panel = tk.Frame(right, bg=C["panel"],
                          highlightbackground=C["border"], highlightthickness=1)
    rank_panel.pack(fill="both", expand=True, ipadx=12, ipady=8)

    if is_tutor:
        tk.Label(rank_panel, text="🏆 Ranking Residenten", bg=C["panel"],
                 fg=C["text"], font=("Helvetica", 10, "bold")).pack(anchor="w", padx=10, pady=(8, 6))
        data = dash_ranking()
        # Cabecera
        hdr = tk.Frame(rank_panel, bg=C["border"])
        hdr.pack(fill="x", padx=10, pady=(0, 2))
        for txt, w in [("#", 3), ("Name", 12), ("Total", 6), ("Op", 5), ("Int", 5), ("Proz", 5)]:
            tk.Label(hdr, text=txt, bg=C["border"], fg=C["muted"],
                     font=("Helvetica", 7, "bold"), width=w).pack(side="left", padx=2)
        for i, (usr, tot, op, inv, prz) in enumerate(data, 1):
            color = C["yellow"] if i == 1 else C["text"]
            row_f = tk.Frame(rank_panel, bg=C["panel"] if i % 2 == 0 else C["bg"])
            row_f.pack(fill="x", padx=10, pady=1)
            for txt, w, col in [
                (str(i), 3, C["muted"]),
                (usr[:12], 12, color),
                (str(tot), 6, color),
                (str(op),  5, C["op"]),
                (str(inv), 5, C["interv"]),
                (str(prz), 5, C["proz"]),
            ]:
                tk.Label(row_f, text=txt, bg=row_f["bg"], fg=col,
                         font=("Helvetica", 8, "bold" if i == 1 else "normal"),
                         width=w, anchor="w").pack(side="left", padx=2)
    else:
        # Tabla resumen con indicadores de color para el residente
        tk.Label(rank_panel, text="Resumen por Eingriff", bg=C["panel"],
                 fg=C["text"], font=("Helvetica", 10, "bold")).pack(anchor="w", padx=10, pady=(8, 6))
        cursor.execute(
            "SELECT eingriff, rolle, COUNT(*) FROM operationen "
            "WHERE username=? GROUP BY eingriff, rolle ORDER BY COUNT(*) DESC LIMIT 10",
            (current_user,))
        rows = cursor.fetchall()
        hdr = tk.Frame(rank_panel, bg=C["border"])
        hdr.pack(fill="x", padx=10, pady=(0, 2))
        for txt, w in [("Eingriff", 22), ("Rolle", 10), ("n", 4)]:
            tk.Label(hdr, text=txt, bg=C["border"], fg=C["muted"],
                     font=("Helvetica", 7, "bold"), width=w, anchor="w").pack(side="left", padx=2)
        for i, (eingriff, rolle, cnt) in enumerate(rows):
            bg    = C["panel"] if i % 2 == 0 else C["bg"]
            r_col = C["accent"] if rolle == "Operateur" else C["accent2"]
            row_f = tk.Frame(rank_panel, bg=bg)
            row_f.pack(fill="x", padx=10, pady=1)
            tk.Label(row_f, text=eingriff[:22], bg=bg, fg=C["text"],
                     font=("Helvetica", 7), width=22, anchor="w").pack(side="left", padx=2)
            tk.Label(row_f, text=rolle,        bg=bg, fg=r_col,
                     font=("Helvetica", 7, "bold"), width=10, anchor="w").pack(side="left", padx=2)
            tk.Label(row_f, text=str(cnt),      bg=bg, fg=C["text"],
                     font=("Helvetica", 7), width=4).pack(side="left", padx=2)

# ─── LOGIN ─────────────────────────────────────────────────────────────────────

def login_window():
    global login_root, current_user, is_tutor
    login_root = tk.Tk()
    login_root.title("OP Katalog")
    login_root.geometry("380x300")
    login_root.configure(bg=C["bg"])

    tk.Label(login_root, text="OP Katalog", bg=C["bg"], fg=C["accent"],
             font=("Helvetica", 22, "bold")).pack(pady=24)
    tk.Label(login_root, text="Chirurgisches Logbuch", bg=C["bg"],
             fg=C["muted"], font=("Helvetica", 10)).pack(pady=(0, 20))

    def dialog(title, action):
        win = tk.Toplevel(login_root)
        win.title(title); win.geometry("300x230"); win.configure(bg=C["bg"])
        entries = {}
        for lbl, key, show in [("Benutzername:", "user", ""), ("Passwort:", "pw", "*")]:
            tk.Label(win, text=lbl, bg=C["bg"], fg=C["text"],
                     font=("Helvetica", 9)).pack(pady=(12, 2))
            e = tk.Entry(win, show=show, bg=C["panel"], fg=C["text"],
                         insertbackground=C["text"], relief="flat",
                         font=("Helvetica", 10))
            e.pack(ipadx=6, ipady=4); entries[key] = e
        tk.Button(win, text=title, bg=C["accent"], fg=C["bg"],
                  font=("Helvetica", 10, "bold"), relief="flat", padx=20, pady=6,
                  command=lambda: action(entries["user"].get(), entries["pw"].get(), win)
                  ).pack(pady=16)

    def do_reg(u, p, win):
        if not u or not p: messagebox.showerror("Fehler", "Alle Felder erforderlich."); return
        try:
            cursor.execute("INSERT INTO users (username,password) VALUES (?,?)", (u, hash_pw(p)))
            conn.commit(); messagebox.showinfo("Erfolg", "Benutzer registriert."); win.destroy()
        except sqlite3.IntegrityError:
            messagebox.showerror("Fehler", "Benutzername existiert bereits.")

    def do_login(u, p, win):
        global current_user, is_tutor
        if not u or not p: messagebox.showerror("Fehler", "Alle Felder erforderlich."); return
        cursor.execute("SELECT id FROM users WHERE username=? AND password=?", (u, hash_pw(p)))
        if cursor.fetchone():
            current_user, is_tutor = u, False
            win.destroy(); login_root.destroy(); main_window()
        else:
            messagebox.showerror("Fehler", "Ungültige Anmeldedaten.")

    def do_tutor(u, p, win):
        global current_user, is_tutor
        if p in ("tutor01", hash_pw("tutor01")):
            current_user, is_tutor = u or "Tutor", True
            win.destroy(); login_root.destroy(); main_window()
        else:
            messagebox.showerror("Fehler", "Ungültiges Tutor-Passwort.")

    s = {"bg": C["panel"], "fg": C["text"], "relief": "flat",
         "padx": 28, "pady": 8, "font": ("Helvetica", 10), "cursor": "hand2"}
    tk.Button(login_root, text="Anmelden",
              **{**s, "bg": C["accent"], "fg": C["bg"], "font": ("Helvetica", 10, "bold")},
              command=lambda: dialog("Anmelden", do_login)).pack(pady=4)
    tk.Button(login_root, text="Registrieren", **s,
              command=lambda: dialog("Registrieren", do_reg)).pack(pady=4)
    tk.Button(login_root, text="Tutor Modus", **s,
              command=lambda: dialog("Tutor Modus", do_tutor)).pack(pady=4)
    login_root.mainloop()

# ─── LÓGICA CRUD ──────────────────────────────────────────────────────────────

def neue_operation():
    datum    = entry_datum.get()
    eingriff = eingriff_var.get()
    rolle    = rolle_var.get()
    pat_id   = entry_patient_id.get()
    diagnose = entry_diagnose.get()
    kat      = kategorie_var.get()
    zugang   = zugang_var.get() if kat == "Intervention" else ""
    vschl    = verschlusssystem_var.get() if kat == "Intervention" and zugang == "Punktion" else ""
    notizen  = entry_notizen.get()

    if not date_ok(datum):
        messagebox.showerror("Fehler","Datum: TT.MM.JJJJ"); return
    if not all([datum, eingriff, rolle, pat_id, diagnose, kat]):
        messagebox.showerror("Fehler","Alle Pflichtfelder ausfüllen"); return
    if kat == "Intervention" and not zugang:
        messagebox.showerror("Fehler","Zugang auswählen"); return
    if kat == "Intervention" and zugang == "Punktion" and not vschl:
        messagebox.showerror("Fehler","Verschlusssystem auswählen"); return

    cursor.execute("SELECT MAX(user_id) FROM operationen WHERE username=?", (current_user,))
    uid = (cursor.fetchone()[0] or 0) + 1
    cursor.execute(
        "INSERT INTO operationen (datum,datum_sort,eingriff,rolle,patient_id,diagnose,"
        "kategorie,zugang,verschlusssystem,notizen,username,user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (datum, to_sort(datum), eingriff, rolle, pat_id, diagnose, kat, zugang, vschl, notizen, current_user, uid))
    conn.commit()
    messagebox.showinfo("Erfolg","Operation registriert.")
    clear_fields()
    refresh_all()

def clear_fields():
    for e in [entry_datum, entry_patient_id, entry_diagnose, entry_notizen]:
        e.delete(0, tk.END)
    rolle_var.set("Operateur"); kategorie_var.set("Operation")
    zugang_var.set("Punktion"); verschlusssystem_var.set("AngioSeal")
    update_eingriff_menu()

def refresh_all():
    """Refresca tabla + dashboard en un solo paso."""
    rows = fetch()
    tree.delete(*tree.get_children())
    for row in rows:
        tree.insert("", tk.END, values=fmt_tutor(row) if is_tutor else row)
    build_dashboard(dash_container, None if is_tutor else current_user, year_var.get())

def do_search():
    k = suche_var.get(); rows = []
    if k == "Datum Range" and is_tutor:
        vom, bis = vom_e.get(), bis_e.get()
        if not all([vom, bis, date_ok(vom), date_ok(bis)]):
            messagebox.showerror("Fehler","Ungültige Datumsangaben"); return
        rows = fetch("datum_sort BETWEEN ? AND ?", (to_sort(vom), to_sort(bis)))
    elif k == "Benutzer" and is_tutor:
        u = user_var.get()
        if not u: messagebox.showerror("Fehler","Benutzer auswählen"); return
        rows = fetch("username=?", (u,))
    elif k == "Datum":
        w = entry_suche.get()
        if not date_ok(w): messagebox.showerror("Fehler","Ungültiges Datum"); return
        rows = fetch("datum_sort=?", (to_sort(w),))
    elif k == "Kategorie":
        rows = fetch("kategorie=?", (entry_suche.get(),))
    tree.delete(*tree.get_children())
    for row in rows:
        tree.insert("", tk.END, values=fmt_tutor(row) if is_tutor else row)

def delete_entry():
    if is_tutor: messagebox.showerror("Fehler","Im Tutor-Modus nicht erlaubt."); return
    sel = tree.selection()
    if not sel: messagebox.showerror("Fehler","Keinen Eintrag ausgewählt."); return
    uid = tree.item(sel)["values"][0]
    cursor.execute("DELETE FROM operationen WHERE user_id=? AND username=?", (uid, current_user))
    conn.commit(); reorder_ids(cursor, conn, current_user)
    messagebox.showinfo("Erfolg","Eintrag gelöscht."); refresh_all()

def export_csv():
    rows = fetch()
    with open("logbuch.csv","w",newline="",encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(list(T_HEADS if is_tutor else U_HEADS))
        for r in rows: w.writerow(fmt_tutor(r) if is_tutor else r)
    messagebox.showinfo("Erfolg","logbuch.csv exportiert.")

def export_pdf():
    _make_pdf("logbuch.pdf","Logbuch - Chirurgischer Bericht", fetch())
    messagebox.showinfo("Erfolg","logbuch.pdf exportiert.")

def print_view():
    data = [tree.item(i)["values"] for i in tree.get_children()]
    _make_pdf("print_view.pdf","Logbuch - Chirurgischer Bericht", data)
    if platform.system() == "Windows": os.startfile("print_view.pdf","print")
    else: messagebox.showinfo("Drucken","PDF generiert. Bitte manuell drucken.")

def _make_pdf(fname, title, rows):
    c = canvas.Canvas(fname, pagesize=letter)
    c.setFont("Helvetica-Bold",14); c.drawString(100,760,title)
    c.setFont("Helvetica",9); y = 730
    for row in rows:
        if is_tutor: row = fmt_tutor(row)
        text = " | ".join(str(v) for v in row if v)
        while len(text) > 110:
            c.drawString(50,y,text[:110]); text = "  "+text[110:]; y -= 14
            if y < 50: c.showPage(); y = 750
        c.drawString(50,y,text); y -= 18
        if y < 50: c.showPage(); y = 750
    c.save()

def abmelden():
    global current_user, is_tutor
    root.destroy(); current_user, is_tutor = None, False; login_window()

# ─── MENÚS DINÁMICOS ──────────────────────────────────────────────────────────

def update_eingriff_menu(*_):
    kat  = kategorie_var.get()
    opts = EINGRIFFE.get(kat, [])
    eingriff_menu["menu"].delete(0,"end")
    for op in opts:
        eingriff_menu["menu"].add_command(label=op, command=lambda v=op: eingriff_var.set(v))
    eingriff_var.set(opts[0] if opts else "")
    zugang_frame.grid()     if kat == "Intervention" else zugang_frame.grid_remove()
    update_vschl_menu()

def update_vschl_menu(*_):
    show = kategorie_var.get() == "Intervention" and zugang_var.get() == "Punktion"
    vschl_frame.grid() if show else vschl_frame.grid_remove()

# ─── VENTANA PRINCIPAL ─────────────────────────────────────────────────────────

def main_window():
    global root, entry_datum, eingriff_var, rolle_var, entry_patient_id
    global entry_diagnose, kategorie_var, zugang_var, verschlusssystem_var
    global entry_notizen, tree, entry_suche, suche_var
    global vom_e, bis_e, user_var, vom_frame, zugang_frame, vschl_frame
    global eingriff_menu, dash_container, year_var

    root = tk.Tk()
    root.title("OP Katalog — Chirurgisches Logbuch")
    root.geometry("1280x860")
    root.configure(bg=C["bg"])

    # ── Estilos ttk ──
    sty = ttk.Style(); sty.theme_use("clam")
    sty.configure("TLabelframe",       background=C["panel"], bordercolor=C["border"])
    sty.configure("TLabelframe.Label", background=C["panel"], foreground=C["accent"],
                  font=("Helvetica",9,"bold"))
    sty.configure("TLabel",  background=C["panel"], foreground=C["text"])
    sty.configure("TEntry",  fieldbackground=C["bg"], foreground=C["text"],
                  insertcolor=C["text"])
    sty.configure("TButton", background=C["border"], foreground=C["text"], relief="flat")
    sty.map("TButton", background=[("active",C["accent"])], foreground=[("active",C["bg"])])
    sty.configure("Treeview", background=C["bg"], fieldbackground=C["bg"],
                  foreground=C["text"], rowheight=22)
    sty.configure("Treeview.Heading", background=C["panel"],
                  foreground=C["accent"], relief="flat")
    sty.map("Treeview", background=[("selected", C["accent3"])],
            foreground=[("selected", C["text"])])

    # ═══════════════════════════════════════════════════════════════════════════
    # LAYOUT: columna izquierda (formulario + búsqueda + tabla)
    #          columna derecha  (dashboard permanente)
    # ═══════════════════════════════════════════════════════════════════════════
    paned = tk.PanedWindow(root, orient="horizontal",
                           bg=C["bg"], sashwidth=6,
                           sashrelief="flat", sashpad=2)
    paned.pack(fill="both", expand=True)

    left_col  = tk.Frame(paned, bg=C["bg"])
    right_col = tk.Frame(paned, bg=C["bg"])
    paned.add(left_col,  minsize=480)
    paned.add(right_col, minsize=540)

    # ── COLUMNA IZQUIERDA ─────────────────────────────────────────────────────

    # Formulario (oculto para tutor)
    frm_input = ttk.LabelFrame(left_col, text="Neue Operation")
    if not is_tutor:
        frm_input.pack(fill="x", padx=10, pady=(8,4))

    def lbl(t, r):
        ttk.Label(frm_input, text=t).grid(row=r, column=0, padx=5, pady=2, sticky="e")

    lbl("Datum (TT.MM.JJJJ):", 0)
    entry_datum = ttk.Entry(frm_input); entry_datum.grid(row=0,column=1,padx=5,pady=2)

    lbl("Kategorie:", 1)
    kategorie_var = tk.StringVar(value="Operation")
    ttk.OptionMenu(frm_input, kategorie_var, "Operation",
                   "Operation","Intervention","Prozedur",
                   command=update_eingriff_menu).grid(row=1,column=1,padx=5,pady=2)

    lbl("Eingriff:", 2)
    eingriff_var  = tk.StringVar()
    eingriff_menu = ttk.OptionMenu(frm_input, eingriff_var, "")
    eingriff_menu.grid(row=2, column=1, padx=5, pady=2)

    zugang_frame = ttk.Frame(frm_input)
    zugang_frame.grid(row=3, column=0, columnspan=2)
    ttk.Label(zugang_frame, text="Zugang:").grid(row=0,column=0,padx=5,sticky="e")
    zugang_var = tk.StringVar(value="Punktion")
    ttk.OptionMenu(zugang_frame, zugang_var, "Punktion","Punktion","Offen",
                   command=update_vschl_menu).grid(row=0,column=1,padx=5)

    vschl_frame = ttk.Frame(frm_input)
    vschl_frame.grid(row=4, column=0, columnspan=2)
    ttk.Label(vschl_frame, text="Verschlusssystem:").grid(row=0,column=0,padx=5,sticky="e")
    verschlusssystem_var = tk.StringVar(value="AngioSeal")
    ttk.OptionMenu(vschl_frame, verschlusssystem_var,
                   "AngioSeal","AngioSeal","ProGlide").grid(row=0,column=1,padx=5)

    lbl("Rolle:", 5)
    rolle_var = tk.StringVar(value="Operateur")
    ttk.OptionMenu(frm_input, rolle_var, "Operateur","Operateur","Assistent"
                   ).grid(row=5,column=1,padx=5,pady=2)

    for r, (lbl_t, gname) in enumerate([
        ("Patienten-ID:","entry_patient_id"),
        ("Diagnose:",    "entry_diagnose"),
        ("Notizen:",     "entry_notizen"),
    ], 6):
        lbl(lbl_t, r)
        e = ttk.Entry(frm_input); e.grid(row=r,column=1,padx=5,pady=2)
        globals()[gname] = e

    ttk.Button(frm_input, text="＋ Hinzufügen",
               command=neue_operation).grid(row=9,column=0,columnspan=2,pady=8)
    update_eingriff_menu()

    # Búsqueda
    frm_search = ttk.LabelFrame(left_col, text="Suchen")
    frm_search.pack(fill="x", padx=10, pady=4)

    search_opts = ["Kategorie","Datum"] + (["Datum Range","Benutzer"] if is_tutor else [])
    suche_var   = tk.StringVar(value=search_opts[0])
    ttk.Label(frm_search, text="Nach:").grid(row=0,column=0,padx=5,pady=4,sticky="e")
    ttk.OptionMenu(frm_search, suche_var, search_opts[0],
                   *search_opts).grid(row=0,column=1,padx=5,pady=4)

    entry_suche = ttk.Entry(frm_search); entry_suche.grid(row=0,column=2,padx=5)
    user_var = tk.StringVar()
    if is_tutor:
        cursor.execute("SELECT username FROM users")
        users_cb = ttk.Combobox(frm_search, textvariable=user_var,
                                values=[r[0] for r in cursor.fetchall()])
        vom_frame = ttk.Frame(frm_search)
        ttk.Label(vom_frame, text="vom:").grid(row=0,column=0)
        vom_e = ttk.Entry(vom_frame, width=10); vom_e.grid(row=0,column=1)
        ttk.Label(vom_frame, text="bis:").grid(row=0,column=2)
        bis_e = ttk.Entry(vom_frame, width=10); bis_e.grid(row=0,column=3)
    else:
        vom_e = bis_e = ttk.Entry(frm_search)

    def update_search_widget(*_):
        k = suche_var.get()
        entry_suche.grid_forget()
        if is_tutor: vom_frame.grid_forget(); users_cb.grid_forget()
        if k in ["Kategorie","Datum"]: entry_suche.grid(row=0,column=2,padx=5)
        elif is_tutor and k=="Datum Range": vom_frame.grid(row=0,column=2,padx=5)
        elif is_tutor and k=="Benutzer":   users_cb.grid(row=0,column=2,padx=5)

    suche_var.trace("w", update_search_widget)
    update_search_widget()
    ttk.Button(frm_search, text="Suchen", command=do_search).grid(row=0,column=3,padx=5)

    # Tabla
    frm_tree = ttk.LabelFrame(left_col, text="Einträge")
    frm_tree.pack(fill="both", expand=True, padx=10, pady=4)

    heads  = T_HEADS if is_tutor else U_HEADS
    widths = [90,170,80,90,90,90] if is_tutor else \
             [40,90,170,80,80,120,90,80,90,110]
    tree = ttk.Treeview(frm_tree, columns=heads, show="headings")
    for h, w in zip(heads, widths):
        tree.heading(h, text=h); tree.column(h, width=w)
    sb = ttk.Scrollbar(frm_tree, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=sb.set)
    sb.pack(side="right", fill="y"); tree.pack(fill="both", expand=True)

    # Botones de acción
    frm_btns = tk.Frame(left_col, bg=C["bg"])
    frm_btns.pack(fill="x", padx=10, pady=6)

    btn_cfg = {"bg": C["panel"], "fg": C["text"], "relief": "flat",
               "padx": 10, "pady": 5, "font": ("Helvetica",8), "cursor": "hand2"}
    btns = [
        ("🔄 Aktualisieren", refresh_all),
        ("🗑  Löschen",       delete_entry),
        ("📁 CSV",            export_csv),
        ("📄 PDF",            export_pdf),
        ("🖨  Drucken",        print_view) if is_tutor else None,
        ("🚪 Abmelden",       abmelden),
    ]
    for item in btns:
        if not item: continue
        lbl_t, cmd = item
        b = tk.Button(frm_btns, text=lbl_t, command=cmd, **btn_cfg)
        if lbl_t == "🗑  Löschen" and is_tutor: b.config(state="disabled")
        b.pack(side="left", padx=3)

    # ── COLUMNA DERECHA: dashboard permanente ─────────────────────────────────

    # Barra superior del dashboard con selector de año
    hdr = tk.Frame(right_col, bg=C["panel"],
                   highlightbackground=C["border"], highlightthickness=1)
    hdr.pack(fill="x", padx=8, pady=(8,4))

    title_txt = "Dashboard — Alle Residenten" if is_tutor else f"Dashboard — {current_user}"
    tk.Label(hdr, text=f"📊  {title_txt}", bg=C["panel"], fg=C["text"],
             font=("Helvetica",11,"bold")).pack(side="left", padx=12, pady=8)

    year_var = tk.IntVar(value=datetime.now().year)

    def change_year(d):
        year_var.set(year_var.get() + d)
        build_dashboard(dash_container, None if is_tutor else current_user, year_var.get())

    tk.Button(hdr, text="◀", bg=C["border"], fg=C["text"], relief="flat",
              font=("Helvetica",9), command=lambda: change_year(-1)).pack(side="right", padx=4)
    tk.Label(hdr, textvariable=year_var, bg=C["panel"], fg=C["accent"],
             font=("Helvetica",11,"bold"), width=5).pack(side="right")
    tk.Button(hdr, text="▶", bg=C["border"], fg=C["text"], relief="flat",
              font=("Helvetica",9), command=lambda: change_year(1)).pack(side="right", padx=4)
    tk.Label(hdr, text="Año:", bg=C["panel"], fg=C["muted"],
             font=("Helvetica",8)).pack(side="right", padx=6)

    # Contenedor del dashboard (se reconstruye en cada refresh)
    dash_container = tk.Frame(right_col, bg=C["bg"])
    dash_container.pack(fill="both", expand=True, padx=8, pady=4)

    # Carga inicial
    refresh_all()
    root.mainloop()

# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn, cursor = init_db()
    current_user = None
    is_tutor     = False
    login_window()
    conn.close()
