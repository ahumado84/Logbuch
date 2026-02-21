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

# ─── UTILIDADES ───────────────────────────────────────────────────────────────

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def validar_fecha(datum):
    try:
        datetime.strptime(datum, '%d.%m.%Y')
        return True
    except ValueError:
        return False

def date_to_sort(datum):
    return datetime.strptime(datum, '%d.%m.%Y').strftime('%Y-%m-%d')

# ─── BASE DE DATOS ────────────────────────────────────────────────────────────

def iniciar_base_datos():
    conn = sqlite3.connect("chirurgischer_bericht.db")
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS operationen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datum TEXT, datum_sort TEXT, eingriff TEXT, rolle TEXT,
        patient_id TEXT, diagnose TEXT, kategorie TEXT, zugang TEXT,
        verschlusssystem TEXT, notizen TEXT, username TEXT, user_id INTEGER
    )''')

    # Migraciones: agregar columnas faltantes si es base existente
    cur.execute("PRAGMA table_info(operationen)")
    cols = [r[1] for r in cur.fetchall()]
    migrations = {
        'zugang': "ALTER TABLE operationen ADD COLUMN zugang TEXT",
        'verschlusssystem': "ALTER TABLE operationen ADD COLUMN verschlusssystem TEXT",
        'username': "ALTER TABLE operationen ADD COLUMN username TEXT",
        'datum_sort': "ALTER TABLE operationen ADD COLUMN datum_sort TEXT",
    }
    for col, sql in migrations.items():
        if col not in cols:
            cur.execute(sql)

    # Migrar user_id si no existe
    if 'user_id' not in cols:
        cur.execute("ALTER TABLE operationen ADD COLUMN user_id INTEGER")
        cur.execute("SELECT DISTINCT username FROM operationen WHERE username IS NOT NULL")
        for (user,) in cur.fetchall():
            cur.execute("SELECT id FROM operationen WHERE username=? ORDER BY id", (user,))
            for idx, (old_id,) in enumerate(cur.fetchall(), 1):
                cur.execute("UPDATE operationen SET user_id=? WHERE id=? AND username=?", (idx, old_id, user))

    # Rellenar datum_sort vacíos
    cur.execute("SELECT id, datum FROM operationen WHERE datum_sort IS NULL AND datum IS NOT NULL")
    for row_id, datum in cur.fetchall():
        try:
            cur.execute("UPDATE operationen SET datum_sort=? WHERE id=?", (date_to_sort(datum), row_id))
        except ValueError:
            pass

    conn.commit()
    return conn, cur

def reorganizar_user_ids(cursor, conn, username):
    cursor.execute("SELECT user_id FROM operationen WHERE username=? ORDER BY user_id", (username,))
    for new_id, (old_id,) in enumerate(cursor.fetchall(), 1):
        cursor.execute("UPDATE operationen SET user_id=? WHERE user_id=? AND username=?", (new_id, old_id, username))
    conn.commit()

# ─── CONFIGURACIÓN DE PROCEDIMIENTOS ──────────────────────────────────────────

EINGRIFFE = {
    "Operation": [
        "Carotis EEA/TEA", "Aortenaneurysma Rohrprothese", "Aortenaneurysma Bypass",
        "Aortobi- oder monoiliakaler Bypass", "Aortobi- oder monofemoraler Bypass",
        "Iliofemoraler Bypass", "Crossover Bypass", "Femoralis TEA",
        "Fem-pop. P1 Bypass", "Fem-pop. P3 Bypass", "Fem-cruraler Bypass",
        "P1-P3 Bypass", "Wunddebridement - VAC Wechsel"
    ],
    "Intervention": [
        "TEVAR", "FEVAR", "EVAR", "BEVAR", "Organstent", "Beckenstent",
        "Beinstent", "Thrombektomie over the wire"
    ],
    "Prozedur": [
        "ZVK-Anlage", "Drainage Thorax", "Drainage Abdomen",
        "Drainage Wunde Extremitäten", "Punktion/PE"
    ],
}

# ─── LÓGICA DE QUERIES (centralizada) ─────────────────────────────────────────

TUTOR_COLS  = "datum, eingriff, rolle, patient_id, kategorie, username"
USER_COLS   = "user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen"
TUTOR_HEADS = ("Datum", "Eingriff", "Rolle", "Patient", "Kategorie", "Benutzer")
USER_HEADS  = ("ID", "Datum", "Eingriff", "Rolle", "Patient", "Diagnose", "Kategorie", "Zugang", "Verschlusssystem", "Notizen")

def fetch_rows(extra_where="", params=()):
    """Ejecuta la query correcta según el rol del usuario."""
    if is_tutor:
        sql = f"SELECT {TUTOR_COLS} FROM operationen"
        if extra_where:
            sql += f" WHERE {extra_where}"
        cursor.execute(sql, params)
    else:
        base = f"WHERE username=?"
        if extra_where:
            base += f" AND ({extra_where})"
        cursor.execute(
            f"SELECT {USER_COLS} FROM operationen {base} ORDER BY user_id",
            (current_user,) + params
        )
    return cursor.fetchall()

def format_tutor_row(row):
    """Muestra '' para rol Assistent en vista tutor."""
    r = list(row)
    r[2] = r[2] if r[2] == "Operateur" else ""
    return r

# ─── HELPERS UI ───────────────────────────────────────────────────────────────

def set_output(text):
    text_ausgabe.config(state='normal')
    text_ausgabe.delete(1.0, tk.END)
    text_ausgabe.insert(tk.END, text + "\n")
    text_ausgabe.config(state='disabled')

def populate_tree(rows):
    tree.delete(*tree.get_children())
    for row in rows:
        display = format_tutor_row(row) if is_tutor else row
        tree.insert("", tk.END, values=display)
    count = len(rows)
    set_output("Keine Einträge vorhanden." if not count else f"{count} Einträge angezeigt.")

# ─── VENTANA LOGIN ─────────────────────────────────────────────────────────────

def login_window():
    global login_root, current_user, is_tutor
    login_root = tk.Tk()
    login_root.title("OP Katalog - Hubertus")
    login_root.geometry("400x300")
    ttk.Label(login_root, text="Willkommen!", font=("Helvetica", 14)).pack(pady=20)

    def open_dialog(title, action):
        """Ventana genérica de autenticación."""
        win = tk.Toplevel(login_root)
        win.title(title)
        win.geometry("300x220")
        ttk.Label(win, text="Benutzername:").pack(pady=5)
        e_user = ttk.Entry(win); e_user.pack(pady=5)
        ttk.Label(win, text="Passwort:").pack(pady=5)
        e_pass = ttk.Entry(win, show="*"); e_pass.pack(pady=5)
        ttk.Button(win, text=title, command=lambda: action(e_user.get(), e_pass.get(), win)).pack(pady=10)

    def do_register(username, password, win):
        if not username or not password:
            messagebox.showerror("Fehler", "Alle Felder erforderlich."); return
        try:
            cursor.execute("INSERT INTO users (username, password) VALUES (?,?)", (username, hash_password(password)))
            conn.commit()
            messagebox.showinfo("Erfolg", "Benutzer registriert.")
            win.destroy()
        except sqlite3.IntegrityError:
            messagebox.showerror("Fehler", "Benutzername existiert bereits.")

    def do_login(username, password, win):
        global current_user, is_tutor
        if not username or not password:
            messagebox.showerror("Fehler", "Alle Felder erforderlich."); return
        cursor.execute("SELECT id FROM users WHERE username=? AND password=?", (username, hash_password(password)))
        if cursor.fetchone():
            current_user, is_tutor = username, False
            win.destroy(); login_root.destroy(); main_window()
        else:
            messagebox.showerror("Fehler", "Ungültige Anmeldedaten.")

    def do_tutor(username, password, win):
        global current_user, is_tutor
        if password == hash_password("tutor01") or password == "tutor01":  # acepta ambos
            current_user, is_tutor = username or "Tutor", True
            win.destroy(); login_root.destroy(); main_window()
        else:
            messagebox.showerror("Fehler", "Ungültiges Tutor-Passwort.")

    ttk.Button(login_root, text="Anmelden",    command=lambda: open_dialog("Anmelden",    do_login)).pack(pady=5)
    ttk.Button(login_root, text="Registrieren",command=lambda: open_dialog("Registrieren",do_register)).pack(pady=5)
    ttk.Button(login_root, text="Tutor Modus", command=lambda: open_dialog("Tutor Modus", do_tutor)).pack(pady=5)
    login_root.mainloop()

# ─── LÓGICA PRINCIPAL ──────────────────────────────────────────────────────────

def neue_operation():
    datum   = entry_datum.get()
    eingriff= eingriff_var.get()
    rolle   = rolle_var.get()
    pat_id  = entry_patient_id.get()
    diagnose= entry_diagnose.get()
    kat     = kategorie_var.get()
    zugang  = zugang_var.get()   if kat == "Intervention" else ""
    vschl   = verschlusssystem_var.get() if kat == "Intervention" and zugang == "Punktion" else ""
    notizen = entry_notizen.get()

    # Validaciones
    if not validar_fecha(datum):
        messagebox.showerror("Fehler", "Ungültiges Datum. Format: TT.MM.JJJJ."); return
    if not all([datum, eingriff, rolle, pat_id, diagnose, kat]):
        messagebox.showerror("Fehler", "Alle Pflichtfelder müssen ausgefüllt sein."); return
    if kat == "Intervention" and not zugang:
        messagebox.showerror("Fehler", "Zugang muss gewählt sein."); return
    if kat == "Intervention" and zugang == "Punktion" and not vschl:
        messagebox.showerror("Fehler", "Verschlusssystem muss gewählt sein."); return

    cursor.execute("SELECT MAX(user_id) FROM operationen WHERE username=?", (current_user,))
    user_id = (cursor.fetchone()[0] or 0) + 1
    cursor.execute('''INSERT INTO operationen
        (datum, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, username, user_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
        (datum, date_to_sort(datum), eingriff, rolle, pat_id, diagnose, kat, zugang, vschl, notizen, current_user, user_id))
    conn.commit()
    messagebox.showinfo("Erfolg", "Operation erfolgreich registriert.")
    limpiar_campos()
    zeige_eintraege()

def limpiar_campos():
    for e in [entry_datum, entry_patient_id, entry_diagnose, entry_notizen]:
        e.delete(0, tk.END)
    rolle_var.set("Operateur")
    kategorie_var.set("Operation")
    zugang_var.set("Punktion")
    verschlusssystem_var.set("AngioSeal")
    actualizar_eingriff_opciones()

def zeige_eintraege():
    populate_tree(fetch_rows())

def suche_eintraege():
    kriterium = suche_kriterium_var.get()
    rows = []

    if kriterium == "Datum Range" and is_tutor:
        vom, bis = vom_entry.get(), bis_entry.get()
        if not all([vom, bis, validar_fecha(vom), validar_fecha(bis)]):
            messagebox.showerror("Fehler", "Ungültige Datumsangaben."); return
        rows = fetch_rows("datum_sort BETWEEN ? AND ?", (date_to_sort(vom), date_to_sort(bis)))

    elif kriterium == "Benutzer" and is_tutor:
        user = user_var.get()
        if not user:
            messagebox.showerror("Fehler", "Bitte Benutzer auswählen."); return
        rows = fetch_rows("username=?", (user,))

    elif kriterium == "Datum":
        wert = entry_suche.get()
        if not validar_fecha(wert):
            messagebox.showerror("Fehler", "Ungültiges Datum."); return
        rows = fetch_rows("datum_sort=?", (date_to_sort(wert),))

    elif kriterium == "Kategorie":
        rows = fetch_rows("kategorie=?", (entry_suche.get(),))

    populate_tree(rows)

def loesche_eintrag():
    if is_tutor:
        messagebox.showerror("Fehler", "Im Tutor-Modus nicht erlaubt."); return
    sel = tree.selection()
    if not sel:
        messagebox.showerror("Fehler", "Keinen Eintrag ausgewählt."); return
    user_id = tree.item(sel)['values'][0]
    cursor.execute("DELETE FROM operationen WHERE user_id=? AND username=?", (user_id, current_user))
    conn.commit()
    reorganizar_user_ids(cursor, conn, current_user)
    messagebox.showinfo("Erfolg", "Eintrag gelöscht.")
    zeige_eintraege()

def zusammenfassung_kategorien():
    where = "" if is_tutor else "WHERE username=?"
    params = () if is_tutor else (current_user,)
    cursor.execute(f"SELECT kategorie, COUNT(*) FROM operationen {where} GROUP BY kategorie", params)
    lines = ["Zusammenfassung nach Kategorien:"] + [f"  {k}: {n} Eingriffe" for k, n in cursor.fetchall()]
    set_output("\n".join(lines))

def exportieren_csv():
    rows = fetch_rows()
    with open("logbuch.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        headers = list(TUTOR_HEADS) if is_tutor else list(USER_HEADS)
        w.writerow(headers)
        for row in rows:
            w.writerow(format_tutor_row(row) if is_tutor else row)
    messagebox.showinfo("Erfolg", "Exportiert als logbuch.csv")

def exportieren_pdf():
    rows = fetch_rows()
    _write_pdf("logbuch.pdf", "Logbuch - Chirurgischer Bericht", rows)
    messagebox.showinfo("Erfolg", "Exportiert als logbuch.pdf")

def print_table():
    data = [tree.item(i)['values'] for i in tree.get_children()]
    _write_pdf("print_view.pdf", "Logbuch - Chirurgischer Bericht", data)
    if platform.system() == "Windows":
        os.startfile("print_view.pdf", "print")
    else:
        messagebox.showinfo("Drucken", "PDF generiert. Bitte manuell drucken.")

def _write_pdf(filename, title, rows):
    """Helper común para generar PDFs."""
    c = canvas.Canvas(filename, pagesize=letter)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(100, 760, title)
    c.setFont("Helvetica", 9)
    y = 730
    for row in rows:
        if is_tutor:
            row = format_tutor_row(row)
        # Dividir texto largo en múltiples líneas
        text = " | ".join(str(v) for v in row if v)
        # Wrap manual simple
        while len(text) > 110:
            c.drawString(50, y, text[:110])
            text = "  " + text[110:]
            y -= 14
            if y < 50: c.showPage(); y = 750
        c.drawString(50, y, text)
        y -= 18
        if y < 50:
            c.showPage()
            y = 750
    c.save()

def abmelden():
    global current_user, is_tutor
    root.destroy()
    current_user, is_tutor = None, False
    login_window()

# ─── VENTANA PRINCIPAL ─────────────────────────────────────────────────────────

def actualizar_eingriff_opciones(*_):
    kat = kategorie_var.get()
    opciones = EINGRIFFE.get(kat, [])
    eingriff_menu['menu'].delete(0, 'end')
    for op in opciones:
        eingriff_menu['menu'].add_command(label=op, command=lambda v=op: eingriff_var.set(v))
    eingriff_var.set(opciones[0] if opciones else "")
    # Mostrar/ocultar frames condicionales
    (zugang_frame.grid() if kat == "Intervention" else zugang_frame.grid_remove())
    actualizar_verschlusssystem_opciones()

def actualizar_verschlusssystem_opciones(*_):
    (verschlusssystem_frame.grid() if kategorie_var.get() == "Intervention" and zugang_var.get() == "Punktion"
     else verschlusssystem_frame.grid_remove())

def main_window():
    global root, entry_datum, eingriff_var, rolle_var, entry_patient_id, entry_diagnose
    global kategorie_var, zugang_var, verschlusssystem_var, entry_notizen
    global tree, text_ausgabe, entry_suche, suche_kriterium_var
    global vom_entry, bis_entry, user_var, vom_frame, zugang_frame, verschlusssystem_frame, eingriff_menu

    root = tk.Tk()
    root.title("System für Chirurgische Berichte und Logbuch")
    root.geometry("1000x800")

    # ── Frame de entrada (oculto para tutor) ──
    frame_entrada = ttk.LabelFrame(root, text="Neue Operation hinzufügen")
    if not is_tutor:
        frame_entrada.pack(padx=10, pady=5, fill="x")

    def lf(text, row):
        ttk.Label(frame_entrada, text=text).grid(row=row, column=0, padx=5, pady=2, sticky="e")

    lf("Datum (TT.MM.JJJJ):", 0);  entry_datum = ttk.Entry(frame_entrada); entry_datum.grid(row=0, column=1, padx=5, pady=2)
    lf("Kategorie:", 1)
    kategorie_var = tk.StringVar(value="Operation")
    ttk.OptionMenu(frame_entrada, kategorie_var, "Operation", "Operation", "Intervention", "Prozedur",
                   command=actualizar_eingriff_opciones).grid(row=1, column=1, padx=5, pady=2)

    lf("Eingriff:", 2)
    eingriff_var = tk.StringVar()
    eingriff_menu = ttk.OptionMenu(frame_entrada, eingriff_var, "")
    eingriff_menu.grid(row=2, column=1, padx=5, pady=2)

    zugang_frame = ttk.Frame(frame_entrada)
    zugang_frame.grid(row=3, column=0, columnspan=2)
    ttk.Label(zugang_frame, text="Zugang:").grid(row=0, column=0, padx=5, sticky="e")
    zugang_var = tk.StringVar(value="Punktion")
    ttk.OptionMenu(zugang_frame, zugang_var, "Punktion", "Punktion", "Offen",
                   command=actualizar_verschlusssystem_opciones).grid(row=0, column=1, padx=5)

    verschlusssystem_frame = ttk.Frame(frame_entrada)
    verschlusssystem_frame.grid(row=4, column=0, columnspan=2)
    ttk.Label(verschlusssystem_frame, text="Verschlusssystem:").grid(row=0, column=0, padx=5, sticky="e")
    verschlusssystem_var = tk.StringVar(value="AngioSeal")
    ttk.OptionMenu(verschlusssystem_frame, verschlusssystem_var, "AngioSeal", "AngioSeal", "ProGlide").grid(row=0, column=1, padx=5)

    for row_n, (label, var, opts) in enumerate([
        ("Rolle:", "rolle_var", ["Operateur", "Assistent"]),
    ], 5):
        lf(label, row_n)
        globals()[var] = tk.StringVar(value=opts[0])
        ttk.OptionMenu(frame_entrada, globals()[var], opts[0], *opts).grid(row=row_n, column=1, padx=5, pady=2)

    rolle_var = tk.StringVar(value="Operateur")
    ttk.OptionMenu(frame_entrada, rolle_var, "Operateur", "Operateur", "Assistent").grid(row=5, column=1, padx=5, pady=2)
    lf("Rolle:", 5)

    for row_n, (label, name) in enumerate([("Patienten-ID:", "entry_patient_id"), ("Diagnose:", "entry_diagnose"), ("Notizen:", "entry_notizen")], 6):
        lf(label, row_n)
        e = ttk.Entry(frame_entrada); e.grid(row=row_n, column=1, padx=5, pady=2)
        globals()[name] = e

    ttk.Button(frame_entrada, text="Hinzufügen", command=neue_operation).grid(row=9, column=0, columnspan=2, pady=10)
    actualizar_eingriff_opciones()

    # ── Frame búsqueda ──
    frame_suche = ttk.LabelFrame(root, text="Einträge suchen")
    frame_suche.pack(padx=10, pady=5, fill="x")

    search_options = ["Kategorie", "Datum"] + (["Datum Range", "Benutzer"] if is_tutor else [])
    suche_kriterium_var = tk.StringVar(value=search_options[0])
    ttk.Label(frame_suche, text="Suchen nach:").grid(row=0, column=0, padx=5, pady=2, sticky="e")
    ttk.OptionMenu(frame_suche, suche_kriterium_var, search_options[0], *search_options).grid(row=0, column=1, padx=5, pady=2)

    entry_suche = ttk.Entry(frame_suche); entry_suche.grid(row=0, column=2, padx=5, pady=2)

    # Widgets extra para tutor
    user_var = tk.StringVar()
    if is_tutor:
        cursor.execute("SELECT username FROM users")
        users = [r[0] for r in cursor.fetchall()]
        user_combobox = ttk.Combobox(frame_suche, textvariable=user_var, values=users)

        vom_frame = ttk.Frame(frame_suche)
        ttk.Label(vom_frame, text="vom:").grid(row=0, column=0)
        vom_entry = ttk.Entry(vom_frame); vom_entry.grid(row=0, column=1)
        ttk.Label(vom_frame, text="bis:").grid(row=0, column=2)
        bis_entry = ttk.Entry(vom_frame); bis_entry.grid(row=0, column=3)
    else:
        vom_entry = bis_entry = ttk.Entry(frame_suche)  # dummies

    def update_search_input(*_):
        k = suche_kriterium_var.get()
        entry_suche.grid_forget()
        if is_tutor:
            vom_frame.grid_forget()
            user_combobox.grid_forget()
        if k in ["Kategorie", "Datum"]:
            entry_suche.grid(row=0, column=2, padx=5, pady=2)
        elif is_tutor and k == "Datum Range":
            vom_frame.grid(row=0, column=2, padx=5, pady=2)
        elif is_tutor and k == "Benutzer":
            user_combobox.grid(row=0, column=2, padx=5, pady=2)

    suche_kriterium_var.trace("w", update_search_input)
    update_search_input()
    ttk.Button(frame_suche, text="Suchen", command=suche_eintraege).grid(row=0, column=3, padx=5, pady=2)

    # ── Treeview ──
    frame_tabla = ttk.LabelFrame(root, text="Einträge")
    frame_tabla.pack(padx=10, pady=5, fill="both", expand=True)

    heads = TUTOR_HEADS if is_tutor else USER_HEADS
    widths = [100, 200, 100, 100, 100, 100] if is_tutor else [50, 100, 200, 100, 100, 150, 100, 100, 100, 150]
    tree = ttk.Treeview(frame_tabla, columns=heads, show="headings")
    for h, w in zip(heads, widths):
        tree.heading(h, text=h); tree.column(h, width=w)
    tree.pack(fill="both", expand=True)

    # ── Botones ──
    frame_botones = ttk.Frame(root)
    frame_botones.pack(padx=10, pady=5, fill="x")

    buttons = [
        ("Alle Einträge anzeigen", zeige_eintraege),
        ("Eintrag löschen", loesche_eintrag),
        ("CSV exportieren", exportieren_csv),
        ("PDF exportieren", exportieren_pdf),
        ("Zusammenfassung", zusammenfassung_kategorien),
        ("Drucken" if is_tutor else None, print_table if is_tutor else None),
        ("Abmelden", abmelden),
    ]
    for label, cmd in buttons:
        if label:
            btn = ttk.Button(frame_botones, text=label, command=cmd)
            btn.pack(side="left", padx=5)
            if label == "Eintrag löschen" and is_tutor:
                btn.config(state="disabled")

    # ── Salida de texto ──
    text_ausgabe = scrolledtext.ScrolledText(root, height=4, state='disabled')
    text_ausgabe.pack(padx=10, pady=5, fill="x")

    zeige_eintraege()
    root.mainloop()

# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn, cursor = iniciar_base_datos()
    current_user = None
    is_tutor = False
    login_window()
    conn.close()
