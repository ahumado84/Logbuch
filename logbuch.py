import streamlit as st
import sqlite3
import csv
from datetime import datetime
import pandas as pd
import io

# Crear/Conectar a la base de datos SQLite y actualizar esquema
def iniciar_base_datos():
    conn = sqlite3.connect("chirurgischer_bericht.db", check_same_thread=False)
    cursor = conn.cursor()
    
    # Crear tabla de usuarios si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password TEXT
        )
    ''')
    
    # Crear tabla de operationen si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operationen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datum TEXT,
            datum_sort TEXT,
            eingriff TEXT,
            rolle TEXT,
            patient_id TEXT,
            diagnose TEXT,
            kategorie TEXT,
            zugang TEXT,
            verschlusssystem TEXT,
            notizen TEXT,
            username TEXT,
            user_id INTEGER
        )
    ''')
    
    # Verificar y agregar columnas si no existen
    cursor.execute("PRAGMA table_info(operationen)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'zugang' not in columns:
        cursor.execute("ALTER TABLE operationen ADD COLUMN zugang TEXT")
    if 'verschlusssystem' not in columns:
        cursor.execute("ALTER TABLE operationen ADD COLUMN verschlusssystem TEXT")
    if 'username' not in columns:
        cursor.execute("ALTER TABLE operationen ADD COLUMN username TEXT")
    if 'user_id' not in columns:
        cursor.execute("ALTER TABLE operationen ADD COLUMN user_id INTEGER")
        # Asignar user_id secuencial por usuario para registros existentes
        cursor.execute("SELECT DISTINCT username FROM operationen WHERE username IS NOT NULL")
        for row in cursor.fetchall():
            user = row[0]
            cursor.execute("SELECT id FROM operationen WHERE username = ? ORDER BY id", (user,))
            ids = [r[0] for r in cursor.fetchall()]
            for idx, old_id in enumerate(ids, 1):
                cursor.execute("UPDATE operationen SET user_id = ? WHERE id = ? AND username = ?", (idx, old_id, user))
        conn.commit()
    if 'datum_sort' not in columns:
        cursor.execute("ALTER TABLE operationen ADD COLUMN datum_sort TEXT")
        # Llenar datum_sort para registros existentes
        cursor.execute("SELECT id, datum FROM operationen")
        for row in cursor.fetchall():
            id, datum = row
            if datum:
                try:
                    datum_sort = datetime.strptime(datum, '%d.%m.%Y').strftime('%Y-%m-%d')
                    cursor.execute("UPDATE operationen SET datum_sort = ? WHERE id = ?", (datum_sort, id))
                except ValueError:
                    pass  # Fecha inválida, saltar
        conn.commit()
    
    conn.commit()
    return conn, cursor

conn, cursor = iniciar_base_datos()

# Validar formato de fecha DD.MM.YYYY
def validar_fecha(datum):
    try:
        datetime.strptime(datum, '%d.%m.%Y')
        return True
    except ValueError:
        return False

# Reorganizar user_id después de eliminar un registro (por usuario)
def reorganizar_user_ids(current_user):
    cursor.execute("SELECT user_id FROM operationen WHERE username = ? ORDER BY user_id", (current_user,))
    ids = [row[0] for row in cursor.fetchall()]
    for new_id, old_id in enumerate(ids, 1):
        cursor.execute("UPDATE operationen SET user_id = ? WHERE user_id = ? AND username = ?", (new_id, old_id, current_user))
    conn.commit()

# App
st.title("OP Katalog - Hubertus")

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.current_user = None
    st.session_state.is_tutor = False

if not st.session_state.logged_in:
    tab1, tab2, tab3 = st.tabs(["Anmelden", "Registrieren", "Tutor Modus"])

    with tab1:
        username = st.text_input("Benutzername", key="login_username")
        password = st.text_input("Passwort", type="password", key="login_password")
        if st.button("Anmelden"):
            if username and password:
                cursor.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, password))
                user = cursor.fetchone()
                if user:
                    st.session_state.logged_in = True
                    st.session_state.current_user = username
                    st.session_state.is_tutor = False
                    st.success("Anmeldung erfolgreich.")
                    st.rerun()
                else:
                    st.error("Ungültiger Benutzername oder Passwort.")
            else:
                st.error("Benutzername und Passwort sind erforderlich.")

    with tab2:
        reg_username = st.text_input("Benutzername", key="reg_username")
        reg_password = st.text_input("Passwort", type="password", key="reg_password")
        if st.button("Registrieren"):
            if reg_username and reg_password:
                try:
                    cursor.execute("INSERT INTO users (username, password) VALUES (?, ?)", (reg_username, reg_password))
                    conn.commit()
                    st.success("Benutzer erfolgreich registriert.")
                except sqlite3.IntegrityError:
                    st.error("Benutzername existiert bereits.")
            else:
                st.error("Benutzername und Passwort sind erforderlich.")

    with tab3:
        tutor_username = st.text_input("Benutzername", key="tutor_username")
        tutor_password = st.text_input("Passwort", type="password", key="tutor_password")
        if st.button("Anmelden als Tutor"):
            if tutor_password == "tutor01":
                st.session_state.logged_in = True
                st.session_state.current_user = tutor_username
                st.session_state.is_tutor = True
                st.success("Tutor-Modus aktiviert.")
                st.rerun()
            else:
                st.error("Ungültiges Passwort für Tutor-Modus.")
else:
    if st.button("Abmelden"):
        st.session_state.logged_in = False
        st.session_state.current_user = None
        st.session_state.is_tutor = False
        st.rerun()

    if not st.session_state.is_tutor:
        st.subheader("Neue Operation hinzufügen")
        with st.form(key="add_form"):
            datum = st.text_input("Datum (TT.MM.JJJJ)")
            kategorie = st.selectbox("Kategorie", ["Operation", "Intervention", "Prozedur"])
            eingriff_options = {
                "Operation": [
                    "Carotis EEA/TEA", "Aortenaneurysma Rohrprothese", "Aortenaneurysma Bypass",
                    "Aortobi- oder monoiliakaler Bypass", "Aortobi- oder monofemoraler Bypass",
                    "Iliofemoraler Bypass", "Crossover Bypass", "Femoralis TEA", "Fem-pop. P1 Bypass",
                    "Fem-pop. P3 Bypass", "Fem-cruraler Bypass", "P1-P3 Bypass", "Wunddebridement - VAC Wechsel"
                ],
                "Intervention": [
                    "TEVAR", "FEVAR", "EVAR", "BEVAR", "Organstent", "Beckenstent", "Beinstent",
                    "Thrombektomie over the wire"
                ],
                "Prozedur": [
                    "ZVK-Anlage", "Drainage Thorax", "Drainage Abdomen", "Drainage Wunde Extremitäten",
                    "Punktion/PE"
                ]
            }
            eingriff = st.selectbox("Eingriff", eingriff_options[kategorie])
            if kategorie == "Intervention":
                zugang = st.selectbox("Zugang", ["Punktion", "Offen"])
                if zugang == "Punktion":
                    verschlusssystem = st.selectbox("Verschlusssystem", ["AngioSeal", "ProGlide"])
                else:
                    verschlusssystem = ""
            else:
                zugang = ""
                verschlusssystem = ""
            rolle = st.selectbox("Rolle", ["Operateur", "Assistent"])
            patient_id = st.text_input("Patienten-ID")
            diagnose = st.text_input("Diagnose")
            notizen = st.text_input("Notizen")
            submitted = st.form_submit_button("Hinzufügen")
            if submitted:
                if validar_fecha(datum):
                    datum_sort = datetime.strptime(datum, '%d.%m.%Y').strftime('%Y-%m-%d')
                    # Calcular user_id
                    cursor.execute("SELECT MAX(user_id) FROM operationen WHERE username = ?", (st.session_state.current_user,))
                    max_id = cursor.fetchone()[0]
                    user_id = (max_id or 0) + 1
                    cursor.execute('''
                        INSERT INTO operationen (datum, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, username, user_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (datum, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, st.session_state.current_user, user_id))
                    conn.commit()
                    st.success("Operation erfolgreich registriert.")
                else:
                    st.error("Ungültiges Datumsformat. Bitte verwenden Sie TT.MM.JJJJ.")

    # Búsqueda
    st.subheader("Einträge suchen")
    if st.session_state.is_tutor:
        suche_kriterium = st.selectbox("Suchen nach", ["Kategorie", "Datum", "Datum Range", "Benutzer"])
        if suche_kriterium in ["Kategorie", "Datum"]:
            wert = st.text_input("Wert", key="suche_wert")
        elif suche_kriterium == "Datum Range":
            vom = st.text_input("vom (TT.MM.JJJJ)", key="vom")
            bis = st.text_input("bis (TT.MM.JJJJ)", key="bis")
            wert = None
        elif suche_kriterium == "Benutzer":
            cursor.execute("SELECT username FROM users")
            users = [row[0] for row in cursor.fetchall()]
            wert = st.selectbox("Benutzer", users)
        else:
            wert = None
    else:
        suche_kriterium = st.selectbox("Suchen nach", ["Kategorie", "Datum"])
        wert = st.text_input("Wert", key="suche_wert_user")
    if st.button("Suchen"):
        suche_eintraege(suche_kriterium, wert, vom if 'vom' in locals() else None, bis if 'bis' in locals() else None)
    if st.button("Alle Einträge anzeigen"):
        zeige_eintraege()

    # Table
    zeige_eintraege()  # Initial

    # Botones
    if not st.session_state.is_tutor:
        if st.button("Eintrag löschen"):
            st.write("Select a row to delete (implementation needed, use session state for selection)")
    if st.button("CSV exportieren"):
        exportieren_csv()
    if st.button("PDF exportieren"):
        exportieren_pdf()
    if not st.session_state.is_tutor:
        if st.button("Zusammenfassung Kategorien"):
            zusammenfassung_kategorien()
    else:
        if st.button("Drucken"):
            print_table()

conn.close()
