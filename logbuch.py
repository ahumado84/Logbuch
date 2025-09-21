import streamlit as st
import sqlite3
import csv
from datetime import datetime
import pandas as pd
import io
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    st.error("PDF export requires reportlab. Please install it locally.")

# Crear/Conectar a la base de datos SQLite y actualizar esquema
def iniciar_base_datos():
    conn = sqlite3.connect("chirurgischer_bericht.db", check_same_thread=False)
    cursor = conn.cursor()
    
    # Crear tabla de usuarios si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password TEXT,
            is_tutor INTEGER DEFAULT 0
        )
    ''')
    
    # Verificar si la columna is_tutor existe y agregarla si no
    cursor.execute("PRAGMA table_info(users)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'is_tutor' not in columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_tutor INTEGER DEFAULT 0")
        conn.commit()
    
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

# Mostrar todos los registros (filtrados por usuario o todos para tutor)
def zeige_eintraege():
    if st.session_state.is_tutor:
        cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen ORDER BY datum_sort DESC")
        eintraege = cursor.fetchall()
        data = []
        for eintrag in eintraege:
            rolle_display = eintrag[2] if eintrag[2] == "Operateur" else ""
            data.append([eintrag[0], eintrag[1], rolle_display, eintrag[3], eintrag[4], eintrag[5]])
        if data:
            df = pd.DataFrame(data, columns=["Datum", "Eingriff", "Rolle", "Patient", "Kategorie", "Benutzer"])
            st.dataframe(df, use_container_width=True)
        else:
            st.write("Keine Einträge vorhanden.")
    else:
        cursor.execute("SELECT user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen FROM operationen WHERE username = ? ORDER BY user_id", (st.session_state.current_user,))
        eintraege = cursor.fetchall()
        if eintraege:
            df = pd.DataFrame(eintraege, columns=["ID", "Datum", "Eingriff", "Rolle", "Patient", "Diagnose", "Kategorie", "Zugang", "Verschlusssystem", "Notizen"])
            st.dataframe(df, use_container_width=True)
        else:
            st.write("Keine Einträge vorhanden.")

# Buscar registros
def suche_eintraege(suche_kriterium, wert, vom=None, bis=None):
    if st.session_state.is_tutor:
        if suche_kriterium == "Datum Range":
            if not vom or not bis or not validar_fecha(vom) or not validar_fecha(bis):
                st.error("Ungültige Datumsangaben. Bitte verwenden Sie TT.MM.JJJJ.")
                return
            vom_sort = datetime.strptime(vom, '%d.%m.%Y').strftime('%Y-%m-%d')
            bis_sort = datetime.strptime(bis, '%d.%m.%Y').strftime('%Y-%m-%d')
            cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen WHERE datum_sort BETWEEN ? AND ? ORDER BY datum_sort DESC", (vom_sort, bis_sort))
        elif suche_kriterium == "Benutzer":
            if not wert:
                st.error("Bitte wählen Sie einen Benutzer aus.")
                return
            cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen WHERE username = ? ORDER BY datum_sort DESC", (wert,))
        elif suche_kriterium == "Datum":
            if not wert or not validar_fecha(wert):
                st.error("Ungültiges Datumsformat. Bitte verwenden Sie TT.MM.JJJJ.")
                return
            wert_sort = datetime.strptime(wert, '%d.%m.%Y').strftime('%Y-%m-%d')
            cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen WHERE datum_sort = ? ORDER BY datum_sort DESC", (wert_sort,))
        elif suche_kriterium == "Kategorie":
            cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen WHERE kategorie = ? ORDER BY datum_sort DESC", (wert,))
        eintraege = cursor.fetchall()
        data = []
        for eintrag in eintraege:
            rolle_display = eintrag[2] if eintrag[2] == "Operateur" else ""
            data.append([eintrag[0], eintrag[1], rolle_display, eintrag[3], eintrag[4], eintrag[5]])
        if data:
            df = pd.DataFrame(data, columns=["Datum", "Eingriff", "Rolle", "Patient", "Kategorie", "Benutzer"])
            st.dataframe(df, use_container_width=True)
        else:
            st.write("Keine Einträge gefunden.")
    else:
        if suche_kriterium == "Datum":
            if not wert or not validar_fecha(wert):
                st.error("Ungültiges Datumsformat. Bitte verwenden Sie TT.MM.JJJJ.")
                return
            wert_sort = datetime.strptime(wert, '%d.%m.%Y').strftime('%Y-%m-%d')
            cursor.execute("SELECT user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen FROM operationen WHERE datum_sort = ? AND username = ? ORDER BY user_id", (wert_sort, st.session_state.current_user))
        elif suche_kriterium == "Kategorie":
            cursor.execute("SELECT user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen FROM operationen WHERE kategorie = ? AND username = ? ORDER BY user_id", (wert, st.session_state.current_user))
        eintraege = cursor.fetchall()
        if eintraege:
            df = pd.DataFrame(eintraege, columns=["ID", "Datum", "Eingriff", "Rolle", "Patient", "Diagnose", "Kategorie", "Zugang", "Verschlusssystem", "Notizen"])
            st.dataframe(df, use_container_width=True)
        else:
            st.write("Keine Einträge gefunden.")

# Eliminar Eintrag
def loesche_eintrag():
    cursor.execute("SELECT user_id, datum FROM operationen WHERE username = ? ORDER BY user_id", (st.session_state.current_user,))
    entries = cursor.fetchall()
    if entries:
        entry_options = [f"ID {row[0]} - Datum {row[1]}" for row in entries]
        selected_entry = st.selectbox("Eintrag zum Löschen auswählen", entry_options)
        if selected_entry and st.button("Löschen bestätigen"):
            user_id = int(selected_entry.split()[1])
            cursor.execute("DELETE FROM operationen WHERE user_id = ? AND username = ?", (user_id, st.session_state.current_user))
            conn.commit()
            reorganizar_user_ids(st.session_state.current_user)
            st.success("Eintrag gelöscht.")
            zeige_eintraege()
    else:
        st.write("Keine Einträge vorhanden.")

# Export CSV
def exportieren_csv():
    if st.session_state.is_tutor:
        cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen ORDER BY datum_sort DESC")
        eintraege = cursor.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Datum", "Eingriff", "Rolle", "Patienten-ID", "Kategorie", "Benutzer"])
        for eintrag in eintraege:
            rolle_display = eintrag[2] if eintrag[2] == "Operateur" else ""
            writer.writerow([eintrag[0], eintrag[1], rolle_display, eintrag[3], eintrag[4], eintrag[5]])
        csv_data = output.getvalue()
    else:
        cursor.execute("SELECT user_id, datum, eingriff

st.caption("Copyright Victor Ahumada Jimenez 2025")
conn.close()
