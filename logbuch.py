import streamlit as st
import sqlite3
import csv
from datetime import datetime
import pandas as pd
import io
import zipfile
import os
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
            is_tutor INTEGER DEFAULT 0,
            is_master INTEGER DEFAULT 0
        )
    ''')
    
    # Verificar si la columna is_tutor y is_master existen y agregarlas si no
    cursor.execute("PRAGMA table_info(users)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'is_tutor' not in columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_tutor INTEGER DEFAULT 0")
    if 'is_master' not in columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_master INTEGER DEFAULT 0")
    
    # Inicializar usuario maestro si no existe
    cursor.execute("SELECT username, password, is_master FROM users WHERE username = ?", ("ahuvic",))
    user = cursor.fetchone()
    if not user:
        try:
            cursor.execute("INSERT INTO users (username, password, is_master, is_tutor) VALUES (?, ?, ?, ?)", ("ahuvic", "rJimenez.1", 1, 0))
            conn.commit()
            st.write("Master user 'ahuvic' created successfully.")
        except sqlite3.Error as e:
            st.error(f"Error creating master user: {e}")
    else:
        if user[1] != "rJimenez.1" or user[2] != 1:
            cursor.execute("UPDATE users SET password = ?, is_master = ? WHERE username = ?", ("rJimenez.1", 1, "ahuvic"))
            conn.commit()
            st.write("Master user 'ahuvic' updated successfully.")
    
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
            user_id INTEGER,
            skills_acquired INTEGER DEFAULT 0
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
        cursor.execute("SELECT id, datum FROM operationen")
        for row in cursor.fetchall():
            id, datum = row
            if datum:
                try:
                    datum_sort = datetime.strptime(datum, '%d.%m.%Y').strftime('%Y-%m-%d')
                    cursor.execute("UPDATE operationen SET datum_sort = ? WHERE id = ?", (datum_sort, id))
                except ValueError:
                    pass
        conn.commit()
    if 'skills_acquired' not in columns:
        cursor.execute("ALTER TABLE operationen ADD COLUMN skills_acquired INTEGER DEFAULT 0")
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

# Mostrar todos los registros (filtrados por usuario o todos para tutor/master)
def zeige_eintraege():
    if st.session_state.is_master:
        cursor.execute("SELECT id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, username, user_id FROM operationen ORDER BY datum_sort DESC")
        eintraege = cursor.fetchall()
        if eintraege:
            df = pd.DataFrame(eintraege, columns=["DB ID", "Datum", "Eingriff", "Rolle", "Patient", "Diagnose", "Kategorie", "Zugang", "Verschlusssystem", "Notizen", "Benutzer", "User ID"])
            st.dataframe(df, use_container_width=True)
        else:
            st.write("Keine Einträge vorhanden.")
    elif st.session_state.is_tutor:
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

# Mostrar Logbuch (para usuarios regulares) mit verbessertem Format
def zeige_logbuch():
    st.subheader("Logbuch zum Facharzt für Gefäßchirurgie")
    st.write(f"Name, Vorname: {st.session_state.current_user} | Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} CEST")
    st.write("WbO der ÄKB 2004, 1. bis 8. Nachtrag")

    # Gefäßchirurgie-spezifische Kategorien und Richtzahlen (Pages 12-15)
    gefaesschirurgie_procedures = {
        "intraoperative angiographische Untersuchungen": 50,
        "Doppler-/Duplex-Untersuchungen (Extremitäten)": 300,
        "Doppler-/Duplex-Untersuchungen (abdominell/retroperitoneal)": 100,
        "Doppler-/Duplex-Untersuchungen (extrakraniell)": 100,
        "hämodynamische Untersuchungen an Venen": 50,
        "rekonstruktive Operationen (supraaortale Arterien)": 25,
        "rekonstruktive Operationen (aortale/iliakale/viszerale/thorakale)": 50,
        "rekonstruktive Operationen (femoro-popliteal/brachial/cruro-pedal)": 50,
        "endovaskuläre Eingriffe": 25,
        "Anlage von Dialyse-Shunts/Port-Implantation": 25,
        "Operationen am Venensystem": 50,
        "Grenzzonenamputationen/Ulkusversorgungen": 25
    }

    logbuch_data = []
    current_year = datetime.now().year
    years = range(current_year - 5, current_year + 1)  # Last 5 years + current year

    for eingriff, richtzahl in gefaesschirurgie_procedures.items():
        annual_counts = {}
        total_count = 0
        dates_skills = {}
        
        # Aggregate counts by year
        for year in years:
            cursor.execute("""
                SELECT COUNT(*), GROUP_CONCAT(datum || ' (' || (CASE WHEN skills_acquired = 1 THEN 'Ja' ELSE 'Nein' END) || ')')
                FROM operationen 
                WHERE username = ? AND eingriff = ? AND strftime('%Y', datum_sort) = ?
            """, (st.session_state.current_user, eingriff, str(year)))
            count_result = cursor.fetchone()
            count = count_result[0] if count_result[0] else 0
            annual_counts[year] = count
            dates_skills[year] = count_result[1] if count_result[1] else ""
            total_count += count

        # Determine progress color
        progress = (total_count / richtzahl) * 100
        color = "green" if progress >= 100 else "yellow" if progress >= 80 else "red"

        # Add row to logbuch_data
        row = [eingriff, richtzahl]
        for year in years:
            row.append(f"{annual_counts[year]} ({dates_skills[year]})" if annual_counts[year] > 0 else "-")
        row.extend([total_count, f"{progress:.1f}%", color, "Unterschrift/Stempel erforderlich"])
        logbuch_data.append(row)

    df = pd.DataFrame(logbuch_data, columns=["Eingriff", "Richtzahl"] + [f"{year}" for year in years] + ["Gesamtanzahl", "Fortschritt", "Status", "Unterschrift/Stempel"])
    st.dataframe(df.style.apply(lambda x: ['background-color: {}'.format(x["Status"]) if i == df.columns.get_loc("Fortschritt") else '' for i in range(len(x))], axis=1), use_container_width=True)

    # Manual signature input
    signature = st.text_area("Fügen Sie die Unterschrift/Stempel des Befugten ein (offline zu bestätigen):")
    if signature:
        st.write("Bitte drucken Sie das Logbuch aus und lassen Sie die Unterschrift vom Befugten bestätigen.")

    # Export options
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Eingriff", "Richtzahl"] + [f"{year}" for year in years] + ["Gesamtanzahl", "Fortschritt", "Unterschrift/Stempel"])
    for row in logbuch_data:
        writer.writerow(row[:-2])  # Exclude Status and color from CSV
    csv_data = output.getvalue()
    st.download_button("Logbuch als CSV herunterladen", csv_data, f"logbuch_{st.session_state.current_user}.csv", "text/csv")
    
    if PDF_AVAILABLE:
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        c.setFont("Helvetica", 12)
        c.drawString(100, 750, f"Logbuch - {st.session_state.current_user}")
        c.drawString(100, 730, f"Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} CEST")
        c.drawString(100, 710, "WbO der ÄKB 2004, 1. bis 8. Nachtrag")
        y = 690
        for row in logbuch_data:
            text = f"Eingriff: {row[0]} | Richtzahl: {row[1]} | "
            for i, year in enumerate(years):
                text += f"{year}: {row[2+i]} | "
            text += f"Gesamt: {row[-4]} | Fortschritt: {row[-3]} | Unterschrift: {row[-1]}"
            c.drawString(50, y, text)
            y -= 20
            if y < 50:
                c.showPage()
                y = 750
        if signature:
            c.drawString(50, y, f"Unterschrift/Stempel: {signature}")
            y -= 20
        c.save()
        buffer.seek(0)
        st.download_button("Logbuch als PDF herunterladen", buffer, f"logbuch_{st.session_state.current_user}.pdf", "application/pdf")

# Buscar registros
def suche_eintraege(suche_kriterium, wert, vom=None, bis=None):
    if st.session_state.is_master or st.session_state.is_tutor:
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

# Eliminar Eintrag (para usuarios regulares)
def loesche_eintrag():
    cursor.execute("SELECT user_id, datum FROM operationen WHERE username = ? ORDER BY user_id", (st.session_state.current_user,))
    entries = cursor.fetchall()
    if entries:
        entry_options = [f"ID {row[0]} - Datum {row[1]}" for row in entries]
        selected_entry = st.selectbox("Eintrag zum Löschen auswählen", entry_options, key="delete_entry")
        if selected_entry and st.button("Löschen bestätigen"):
            user_id = int(selected_entry.split()[1])
            cursor.execute("DELETE FROM operationen WHERE user_id = ? AND username = ?", (user_id, st.session_state.current_user))
            conn.commit()
            reorganizar_user_ids(st.session_state.current_user)
            st.success("Eintrag gelöscht.")
            zeige_eintraege()
    else:
        st.write("Keine Einträge vorhanden.")

# Eliminar Eintrag (para master)
def master_loesche_eintrag():
    cursor.execute("SELECT id, datum, username FROM operationen ORDER BY id")
    entries = cursor.fetchall()
    if entries:
        entry_options = [f"DB ID {row[0]} - Datum {row[1]} - Benutzer {row[2]}" for row in entries]
        selected_entry = st.selectbox("Eintrag zum Löschen auswählen", entry_options, key="master_delete_entry")
        if selected_entry and st.button("Eintrag löschen bestätigen"):
            db_id = int(selected_entry.split()[2])
            cursor.execute("SELECT username FROM operationen WHERE id = ?", (db_id,))
            username = cursor.fetchone()[0]
            cursor.execute("DELETE FROM operationen WHERE id = ?", (db_id,))
            conn.commit()
            reorganizar_user_ids(username)
            st.success("Eintrag gelöscht.")
            zeige_eintraege()
    else:
        st.write("Keine Einträge vorhanden.")

# Editar Eintrag (para master)
def master_edit_eintrag():
    cursor.execute("SELECT id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, username FROM operationen ORDER BY id")
    entries = cursor.fetchall()
    if entries:
        entry_options = [f"DB ID {row[0]} - Datum {row[1]} - Benutzer {row[10]}" for row in entries]
        selected_entry = st.selectbox("Eintrag zum Bearbeiten auswählen", entry_options, key="master_edit_entry")
        if selected_entry:
            db_id = int(selected_entry.split()[2])
            cursor.execute("SELECT * FROM operationen WHERE id = ?", (db_id,))
            entry = cursor.fetchone()
            with st.form(key="edit_form"):
                datum = st.date_input("Datum", value=datetime.strptime(entry[1], '%d.%m.%Y') if entry[1] else None, format="DD.MM.YYYY")
                kategorie = st.selectbox("Kategorie", ["Operation", "Intervention", "Prozedur"], index=["Operation", "Intervention", "Prozedur"].index(entry[7]) if entry[7] in ["Operation", "Intervention", "Prozedur"] else 0)
                eingriff_options = {
                    "Operation": [
                        "Carotis EEA/TEA", "Aortenaneurysma Rohrprothese", "Aortenaneurysma Bypass",
                        "Aortobi- oder monoiliakaler Bypass", "Aortobi- oder monofemoraler Bypass",
                        "Iliofemoraler Bypass", "Crossover Bypass", "Femoralis TEA", "Fem-pop. P1 Bypass",
                        "Fem-pop. P3 Bypass", "Fem-cruraler Bypass", "P1-P3 Bypass", "Wunddebridement - VAC Wechsel",
                        "rekonstruktive Operationen (supraaortale Arterien)", "rekonstruktive Operationen (aortale/iliakale/viszerale/thorakale)",
                        "rekonstruktive Operationen (femoro-popliteal/brachial/cruro-pedal)", "Operationen am Venensystem",
                        "Grenzzonenamputationen/Ulkusversorgungen"
                    ],
                    "Intervention": [
                        "TEVAR", "FEVAR", "EVAR", "BEVAR", "Organstent", "Beckenstent", "Beinstent",
                        "Thrombektomie over the wire", "endovaskuläre Eingriffe", "Anlage von Dialyse-Shunts/Port-Implantation"
                    ],
                    "Prozedur": [
                        "ZVK-Anlage", "Drainage Thorax", "Drainage Abdomen", "Drainage Wunde Extremitäten",
                        "Punktion/PE", "intraoperative angiographische Untersuchungen", "hämodynamische Untersuchungen an Venen",
                        "Doppler-/Duplex-Untersuchungen (Extremitäten)", "Doppler-/Duplex-Untersuchungen (abdominell/retroperitoneal)",
                        "Doppler-/Duplex-Untersuchungen (extrakraniell)"
                    ]
                }
                eingriff = st.selectbox("Eingriff", eingriff_options[kategorie], index=eingriff_options[kategorie].index(entry[3]) if entry[3] in eingriff_options[kategorie] else 0)
                if kategorie == "Intervention":
                    zugang = st.selectbox("Zugang", ["Punktion", "Offen"], index=["Punktion", "Offen"].index(entry[8]) if entry[8] in ["Punktion", "Offen"] else 0)
                    if zugang == "Punktion":
                        verschlusssystem = st.selectbox("Verschlusssystem", ["AngioSeal", "ProGlide"], index=["AngioSeal", "ProGlide"].index(entry[9]) if entry[9] in ["AngioSeal", "ProGlide"] else 0)
                    else:
                        verschlusssystem = ""
                else:
                    zugang = ""
                    verschlusssystem = ""
                rolle = st.selectbox("Rolle", ["Operateur", "Assistent"], index=["Operateur", "Assistent"].index(entry[4]) if entry[4] in ["Operateur", "Assistent"] else 0)
                patient_id = st.text_input("Patienten-ID", value=entry[5])
                diagnose = st.text_input("Diagnose", value=entry[6])
                notizen = st.text_input("Notizen", value=entry[10] if entry[10] else "")
                skills_acquired = st.checkbox("Kenntnisse, Erfahrungen und Fertigkeiten erworben", value=bool(entry[12]))
                if st.form_submit_button("Änderungen speichern"):
                    if not datum or not eingriff or not rolle or not patient_id or not diagnose or not kategorie:
                        st.error("Alle Pflichtfelder müssen ausgefüllt sein.")
                    elif kategorie == "Intervention" and not zugang:
                        st.error("Zugang muss für Intervention ausgewählt sein.")
                    elif kategorie == "Intervention" and zugang == "Punktion" and not verschlusssystem:
                        st.error("Verschlusssystem muss für Punktion ausgewählt sein.")
                    else:
                        datum_sort = datum.strftime('%Y-%m-%d')
                        datum_str = datum.strftime('%d.%m.%Y')
                        try:
                            cursor.execute('''
                                UPDATE operationen SET datum = ?, datum_sort = ?, eingriff = ?, rolle = ?, patient_id = ?, diagnose = ?, kategorie = ?, zugang = ?, verschlusssystem = ?, notizen = ?, skills_acquired = ?
                                WHERE id = ?
                            ''', (datum_str, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, 1 if skills_acquired else 0, db_id))
                            conn.commit()
                            st.success("Eintrag erfolgreich bearbeitet.")
                            zeige_eintraege()
                        except sqlite3.Error as e:
                            st.error(f"Fehler beim Bearbeiten: {e}")
    else:
        st.write("Keine Einträge vorhanden.")

# Eliminar usuario (para master)
def master_loesche_benutzer():
    cursor.execute("SELECT username FROM users WHERE username != ? AND is_master = 0", (st.session_state.current_user,))
    users = [row[0] for row in cursor.fetchall()]
    if users:
        selected_user = st.selectbox("Benutzer zum Löschen auswählen", users, key="master_delete_user")
        if selected_user and st.button("Benutzer löschen bestätigen"):
            cursor.execute("DELETE FROM operationen WHERE username = ?", (selected_user,))
            cursor.execute("DELETE FROM users WHERE username = ?", (selected_user,))
            conn.commit()
            st.success(f"Benutzer {selected_user} und alle zugehörigen Einträge gelöscht.")
    else:
        st.write("Keine Benutzer vorhanden.")

# Backup Database
def backup_database():
    if not os.path.exists("chirurgischer_bericht.db"):
        st.error("Datenbankdatei nicht gefunden.")
        return
    with open("chirurgischer_bericht.db", "rb") as db_file:
        db_data = db_file.read()
    st.download_button(
        label="Datenbank herunterladen (.db)",
        data=db_data,
        file_name="chirurgischer_bericht_backup.db",
        mime="application/octet-stream"
    )

# Backup Tables as CSV
def backup_tables_csv():
    cursor.execute("SELECT * FROM users")
    users_data = cursor.fetchall()
    users_output = io.StringIO()
    users_writer = csv.writer(users_output)
    users_writer.writerow(["id", "username", "password", "is_tutor", "is_master"])
    users_writer.writerows(users_data)
    users_csv = users_output.getvalue()
    
    cursor.execute("SELECT * FROM operationen")
    operationen_data = cursor.fetchall()
    operationen_output = io.StringIO()
    operationen_writer = csv.writer(operationen_output)
    operationen_writer.writerow(["id", "datum", "datum_sort", "eingriff", "rolle", "patient_id", "diagnose", "kategorie", "zugang", "verschlusssystem", "notizen", "username", "user_id", "skills_acquired"])
    operationen_writer.writerows(operationen_data)
    operationen_csv = operationen_output.getvalue()
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("users.csv", users_csv)
        zip_file.writestr("operationen.csv", operationen_csv)
    zip_buffer.seek(0)
    
    st.download_button(
        label="Tabellen als CSV herunterladen (ZIP)",
        data=zip_buffer,
        file_name="logbuch_backup.zip",
        mime="application/zip"
    )

# Export CSV
def exportieren_csv():
    if st.session_state.is_master or st.session_state.is_tutor:
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
        cursor.execute("SELECT user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen FROM operationen WHERE username = ? ORDER BY user_id", (st.session_state.current_user,))
        eintraege = cursor.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Datum", "Eingriff", "Rolle", "Patienten-ID", "Diagnose", "Kategorie", "Zugang", "Verschlusssystem", "Notizen"])
        writer.writerows(eintraege)
        csv_data = output.getvalue()
    st.download_button("CSV herunterladen", csv_data, "logbuch.csv", "text/csv")

# Export PDF
def exportieren_pdf():
    if not PDF_AVAILABLE:
        st.error("PDF-Export nicht verfügbar. Bitte installieren Sie reportlab.")
        return
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    c.setFont("Helvetica", 12)
    c.drawString(100, 750, "Logbuch - Chirurgischer Bericht")
    y = 700
    if st.session_state.is_master or st.session_state.is_tutor:
        cursor.execute("SELECT datum, eingriff, rolle, patient_id, kategorie, username FROM operationen ORDER BY datum_sort DESC")
        eintraege = cursor.fetchall()
        for eintrag in eintraege:
            rolle_display = eintrag[2] if eintrag[2] == "Operateur" else ""
            text = f"Datum: {eintrag[0]} | Eingriff: {eintrag[1]} | Rolle: {rolle_display} | Patient: {eintrag[3]} | Kategorie: {eintrag[4]} | Benutzer: {eintrag[5]}"
            c.drawString(50, y, text)
            y -= 20
            if y < 50:
                c.showPage()
                y = 750
    else:
        cursor.execute("SELECT user_id, datum, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen FROM operationen WHERE username = ? ORDER BY user_id", (st.session_state.current_user,))
        eintraege = cursor.fetchall()
        for eintrag in eintraege:
            text = f"ID: {eintrag[0]} | Datum: {eintrag[1]} | Eingriff: {eintrag[2]} | Rolle: {eintrag[3]} | Patient: {eintrag[4]} | Diagnose: {eintrag[5]} | Kategorie: {eintrag[6]}"
            if eintrag[7]:
                text += f" | Zugang: {eintrag[7]}"
            if eintrag[8]:
                text += f" | Verschlusssystem: {eintrag[8]}"
            c.drawString(50, y, text)
            y -= 20
            if y < 50:
                c.showPage()
                y = 750
    c.save()
    buffer.seek(0)
    st.download_button("PDF herunterladen", buffer, "logbuch.pdf", "application/pdf")

# Zusammenfassung Kategorien
def zusammenfassung_kategorien():
    if st.session_state.is_master or st.session_state.is_tutor:
        cursor.execute("SELECT kategorie, COUNT(*) FROM operationen GROUP BY kategorie")
    else:
        cursor.execute("SELECT kategorie, COUNT(*) FROM operationen WHERE username = ? GROUP BY kategorie", (st.session_state.current_user,))
    ergebnisse = cursor.fetchall()
    for kategorie, anzahl in ergebnisse:
        st.write(f"{kategorie}: {anzahl} Eingriffe")

# Print table (for tutor and master)
def print_table():
    st.markdown('<button onclick="window.print()">Drucken</button>', unsafe_allow_html=True)

# App
st.title("OP Katalog - Hubertus")
st.caption("Copyright Victor Ahumada Jimenez 2025")

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.current_user = None
    st.session_state.is_tutor = False
    st.session_state.is_master = False

if not st.session_state.logged_in:
    tab1, tab2, tab3 = st.tabs(["Anmelden", "Registrieren", "Tutor Modus"])
    
    with tab1:
        st.caption("Copyright Victor Ahumada Jimenez 2025")
        username = st.text_input("Benutzername", key="login_username")
        password = st.text_input("Passwort", type="password", key="login_password")
        if st.button("Anmelden"):
            if username and password:
                cursor.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, password))
                user = cursor.fetchone()
                if user:
                    st.session_state.logged_in = True
                    st.session_state.current_user = username
                    st.session_state.is_tutor = bool(user[3])
                    st.session_state.is_master = bool(user[4])
                    st.success(f"Anmeldung erfolgreich als {'Master' if user[4] else 'Tutor' if user[3] else 'Benutzer'}.")
                    st.rerun()
                else:
                    st.error("Ungültiger Benutzername oder Passwort.")
                    cursor.execute("SELECT username, password, is_master FROM users WHERE username = ?", ("ahuvic",))
                    master_user = cursor.fetchone()
                    if master_user:
                        st.write(f"Debug: Master user exists with username: {master_user[0]}, is_master: {master_user[2]}")
                    else:
                        st.write("Debug: Master user 'ahuvic' not found in database.")
            else:
                st.error("Benutzername und Passwort sind erforderlich.")

    with tab2:
        st.caption("Copyright Victor Ahumada Jimenez 2025")
        reg_type = st.selectbox("Registrieren als", ["Benutzer", "Tutor"])
        reg_username = st.text_input("Benutzername", key="reg_username")
        reg_password = st.text_input("Passwort", type="password", key="reg_password")
        if reg_type == "Tutor":
            tutor_reg_password = st.text_input("Tutor-Passwort", type="password", key="tutor_reg_password")
        else:
            tutor_reg_password = None
        if st.button("Registrieren"):
            if reg_username and reg_password:
                if reg_type == "Tutor" and tutor_reg_password != "tutor01":
                    st.error("Ungültiges Tutor-Passwort. Verwenden Sie 'tutor01'.")
                else:
                    is_tutor_reg = 1 if reg_type == "Tutor" else 0
                    try:
                        cursor.execute("INSERT INTO users (username, password, is_tutor, is_master) VALUES (?, ?, ?, 0)", (reg_username, reg_password, is_tutor_reg))
                        conn.commit()
                        st.success("Benutzer erfolgreich registriert.")
                    except sqlite3.IntegrityError:
                        st.error("Benutzername existiert bereits.")
            else:
                st.error("Benutzername und Passwort sind erforderlich.")

    with tab3:
        st.caption("Copyright Victor Ahumada Jimenez 2025")
        tutor_username = st.text_input("Benutzername", key="tutor_username")
        tutor_password = st.text_input("Passwort", type="password", key="tutor_password")
        if st.button("Anmelden als Tutor"):
            if tutor_password == "tutor01":
                st.session_state.logged_in = True
                st.session_state.current_user = tutor_username
                st.session_state.is_tutor = True
                st.session_state.is_master = False
                st.success("Tutor-Modus aktiviert.")
                st.rerun()
            else:
                st.error("Ungültiges Passwort für Tutor-Modus.")
else:
    if st.button("Abmelden"):
        st.session_state.logged_in = False
        st.session_state.current_user = None
        st.session_state.is_tutor = False
        st.session_state.is_master = False
        st.rerun()

    if st.session_state.is_master:
        st.subheader("Master-Verwaltungspanel")
        st.caption("Copyright Victor Ahumada Jimenez 2025")
        st.subheader("Alle Einträge anzeigen")
        zeige_eintraege()
        st.subheader("Eintrag bearbeiten")
        master_edit_eintrag()
        st.subheader("Eintrag löschen")
        master_loesche_eintrag()
        st.subheader("Benutzer löschen")
        master_loesche_benutzer()
        st.subheader("Datenbank sichern")
        backup_database()
        backup_tables_csv()
    elif not st.session_state.is_tutor:
        st.subheader("Neue Operation hinzufügen")
        with st.form(key="add_form"):
            datum = st.date_input("Datum", value=None, format="DD.MM.YYYY")
            kategorie = st.selectbox("Kategorie", ["Operation", "Intervention", "Prozedur"])
            eingriff_options = {
                "Operation": [
                    "Carotis EEA/TEA", "Aortenaneurysma Rohrprothese", "Aortenaneurysma Bypass",
                    "Aortobi- oder monoiliakaler Bypass", "Aortobi- oder monofemoraler Bypass",
                    "Iliofemoraler Bypass", "Crossover Bypass", "Femoralis TEA", "Fem-pop. P1 Bypass",
                    "Fem-pop. P3 Bypass", "Fem-cruraler Bypass", "P1-P3 Bypass", "Wunddebridement - VAC Wechsel",
                    "rekonstruktive Operationen (supraaortale Arterien)", "rekonstruktive Operationen (aortale/iliakale/viszerale/thorakale)",
                    "rekonstruktive Operationen (femoro-popliteal/brachial/cruro-pedal)", "Operationen am Venensystem",
                    "Grenzzonenamputationen/Ulkusversorgungen"
                ],
                "Intervention": [
                    "TEVAR", "FEVAR", "EVAR", "BEVAR", "Organstent", "Beckenstent", "Beinstent",
                    "Thrombektomie over the wire", "endovaskuläre Eingriffe", "Anlage von Dialyse-Shunts/Port-Implantation"
                ],
                "Prozedur": [
                    "ZVK-Anlage", "Drainage Thorax", "Drainage Abdomen", "Drainage Wunde Extremitäten",
                    "Punktion/PE", "intraoperative angiographische Untersuchungen", "hämodynamische Untersuchungen an Venen",
                    "Doppler-/Duplex-Untersuchungen (Extremitäten)", "Doppler-/Duplex-Untersuchungen (abdominell/retroperitoneal)",
                    "Doppler-/Duplex-Untersuchungen (extrakraniell)"
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
            skills_acquired = st.checkbox("Kenntnisse, Erfahrungen und Fertigkeiten erworben")
            submitted = st.form_submit_button("Hinzufügen")
            if submitted:
                if not datum or not eingriff or not rolle or not patient_id or not diagnose or not kategorie:
                    st.error("Alle Pflichtfelder müssen ausgefüllt sein.")
                elif not datum:
                    st.error("Bitte wählen Sie ein Datum aus.")
                elif kategorie == "Intervention" and not zugang:
                    st.error("Zugang muss für Intervention ausgewählt sein.")
                elif kategorie == "Intervention" and zugang == "Punktion" and not verschlusssystem:
                    st.error("Verschlusssystem muss für Punktion ausgewählt sein.")
                else:
                    datum_sort = datum.strftime('%Y-%m-%d')
                    datum_str = datum.strftime('%d.%m.%Y')
                    cursor.execute("SELECT MAX(user_id) FROM operationen WHERE username = ?", (st.session_state.current_user,))
                    max_id = cursor.fetchone()[0]
                    user_id = (max_id or 0) + 1
                    try:
                        cursor.execute('''
                            INSERT INTO operationen (datum, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, username, user_id, skills_acquired)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (datum_str, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, zugang, verschlusssystem, notizen, st.session_state.current_user, user_id, 1 if skills_acquired else 0))
                        conn.commit()
                        st.success("Operation erfolgreich registriert.")
                        zeige_eintraege()
                    except sqlite3.Error as e:
                        st.error(f"Fehler beim Hinzufügen: {e}")
        
        st.subheader("Operationen aus externer Quelle hinzufügen")
        with st.form(key="add_external_form"):
            st.write("Fügen Sie die Anzahl der zuvor durchgeführten Operationen pro Kategorie hinzu:")
            external_counts = {}
            for eingriff in [
                "intraoperative angiographische Untersuchungen",
                "Doppler-/Duplex-Untersuchungen (Extremitäten)",
                "Doppler-/Duplex-Untersuchungen (abdominell/retroperitoneal)",
                "Doppler-/Duplex-Untersuchungen (extrakraniell)",
                "hämodynamische Untersuchungen an Venen",
                "rekonstruktive Operationen (supraaortale Arterien)",
                "rekonstruktive Operationen (aortale/iliakale/viszerale/thorakale)",
                "rekonstruktive Operationen (femoro-popliteal/brachial/cruro-pedal)",
                "endovaskuläre Eingriffe",
                "Anlage von Dialyse-Shunts/Port-Implantation",
                "Operationen am Venensystem",
                "Grenzzonenamputationen/Ulkusversorgungen"
            ]:
                external_counts[eingriff] = st.number_input(f"Anzahl für {eingriff}", min_value=0, step=1, key=f"ext_{eingriff}")
            external_date = st.date_input("Datum der Eingabe", value=datetime.now(), format="DD.MM.YYYY")
            external_submitted = st.form_submit_button("Externe Operationen hinzufügen")
            if external_submitted:
                if not external_date:
                    st.error("Bitte wählen Sie ein Datum aus.")
                else:
                    try:
                        datum_str = external_date.strftime('%d.%m.%Y')
                        datum_sort = external_date.strftime('%Y-%m-%d')
                        cursor.execute("SELECT MAX(user_id) FROM operationen WHERE username = ?", (st.session_state.current_user,))
                        max_id = cursor.fetchone()[0]
                        user_id = (max_id or 0) + 1
                        for eingriff, count in external_counts.items():
                            if count > 0:
                                for _ in range(count):
                                    cursor.execute('''
                                        INSERT INTO operationen (datum, datum_sort, eingriff, rolle, patient_id, diagnose, kategorie, notizen, username, user_id, skills_acquired)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ''', (datum_str, datum_sort, eingriff, "Operateur", "Extern", "Externe Eingabe", "Operation", "Externe Operation", st.session_state.current_user, user_id, 1))
                                    user_id += 1
                        conn.commit()
                        st.success("Externe Operationen erfolgreich hinzugefügt.")
                        zeige_eintraege()
                    except sqlite3.Error as e:
                        st.error(f"Fehler beim Hinzufügen externer Operationen: {e}")

        # Logbuch Button
        if st.button("Logbuch anzeigen"):
            zeige_logbuch()

    # Búsqueda
    st.subheader("Einträge suchen")
    if st.session_state.is_master or st.session_state.is_tutor:
        suche_kriterium = st.selectbox("Suchen nach", ["Kategorie", "Datum", "Datum Range", "Benutzer"], key="suche_kriterium")
        if suche_kriterium in ["Kategorie", "Datum"]:
            wert = st.text_input("Wert", key="suche_wert")
        elif suche_kriterium == "Datum Range":
            vom = st.text_input("vom (TT.MM.JJJJ)", key="vom")
            bis = st.text_input("bis (TT.MM.JJJJ)", key="bis")
            wert = None
        elif suche_kriterium == "Benutzer":
            cursor.execute("SELECT username FROM users")
            users = [row[0] for row in cursor.fetchall()]
            wert = st.selectbox("Benutzer", users, key="benutzer_select")
        else:
            wert = None
    else:
        suche_kriterium = st.selectbox("Suchen nach", ["Kategorie", "Datum"], key="suche_kriterium_user")
        wert = st.text_input("Wert", key="suche_wert_user")
    if st.button("Suchen"):
        suche_eintraege(suche_kriterium, wert, vom if 'vom' in locals() else None, bis if 'bis' in locals() else None)
    if st.button("Alle Einträge anzeigen"):
        zeige_eintraege()

    # Table
    st.subheader("Einträge")
    zeige_eintraege()

    # Botones
    if not st.session_state.is_master and not st.session_state.is_tutor:
        st.subheader("Eintrag löschen")
        loesche_eintrag()
    if st.button("CSV exportieren"):
        exportieren_csv()
    if st.button("PDF exportieren"):
        exportieren_pdf()
    if not st.session_state.is_master and not st.session_state.is_tutor:
        if st.button("Zusammenfassung Kategorien"):
            zusammenfassung_kategorien()
    elif st.session_state.is_tutor:
        if st.button("Drucken"):
            print_table()

st.caption("Copyright Victor Ahumada Jimenez 2025")
conn.close()
