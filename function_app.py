import azure.functions as func
import logging
import feedparser
import pandas as pd
from datetime import datetime, timezone, timedelta
import re
import json
from openai import OpenAI
from email.utils import parsedate_to_datetime
import pymssql
import os
import pytz
import time

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False)
def etl_google_trends(myTimer: func.TimerRequest) -> None:
    logging.info("▶️ ETL-Pipeline gestartet")

    # Datenbank aufwecken bevor ETL startet
    wecke_datenbank()

    try:
        df = lade_trends()
        logging.info(f"✅ Trends geladen: {len(df)} Einträge")
    except Exception as e:
        logging.error(f"❌ Fehler beim Laden der Trends: {e}")
        raise

    try:
        speichere_in_azure_sql(df)
        logging.info("✅ ETL-Pipeline abgeschlossen")
    except Exception as e:
        logging.error(f"❌ Fehler beim Speichern in SQL: {e}")
        raise


def wecke_datenbank():
    """
    Sendet eine einfache Testabfrage an die DB, um sie aus dem
    Auto-Pause-Modus (Serverless) aufzuwecken. Wartet danach,
    bis die DB vollständig bereit ist.
    """
    logging.info("🔔 Versuche Datenbank aufzuwecken...")

    for versuch in range(10):
        try:
            conn = pymssql.connect(
                server=os.environ["SQL_SERVER"],
                user=os.environ["SQL_USERNAME"],
                password=os.environ["SQL_PASSWORD"],
                database="engineerdb",
                timeout=30  # Verbindungs-Timeout pro Versuch
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            logging.info(f"✅ Datenbank ist bereit (Versuch {versuch + 1})")
            time.sleep(3)  # Kurze Pause nach erfolgreichem Aufwecken
            return

        except Exception as e:
            wartezeit = 20 if versuch < 3 else 40  # Erst 20s, dann 40s warten
            logging.warning(f"⏳ DB noch nicht bereit, Versuch {versuch + 1}/10. Warte {wartezeit}s... ({e})")
            time.sleep(wartezeit)

    raise Exception("❌ Datenbank konnte nach 10 Versuchen nicht aufgeweckt werden")


def lade_trends():
    url = "https://trends.google.com/trending/rss?geo=DE"
    feed = feedparser.parse(url)

    if not feed.entries:
        raise ValueError("RSS-Feed ist leer oder nicht erreichbar")

    daten = []

    for entry in feed.entries[:20]:
        traffic_raw = entry.get("ht_approx_traffic", "0")
        traffic_clean = re.sub(r"[+,]", "", traffic_raw)

        if "K" in traffic_clean:
            traffic_num = float(traffic_clean.replace("K", "")) * 1_000
        elif "M" in traffic_clean:
            traffic_num = float(traffic_clean.replace("M", "")) * 1_000_000
        else:
            try:
                traffic_num = float(traffic_clean)
            except ValueError:
                traffic_num = 0

        daten.append({
            "Suchbegriff": entry.title,
            "Traffic": traffic_raw,
            "Traffic_Numerisch": traffic_num,
            "Zeitpunkt": datetime.now(pytz.timezone("Europe/Berlin")).replace(tzinfo=None)
        })

    df = pd.DataFrame(daten)
    df = df.sort_values("Traffic_Numerisch", ascending=False).reset_index(drop=True)

    suchbegriffe = df["Suchbegriff"].tolist()
    ki_kategorien = kategorisiere_mit_ki(suchbegriffe)
    df["Kategorie"] = df["Suchbegriff"].apply(lambda x: bestimme_kategorie(x, ki_kategorien))
    df["Rang"] = range(1, len(df) + 1)
    df = df[["Rang", "Suchbegriff", "Kategorie", "Traffic", "Traffic_Numerisch", "Zeitpunkt"]]

    return df


def kategorisiere_mit_ki(suchbegriffe):
    try:
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        begriffe_text = "\n".join([f"- {begriff}" for begriff in suchbegriffe])
        prompt = f"""
Du bist ein Klassifikationssystem für deutsche Google-Suchanfragen.
Ordne JEDEN Suchbegriff GENAU EINER dieser Kategorien zu:
Sport, Persönlichkeiten, Politik, Unterhaltung, Wirtschaft, Technologie, Gesundheit, Wetter, Reisen & Orte, Bildung & Schule, Medien & TV, Sonstiges
Antworte NUR mit gültigem JSON. Kein Markdown.
Suchanfragen:
{begriffe_text}
Format: {{"Begriff1": "Kategorie1"}}
"""
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return json.loads(response.choices[0].message.content.strip())
    except Exception as e:
        logging.warning(f"KI-Kategorisierung fehlgeschlagen: {e}")
        return None


def bestimme_kategorie(begriff, ki_dict):
    if ki_dict and begriff in ki_dict:
        return ki_dict[begriff]
    return "Sonstiges"


def speichere_in_azure_sql(df):
    retries = 5
    wait_times = [10, 30, 60, 90, 120]

    for attempt in range(retries):
        try:
            conn = pymssql.connect(
                server=os.environ["SQL_SERVER"],
                user=os.environ["SQL_USERNAME"],
                password=os.environ["SQL_PASSWORD"],
                database="engineerdb"
            )

            cursor = conn.cursor()

            insert_sql = """
                INSERT INTO google_trends
                (rang, suchbegriff, kategorie, traffic, traffic_numerisch, zeitpunkt)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            for _, row in df.iterrows():
                cursor.execute(insert_sql, (
                    int(row["Rang"]),
                    row["Suchbegriff"],
                    row["Kategorie"],
                    row["Traffic"],
                    float(row["Traffic_Numerisch"]),
                    row["Zeitpunkt"]
                ))

            conn.commit()
            cursor.close()
            conn.close()

            logging.info("✅ Daten in Azure SQL gespeichert")
            return

        except Exception as e:
            wait = wait_times[attempt]
            logging.warning(f"SQL Verbindung fehlgeschlagen Versuch {attempt + 1}/{retries}. Warte {wait}s: {e}")
            if attempt < retries - 1:
                time.sleep(wait)

    raise Exception("❌ SQL Verbindung nach mehreren Versuchen fehlgeschlagen")
