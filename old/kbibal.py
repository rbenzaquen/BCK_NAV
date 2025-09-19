#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import time
from datetime import datetime
from contextlib import contextmanager

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
from google.oauth2.service_account import Credentials

import mysql.connector
from mysql.connector import Error

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

# Planilla extra
SPREADSHEET_EXTRA = os.getenv("SPREADSHEET_EXTRA", "KB CAPITAL NEW")
# Credenciales
GECKO_PATH   = os.getenv("GECKO_PATH", "/usr/local/bin/geckodriver")
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")

# URL y fund ID únicos
URL_EXTRA   = "https://debank.com/profile/0xad920e4870b3ff09d56261f571955990200b863e"
FUNDID_EXTRA = 1

# ─── MySQL helper ──────────────────────────────────────────────────────────────
@contextmanager
def mysql_connection():
    try:
        conn = mysql.connector.connect(
            host     = os.getenv("MYSQL_HOST"),
            port     = int(os.getenv("MYSQL_PORT", "3306")),
            user     = os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            database = os.getenv("MYSQL_DATABASE")
        )
        yield conn
    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()


def update_nav_value(record_id: str, value: float):
    """Actualiza Nav.value en MySQL para kbi_1"""
    sql = "UPDATE `Nav` SET `value` = %s WHERE `id` = %s"
    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (value, record_id))
        conn.commit()
        cur.close()
    print(f"✅ MySQL Nav[{record_id}] = {value}")

# ─── Google Sheets helper ────────────────────────────────────────────────────
def get_gspread_client(keyfile: str = None) -> gspread.Client:
    keyfile = keyfile or GOOGLE_CREDS
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    return gspread.authorize(creds)

# ─── Debank scraping ───────────────────────────────────────────────────────────
def scrape_balance(url: str) -> tuple[float, str] | tuple[None, None]:
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    service = Service(GECKO_PATH)
    driver = webdriver.Firefox(service=service, options=opts)
    try:
        driver.get(url)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".HeaderInfo_totalAssetInner__HyrdC"))
        )
        time.sleep(10)
        text = driver.find_element(By.CSS_SELECTOR, ".HeaderInfo_totalAssetInner__HyrdC").text
        bal = float(text.split()[0].replace(",", "").replace("$", ""))
        ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"URL: {url}\nBalance: {bal}\nTimestamp: {ts}\n")
        return bal, ts
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None, None
    finally:
        driver.quit()

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Scrape solo fundid_extra
    bal, ts = scrape_balance(URL_EXTRA)
    if bal is None:
        exit(1)

    # Actualizar MySQL
    update_nav_value("kbi_1", bal)

    # Actualizar Google Sheets
    client = get_gspread_client()
    try:
        ws = client.open(SPREADSHEET_EXTRA).worksheet("RAW")
        ws.append_row([FUNDID_EXTRA, bal, ts])
        print(f"✅ Google Sheet '{SPREADSHEET_EXTRA}' RAW actualizada para fundid {FUNDID_EXTRA}.")
    except Exception as e:
        print(f"⚠️ No se pudo abrir la planilla '{SPREADSHEET_EXTRA}'. Asegúrate de compartirla con la cuenta de servicio. Error: {e}")

