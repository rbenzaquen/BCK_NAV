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

from moralis import evm_api

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "YOUR_MORALIS_API_KEY")
GECKO_PATH      = "/usr/local/bin/geckodriver"
GOOGLE_CREDS    = "/home/benzarod/blck/blck-456015-02d4cfc790b7.json"
SPREADSHEET     = "BCK_CAP"

url_yield,  fundid_yield  = (
    "https://debank.com/profile/0x4380927070ccb0dd069d6412371a72e972239e06",
    3
)
url_assets, fundid_assets = (
    "https://debank.com/profile/0x0a152c957fd7bcc1212eab27233da0433b7c8ea4",
    2
)
url_fundid_list = [
    (url_yield,    fundid_yield),
    (url_assets,   fundid_assets),
]

# ─── MySQL helpers ─────────────────────────────────────────────────────────────
@contextmanager
def mysql_connection():
    """Context manager para abrir/cerrar la conexión MySQL."""
    conn = None
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
        if conn and conn.is_connected():
            conn.close()

def update_nav_value(record_id: str, value: float):
    """Actualiza el campo `value` de la tabla Nav para un id dado."""
    sql = "UPDATE `Nav` SET `value` = %s WHERE `id` = %s"
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, (value, record_id))
        conn.commit()
        cursor.close()
    print(f"✅ MySQL Nav[{record_id}] = {value}")

# ─── Google Sheets helpers ─────────────────────────────────────────────────────
def get_gspread_client(json_keyfile: str = None) -> gspread.Client:
    """
    Inicializa y devuelve un cliente gspread.
    """
    keyfile = json_keyfile or GOOGLE_CREDS
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    return gspread.authorize(creds)

def open_worksheet(spreadsheet_name: str, worksheet_name: str):
    """
    Abre y devuelve la worksheet indicada.
    """
    client = get_gspread_client()
    sh = client.open(spreadsheet_name)
    return sh.worksheet(worksheet_name)

def read_sheet_cell(worksheet_name: str, cell_address: str) -> str | None:
    """
    Lee y devuelve el valor de una celda.
    """
    try:
        ws = open_worksheet(SPREADSHEET, worksheet_name)
        return ws.acell(cell_address).value
    except Exception as e:
        print(f"❌ Error leyendo {worksheet_name}!{cell_address}: {e}")
        return None


# ─── Debank scrape + spreadsheet + MySQL helpers ─────────────────────────────
def scrape_balance(url: str) -> tuple[float, str] | tuple[None, None]:
    options = FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(GECKO_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    try:
        driver.get(url)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".HeaderInfo_totalAssetInner__HyrdC"))
        )
        time.sleep(5)
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

def update_google_sheet(data: list[tuple[int, float, str]]):
    ws = open_worksheet(SPREADSHEET, "RAW")
    for fundid, bal, ts in data:
        if bal is not None:
            ws.append_row([fundid, bal, ts])
    print("✅ Google Sheet RAW actualizada.")

def update_mysql_balance(scraped_balance: float):
    cell = read_sheet_cell("NAV Yield", "D10")
    add = float(cell.replace(",", "").replace("$", "").strip()) if cell else 0.0
    new_bal = scraped_balance + add
    update_nav_value("cm2uauagx000109tiw2seaoiao", new_bal)

def update_user_tokens(cell_address: str, user_id: str):
    """
    Lee el valor de la celda (p.ej. "E6"), lo limpia y
    actualiza User.tokens para el user_id indicado, solo si existe.
    """
    # 1) Leer de Sheets
    cell = read_sheet_cell("Clients", cell_address)
    if not cell:
        print(f"⚠️ Celda {cell_address} vacía; omitiendo.")
        return

    # 2) Limpia formato “$1,234.56” → 1234.56
    val = float(cell.replace(",", "").replace("$", "").strip())

    # 3) Conexión única: comprueba existencia y luego UPDATE
    with mysql_connection() as conn:
        cur = conn.cursor()

        # 3a) Comprueba que el usuario exista
        cur.execute("SELECT 1 FROM User WHERE id = %s", (user_id,))
        if not cur.fetchone():
            print(f"⚠️ User.id = {user_id} no existe en la BD.")
            cur.close()
            return

        # 3b) Si existe, actualiza tokens
        cur.execute(
            "UPDATE User SET tokens = %s WHERE id = %s",
            (val, user_id)
        )
        conn.commit()
        print(f"✅ User[{user_id}].tokens actualizado a {val}")

        cur.close()





if __name__ == "__main__":
    results = []

    for url, fundid in url_fundid_list:
        # Una única llamada por URL
        bal, ts = scrape_balance(url)
        results.append((fundid, bal, ts))

        if bal is None:
            print(f"⚠️ No se pudo obtener balance para fundid {fundid} en {url}")
            continue

        if fundid == fundid_yield:
            # yield sigue igual
            update_mysql_balance(bal)

        elif fundid == fundid_assets:
            # assets ahora usa directamente ese balance
            update_nav_value("bck_assets_2", bal)

        # futuros fundid irían aquí como más elif…

    # luego, subimos todo lo scrapeado a Google Sheets y actualizamos clientes
    update_google_sheet(results)
    update_user_tokens("E6", "BLCA_10005")
    update_user_tokens("E5", "BLCY_10006")
