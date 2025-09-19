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

import requests
import gspread
from google.oauth2.service_account import Credentials

import mysql.connector
from mysql.connector import Error

from moralis import evm_api

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "YOUR_MORALIS_API_KEY")
GECKO_PATH      = os.getenv("GECKO_PATH", "/usr/local/bin/geckodriver")
GOOGLE_CREDS    = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")
SPREADSHEET     = os.getenv("SPREADSHEET_NAME", "BCK_CAP")

# Native ETH staking validator
VALIDATOR_ID = '0xab6aeaae3890abaa765ff7fa79c2f8f271a4300aec848252f5801ea155c5c4581308de0e2c06c2af42fd3aa3da0890ab'
BEACON_API   = f'https://beaconcha.in/api/v1/validator/{VALIDATOR_ID}'

# URLs for DeBank profiles
URL_YIELD,  FUNDID_YIELD  = (
    "https://debank.com/profile/0x4380927070ccb0dd069d6412371a72e972239e06", 3
)
URL_ASSETS, FUNDID_ASSETS = (
    "https://debank.com/profile/0x0a152c957fd7bcc1212eab27233da0433b7c8ea4", 2
)
URL_FUNDID_LIST = [
    (URL_YIELD,    FUNDID_YIELD),
    (URL_ASSETS,   FUNDID_ASSETS),
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
        cur = conn.cursor()
        cur.execute(sql, (value, record_id))
        conn.commit()
        cur.close()
    print(f"✅ MySQL Nav[{record_id}] = {value}")


def update_mysql_balance(scraped_balance: float):
    """Actualiza el balance de yield en Nav usando NAV Yield!D10."""
    cell = read_sheet_cell("NAV Yield", "D10")
    add = float(cell.replace(",", "").replace("$", "").strip()) if cell else 0.0
    new_bal = scraped_balance + add
    update_nav_value("cm2uauagx000109tiw2seaoiao", new_bal)

# ─── Google Sheets helpers ─────────────────────────────────────────────────────

def get_gspread_client(json_keyfile: str = None) -> gspread.Client:
    keyfile = json_keyfile or GOOGLE_CREDS
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    return gspread.authorize(creds)


def open_worksheet(spreadsheet_name: str, worksheet_name: str):
    client = get_gspread_client()
    sh = client.open(spreadsheet_name)
    return sh.worksheet(worksheet_name)


def read_sheet_cell(worksheet_name: str, cell_address: str) -> str | None:
    try:
        ws = open_worksheet(SPREADSHEET, worksheet_name)
        return ws.acell(cell_address).value
    except Exception as e:
        print(f"❌ Error leyendo {worksheet_name}!{cell_address}: {e}")
        return None


def update_user_tokens(cell_address: str, user_id: str):
    """
    Lee el valor de la celda (p.ej. "E6"), lo limpia y actualiza User.tokens.
    """
    cell = read_sheet_cell("Clients", cell_address)
    if not cell:
        print(f"⚠️ Celda {cell_address} vacía; omitiendo.")
        return
    val = float(cell.replace(",", "").replace("$", "").strip())
    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM User WHERE id = %s", (user_id,))
        if not cur.fetchone():
            print(f"⚠️ User.id = {user_id} no existe.")
            cur.close()
            return
        cur.execute("UPDATE User SET tokens = %s WHERE id = %s", (val, user_id))
        conn.commit()
        cur.close()
    print(f"✅ User[{user_id}].tokens = {val}")

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

# ─── Beacon validator monitor ───────────────────────────────────────────────────

def fetch_validator_balance() -> float | None:
    """
    Consulta la API de beaconcha.in y devuelve el balance del validador en ETH.
    """
    try:
        resp = requests.get(BEACON_API)
        resp.raise_for_status()
        data = resp.json().get('data', {})
        balance_gwei = int(data.get('balance', 0))
        balance_eth = balance_gwei / 1e9
        print(f"🔍 Validator balance: {balance_eth} ETH")
        return balance_eth
    except Exception as e:
        print(f"❌ Error fetching validator balance: {e}")
        return None


def update_validator_sheet(balance_eth: float):
    """
    Escribe el balance del validador y fundid=2 en la sheet RAW con etiqueta de red.
    """
    try:
        ws = open_worksheet(SPREADSHEET, 'RAW')
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([FUNDID_ASSETS, balance_eth, timestamp, 'ETH2'])
        print("✅ Validator RAW entry added.")
    except Exception as e:
        print(f"❌ Error updating validator RAW: {e}")

# ─── Main execution ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1) Debank scraping
    results = []
    for url, fundid in URL_FUNDID_LIST:
        bal, ts = scrape_balance(url)
        results.append((fundid, bal, ts))

        if bal is None:
            print(f"⚠️ No se pudo obtener balance para fundid {fundid} en {url}")
            continue

        if fundid == FUNDID_YIELD:
            update_mysql_balance(bal)
        elif fundid == FUNDID_ASSETS:
            update_nav_value("bck_assets_2", bal)

    # 2) Beacon validator
    val_bal = fetch_validator_balance()
    if val_bal is not None:
        update_validator_sheet(val_bal)
        update_nav_value("bck_assets_2_ETH2", val_bal)

    # 3) write all Debank results to RAW and update user tokens
    update_google_sheet(results)
    update_user_tokens("E6", "BLCA_10005")
    update_user_tokens("E5", "BLCY_10006")

