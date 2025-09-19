#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
from datetime import datetime
from contextlib import contextmanager
import argparse

import requests
from requests.auth import HTTPBasicAuth

import gspread
from google.oauth2.service_account import Credentials

import mysql.connector
from mysql.connector import Error

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()
GOOGLE_CREDS   = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")
SPREADSHEET    = os.getenv("SPREADSHEET_NAME", "BCK_CAP")
ZERION_API_KEY = os.getenv("ZERION_API_KEY")
if not ZERION_API_KEY:
    raise RuntimeError("❌ Falta la variable de entorno ZERION_API_KEY")

URL_YIELD,  FUNDID_YIELD  = ("0x4380927070ccb0dd069d6412371a72e972239e06", 3)
URL_ASSETS, FUNDID_ASSETS = ("0x0a152c957fd7bcc1212eab27233da0433b7c8ea4", 2)
URL_FUNDID_LIST = [(URL_YIELD, FUNDID_YIELD), (URL_ASSETS, FUNDID_ASSETS)]

VALIDATOR_ID = '0xab6aeaae3890abaa765ff7fa79c2f8f271a4300aec848252f5801ea155c5c4581308de0e2c06c2af42fd3aa3da0890ab'
BEACON_API   = f'https://beaconcha.in/api/v1/validator/{VALIDATOR_ID}'

# ─── MySQL helpers ─────────────────────────────────────────────────────────────
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
        if conn and conn.is_connected():
            conn.close()

def update_nav_value(record_id: str, value: float):
    sql = "UPDATE `Nav` SET `value` = %s WHERE `id` = %s"
    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (value, record_id))
        conn.commit()
        cur.close()
    print(f"✅ MySQL Nav[{record_id}] = {value:,.2f}")

def update_mysql_balance(scraped_balance: float):
    cell = read_sheet_cell("NAV Yield", "D10")
    add = float(cell.replace(",", "").replace("$", "").strip()) if cell else 0.0
    update_nav_value("cm2uauagx000109tiw2seaoiao", scraped_balance + add)

# ─── Google Sheets helpers ─────────────────────────────────────────────────────
def get_gspread_client(json_keyfile: str = None) -> gspread.Client:
    keyfile = json_keyfile or GOOGLE_CREDS
    scopes = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    return gspread.authorize(creds)

def open_worksheet(spreadsheet_name: str, worksheet_name: str):
    client = get_gspread_client()
    sh = client.open(spreadsheet_name)
    return sh.worksheet(worksheet_name)

def read_sheet_cell(worksheet_name: str, cell_address: str) -> str | None:
    try:
        return open_worksheet(SPREADSHEET, worksheet_name).acell(cell_address).value
    except Exception as e:
        print(f"❌ Error leyendo {worksheet_name}!{cell_address}: {e}")
        return None

def update_user_tokens(cell_address: str, user_id: str):
    cell = read_sheet_cell("Clients", cell_address)
    if not cell:
        print(f"⚠️ Clients!{cell_address} está vacío; omitiendo.")
        return
    try:
        val = float(cell.replace(",", "").replace("$", "").strip())
    except ValueError:
        print(f"⚠️ No pude parsear '{cell}' de Clients!{cell_address}.")
        return

    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM `User` WHERE id = %s", (user_id,))
        if not cur.fetchone():
            print(f"⚠️ User.id = {user_id} no existe; omitiendo.")
            cur.close()
            return
        cur.execute("UPDATE `User` SET tokens = %s WHERE id = %s", (val, user_id))
        conn.commit()
        cur.close()
    print(f"✅ User[{user_id}].tokens = {val:,.2f}")

# ─── Zerion REST helper ────────────────────────────────────────────────────────
def fetch_net_worth_zerion(address: str) -> float:
    url = f"https://api.zerion.io/v1/wallets/{address}/portfolio/"
    resp = requests.get(url,
        headers={"Accept": "application/json"},
        auth=HTTPBasicAuth(ZERION_API_KEY, ""),
        params={"currency": "usd","filter[positions]":"no_filter"}
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Error HTTP {resp.status_code} de Zerion: {resp.text}")
    data = resp.json()
    return float(data["data"]["attributes"]["total"]["positions"] or 0.0)

# ─── Google Sheets – RAW updater ───────────────────────────────────────────────
def update_google_sheet(data: list[tuple[int, float | None, str]]):
    try:
        ws = open_worksheet(SPREADSHEET, "RAW")
    except Exception as e:
        print(f"❌ Error abriendo RAW: {e}")
        return
    for fundid, bal, ts in data:
        if bal is not None:
            ws.append_row([fundid, bal, ts])
    print("✅ RAW actualizada.")

# ─── Beacon validator monitor ───────────────────────────────────────────────────
def fetch_validator_balance() -> float | None:
    try:
        resp = requests.get(BEACON_API)
        resp.raise_for_status()
        d = resp.json().get('data', {})
        return int(d.get('balance',0)) / 1e9
    except Exception as e:
        print(f"❌ Error fetching validator balance: {e}")
        return None

def update_validator_sheet(balance_eth: float):
    try:
        ws = open_worksheet(SPREADSHEET, 'RAW')
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([FUNDID_ASSETS, balance_eth, ts, 'ETH2'])
        print("✅ RAW validator ETH2 añadida.")
    except Exception as e:
        print(f"❌ Error updating validator RAW: {e}")

# ─── Midnight-only task ────────────────────────────────────────────────────────
from datetime import datetime

def log_assets_nav():
    """
    Registra en Log_assets:
      A: fecha (YYYY-MM-DD)
      B: Token!C7
      C: Token!C6
      I: Token!C12
    """
    # 1) Leer valores desde Token
    date_str = datetime.now().strftime("%Y-%m-%d")
    nav_b    = read_sheet_cell("Token", "C7") or ""
    nav_c    = read_sheet_cell("Token", "C6") or ""
    nav_i    = read_sheet_cell("Token", "C12") or ""

    # 2) Abrir hoja Log_assets
    ws = open_worksheet(SPREADSHEET, "Log Assets")

    # 3) Calcular siguiente fila libre (mirando la columna A)
    next_row = len(ws.col_values(1)) + 1

    # 4) Escribir en A, B, C e I
    ws.update_acell(f"A{next_row}", date_str)
    ws.update_acell(f"B{next_row}", nav_b)
    ws.update_acell(f"C{next_row}", nav_c)
    ws.update_acell(f"I{next_row}", nav_i)

    print(f"✅ Log_assets!A{next_row}–B{next_row}–C{next_row}–I{next_row} registrados:")
    print(f"   Fecha={date_str}, C7={nav_b}, C6={nav_c}, C12={nav_i}")

def print_token_c6():
    """
    Lee Token!C6 del Google Sheet y la imprime.
    """
    val = read_sheet_cell("Token", "C6")
    print(f"🔖 Token!C6 = {val}")


# ─── Main tasks ────────────────────────────────────────────────────────────────
def run_full():
    # 1) Zerion
    results = []
    for addr, fid in URL_FUNDID_LIST:
        try:
            nw = fetch_net_worth_zerion(addr)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"🔍 Zerion[{addr}] = ${nw:,.2f} ({ts})")
            results.append((fid, nw, ts))
            if fid == FUNDID_YIELD: update_mysql_balance(nw)
            if fid == FUNDID_ASSETS: update_nav_value("bck_assets_2", nw)
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"❌ Zerion[{addr}] fallo: {e}")
            results.append((fid, None, ts))

    # 2) Beacon
    val = fetch_validator_balance()
    if val is not None:
        update_validator_sheet(val)
        update_nav_value("bck_assets_2_ETH2", val)

    # 3) RAW + users
    update_google_sheet(results)
    update_user_tokens("E6", "BLCA_10005")
    update_user_tokens("E5", "BLCY_10006")

    print("\n🟢 Proceso FULL completado.")

# ─── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--midnight-only", action="store_true",
                       help="Solo registra NAV en Log_assets y sale")
    group.add_argument("--full-only",     action="store_true",
                       help="Solo ejecuta el flujo completo (sin log_assets_nav)")

    args = parser.parse_args()

    if args.midnight_only:
        print("🌙 Ejecutando solo tareas de medianoche…")
        log_assets_nav()
    else:
        # si pasas --full-only o ninguno, corre todo
        run_full()
        print("\n🕛 Comprobando si toca midnight…")
        if not args.full_only and datetime.now().hour == 0:
            log_assets_nav()
    
    # --- al final, imprimimos Token!C6 ---
    print_token_c6()


