#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import time
from datetime import datetime
from contextlib import contextmanager

import requests
from requests.auth import HTTPBasicAuth

import gspread
from google.oauth2.service_account import Credentials

import mysql.connector
from mysql.connector import Error

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

# Ruta al JSON de credenciales de Google Service Account
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")

# Nombre de la planilla de Google Sheets donde trabajamos
SPREADSHEET   = os.getenv("SPREADSHEET_NAME", "BCK_CAP")

# Clave de Zerion (debe estar en .env como ZERION_API_KEY=tu_api_key)
ZERION_API_KEY = os.getenv("ZERION_API_KEY")
if not ZERION_API_KEY:
    raise RuntimeError("❌ Falta la variable de entorno ZERION_API_KEY")

# Lista de tuplas (address, fundid) que antes usábamos con DeBank
URL_YIELD,  FUNDID_YIELD  = ("0x4380927070ccb0dd069d6412371a72e972239e06", 3)
URL_ASSETS, FUNDID_ASSETS = ("0x0a152c957fd7bcc1212eab27233da0433b7c8ea4", 2)
URL_FUNDID_LIST = [
    (URL_YIELD,    FUNDID_YIELD),
    (URL_ASSETS,   FUNDID_ASSETS),
]

# Clave del validador ETH en beaconcha.in
VALIDATOR_ID  = '0xab6aeaae3890abaa765ff7fa79c2f8f271a4300aec848252f5801ea155c5c4581308de0e2c06c2af42fd3aa3da0890ab'
BEACON_API    = f'https://beaconcha.in/api/v1/validator/{VALIDATOR_ID}'

# ─── MySQL helpers ─────────────────────────────────────────────────────────────

@contextmanager
def mysql_connection():
    """
    Context manager para abrir/cerrar la conexión MySQL.
    """
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
    """
    Actualiza el campo `value` de la tabla Nav para un id dado.
    """
    sql = "UPDATE `Nav` SET `value` = %s WHERE `id` = %s"
    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (value, record_id))
        conn.commit()
        cur.close()
    print(f"✅ MySQL Nav[{record_id}] = {value:,.2f}")


def update_mysql_balance(scraped_balance: float):
    """
    Actualiza el balance "Yield" en Nav: lee NAV Yield!D10, lo suma al scraped_balance y
    escribe el total en `Nav` con el id correspondiente.
    """
    cell = read_sheet_cell("NAV Yield", "D10")
    add = 0.0
    if cell:
        add = float(cell.replace(",", "").replace("$", "").strip())
    new_bal = scraped_balance + add
    update_nav_value("cm2uauagx000109tiw2seaoiao", new_bal)

# ─── Google Sheets helpers ─────────────────────────────────────────────────────

def get_gspread_client(json_keyfile: str = None) -> gspread.Client:
    """
    Inicializa y devuelve un cliente de gspread usando la cuenta de servicio.
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
    Abre la hoja `worksheet_name` dentro del spreadsheet `spreadsheet_name`.
    """
    client = get_gspread_client()
    sh = client.open(spreadsheet_name)
    return sh.worksheet(worksheet_name)


def read_sheet_cell(worksheet_name: str, cell_address: str) -> str | None:
    """
    Lee el valor de una celda (p. ej. "E6") en la hoja `worksheet_name`.
    """
    try:
        ws = open_worksheet(SPREADSHEET, worksheet_name)
        return ws.acell(cell_address).value
    except Exception as e:
        print(f"❌ Error leyendo {worksheet_name}!{cell_address}: {e}")
        return None


def update_user_tokens(cell_address: str, user_id: str):
    """
    Lee el valor de la celda (por ejemplo, "E6") en la hoja "Clients", lo convierte a float,
    y actualiza `User.tokens` en MySQL para el `user_id` dado.
    """
    cell = read_sheet_cell("Clients", cell_address)
    if not cell:
        print(f"⚠️ Celda Clients!{cell_address} vacía; omitiendo.")
        return
    try:
        val = float(cell.replace(",", "").replace("$", "").strip())
    except ValueError:
        print(f"⚠️ No pude parsear el valor '{cell}' de Clients!{cell_address}.")
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
    """
    Llama al endpoint REST de Zerion para obtener el net worth (USD) de una dirección,
    incluyendo staking (ETH2, Lido, etc.). Retorna el valor total en USD como float.
    Lanza excepción si hay error HTTP o parseo.
    """
    url = f"https://api.zerion.io/v1/wallets/{address}/portfolio/"
    params = {
        "currency": "usd",
        "filter[positions]": "no_filter"
    }
    headers = {
        "Accept": "application/json"
    }
    auth = HTTPBasicAuth(ZERION_API_KEY, "")

    response = requests.get(url, headers=headers, auth=auth, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"Error HTTP {response.status_code} de Zerion: {response.text}")

    data = response.json()
    try:
        # total.positions incluye tokens, apps y ETH2/staking
        return float(data["data"]["attributes"]["total"]["positions"] or 0.0)
    except (KeyError, TypeError) as e:
        raise RuntimeError(f"No se pudo parsear respuesta de Zerion: {e}\nRespuesta completa: {data}")

# ─── Google Sheets – RAW updater ───────────────────────────────────────────────

def update_google_sheet(data: list[tuple[int, float | None, str]]):
    """
    Recibe una lista de tuplas (fundid, balance, timestamp) y las anexa en RAW.
    """
    try:
        ws = open_worksheet(SPREADSHEET, "RAW")
    except Exception as e:
        print(f"❌ Error abriendo sheet RAW: {e}")
        return

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
        # El endpoint devuelve el balance en Gwei; dividimos por 1e9 para pasarlo a ETH.
        balance_eth = balance_gwei / 1e9
        print(f"🔍 Validator balance: {balance_eth:.9f} ETH")
        return balance_eth
    except Exception as e:
        print(f"❌ Error fetching validator balance: {e}")
        return None


def update_validator_sheet(balance_eth: float):
    """
    Escribe el balance del validador (balance_eth) en la hoja RAW con etiqueta 'ETH2'.
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
    # 1) Obtener balances desde Zerion (net worth en USD) para cada (address, fundid)
    results: list[tuple[int, float | None, str]] = []
    for address, fundid in URL_FUNDID_LIST:
        try:
            net_worth = fetch_net_worth_zerion(address)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"🔍 Zerion[{address}] = ${net_worth:,.2f} (a las {ts})")
            results.append((fundid, net_worth, ts))
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"❌ Falló la consulta a Zerion[{address}]: {e}")
            results.append((fundid, None, ts))
            continue

        # Actualizaciones en MySQL según fundid
        if fundid == FUNDID_YIELD:
            update_mysql_balance(net_worth)
        elif fundid == FUNDID_ASSETS:
            update_nav_value("bck_assets_2", net_worth)

    # 2) Consulta del validador Beacon (ETH2)
    val_bal = fetch_validator_balance()
    if val_bal is not None:
        update_validator_sheet(val_bal)
        update_nav_value("bck_assets_2_ETH2", val_bal)

    # 3) Escribo todos los resultados de Zerion en RAW y actualizo tokens de usuarios
    update_google_sheet(results)

    # 4) Actualizo tokens específicos de usuarios (Clients!E6 -> BLCA_10005; Clients!E5 -> BLCY_10006)
    update_user_tokens("E6", "BLCA_10005")
    update_user_tokens("E5", "BLCY_10006")

    print("\n🟢 Proceso completado.")

