#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
from datetime import datetime
from contextlib import contextmanager

import requests
import gspread
from google.oauth2.service_account import Credentials

import mysql.connector
from mysql.connector import Error

# ─── Cargar variables de entorno ────────────────────────────────────────────────
load_dotenv()

# ─── Configuración general ──────────────────────────────────────────────────────

SPREADSHEET_EXTRA = os.getenv("SPREADSHEET_EXTRA", "KB CAPITAL NEW")
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")

# ID de tu fondo / proyecto
FUNDID_EXTRA = 1

# Clave de DeBank (debe estar en tu .env como DEBANK_ACCESS_KEY=tu_api_key)
DEBANK_ACCESS_KEY = os.getenv("DEBANK_ACCESS_KEY")
if not DEBANK_ACCESS_KEY:
    raise RuntimeError("❌ Falta la variable de entorno DEBANK_ACCESS_KEY")

# ─── Funciones de MySQL ──────────────────────────────────────────────────────────

@contextmanager
def mysql_connection():
    """Context manager para conectarse a la base de datos MySQL."""
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
    """Actualiza el campo Nav.value en MySQL para el registro especificado."""
    sql = "UPDATE `Nav` SET `value` = %s WHERE `id` = %s"
    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (value, record_id))
        conn.commit()
        cur.close()
    print(f"✅ MySQL Nav[{record_id}] = {value:,.2f}")

# ─── Función para inicializar cliente de Google Sheets ──────────────────────────

def get_gspread_client(keyfile: str = None) -> gspread.Client:
    keyfile = keyfile or GOOGLE_CREDS
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    return gspread.authorize(creds)

# ─── Función para obtener net worth desde DeBank ────────────────────────────────

def fetch_net_worth_debank(address: str) -> float:
    """
    Llama al endpoint de DeBank Cloud para obtener el net worth de una address.
    Retorna el valor total en USD como float.
    """
    url = f"https://pro-openapi.debank.com/v1/user/total_balance?id={address}"
    headers = {
        "accept": "application/json",
        "AccessKey": DEBANK_ACCESS_KEY
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f"Error HTTP {response.status_code} de DeBank: {response.text}")

    data = response.json()
    try:
        return float(data.get("total_usd_value", 0.0))
    except (KeyError, TypeError) as e:
        raise RuntimeError(f"No se pudo parsear la respuesta de DeBank: {e}\nRespuesta completa: {data}")

# ─── Bloque principal ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Dirección que queremos consultar en DeBank
    address = "0xaD920e4870b3Ff09D56261F571955990200b863e"

    # 2. Obtenemos el net worth (USD) desde DeBank
    try:
        net_worth = fetch_net_worth_debank(address)
    except Exception as e:
        print(f"❌ Falló la consulta a DeBank: {e}")
        exit(1)

    # 3. Creamos un timestamp
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 4. Actualizamos el valor en MySQL
    try:
        update_nav_value("kbi_1", net_worth)
    except Exception as e:
        print(f"❌ Error al actualizar MySQL: {e}")
        exit(1)

    # 5. Actualizamos la Google Sheet
    try:
        client = get_gspread_client()
        ws = client.open(SPREADSHEET_EXTRA).worksheet("RAW")
        ws.append_row([FUNDID_EXTRA, net_worth, ts])
        print(f"✅ Google Sheet '{SPREADSHEET_EXTRA}' RAW actualizada para fundid {FUNDID_EXTRA}.")
    except Exception as e:
        print(f"⚠️ No se pudo abrir o actualizar la planilla '{SPREADSHEET_EXTRA}'. Error: {e}")

    print(f"\nProceso completado a las {ts}. Net worth = ${net_worth:,.2f}")

