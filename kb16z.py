#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import time
import random
from datetime import datetime
from contextlib import contextmanager

import requests
from requests.auth import HTTPBasicAuth

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.proxy import Proxy, ProxyType

import gspread
from google.oauth2.service_account import Credentials

import mysql.connector
from mysql.connector import Error

# ─── Cargar variables de entorno ────────────────────────────────────────────────
load_dotenv()

# ─── Configuración general ──────────────────────────────────────────────────────

# Nombre de la planilla de Google Sheets (asegúrate de compartirla con la cuenta de servicio)
SPREADSHEET_EXTRA = os.getenv("SPREADSHEET_EXTRA", "KB CAPITAL NEW")

# Ruta a geckodriver (si luego quieres usar Selenium)
GECKO_PATH = os.getenv("GECKO_PATH", "/usr/local/bin/geckodriver")

# Ruta al archivo de credenciales de Google Service Account
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")

# ID de tu fondo / proyecto, para insertar en Google Sheets
FUNDID_EXTRA = 1

# Clave de Zerion (debe estar en tu .env como ZERION_API_KEY=tu_api_key)
ZERION_API_KEY = os.getenv("ZERION_API_KEY")
if not ZERION_API_KEY:
    raise RuntimeError("❌ Falta la variable de entorno ZERION_API_KEY")

# ─── Funciones de MySQL ──────────────────────────────────────────────────────────

@contextmanager
def mysql_connection():
    """
    Context manager para conectarse a la base de datos MySQL.
    Cierra la conexión automáticamente al terminar.
    """
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
    """
    Actualiza el campo Nav.value en MySQL para el registro especificado.
    """
    sql = "UPDATE `Nav` SET `value` = %s WHERE `id` = %s"
    with mysql_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (value, record_id))
        conn.commit()
        cur.close()
    print(f"✅ MySQL Nav[{record_id}] = {value:,.2f}")

# ─── Función para inicializar cliente de Google Sheets ──────────────────────────

def get_gspread_client(keyfile: str = None) -> gspread.Client:
    """
    Inicializa y devuelve un cliente de gspread autorizado con la cuenta de servicio.
    """
    keyfile = keyfile or GOOGLE_CREDS
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    return gspread.authorize(creds)

# ─── Función para obtener net worth desde Zerion ─────────────────────────────────

def fetch_net_worth_zerion(address: str) -> float:
    """
    Llama al endpoint REST de Zerion para obtener el net worth (suma de tokens, apps y NFTs)
    de una dirección, incluyendo staking (ETH2, Lido, etc.).
    Retorna el valor total en USD como float. Lanza excepción si hay error HTTP o respuesta mal formada.
    """
    url = f"https://api.zerion.io/v1/wallets/{address}/portfolio/"
    params = {
        "currency": "usd",
        "filter[positions]": "no_filter"
    }
    headers = {
        "Accept": "application/json"
    }
    # Zerion usa autenticación Basic Auth: API key como usuario, contraseña vacía.
    auth = HTTPBasicAuth(ZERION_API_KEY, "")

    response = requests.get(url, headers=headers, auth=auth, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"Error HTTP {response.status_code} de Zerion: {response.text}")

    data = response.json()
    try:
        # El net worth total se encuentra en data["data"]["attributes"]["total"]["positions"]
        return float(data["data"]["attributes"]["total"]["positions"] or 0.0)
    except (KeyError, TypeError) as e:
        raise RuntimeError(f"No se pudo parsear la respuesta de Zerion: {e}\nRespuesta completa: {data}")

# ─── (Opcional) Funciones relacionadas a Selenium ────────────────────────────────
# Si necesitas de aquí en adelante scrapear con Selenium, puedes reutilizar tu código.
# Por ahora lo dejamos fuera del “main” para enfocarnos en Zerion + MySQL + Google Sheets.

# def create_stealth_firefox_driver( … ):
#     …

# def scrape_balance(url_extra: str) -> (float, str):
#     …

# ─── Bloque principal ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Dirección que queremos consultar en Zerion
    address = "0xaD920e4870b3Ff09D56261F571955990200b863e"

    # 2. Obtenemos el net worth (USD) desde Zerion
    try:
        net_worth = fetch_net_worth_zerion(address)
    except Exception as e:
        print(f"❌ Falló la consulta a Zerion: {e}")
        exit(1)

    # 3. Creamos un timestamp (por ejemplo: "2025-06-06 10:15:42")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 4. Actualizamos el valor en MySQL
    #    Aquí “kbi_1” es el id en tu tabla Nav que quieres actualizar
    try:
        update_nav_value("kbi_1", net_worth)
    except Exception as e:
        print(f"❌ Error al actualizar MySQL: {e}")
        exit(1)

    # 5. Actualizamos la Google Sheet agregando una fila en la pestaña "RAW"
    try:
        client = get_gspread_client()
        ws = client.open(SPREADSHEET_EXTRA).worksheet("RAW")
        # La fila que insertamos: [FUNDID_EXTRA, valor, timestamp]
        ws.append_row([FUNDID_EXTRA, net_worth, ts])
        print(f"✅ Google Sheet '{SPREADSHEET_EXTRA}' RAW actualizada para fundid {FUNDID_EXTRA}.")
    except Exception as e:
        print(
            f"⚠️ No se pudo abrir o actualizar la planilla '{SPREADSHEET_EXTRA}'. "
            f"Asegúrate de compartirla con la cuenta de servicio. Error: {e}"
        )

    # 6. Si luego quieres usar Selenium para otras cosas, lo harías aquí.
    #    Ejemplo (comentado):
    # driver = create_stealth_firefox_driver(headless=True, proxy_address=None)
    # bal_selenium, ts_selenium = scrape_balance(URL_EXTRA)
    # … etc …

    print(f"\nProceso completado a las {ts}. Net worth = ${net_worth:,.2f}")

