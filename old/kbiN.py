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

# ─── Debank scraping (función corregida) ───────────────────────────────────────────
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def scrape_balance(url: str) -> tuple[float, str] | tuple[None, None]:
    """
    Intenta obtener el balance total de Debank para la dirección (URL_EXTRA).
    - Usa varios selectores (CSS/XPath) de forma secuencial para mayor robustez.
    - Espera explícitamente a que el <body> cargue y a que aparezca un valor con '$'.
    - Imprime un fragmento de HTML para debug si no encuentra ningún match.
    """
    # 1) Configurar Firefox headless con user agent real
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )

    service = Service(GECKO_PATH)
    driver = webdriver.Firefox(service=service, options=opts)

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 30)

        # 2) Esperar a que el <body> esté presente
        try:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            print("❌ Timeout al cargar <body> en 30s")
            return None, None

        # 3) Cerrar overlay de cookies si aparece (click en botón que contenga "aceptar")
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(),'ACEPTAR','aceptar'),'aceptar')]"
            )
            cookie_btn.click()
            time.sleep(1)
        except NoSuchElementException:
            pass
        except Exception:
            pass

        # 4) Intentar extraer el texto del balance con varios selectores
        text = None
        posibles_selectores = [
            # Selector CSS conocido (puede cambiar según versión de Debank)
            (By.CSS_SELECTOR, ".HeaderInfo_totalAssetInner__HyrdC"),
            # XPath que busque algún label con "Total Assets" y obtenga el sibling
            (By.XPATH, "//div[contains(text(),'Total Assets')]/following-sibling::div"),
            # XPath genérico: cualquier <div> que contenga "$"
            (By.XPATH, "//div[contains(text(),'$')]"),
        ]

        for by, selector in posibles_selectores:
            try:
                elem = wait.until(EC.visibility_of_element_located((by, selector)))
                contenido = elem.text.strip()
                if "$" in contenido:
                    text = contenido
                    break
            except TimeoutException:
                continue
            except Exception as e_sel:
                print(f"❌ Error con selector ({by}, '{selector}'): {e_sel}")
                continue

        # 5) Si no se obtuvo texto válido, mostrar fragmento de HTML para debugging
        if not text:
            print("❌ No encontré ningún elemento con '$' tras probar múltiples selectores.")
            snippet = driver.page_source[:1000].replace("\n", " ")
            print("=== HTML devuelto (primeros 1000 chars) ===")
            print(snippet)
            print("===========================================")
            return None, None

        # 6) Parsear el texto obtenido (ej.: "US$ 1,234.56" o "$1,234.56")
        num_str = text.replace("US$", "").replace("$", "").replace(",", "").strip()
        try:
            bal = float(num_str)
        except ValueError:
            print(f"❌ No pude parsear '{text}' a float.")
            return None, None

        # 7) Generar timestamp
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"URL: {url}\nBalance: {bal}\nTimestamp: {ts}\n")
        return bal, ts

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        snippet = driver.page_source[:500].replace("\n", " ")
        print("=== HTML parcial en excepción ===")
        print(snippet)
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
