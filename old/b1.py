#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import time
import random
from datetime import datetime
from contextlib import contextmanager

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

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

# Nombre de la planilla de Google Sheets (asegúrate de compartirla con la cuenta de servicio)
SPREADSHEET_EXTRA = os.getenv("SPREADSHEET_EXTRA", "KB CAPITAL NEW")
# Ruta a geckodriver
GECKO_PATH       = os.getenv("GECKO_PATH", "/usr/local/bin/geckodriver")
# Ruta al archivo de credenciales de Google
GOOGLE_CREDS     = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")

# URL de Debank y ID de fondo
URL_EXTRA   = "https://debank.com/profile/0xad920e4870b3ff09d56261f571955990200b863e"
FUNDID_EXTRA = 1

# ─── MySQL helper ──────────────────────────────────────────────────────────────
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
    print(f"✅ MySQL Nav[{record_id}] = {value}")

# ─── Google Sheets helper ────────────────────────────────────────────────────
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

# ─── Función para crear un driver “stealth” de Firefox ─────────────────────────
def create_stealth_firefox_driver(headless: bool = True, proxy_address: str = None) -> webdriver.Firefox:
    """
    Crea y devuelve un objeto Firefox WebDriver configurado para no ser detectado como Selenium.
    - headless: si es True, ejecuta Firefox en modo headless (sin interfaz).
    - proxy_address: si no es None, debe tener la forma "ip:puerto" para enrutar el tráfico.
    """
    # 1. Creamos un perfil de Firefox con ajustes para disimular Selenium
    profile = FirefoxProfile()
    profile.set_preference("dom.webdriver.enabled", False)
    profile.set_preference("useAutomationExtension", False)
    profile.set_preference("media.peerconnection.enabled", False)
    profile.set_preference("webdriver.assume_untrusted_issuer", False)

    # 1.1. Elegimos un User-Agent real de forma aleatoria
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:114.0) Gecko/20100101 Firefox/114.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:113.0) Gecko/20100101 Firefox/113.0",
        # Agrega más User-Agents si quieres ampliar
    ]
    fake_ua = random.choice(user_agents)
    profile.set_preference("general.useragent.override", fake_ua)

    # 2. Asignamos el perfil a las opciones de Firefox
    opts = FirefoxOptions()
    opts.profile = profile

    # ─── CAMBIO PRINCIPAL: forzamos headless=True para evitar fallos si no hay display ───
    if headless:
        opts.add_argument("--headless")
        # Algunas veces conviene añadir:
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        # Con estas flags nos aseguramos de que Firefox no intente abrir ventana gráfica

    # 3. Configuramos el servicio de geckodriver
    service = Service(GECKO_PATH)

    # 4. Si se solicita un proxy, lo añadimos a las capacidades
    if proxy_address:
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': proxy_address,
            'sslProxy': proxy_address
        })
        caps = webdriver.DesiredCapabilities.FIREFOX.copy()
        proxy.add_to_capabilities(caps)
        driver = webdriver.Firefox(service=service, options=opts, capabilities=caps)
    else:
        driver = webdriver.Firefox(service=service, options=opts)

    # 5. Inyectamos un pequeño script para que navigator.webdriver sea undefined
    driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """)
    return driver

# ─── Debank scraping ───────────────────────────────────────────────────────────
def scrape_balance(url: str) -> tuple[float, str] | tuple[None, None]:
    """
    Abre Debank en el navegador “stealth” (modo headless), espera a que cargue el balance,
    simula interacciones humanas, extrae el valor del balance y el timestamp.
    Retorna (balance, timestamp) o (None, None) en caso de error.
    """
    # Si deseas rotar IPs, asigna aquí "ip:puerto"; si no, déjalo en None.
    proxy_ip = None

    # Inicia el driver stealth en modo headless (para que no intente abrir ventana gráfica)
    driver = create_stealth_firefox_driver(headless=True, proxy_address=proxy_ip)

    try:
        driver.get(url)

        # Esperamos hasta que el elemento del balance esté presente
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".HeaderInfo_totalAssetInner__HyrdC"))
        )

        # Encontramos el elemento y hacemos scroll (aunque esté headless, esto “fuerza” la carga completa)
        element = driver.find_element(By.CSS_SELECTOR, ".HeaderInfo_totalAssetInner__HyrdC")
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
            element
        )
        # Pausa breve después del scroll
        time.sleep(random.uniform(0.5, 1.5))

        # Movimiento de mouse aleatorio para simular navegación humana (opcional,
        # aunque en modo headless no se ve, ayuda al “skinscreen” de algunos sitios)
        try:
            actions = ActionChains(driver)
            width = driver.execute_script("return window.innerWidth")
            height = driver.execute_script("return window.innerHeight")
            x_offset = random.randint(int(width * 0.2), int(width * 0.8))
            y_offset = random.randint(int(height * 0.2), int(height * 0.8))
            actions.move_by_offset(x_offset, y_offset).perform()
            time.sleep(random.uniform(0.2, 0.6))
            actions.move_by_offset(-x_offset, -y_offset).perform()
        except Exception:
            # Si algo falla en el movimiento, continuamos sin interrupción
            pass

        # Espera adicional aleatoria antes de extraer el texto
        time.sleep(random.uniform(5.0, 12.0))

        # Extraemos el texto del balance (e.g. "$12,345.67")
        text = element.text
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
    # 1. Llamamos a scrape_balance para obtener balance y timestamp
    bal, ts = scrape_balance(URL_EXTRA)
    if bal is None:
        exit(1)

    # 2. Actualizamos el valor en MySQL
    update_nav_value("kbi_1", bal)

    # 3. Actualizamos la Google Sheet agregando una fila en la pestaña "RAW"
    client = get_gspread_client()
    try:
        ws = client.open(SPREADSHEET_EXTRA).worksheet("RAW")
        ws.append_row([FUNDID_EXTRA, bal, ts])
        print(f"✅ Google Sheet '{SPREADSHEET_EXTRA}' RAW actualizada para fundid {FUNDID_EXTRA}.")
    except Exception as e:
        print(
            f"⚠️ No se pudo abrir la planilla '{SPREADSHEET_EXTRA}'. "
            f"Asegúrate de compartirla con la cuenta de servicio. Error: {e}"
        )

