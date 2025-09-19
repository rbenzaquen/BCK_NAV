#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import time
import random
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.proxy import Proxy, ProxyType

# ─── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

# Ruta a geckodriver (puede venir de tu .env o usar un valor por defecto)
GECKO_PATH = os.getenv("GECKO_PATH", "/usr/local/bin/geckodriver")

# URL a scrapear
URL_TO_SCRAPE = "https://coinstats.app/address/0xaD920e4870b3Ff09D56261F571955990200b863e/"

# Selector CSS del elemento que contiene el precio del portafolio
PRICE_CSS_SELECTOR = ".PortfolioPriceInfo_PT-price-info_price__yirGm"

# ─── Función para crear un driver “stealth” de Firefox ─────────────────────────
def create_stealth_firefox_driver(headless: bool = True, proxy_address: str = None) -> webdriver.Firefox:
    """
    Crea y devuelve un objeto Firefox WebDriver configurado para no ser detectado como Selenium.
      - headless: si es True, ejecuta Firefox en modo headless (sin interfaz).
      - proxy_address: si no es None, debe tener la forma "ip:puerto" para enrutar el tráfico.
    """
    # 1. Creamos un perfil de Firefox con ajustes para disfrazar Selenium
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
        # Puedes añadir más User-Agents actualizados aquí
    ]
    fake_ua = random.choice(user_agents)
    profile.set_preference("general.useragent.override", fake_ua)

    # 2. Asignamos el perfil a las opciones de Firefox
    opts = FirefoxOptions()
    opts.profile = profile

    # ─── Ejecución en modo headless para evitar fallos en entornos sin display ───
    if headless:
        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")

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

# ─── Función que hace el scraping del precio ─────────────────────────────────
def scrape_portfolio_price(url: str, css_selector: str) -> tuple[str, str] | tuple[None, None]:
    """
    Abre la URL especificada en un navegador “stealth” (modo headless), espera a que cargue el
    elemento que coincide con css_selector, simula interacciones humanas, extrae el texto y
    retorna (precio_en_texto, timestamp). Si hay error, retorna (None, None).
    """
    # Si deseas rotar IPs, asigna aquí "ip:puerto"; si no, déjalo en None
    proxy_ip = None

    # Inicia el driver stealth en modo headless
    driver = create_stealth_firefox_driver(headless=True, proxy_address=proxy_ip)

    try:
        driver.get(url)

        # 1) Esperamos hasta que el elemento con el precio esté presente en el DOM
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )

        # 2) Localizamos el elemento y hacemos scroll suave hacia él
        element = driver.find_element(By.CSS_SELECTOR, css_selector)
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
            element
        )
        # Pausa breve después del scroll
        time.sleep(random.uniform(0.5, 1.5))

        # 3) Simulamos un movimiento de mouse pseudo-aleatorio (opcional)
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
            # Si falla el movimiento de mouse, no interrumpe el scraping
            pass

        # 4) Espera adicional aleatoria para simular lectura humana
        time.sleep(random.uniform(3.0, 8.0))

        # 5) Extraemos el texto del elemento (por ejemplo: "$1,234.56")
        price_text = element.text.strip()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"URL: {url}\nPrice Text: {price_text}\nTimestamp: {timestamp}\n")
        return price_text, timestamp

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None, None

    finally:
        driver.quit()

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    price, ts = scrape_portfolio_price(URL_TO_SCRAPE, PRICE_CSS_SELECTOR)
    if price is None:
        print("No se pudo obtener el precio.")
        exit(1)
    # Aquí podrías agregar lógica extra, por ejemplo, guardar en base de datos o Google Sheets

