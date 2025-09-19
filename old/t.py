from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service
import os

GECKO_PATH = os.getenv("GECKO_PATH", "/usr/local/bin/geckodriver")

opts = FirefoxOptions()
opts.add_argument("--headless")                 # o coméntalo si quieres ver la ventana
opts.binary_location = "/usr/bin/firefox"       # AJUSTA según tu SO

service = Service(GECKO_PATH)
driver = webdriver.Firefox(service=service, options=opts)
print("Firefox arrancó correctamente. Capabilities:")
print(driver.capabilities)
driver.quit()

