from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_debank_total(address: str, timeout: int = 15) -> str:
    url = f"https://debank.com/profile/{address}"

    options = Options()
    options.headless = True
    # PON AQUÍ la ruta de tu firefox (detectada con `which firefox`)
    options.binary_location = "/usr/bin/firefox"

    # Y la ruta de geckodriver (detectada con `which geckodriver`)
    service = Service(executable_path="/usr/local/bin/geckodriver", log_path="/tmp/geckodriver.log")

    driver = webdriver.Firefox(service=service, options=options)
    try:
        driver.get(url)
        total_el = WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, "span[class^='PortfolioHeader_net_asset_value__']")
            )
        )
        return total_el.text
    finally:
        driver.quit()

if __name__ == "__main__":
    addr = "0xad920e4870b3ff09d56261f571955990200b863e"
    print("Total:", get_debank_total(addr))

