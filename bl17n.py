#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
kb16nav.py
Calcula el NAV total (DeBank total_usd_value + opcional valor del validador ETH→USDT)
y **devuelve SOLO**:
{"nav_total_usd": <float|null>}

Requiere:
  - ENV: DEBANK_ACCESS_KEY, ADDRESS_BCK
  - Opcional: VALIDATOR_URL o VALIDATOR_ID, ETH_TICKER_SYMBOL (default ETHUSDT)
  - pip install requests python-dotenv
"""

import os
import sys
import json
import time
from typing import Dict, Any, Optional
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

# --- Configs ---
DEBANK_BASE = "https://pro-openapi.debank.com"
DEBANK_ENDPOINT = "/v1/user/total_balance"
BEACON_API_BASE = "https://beaconcha.in/api/v1/validator/"
BINANCE_TICKER_BASE = "https://api.binance.com/api/v3/ticker/price"


class DeBankAPIError(Exception):
    pass


def _get_with_retries(url: str, timeout: int = 15, attempts: int = 4) -> Dict[str, Any]:
    backoff = 1.5
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** i)
                continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if i == attempts - 1:
                raise
            time.sleep(backoff ** i)
    raise RuntimeError(f"Request failed: {last_err}")


def _get_total_balance_debank(address: str, access_key: str, timeout: int = 15) -> Dict[str, Any]:
    url = f"{DEBANK_BASE}{DEBANK_ENDPOINT}"
    headers = {"Accept": "application/json", "AccessKey": access_key}
    params = {"id": address}

    backoff = 1.5
    attempts = 4
    last_err: Optional[Exception] = None

    for i in range(attempts):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** i)
                continue
            else:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                raise DeBankAPIError(f"HTTP {resp.status_code}: {detail}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            time.sleep(backoff ** i)

    if last_err:
        raise DeBankAPIError(f"Request failed after retries: {last_err}")
    raise DeBankAPIError("Request failed after retries (unknown error)")


def _extract_validator_id_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0].lower() == "validator":
        return parts[1]
    raise ValueError("VALIDATOR_URL inválida: esperaba .../validator/<pubkey|index>")


def _fetch_validator_balance_eth(validator_id: str) -> float:
    payload = _get_with_retries(BEACON_API_BASE + validator_id)
    data = payload.get("data", {})
    gwei = None
    if isinstance(data, dict):
        if "balance" in data:
            gwei = float(data["balance"])
        elif isinstance(data.get("validator"), dict) and "balance" in data["validator"]:
            gwei = float(data["validator"]["balance"])
    if gwei is None:
        raise KeyError("No se encontró 'balance' en la respuesta de beaconcha.in")
    return gwei / 1e9  # Gwei -> ETH


def _fetch_eth_usdt_price(symbol: Optional[str] = None) -> float:
    sym = symbol or os.getenv("ETH_TICKER_SYMBOL", "ETHUSDT")
    payload = _get_with_retries(f"{BINANCE_TICKER_BASE}?symbol={sym}")
    return float(payload["price"])  # USDT por 1 ETH


def get_kb_nav() -> Dict[str, Any]:
    """
    Devuelve dict con:
      - total_usd_value (DeBank)
      - validator_usdt (opcional)
      - nav_total_usd = total_usd_value + validator_usdt
    """
    access_key = os.getenv("DEBANK_ACCESS_KEY")
    address_bck = os.getenv("ADDRESS_BCK")
    if not access_key or not address_bck:
        raise DeBankAPIError("Faltan variables de entorno: DEBANK_ACCESS_KEY o ADDRESS_BCK")

    # 1) DeBank total_usd_value
    debank = _get_total_balance_debank(address_bck, access_key)
    total_usd = debank.get("total_usd_value")

    # 2) (Opcional) Validador ETH -> USDT
    validator_url = os.getenv("VALIDATOR_URL")
    validator_id = os.getenv("VALIDATOR_ID")
    val_usdt = None
    try:
        if validator_url or validator_id:
            vid = _extract_validator_id_from_url(validator_url) if validator_url else validator_id
            val_balance_eth = _fetch_validator_balance_eth(vid)
            eth_usdt = _fetch_eth_usdt_price()
            val_usdt = val_balance_eth * eth_usdt
    except Exception:
        val_usdt = None  # no romper si falla

    # 3) NAV final
    nav_total_usd = None
    if total_usd is not None:
        nav_total_usd = total_usd + (val_usdt or 0.0)

    return {"nav_total_usd": nav_total_usd}


if __name__ == "__main__":
    try:
        out = get_kb_nav()
        # Solo nav_total_usd
        print(json.dumps({"nav_total_usd": out.get("nav_total_usd")}, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": str(e)}, ensure_ascii=False))
        sys.exit(1)

