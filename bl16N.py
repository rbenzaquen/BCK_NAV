#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lee VALIDATOR_URL o VALIDATOR_ID desde .env y devuelve JSON con:
{
  "validator_id": "<pubkey|index>",
  "balance_eth": <float>,
  "eth_usdt": <float>,
  "balance_usdt": <float>
}
"""

import os
import json
import time
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

BEACON_API_BASE = "https://beaconcha.in/api/v1/validator/"
BINANCE_BASE = "https://api.binance.com/api/v3/ticker/price"

def _extract_validator_id_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0].lower() == "validator":
        return parts[1]
    raise ValueError("VALIDATOR_URL inválida: esperaba .../validator/<pubkey|index>")

def _get_with_retries(url: str, timeout: int = 15, attempts: int = 4):
    backoff = 1.5
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** i); continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last = e
            if i == attempts - 1: raise
            time.sleep(backoff ** i)
    raise RuntimeError(f"Request failed: {last}")

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

def _fetch_eth_usdt_price(symbol: str = None) -> float:
    sym = symbol or os.getenv("ETH_TICKER_SYMBOL", "ETHUSDT")
    payload = _get_with_retries(f"{BINANCE_BASE}?symbol={sym}")
    return float(payload["price"])

def get_validator_usdt_from_env():
    url = os.getenv("VALIDATOR_URL")
    validator_id = os.getenv("VALIDATOR_ID")

    if url:
        validator_id = _extract_validator_id_from_url(url)
    if not validator_id:
        raise ValueError("Falta VALIDATOR_URL o VALIDATOR_ID en .env")

    balance_eth = _fetch_validator_balance_eth(validator_id)
    eth_usdt = _fetch_eth_usdt_price()
    return {
        "validator_id": validator_id,
        "balance_eth": balance_eth,
        "eth_usdt": eth_usdt,
        "balance_usdt": balance_eth * eth_usdt
    }

if __name__ == "__main__":
    try:
        out = get_validator_usdt_from_env()
        print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        raise

