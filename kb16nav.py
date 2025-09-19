#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
kb16nav.py
Devuelve el NAV (total_usd_value) del address ADDRESS_KB usando DeBank Cloud,
sumado al valor de la celda D4 en la hoja "NAV" de la planilla indicada.

Uso desde FastAPI:
  from kb16nav import get_kb_nav as get_nav_min, DeBankAPIError as DeBankAPIErrorKB

CLI:
  python kb16nav.py   -> imprime JSON con {"address", "total_usd_value"}

ENV requeridas:
  - DEBANK_ACCESS_KEY
  - ADDRESS_KB
  - GOOGLE_CREDS (default: "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")
  - SPREADSHEET_EXTRA (default: "KB CAPITAL NEW")   # abrir por nombre
  - SPREADSHEET_EXTRA_ID (opcional, recomendado)    # abrir por ID (evita scope de Drive)

Dependencias:
  - pip install requests python-dotenv gspread google-auth
"""

import os
import sys
import json
import time
from typing import Dict, Any, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

DEBANK_BASE = "https://pro-openapi.debank.com"
ENDPOINT = "/v1/user/total_balance"

# Google Sheets (desde entorno, con defaults)
SPREADSHEET_EXTRA = os.getenv("SPREADSHEET_EXTRA", "KB CAPITAL NEW")
SPREADSHEET_EXTRA_ID = os.getenv("SPREADSHEET_EXTRA_ID")  # si está, usamos open_by_key
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS", "/home/benzarod/blck/blck-456015-02d4cfc790b7.json")

WORKSHEET_NAME = "NAV"
CELL = "D4"


class DeBankAPIError(Exception):
    """Errores al consultar DeBank o precondiciones faltantes."""
    pass


def _get_total_balance(address: str, access_key: str, timeout: int = 15) -> Dict[str, Any]:
    url = f"{DEBANK_BASE}{ENDPOINT}"
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


def _gspread_client():
    """
    Si hay SPREADSHEET_EXTRA_ID -> sólo 'spreadsheets.readonly'.
    Si abrimos por nombre -> también 'drive.readonly' (gspread busca por Drive).
    """
    if not GOOGLE_CREDS:
        raise RuntimeError("Falta GOOGLE_CREDS (ruta al JSON de la cuenta de servicio).")

    if SPREADSHEET_EXTRA_ID:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    else:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

    creds = Credentials.from_service_account_file(GOOGLE_CREDS, scopes=scopes)
    return gspread.authorize(creds)


def _open_worksheet(client) -> gspread.Worksheet:
    if SPREADSHEET_EXTRA_ID:
        sh = client.open_by_key(SPREADSHEET_EXTRA_ID)
    else:
        sh = client.open(SPREADSHEET_EXTRA)
    return sh.worksheet(WORKSHEET_NAME)


def _get_sheet_value() -> float:
    """Lee D4 en hoja NAV."""
    client = _gspread_client()
    ws = _open_worksheet(client)
    raw = ws.acell(CELL).value
    if raw is None:
        return 0.0
    txt = str(raw).strip().replace(",", "")  # remueve separador de miles
    try:
        return float(txt)
    except Exception:
        return 0.0


def get_total_sum() -> float:
    """Retorna (DeBank total_usd_value) + (Sheet D4)."""
    access_key = os.getenv("DEBANK_ACCESS_KEY")
    address_kb = os.getenv("ADDRESS_KB")
    if not access_key or not address_kb:
        raise DeBankAPIError("Faltan variables de entorno: DEBANK_ACCESS_KEY o ADDRESS_KB")

    data = _get_total_balance(address_kb, access_key)
    nav_value = float(data.get("total_usd_value") or 0.0)
    sheet_val = _get_sheet_value()
    return nav_value + sheet_val


def get_kb_nav() -> Dict[str, Any]:
    """
    Retorna JSON minimal compatible con tu FastAPI:
    {
      "address": "<ADDRESS_KB>",
      "total_usd_value": <float>  # suma (DeBank + D4)
    }
    """
    address_kb = os.getenv("ADDRESS_KB")
    if not address_kb:
        raise DeBankAPIError("Falta ADDRESS_KB en el entorno")

    total_sum = get_total_sum()
    return {
        "address": address_kb,
        "total_usd_value": total_sum,
    }


if __name__ == "__main__":
    try:
        out = get_kb_nav()
        print(json.dumps(out, ensure_ascii=False, indent=2))
    except Exception as e:
        print(json.dumps({"error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)

