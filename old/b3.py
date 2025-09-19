#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests

def get_total_assets(address: str, api_key: str) -> float | None:
    """
    Llama al endpoint /v1/wallets/{address}/portfolio/?currency=usd
    y devuelve el total de posiciones (attributes.total.positions) como float.
    Retorna None si ocurre algún error en la petición o parsing.
    """
    url = f"https://api.zerion.io/v1/wallets/{address}/portfolio/"
    headers = {
        "Accept": "application/json"
    }
    # Autenticación usando Basic Auth: usuario = api_key, contraseña = ""
    auth = (api_key, "")

    # Incluimos el parámetro de query para pedir USD
    params = {"currency": "usd"}

    try:
        resp = requests.get(url, headers=headers, auth=auth, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error en la petición HTTP: {e}")
        return None

    try:
        data = resp.json()
        # Ruta en JSON: data → data → attributes → total → positions
        total_positions = data["data"]["attributes"]["total"]["positions"]
        return float(total_positions)
    except (KeyError, ValueError, TypeError) as e:
        print(f"❌ No se pudo extraer el valor total de la respuesta: {e}")
        return None

if __name__ == "__main__":
    # 1) Leemos la dirección y la API key desde entorno o argumentos
    if len(sys.argv) < 2:
        print("Uso: python3 get_zerion_total.py <wallet_address>")
        sys.exit(1)

    wallet_address = sys.argv[1].strip()
    api_key = os.getenv("ZERION_API_KEY")
    if not api_key:
        print("❌ Debes definir la variable de entorno ZERION_API_KEY con tu API key de Zerion.")
        sys.exit(1)

    # 2) Llamamos a la función para obtener el total de assets
    total_usd = get_total_assets(wallet_address, api_key)
    if total_usd is None:
        print("❌ No se pudo recuperar el total de assets.")
        sys.exit(1)

    # 3) Mostramos el resultado
    print(f"Wallet: {wallet_address}")
    print(f"Total assets en USD: {total_usd:.2f}")

