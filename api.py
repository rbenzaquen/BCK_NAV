import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import uvicorn

# Funciones de tu pipeline original
from bl16d import run_full, log_assets_nav, read_sheet_cell

# NAV completo (DeBank + validator) desde bl17n.py
from bl17n import get_kb_nav as get_nav_full, DeBankAPIError as DeBankAPIErrorFull

# NAV minimal (solo nav_total_usd) desde kb16nav.py
from kb16nav import get_kb_nav as get_nav_min, DeBankAPIError as DeBankAPIErrorKB

app = FastAPI(
    title="Assets NAV & Balance Oracle",
    version="1.3.0",
    description="Punto de acceso público para consultar NAV y balances como oráculo"
)

# CORS (GET público)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,   # si vas a enviar cookies/Authorization, considera listar orígenes específicos
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/v1/full", summary="Run full update")
async def endpoint_run_full():
    """
    Ejecuta todo el flujo: Zerion, Beacon, RAW y usuarios.
    """
    try:
        run_full()
        return {"status": "success", "message": "Proceso FULL completado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/midnight", summary="Run midnight-only task")
async def endpoint_midnight():
    """
    Ejecuta solo la tarea de medianoche: log_assets_nav.
    """
    try:
        log_assets_nav()
        return {"status": "success", "message": "Log_assets_nav ejecutado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/token_c6", summary="Get Token!C6 value")
async def endpoint_token_c6():
    """
    Devuelve {status, price(2 decimales como string o null), lastUpdated}
    leyendo Token!C6 del Google Sheet.
    """
    try:
        raw = read_sheet_cell("Token", "C6")

        def _clean(s: str) -> str:
            return s.replace("$", "").replace(",", "").strip()

        price_val = None
        if raw is not None and _clean(raw) != "":
            try:
                price_val = float(_clean(raw))
            except Exception:
                price_val = None

        status = 200 if price_val is not None else 500
        price_out = None if price_val is None else f"{price_val:.2f}"
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        return {"status": status, "price": price_out, "lastUpdated": now}
    except Exception as e:
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        return {"status": 500, "price": None, "lastUpdated": now, "error": str(e)}

@app.get(
    "/v1/nav_bck",
    summary="Get NAV BCK (DeBank + validator, desde bl17n.py)",
    description="Usa DEBANK_ACCESS_KEY y ADDRESS_BCK; opcional VALIDATOR_URL/VALIDATOR_ID y ETH_TICKER_SYMBOL."
)
async def endpoint_nav_bck():
    """
    Respuesta (bl17n.py):
      {
        "address": "...",
        "total_usd_value": <float|null>,
        "validator_balance_eth": <float|null>,
        "eth_usdt": <float|null>,
        "validator_usdt": <float|null>,
        "nav_total_usd": <float|null>
      }
    """
    try:
        return get_nav_full()
    except DeBankAPIErrorFull as e:
        raise HTTPException(status_code=502, detail=f"DeBank error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    "/v1/nav_kb",
    summary="Get NAV KB (solo nav_total_usd, desde kb16nav.py)",
    description='Devuelve únicamente {"nav_total_usd": <float|null>}.'
)
async def endpoint_nav_kb():
    try:
        return get_nav_min()  # kb16nav.py ya retorna {"nav_total_usd": ...}
    except DeBankAPIErrorKB as e:
        raise HTTPException(status_code=502, detail=f"DeBank error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)

