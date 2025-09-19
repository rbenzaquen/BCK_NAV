import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import uvicorn

# Importación de funciones de tu script original
from bl16d import run_full, log_assets_nav, read_sheet_cell

# Importar el módulo nuevo para NAV de KB (DeBank)
from kb16nav import get_kb_nav, DeBankAPIError  # <-- importa la función

app = FastAPI(
    title="Assets NAV & Balance Oracle",
    version="1.1.0",
    description="Punto de acceso público para consultar NAV y balances como oráculo"
)

# Habilitar CORS para permitir llamadas públicas desde cualquier origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
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

        return {
            "status": status,
            "price": price_out,
            "lastUpdated": now
        }
    except Exception as e:
        # Falla inesperada
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        return {"status": 500, "price": None, "lastUpdated": now, "error": str(e)}

@app.get("/v1/nav_kb", summary="Get NAV de KB (DeBank total_usd_value)")
async def endpoint_nav_kb():
    """
    Devuelve el NAV (total_usd_value) del address ADDRESS_KB usando DeBank Cloud.
    Requiere variables de entorno: DEBANK_ACCESS_KEY y ADDRESS_KB.
    Respuesta:
      {
        "address": "...",
        "total_usd_value": <float | null>
      }
    """
    try:
        return get_kb_nav()
    except DeBankAPIError as e:
        raise HTTPException(status_code=502, detail=f"DeBank error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)

