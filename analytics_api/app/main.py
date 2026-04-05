from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.routers import marts

app = FastAPI(
    title="Bank DWH - API витрин",
    description="Чтение аналитических витрин из ClickHouse (база bank_marts).",
    version="1.0.0",
)

app.include_router(marts.router, prefix="/v1/marts", tags=["marts"])

_PUBLIC_PATHS = frozenset({"/health", "/docs", "/openapi.json", "/redoc", "/"})


@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    key = settings.analytics_api_key.strip()
    if key and request.url.path not in _PUBLIC_PATHS:
        if request.headers.get("x-api-key") != key:
            return JSONResponse(status_code=401, content={"detail": "Нужен заголовок X-API-Key"})
    return await call_next(request)


@app.get("/health", tags=["service"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", tags=["service"])
def root() -> dict[str, str]:
    return {"service": "bank-dwh-analytics-api", "docs": "/docs"}
