from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query

from market_data.config import Settings
from market_data.doris import DorisError, DorisRepository
from market_data.serialize import json_ready, present_ticker

app = FastAPI(title="Market Data", version="0.1.0")


def get_repository() -> DorisRepository:
    return DorisRepository(Settings.from_env())


@app.get("/")
def root() -> dict:
    return {"service": "market-data", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health() -> dict:
    try:
        get_repository().ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"status": "ok"}


@app.get("/api/v1/symbols")
def symbols() -> list[dict]:
    return json_ready(_call(lambda repo: repo.list_symbols()))


@app.get("/api/v1/ticker")
def ticker(
    exchange: str = Query("simulator"),
    symbol: str = Query("BTC-USDT"),
) -> dict:
    def load(repo: DorisRepository) -> dict:
        row = repo.get_ticker(exchange, symbol)
        if row is None:
            raise HTTPException(status_code=404, detail="没有这个交易对的最新价")
        since = datetime.now(timezone.utc) - timedelta(days=1)
        stats = repo.kline_stats(row["exchange"], row["symbol"], since)
        open_24h = repo.kline_open(row["exchange"], row["symbol"], since)
        return present_ticker(row, stats, open_24h)

    return _call(load)


@app.get("/api/v1/klines")
def klines(
    exchange: str = Query("simulator"),
    symbol: str = Query("BTC-USDT"),
    timeframe: str = Query("1m"),
    limit: int = Query(200, ge=1, le=1000),
) -> list[dict]:
    return json_ready(_call(lambda repo: repo.klines(exchange, symbol, timeframe, limit)))


@app.get("/api/v1/trades")
def trades(
    exchange: str = Query("simulator"),
    symbol: str = Query("BTC-USDT"),
    limit: int = Query(100, ge=1, le=1000),
    lookback_hours: int = Query(24, ge=1, le=720),
) -> list[dict]:
    return json_ready(
        _call(lambda repo: repo.trades(exchange, symbol, limit, lookback_hours))
    )


def _call(action):
    try:
        return action(get_repository())
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DorisError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
