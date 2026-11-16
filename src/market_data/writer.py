from __future__ import annotations

from datetime import datetime, timezone

from market_data.aggregator import CandleSnapshot, TickerSnapshot
from market_data.config import Settings
from market_data.models import Trade
from market_data.stream_load import StreamLoader

TRADE_COLUMNS = [
    "exchange",
    "symbol",
    "trade_time",
    "trade_id",
    "price",
    "qty",
    "quote_qty",
    "side",
    "ingest_time",
]

KLINE_COLUMNS = [
    "exchange",
    "symbol",
    "timeframe",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "update_time",
]

TICKER_COLUMNS = [
    "exchange",
    "symbol",
    "last_price",
    "last_qty",
    "side",
    "open_price",
    "high_price",
    "low_price",
    "volume",
    "quote_volume",
    "trade_count",
    "update_time",
]


class DorisWriter:
    def __init__(self, settings: Settings, loader: StreamLoader | None = None) -> None:
        self.loader = loader or StreamLoader(settings)

    def write_trades(self, trades: list[Trade], ingest_time: datetime | None = None) -> dict:
        moment = ingest_time or datetime.now(timezone.utc)
        rows = [
            [
                trade.exchange,
                trade.symbol,
                trade.trade_time,
                trade.trade_id,
                trade.price,
                trade.qty,
                trade.quote_qty,
                trade.side,
                moment,
            ]
            for trade in trades
        ]
        return self.loader.load("md_trade", TRADE_COLUMNS, rows)

    def write_klines(self, snapshots: list[CandleSnapshot], update_time: datetime | None = None) -> dict:
        moment = update_time or datetime.now(timezone.utc)
        rows = []
        for item in snapshots:
            candle = item.candle
            rows.append(
                [
                    candle.exchange,
                    candle.symbol,
                    candle.timeframe,
                    candle.open_time,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                    candle.quote_volume,
                    candle.trade_count,
                    moment,
                ]
            )
        return self.loader.load("md_kline", KLINE_COLUMNS, rows)

    def write_tickers(self, snapshots: list[TickerSnapshot]) -> dict:
        rows = []
        for item in snapshots:
            ticker = item.ticker
            rows.append(
                [
                    ticker.exchange,
                    ticker.symbol,
                    ticker.last_price,
                    ticker.last_qty,
                    ticker.side,
                    ticker.open_price,
                    ticker.high_price,
                    ticker.low_price,
                    ticker.volume,
                    ticker.quote_volume,
                    ticker.trade_count,
                    ticker.update_time,
                ]
            )
        return self.loader.load("md_ticker", TICKER_COLUMNS, rows)
