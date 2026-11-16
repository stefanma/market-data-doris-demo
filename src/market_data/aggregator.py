from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from market_data.models import TIMEFRAMES, Trade, bucket_open

CandleKey = tuple[str, str, str, datetime]
TickerKey = tuple[str, str]


@dataclass
class Candle:
    exchange: str
    symbol: str
    timeframe: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    first_trade_time: datetime
    last_trade_time: datetime
    version: int = 0

    @classmethod
    def from_trade(cls, trade: Trade, timeframe: str, open_time: datetime) -> Candle:
        return cls(
            exchange=trade.exchange,
            symbol=trade.symbol,
            timeframe=timeframe,
            open_time=open_time,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.qty,
            quote_volume=trade.quote_qty,
            trade_count=1,
            first_trade_time=trade.trade_time,
            last_trade_time=trade.trade_time,
            version=1,
        )

    def apply(self, trade: Trade) -> None:
        if trade.trade_time < self.first_trade_time:
            self.open = trade.price
            self.first_trade_time = trade.trade_time
        if trade.trade_time >= self.last_trade_time:
            self.close = trade.price
            self.last_trade_time = trade.trade_time
        if trade.price > self.high:
            self.high = trade.price
        if trade.price < self.low:
            self.low = trade.price
        self.volume += trade.qty
        self.quote_volume += trade.quote_qty
        self.trade_count += 1
        self.version += 1


@dataclass
class TickerState:
    exchange: str
    symbol: str
    last_price: Decimal
    last_qty: Decimal
    side: str
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    update_time: datetime
    version: int = 1

    @classmethod
    def from_trade(cls, trade: Trade) -> TickerState:
        return cls(
            exchange=trade.exchange,
            symbol=trade.symbol,
            last_price=trade.price,
            last_qty=trade.qty,
            side=trade.side,
            open_price=trade.price,
            high_price=trade.price,
            low_price=trade.price,
            volume=trade.qty,
            quote_volume=trade.quote_qty,
            trade_count=1,
            update_time=trade.trade_time,
        )

    def apply(self, trade: Trade) -> None:
        if trade.trade_time >= self.update_time:
            self.last_price = trade.price
            self.last_qty = trade.qty
            self.side = trade.side
            self.update_time = trade.trade_time
        if trade.price > self.high_price:
            self.high_price = trade.price
        if trade.price < self.low_price:
            self.low_price = trade.price
        self.volume += trade.qty
        self.quote_volume += trade.quote_qty
        self.trade_count += 1
        self.version += 1


@dataclass(frozen=True)
class CandleSnapshot:
    candle: Candle
    version: int


@dataclass(frozen=True)
class TickerSnapshot:
    ticker: TickerState
    version: int


@dataclass
class Aggregator:
    """单写入者的内存聚合。调用方需要保证同一时刻只有一个协程在改它。"""

    candles: dict[CandleKey, Candle] = field(default_factory=dict)
    tickers: dict[TickerKey, TickerState] = field(default_factory=dict)
    dirty_klines: dict[CandleKey, int] = field(default_factory=dict)
    dirty_tickers: dict[TickerKey, int] = field(default_factory=dict)

    def add(self, trade: Trade) -> None:
        self._add_ticker(trade)
        for timeframe, width in TIMEFRAMES.items():
            open_time = bucket_open(trade.trade_time, width)
            key: CandleKey = (trade.exchange, trade.symbol, timeframe, open_time)
            candle = self.candles.get(key)
            if candle is None:
                candle = Candle.from_trade(trade, timeframe, open_time)
                self.candles[key] = candle
            else:
                candle.apply(trade)
            self.dirty_klines[key] = candle.version

    def snapshot_klines(self) -> list[CandleSnapshot]:
        snapshots: list[CandleSnapshot] = []
        for key, version in self.dirty_klines.items():
            snapshots.append(CandleSnapshot(copy.copy(self.candles[key]), version))
        return snapshots

    def snapshot_tickers(self) -> list[TickerSnapshot]:
        snapshots: list[TickerSnapshot] = []
        for key, version in self.dirty_tickers.items():
            snapshots.append(TickerSnapshot(copy.copy(self.tickers[key]), version))
        return snapshots

    def ack_klines(self, snapshots: list[CandleSnapshot]) -> None:
        for item in snapshots:
            key = _candle_key(item.candle)
            if self.dirty_klines.get(key) == item.version:
                self.dirty_klines.pop(key, None)

    def ack_tickers(self, snapshots: list[TickerSnapshot]) -> None:
        for item in snapshots:
            key = (item.ticker.exchange, item.ticker.symbol)
            if self.dirty_tickers.get(key) == item.version:
                self.dirty_tickers.pop(key, None)

    def gc(self, now: datetime, retain: timedelta = timedelta(seconds=120)) -> int:
        """丢掉已收盘、已写出、且超出保留窗口的柱子。返回删除根数。"""
        removed = 0
        stale: list[CandleKey] = []
        for key, candle in self.candles.items():
            if key in self.dirty_klines:
                continue
            width = TIMEFRAMES[candle.timeframe]
            close_at = candle.open_time + timedelta(seconds=width)
            if close_at + retain <= now:
                stale.append(key)
        for key in stale:
            del self.candles[key]
            removed += 1
        return removed

    def _add_ticker(self, trade: Trade) -> None:
        key: TickerKey = (trade.exchange, trade.symbol)
        ticker = self.tickers.get(key)
        if ticker is None:
            ticker = TickerState.from_trade(trade)
            self.tickers[key] = ticker
        else:
            ticker.apply(trade)
        self.dirty_tickers[key] = ticker.version


def _candle_key(candle: Candle) -> CandleKey:
    return (candle.exchange, candle.symbol, candle.timeframe, candle.open_time)
