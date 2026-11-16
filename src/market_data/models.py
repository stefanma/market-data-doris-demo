from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

# 周期宽度，单位秒。全部按 UTC 纪元对齐，4h 和 1d 因此落在整点日切上。
TIMEFRAMES: dict[str, int] = {
    "1m": 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def bucket_open(value: datetime, width_seconds: int) -> datetime:
    """返回 value 所在周期的开盘时间，UTC。"""
    moment = ensure_utc(value)
    epoch = int(moment.timestamp())
    start = epoch - (epoch % width_seconds)
    return datetime.fromtimestamp(start, tz=timezone.utc)


@dataclass(frozen=True)
class Trade:
    exchange: str
    symbol: str
    trade_id: str
    trade_time: datetime
    price: Decimal
    qty: Decimal
    side: str

    def __post_init__(self) -> None:
        if self.side not in ("buy", "sell"):
            raise ValueError(f"side 必须是 buy 或 sell，收到 {self.side!r}")
        if self.price <= 0 or self.qty <= 0:
            raise ValueError("price 和 qty 必须为正数")
        object.__setattr__(self, "trade_time", ensure_utc(self.trade_time))

    @property
    def quote_qty(self) -> Decimal:
        return self.price * self.qty
