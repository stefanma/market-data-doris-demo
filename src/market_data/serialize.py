from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal


def change_pct(last: Decimal, open_price: Decimal) -> str | None:
    if open_price == 0:
        return None
    pct = ((last - open_price) / open_price) * Decimal(100)
    return format(pct.quantize(Decimal("0.0001")), "f")


def json_ready(value):
    """把 Decimal 和 datetime 收成 JSON 友好的值。价格保持十进制字符串。"""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        moment = value if value.tzinfo is None else value.astimezone(timezone.utc)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    return value


def present_ticker(row: dict, stats: dict | None, open_24h) -> dict:
    payload = {
        "exchange": row["exchange"],
        "symbol": row["symbol"],
        "last_price": row["last_price"],
        "last_qty": row["last_qty"],
        "side": row["side"],
        "session_open": row["open_price"],
        "session_high": row["high_price"],
        "session_low": row["low_price"],
        "session_volume": row["volume"],
        "session_quote_volume": row["quote_volume"],
        "session_trade_count": row["trade_count"],
        "update_time": row["update_time"],
        "high_24h": None if not stats else stats.get("high_24h"),
        "low_24h": None if not stats else stats.get("low_24h"),
        "volume_24h": None if not stats else stats.get("volume_24h"),
        "quote_volume_24h": None if not stats else stats.get("quote_volume_24h"),
        "trade_count_24h": None if not stats else stats.get("trade_count_24h"),
        "open_24h": open_24h,
        "change_24h_pct": None,
    }
    if open_24h is not None:
        payload["change_24h_pct"] = change_pct(Decimal(str(row["last_price"])), Decimal(str(open_24h)))
    return json_ready(payload)
