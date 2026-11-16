from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from market_data.models import Trade
from market_data.symbols import to_binance_stream, to_canonical

logger = logging.getLogger(__name__)

_BINANCE_STREAM = "wss://stream.binance.com:9443/stream?streams={streams}"


def parse_binance_trade(message: dict) -> Trade:
    """解析单流或组合流里的一条 trade。m=true 表示买方是 maker，taker 为 sell。"""
    data = message.get("data", message)
    if data.get("e") != "trade":
        raise ValueError(f"不是 trade 事件: {data.get('e')!r}")
    side = "sell" if data["m"] else "buy"
    millis = int(data["T"])
    seconds, milli = divmod(millis, 1000)
    # 用整数秒加毫秒，避免把毫秒除以 1000 变成二进制浮点。
    trade_time = datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(milliseconds=milli)
    return Trade(
        exchange="binance",
        symbol=to_canonical(data["s"]),
        trade_id=str(data["t"]),
        trade_time=trade_time,
        price=Decimal(data["p"]),
        qty=Decimal(data["q"]),
        side=side,
    )


async def stream_binance(symbols: list[str]) -> AsyncIterator[Trade]:
    import websockets

    streams = "/".join(f"{to_binance_stream(symbol)}@trade" for symbol in symbols)
    url = _BINANCE_STREAM.format(streams=streams)
    while True:
        try:
            logger.info("连接 Binance %s", url)
            async with websockets.connect(url, ping_interval=20, max_queue=2048) as ws:
                async for raw in ws:
                    payload = json.loads(raw)
                    try:
                        yield parse_binance_trade(payload)
                    except (KeyError, ValueError) as exc:
                        logger.warning("跳过无法解析的消息: %s", exc)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Binance 成交流断开，2 秒后重连")
            await asyncio.sleep(2)
