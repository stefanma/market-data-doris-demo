from __future__ import annotations

import logging
from datetime import datetime

from market_data.models import Trade

logger = logging.getLogger(__name__)

MAX_TRADE_BUFFER = 100_000


class TradeBuffer:
    """只在尾部追加。确认时删掉已经成功写入的前缀。"""

    def __init__(self, limit: int = MAX_TRADE_BUFFER) -> None:
        self.rows: list[Trade] = []
        self.limit = limit
        self.dropped = 0

    def append(self, trade: Trade) -> None:
        self.rows.append(trade)

    def snapshot(self) -> list[Trade]:
        if len(self.rows) > self.limit:
            extra = len(self.rows) - self.limit
            del self.rows[:extra]
            self.dropped += extra
            logger.error("成交缓冲超过 %s 条，丢弃最旧 %s 条", self.limit, extra)
        return list(self.rows)

    def ack(self, count: int) -> None:
        if count < 0 or count > len(self.rows):
            raise ValueError(f"无法确认 {count} 条，当前缓冲 {len(self.rows)} 条")
        del self.rows[:count]


async def run_collect(
    settings,
    source: str,
    symbols: str,
    rate: float,
    flush_interval: float,
    seed: int,
) -> None:
    import asyncio

    from market_data.aggregator import Aggregator
    from market_data.binance import stream_binance
    from market_data.simulator import TradeSimulator
    from market_data.symbols import parse_symbol_list
    from market_data.writer import DorisWriter

    if flush_interval <= 0:
        raise ValueError("flush_interval 必须大于 0")
    symbol_list = parse_symbol_list(symbols)
    aggregator = Aggregator()
    writer = DorisWriter(settings)
    buffer = TradeBuffer()
    state = asyncio.Lock()
    flushing = asyncio.Lock()

    async def incoming():
        if source == "simulator":
            simulator = TradeSimulator(symbol_list, rate=rate, seed=seed)
            async for trade in simulator.stream():
                yield trade
        elif source == "binance":
            async for trade in stream_binance(symbol_list):
                yield trade
        else:
            raise ValueError(f"未知来源 {source}")

    async def consume() -> None:
        async for trade in incoming():
            async with state:
                aggregator.add(trade)
                buffer.append(trade)

    async def flush_once() -> None:
        async with flushing:
            async with state:
                trades = buffer.snapshot()
                klines = aggregator.snapshot_klines()
                tickers = aggregator.snapshot_tickers()
            if not trades and not klines and not tickers:
                return
            written = {"trades": 0, "klines": 0, "tickers": 0}
            if trades:
                try:
                    await asyncio.to_thread(writer.write_trades, trades)
                except Exception:
                    logger.exception("成交写入失败，%s 条留待重试", len(trades))
                else:
                    async with state:
                        buffer.ack(len(trades))
                    written["trades"] = len(trades)
            if klines:
                try:
                    await asyncio.to_thread(writer.write_klines, klines)
                except Exception:
                    logger.exception("K 线写入失败，%s 根留待重试", len(klines))
                else:
                    async with state:
                        aggregator.ack_klines(klines)
                        removed = aggregator.gc(_utcnow())
                    written["klines"] = len(klines)
                    if removed:
                        logger.info("回收已收盘 K 线 %s 根", removed)
            if tickers:
                try:
                    await asyncio.to_thread(writer.write_tickers, tickers)
                except Exception:
                    logger.exception("最新价写入失败，%s 个交易对留待重试", len(tickers))
                else:
                    async with state:
                        aggregator.ack_tickers(tickers)
                    written["tickers"] = len(tickers)
            logger.info(
                "刷新完成 成交 %s  K线 %s  最新价 %s",
                written["trades"],
                written["klines"],
                written["tickers"],
            )

    async def flush_loop() -> None:
        while True:
            await asyncio.sleep(flush_interval)
            await flush_once()

    logger.info(
        "开始采集 source=%s symbols=%s flush=%ss",
        source,
        ",".join(symbol_list),
        flush_interval,
    )
    try:
        await asyncio.gather(consume(), flush_loop())
    except asyncio.CancelledError:
        logger.info("收到停止信号，写出最后一批")
        await flush_once()
        raise


def _utcnow() -> datetime:
    from datetime import timezone

    return datetime.now(timezone.utc)
