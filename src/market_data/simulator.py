from __future__ import annotations

import random
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal

from market_data.models import Trade
from market_data.symbols import to_canonical

_DEFAULT_PRICES = {
    "BTC-USDT": Decimal("67000"),
    "ETH-USDT": Decimal("3200"),
    "SOL-USDT": Decimal("150"),
}


class TradeSimulator:
    """固定种子的随机游走。价格步进用整数基点，避免浮点格式差异。"""

    def __init__(
        self,
        symbols: list[str],
        rate: float,
        seed: int = 7,
        exchange: str = "simulator",
    ) -> None:
        if rate <= 0:
            raise ValueError("rate 必须大于 0")
        self.symbols = [to_canonical(symbol) for symbol in symbols]
        self.rate = rate
        self.exchange = exchange
        self._rng = random.Random(seed)
        self._prices = {
            symbol: _DEFAULT_PRICES.get(symbol, Decimal("100")) for symbol in self.symbols
        }
        self._seq = 0

    def next_trade(self, symbol: str, now: datetime) -> Trade:
        canonical = to_canonical(symbol)
        price = self._prices[canonical]
        bps = Decimal(self._rng.randrange(-8, 9)) / Decimal("10000")
        price = (price * (Decimal(1) + bps)).quantize(Decimal("0.00000001"))
        if price <= 0:
            price = Decimal("0.00000001")
        self._prices[canonical] = price
        qty = Decimal(self._rng.randrange(1, 5000)) / Decimal("10000")
        side = "buy" if self._rng.randrange(2) == 0 else "sell"
        self._seq += 1
        return Trade(
            exchange=self.exchange,
            symbol=canonical,
            trade_id=str(self._seq),
            trade_time=now,
            price=price,
            qty=qty,
            side=side,
        )

    async def stream(self) -> AsyncIterator[Trade]:
        import asyncio

        interval = 1.0 / self.rate
        while True:
            now = datetime.now(timezone.utc)
            for symbol in self.symbols:
                yield self.next_trade(symbol, now)
            await asyncio.sleep(interval)
