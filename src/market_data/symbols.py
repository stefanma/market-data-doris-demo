from __future__ import annotations

import re

# 长的计价资产放前面，避免 USDT 被更短的后缀截走。
_QUOTES = ("USDT", "USDC", "FDUSD", "BUSD", "BTC", "ETH", "BNB")
_CANONICAL = re.compile(r"^[A-Z0-9]{1,20}-[A-Z0-9]{2,10}$")
_EXCHANGE = re.compile(r"^[a-z0-9]{1,32}$")


def to_canonical(raw: str) -> str:
    """把 BTCUSDT、btc-usdt、BTC_USDT 收成 BTC-USDT。"""
    compact = raw.strip().upper().replace("-", "").replace("_", "")
    if not compact:
        raise ValueError("交易对为空")
    for quote in _QUOTES:
        if compact.endswith(quote) and len(compact) > len(quote):
            return f"{compact[: -len(quote)]}-{quote}"
    raise ValueError(f"无法识别交易对 {raw!r}，需要已知的计价资产后缀")


def to_binance_stream(symbol: str) -> str:
    return to_canonical(symbol).replace("-", "").lower()


def validate_symbol(symbol: str) -> str:
    canonical = symbol.strip().upper()
    if not _CANONICAL.fullmatch(canonical):
        raise ValueError(f"交易对必须是 BASE-QUOTE，收到 {symbol!r}")
    return canonical


def validate_exchange(exchange: str) -> str:
    value = exchange.strip().lower()
    if not _EXCHANGE.fullmatch(value):
        raise ValueError(f"交易所名不合法: {exchange!r}")
    return value


def parse_symbol_list(raw: str) -> list[str]:
    symbols = [to_canonical(part) for part in raw.split(",") if part.strip()]
    if not symbols:
        raise ValueError("至少提供一个交易对")
    return symbols
