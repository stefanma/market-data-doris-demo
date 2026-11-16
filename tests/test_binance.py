from datetime import datetime, timezone
from decimal import Decimal

from market_data.binance import parse_binance_trade


def test_combined_stream_taker_side_and_millis() -> None:
    message = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 99,
            "p": "67000.10",
            "q": "0.002",
            "T": 1735689600123,
            "m": True,
        },
    }
    trade = parse_binance_trade(message)
    assert trade.exchange == "binance"
    assert trade.symbol == "BTC-USDT"
    assert trade.trade_id == "99"
    assert trade.side == "sell"
    assert trade.price == Decimal("67000.10")
    assert trade.qty == Decimal("0.002")
    assert trade.trade_time == datetime(2025, 1, 1, 0, 0, 0, 123000, tzinfo=timezone.utc)


def test_raw_trade_buy() -> None:
    trade = parse_binance_trade(
        {
            "e": "trade",
            "s": "ethusdt",
            "t": 7,
            "p": "3200",
            "q": "1",
            "T": 1735689600000,
            "m": False,
        }
    )
    assert trade.symbol == "ETH-USDT"
    assert trade.side == "buy"
