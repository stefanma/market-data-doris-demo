from datetime import datetime, timedelta, timezone
from decimal import Decimal

from market_data.aggregator import Aggregator
from market_data.models import TIMEFRAMES, Trade, bucket_open


def trade(price: str, second: int, qty: str = "1", trade_id: str | None = None) -> Trade:
    return Trade(
        exchange="simulator",
        symbol="BTC-USDT",
        trade_id=trade_id or f"{second}-{price}",
        trade_time=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
        price=Decimal(price),
        qty=Decimal(qty),
        side="buy",
    )


def test_bucket_boundaries() -> None:
    midnight = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert bucket_open(midnight, TIMEFRAMES["1m"]) == midnight
    at_59 = datetime(2026, 1, 1, 0, 0, 59, tzinfo=timezone.utc)
    assert bucket_open(at_59, TIMEFRAMES["1m"]) == midnight
    at_next = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert bucket_open(at_next, TIMEFRAMES["1m"]) == at_next

    five = datetime(2026, 1, 1, 5, tzinfo=timezone.utc)
    assert bucket_open(five, TIMEFRAMES["4h"]) == datetime(2026, 1, 1, 4, tzinfo=timezone.utc)
    late = datetime(2026, 1, 1, 23, 59, tzinfo=timezone.utc)
    assert bucket_open(late, TIMEFRAMES["1d"]) == midnight
    next_day = datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert bucket_open(next_day, TIMEFRAMES["1d"]) == next_day


def test_candle_ohlc_is_order_independent() -> None:
    agg = Aggregator()
    agg.add(trade("10", 2, "1", "a"))
    agg.add(trade("7", 1, "2", "b"))
    agg.add(trade("12", 3, "3", "c"))
    candle = agg.candles[("simulator", "BTC-USDT", "1m", datetime(2026, 1, 1, tzinfo=timezone.utc))]
    assert candle.open == Decimal("7")
    assert candle.close == Decimal("12")
    assert candle.high == Decimal("12")
    assert candle.low == Decimal("7")
    assert candle.volume == Decimal("6")
    assert candle.quote_volume == Decimal("7") * 2 + Decimal("10") + Decimal("12") * 3
    assert candle.trade_count == 3
    assert len(agg.candles) == len(TIMEFRAMES)


def test_ticker_keeps_latest_price_when_older_trade_arrives() -> None:
    agg = Aggregator()
    later = trade("10", 2, "1", "later")
    earlier = trade("8", 1, "4", "earlier")
    agg.add(later)
    agg.add(earlier)
    ticker = agg.tickers[("simulator", "BTC-USDT")]
    assert ticker.open_price == Decimal("10")
    assert ticker.last_price == Decimal("10")
    assert ticker.low_price == Decimal("8")
    assert ticker.high_price == Decimal("10")
    assert ticker.volume == Decimal("5")
    assert ticker.trade_count == 2


def test_ack_does_not_clear_a_newer_version() -> None:
    agg = Aggregator()
    agg.add(trade("10", 1, trade_id="a"))
    snapshot = agg.snapshot_klines()
    copied_close = snapshot[0].candle.close
    agg.add(trade("11", 2, trade_id="b"))
    agg.ack_klines(snapshot)
    key = ("simulator", "BTC-USDT", "1m", datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert key in agg.dirty_klines
    assert agg.candles[key].close == Decimal("11")
    assert copied_close == Decimal("10")


def test_gc_drops_closed_clean_candles_only() -> None:
    agg = Aggregator()
    agg.add(trade("10", 0))
    far = datetime(2026, 1, 1, 2, tzinfo=timezone.utc)
    assert agg.gc(far, retain=timedelta(0)) == 0

    agg.ack_klines(agg.snapshot_klines())
    agg.ack_tickers(agg.snapshot_tickers())
    removed = agg.gc(far, retain=timedelta(0))
    assert removed == 4
    assert ("simulator", "BTC-USDT", "1m", datetime(2026, 1, 1, tzinfo=timezone.utc)) not in agg.candles
    assert ("simulator", "BTC-USDT", "4h", datetime(2026, 1, 1, tzinfo=timezone.utc)) in agg.candles
    assert ("simulator", "BTC-USDT", "1d", datetime(2026, 1, 1, tzinfo=timezone.utc)) in agg.candles
