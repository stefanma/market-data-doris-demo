from datetime import datetime, timezone
from decimal import Decimal

from market_data.models import Trade
from market_data.pipeline import TradeBuffer
from market_data.serialize import change_pct, present_ticker
from market_data.simulator import TradeSimulator
from market_data.writer import KLINE_COLUMNS, TICKER_COLUMNS, TRADE_COLUMNS, DorisWriter
from market_data.config import Settings
from market_data.aggregator import Aggregator


def test_simulator_seed_is_stable() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    left = TradeSimulator(["btc-usdt"], rate=1, seed=7)
    right = TradeSimulator(["BTC-USDT"], rate=1, seed=7)
    a = left.next_trade("BTC-USDT", now)
    b = right.next_trade("BTC-USDT", now)
    assert (a.price, a.qty, a.side, a.trade_id) == (b.price, b.qty, b.side, b.trade_id)
    assert left.next_trade("BTC-USDT", now).trade_id == "2"


def test_buffer_drops_oldest_and_ack_keeps_the_tail() -> None:
    buffer = TradeBuffer(limit=2)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        Trade("simulator", "BTC-USDT", str(i), now, Decimal("1"), Decimal("1"), "buy")
        for i in range(4)
    ]
    for row in rows:
        buffer.append(row)
    snapshot = buffer.snapshot()
    assert [row.trade_id for row in snapshot] == ["2", "3"]
    assert buffer.dropped == 2
    buffer.append(rows[0])
    buffer.ack(2)
    assert [row.trade_id for row in buffer.rows] == ["0"]


def test_writer_column_order_matches_schema() -> None:
    class Recorder:
        def __init__(self):
            self.calls = []

        def load(self, table, columns, rows):
            self.calls.append((table, columns, rows))
            return {"NumberLoadedRows": len(rows)}

    recorder = Recorder()
    writer = DorisWriter(Settings(), loader=recorder)  # type: ignore[arg-type]
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sample = Trade("simulator", "BTC-USDT", "9", now, Decimal("2"), Decimal("3"), "sell")
    writer.write_trades([sample], ingest_time=now)
    agg = Aggregator()
    agg.add(sample)
    writer.write_klines(agg.snapshot_klines(), update_time=now)
    writer.write_tickers(agg.snapshot_tickers())

    tables = [call[0] for call in recorder.calls]
    assert tables == ["md_trade", "md_kline", "md_ticker"]
    assert recorder.calls[0][1] == TRADE_COLUMNS
    assert recorder.calls[1][1] == KLINE_COLUMNS
    assert recorder.calls[2][1] == TICKER_COLUMNS
    assert recorder.calls[0][2][0][6] == Decimal("6")
    assert len(recorder.calls[1][2]) == 6


def test_change_pct_and_ticker_payload() -> None:
    assert change_pct(Decimal("110"), Decimal("100")) == "10.0000"
    assert change_pct(Decimal("90"), Decimal("100")) == "-10.0000"
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = present_ticker(
        {
            "exchange": "simulator",
            "symbol": "BTC-USDT",
            "last_price": Decimal("110"),
            "last_qty": Decimal("1"),
            "side": "buy",
            "open_price": Decimal("100"),
            "high_price": Decimal("110"),
            "low_price": Decimal("100"),
            "volume": Decimal("2"),
            "quote_volume": Decimal("210"),
            "trade_count": 2,
            "update_time": now,
        },
        {
            "high_24h": Decimal("110"),
            "low_24h": Decimal("90"),
            "volume_24h": Decimal("5"),
            "quote_volume_24h": Decimal("500"),
            "trade_count_24h": 8,
        },
        Decimal("100"),
    )
    assert payload["last_price"] == "110"
    assert payload["session_open"] == "100"
    assert payload["change_24h_pct"] == "10.0000"
    assert payload["update_time"] == "2026-01-01T00:00:00.000Z"
    assert payload["volume_24h"] == "5"
