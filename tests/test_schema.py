import pytest

from market_data.doris import schema_path, split_sql
from market_data.symbols import parse_symbol_list, to_binance_stream, to_canonical, validate_symbol


def test_canonical_symbols() -> None:
    assert to_canonical("btcusdt") == "BTC-USDT"
    assert to_canonical("BTC-USDT") == "BTC-USDT"
    assert to_canonical("ETH_BTC") == "ETH-BTC"
    assert to_binance_stream("BTC-USDT") == "btcusdt"
    assert parse_symbol_list("btc-usdt, ETH-USDT") == ["BTC-USDT", "ETH-USDT"]
    with pytest.raises(ValueError):
        validate_symbol("BTC/USDT")


def test_schema_script_splits_into_executable_statements() -> None:
    statements = split_sql(schema_path().read_text(encoding="utf-8"))
    heads = [statement.split()[0] for statement in statements]
    assert heads == ["CREATE", "USE", "CREATE", "CREATE", "CREATE"]
    joined = "\n".join(statements)
    assert "UNIQUE KEY(exchange, symbol, trade_time, trade_id)" in joined
    assert "UNIQUE KEY(exchange, symbol, timeframe, open_time)" in joined
    assert 'dynamic_partition.time_unit" = "DAY"' in joined or "dynamic_partition.time_unit" in joined
    assert "enable_unique_key_merge_on_write" in joined


def test_split_sql_strips_comments() -> None:
    assert split_sql("SELECT 1; -- tail\nSELECT 2 -- comment\n;") == ["SELECT 1", "SELECT 2"]
