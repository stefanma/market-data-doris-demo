from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

from market_data.config import Settings
from market_data.models import TIMEFRAMES
from market_data.symbols import validate_exchange, validate_symbol

logger = logging.getLogger(__name__)

def schema_path() -> Path:
    """从源码树向上找 sql/001_schema.sql。editable 安装时文件不在 site-packages 里。"""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "sql" / "001_schema.sql"
        if candidate.is_file():
            return candidate
    raise DorisError("找不到 sql/001_schema.sql，请在项目根目录运行")


class DorisError(RuntimeError):
    pass


def split_sql(script: str) -> list[str]:
    """按分号切开语句。只支持整行或行尾的 `--` 注释，不支持字符串里的分号。"""
    lines: list[str] = []
    for line in script.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        lines.append(line)
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


class DorisRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def connect(self, database: str | None = None) -> pymysql.Connection:
        kwargs: dict = {
            "host": self.settings.doris_host,
            "port": self.settings.doris_query_port,
            "user": self.settings.doris_user,
            "password": self.settings.doris_password,
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
            "autocommit": True,
            "connect_timeout": 5,
            "read_timeout": 30,
        }
        if database:
            kwargs["database"] = database
        return pymysql.connect(**kwargs)

    def ping(self) -> None:
        with self.connect(self.settings.doris_database) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 AS ok")

    def init_schema(self, path: Path | None = None) -> list[str]:
        script = (path or schema_path()).read_text(encoding="utf-8")
        statements = split_sql(script)
        with self.connect() as conn:
            with conn.cursor() as cursor:
                for statement in statements:
                    logger.info("执行: %s", " ".join(statement.split())[:120])
                    cursor.execute(statement)
                cursor.execute("USE market_data")
                cursor.execute("SHOW TABLES")
                tables = [next(iter(row.values())) for row in cursor.fetchall()]
        logger.info("当前表: %s", ", ".join(tables))
        return [str(name) for name in tables]

    def list_symbols(self) -> list[dict]:
        sql = """
            SELECT exchange, symbol, last_price, update_time
            FROM md_ticker
            ORDER BY exchange, symbol
        """
        return self._query(sql)

    def get_ticker(self, exchange: str, symbol: str) -> dict | None:
        exchange = validate_exchange(exchange)
        symbol = validate_symbol(symbol)
        rows = self._query(
            """
            SELECT exchange, symbol, last_price, last_qty, side,
                   open_price, high_price, low_price, volume, quote_volume,
                   trade_count, update_time
            FROM md_ticker
            WHERE exchange = %s AND symbol = %s
            """,
            (exchange, symbol),
        )
        return rows[0] if rows else None

    def kline_stats(self, exchange: str, symbol: str, since: datetime) -> dict | None:
        rows = self._query(
            """
            SELECT MAX(high) AS high_24h,
                   MIN(low) AS low_24h,
                   SUM(volume) AS volume_24h,
                   SUM(quote_volume) AS quote_volume_24h,
                   SUM(trade_count) AS trade_count_24h
            FROM md_kline
            WHERE exchange = %s AND symbol = %s AND timeframe = '1m' AND open_time >= %s
            """,
            (exchange, symbol, _naive_utc(since)),
        )
        if not rows or rows[0]["high_24h"] is None:
            return None
        return rows[0]

    def kline_open(self, exchange: str, symbol: str, since: datetime) -> object | None:
        rows = self._query(
            """
            SELECT open
            FROM md_kline
            WHERE exchange = %s AND symbol = %s AND timeframe = '1m' AND open_time >= %s
            ORDER BY open_time ASC
            LIMIT 1
            """,
            (exchange, symbol, _naive_utc(since)),
        )
        if not rows:
            return None
        return rows[0]["open"]

    def klines(self, exchange: str, symbol: str, timeframe: str, limit: int) -> list[dict]:
        exchange = validate_exchange(exchange)
        symbol = validate_symbol(symbol)
        if timeframe not in TIMEFRAMES:
            allowed = ", ".join(TIMEFRAMES)
            raise ValueError(f"timeframe 必须是 {allowed}")
        limit = _limit(limit)
        rows = self._query(
            """
            SELECT exchange, symbol, timeframe, open_time, open, high, low, close,
                   volume, quote_volume, trade_count, update_time
            FROM md_kline
            WHERE exchange = %s AND symbol = %s AND timeframe = %s
            ORDER BY open_time DESC
            LIMIT %s
            """,
            (exchange, symbol, timeframe, limit),
        )
        rows.reverse()
        return rows

    def trades(self, exchange: str, symbol: str, limit: int, lookback_hours: int) -> list[dict]:
        exchange = validate_exchange(exchange)
        symbol = validate_symbol(symbol)
        limit = _limit(limit)
        if lookback_hours < 1 or lookback_hours > 24 * 30:
            raise ValueError("lookback_hours 必须在 1 到 720 之间")
        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        return self._query(
            """
            SELECT exchange, symbol, trade_time, trade_id, price, qty, quote_qty, side
            FROM md_trade
            WHERE exchange = %s AND symbol = %s AND trade_time >= %s
            ORDER BY trade_time DESC
            LIMIT %s
            """,
            (exchange, symbol, _naive_utc(since), limit),
        )

    def _query(self, sql: str, params: tuple | None = None) -> list[dict]:
        try:
            with self.connect(self.settings.doris_database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    return list(cursor.fetchall())
        except pymysql.MySQLError as exc:
            raise DorisError(str(exc)) from exc


def _naive_utc(value: datetime) -> datetime:
    """Doris DATETIME 不带时区。传入的时刻按 UTC 墙上时间写入参数。"""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.replace(microsecond=0)


def _limit(limit: int) -> int:
    if limit < 1 or limit > 1000:
        raise ValueError("limit 必须在 1 到 1000 之间")
    return limit
