-- 手工核对用。先 USE market_data，时间参数按 UTC 理解。
USE market_data;

-- 最新价
SELECT exchange, symbol, last_price, last_qty, side, update_time
FROM md_ticker
ORDER BY exchange, symbol;

-- 最近 200 根 1 分钟 K 线。查询走 (exchange, symbol, timeframe) 前缀。
SELECT open_time, open, high, low, close, volume, trade_count
FROM md_kline
WHERE exchange = 'simulator'
  AND symbol = 'BTC-USDT'
  AND timeframe = '1m'
ORDER BY open_time DESC
LIMIT 200;

-- 用 1 分钟 K 线回算 24 小时高低和成交量。API 的 24h 字段就是这两段查询。
SELECT
    MAX(high)         AS high_24h,
    MIN(low)          AS low_24h,
    SUM(volume)       AS volume_24h,
    SUM(quote_volume) AS quote_volume_24h,
    SUM(trade_count)  AS trade_count_24h
FROM md_kline
WHERE exchange = 'simulator'
  AND symbol = 'BTC-USDT'
  AND timeframe = '1m'
  AND open_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY);

-- 最近成交。带上 trade_time 下界，避免扫掉已过期分区之外的全部历史。
SELECT trade_time, trade_id, price, qty, quote_qty, side
FROM md_trade
WHERE exchange = 'simulator'
  AND symbol = 'BTC-USDT'
  AND trade_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY trade_time DESC
LIMIT 50;

-- 核对某根正在形成的 K 线是否等于这段成交的聚合。
SELECT
    MIN(price) AS low,
    MAX(price) AS high,
    SUM(qty) AS volume,
    COUNT(*) AS trade_count
FROM md_trade
WHERE exchange = 'simulator'
  AND symbol = 'BTC-USDT'
  AND trade_time >= '2026-01-01 00:00:00'
  AND trade_time <  '2026-01-01 00:01:00';
