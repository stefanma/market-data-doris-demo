-- 行情库。单节点 demo 的副本数固定为 1。
-- all-in-one 镜像本身也会把默认副本数压成 1，这里写明，换到自建集群时不会悄悄变成 3 副本然后建表失败。

CREATE DATABASE IF NOT EXISTS market_data;

USE market_data;

-- 逐笔成交。
-- Unique Key + Merge-on-Write：同一笔成交重复导入会被覆盖，采集端重试不会把成交量加倍。
-- 排序键以 exchange, symbol 开头，查询「某个交易对的一段时间」能走前缀索引。
-- trade_time 同时是分区列。分区列必须出现在 Key 里，但不必是第一列。
CREATE TABLE IF NOT EXISTS md_trade (
    exchange    VARCHAR(32)     NOT NULL COMMENT "交易所，如 binance / simulator",
    symbol      VARCHAR(64)     NOT NULL COMMENT "标准交易对，如 BTC-USDT",
    trade_time  DATETIME(3)     NOT NULL COMMENT "成交时间 UTC",
    trade_id    VARCHAR(64)     NOT NULL COMMENT "交易所成交号，同一交易所内唯一",
    price       DECIMAL(38, 18) NOT NULL COMMENT "成交价",
    qty         DECIMAL(38, 18) NOT NULL COMMENT "成交量，base 资产",
    quote_qty   DECIMAL(38, 18) NOT NULL COMMENT "成交额，quote 资产",
    side        VARCHAR(8)      NOT NULL COMMENT "taker 方向：buy 或 sell",
    ingest_time DATETIME(3)     NOT NULL COMMENT "进入本系统的时间 UTC"
) UNIQUE KEY(exchange, symbol, trade_time, trade_id)
PARTITION BY RANGE(trade_time) ()
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "3",
    "compression" = "zstd"
);

-- K 线。同一根柱子在形成过程中会被反复覆盖，所以用 Unique Key，而不是追加写。
-- 应用侧先把一分钟内的成交收成一根柱子，再整行 upsert，Doris 不负责从逐笔做聚合。
CREATE TABLE IF NOT EXISTS md_kline (
    exchange      VARCHAR(32)     NOT NULL COMMENT "交易所",
    symbol        VARCHAR(64)     NOT NULL COMMENT "标准交易对",
    timeframe     VARCHAR(8)      NOT NULL COMMENT "1m / 5m / 15m / 1h / 4h / 1d，按 UTC 对齐",
    open_time     DATETIME(3)     NOT NULL COMMENT "这根 K 线的开盘时间 UTC",
    open          DECIMAL(38, 18) NOT NULL,
    high          DECIMAL(38, 18) NOT NULL,
    low           DECIMAL(38, 18) NOT NULL,
    close         DECIMAL(38, 18) NOT NULL,
    volume        DECIMAL(38, 18) NOT NULL COMMENT "base 成交量",
    quote_volume  DECIMAL(38, 18) NOT NULL COMMENT "quote 成交额",
    trade_count   BIGINT          NOT NULL,
    update_time   DATETIME(3)     NOT NULL COMMENT "这根柱子最后一次被写入的时间"
) UNIQUE KEY(exchange, symbol, timeframe, open_time)
PARTITION BY RANGE(open_time) ()
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "3",
    "compression" = "zstd"
);

-- 每个交易对一行最新状态。采集进程内的开高低收和成交量从进程启动开始累计，
-- 不是滚动 24 小时。24 小时统计由查询服务从 md_kline 现算。
CREATE TABLE IF NOT EXISTS md_ticker (
    exchange      VARCHAR(32)     NOT NULL,
    symbol        VARCHAR(64)     NOT NULL,
    last_price    DECIMAL(38, 18) NOT NULL,
    last_qty      DECIMAL(38, 18) NOT NULL,
    side          VARCHAR(8)      NOT NULL COMMENT "最后一笔的 taker 方向",
    open_price    DECIMAL(38, 18) NOT NULL COMMENT "本采集进程看到的第一笔价格",
    high_price    DECIMAL(38, 18) NOT NULL,
    low_price     DECIMAL(38, 18) NOT NULL,
    volume        DECIMAL(38, 18) NOT NULL,
    quote_volume  DECIMAL(38, 18) NOT NULL,
    trade_count   BIGINT          NOT NULL,
    update_time   DATETIME(3)     NOT NULL
) UNIQUE KEY(exchange, symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "compression" = "zstd"
);
