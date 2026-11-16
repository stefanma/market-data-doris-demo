# 用 Doris 承载的行情 demo

一个最小可运行的行情链路：模拟器或 Binance 公共成交流进来，进程内收成 K 线和最新价，通过 Stream Load 写入 Apache Doris，再用 HTTP API 查询。

设计说明见 [docs/design.md](docs/design.md)。

## 环境

- Docker Desktop，建议至少 6 GB 内存
- Python 3.10 及以上

Doris 使用官方 all-in-one 镜像，单容器里一个 FE 加一个 BE，只适合本 demo。第一次拉取大约 2.7 GB。

## 启动

```powershell
docker compose up -d
docker inspect --format "{{.State.Health.Status}}" market-data-doris

python -m pip install -e ".[dev]"
copy .env.example .env
python -m market_data init
```

`healthy` 之前不要建表。镜像自己带健康检查，通常几十秒。

另开一个终端采集，再开一个终端提供查询。不传 `--symbols` 时只采集 `BTC-USDT` 和 `ETH-USDT`：

```powershell
python -m market_data collect --source simulator --rate 5
python -m market_data serve
```

真实行情不需要 API Key：

```powershell
python -m market_data collect --source binance
```

安装后也可以直接用 `market-data`，前提是 Python 的 `Scripts` 目录在 `PATH` 里。

## 查询

交互式文档在 <http://127.0.0.1:8000/docs>。

```powershell
curl.exe "http://127.0.0.1:8000/health"
curl.exe "http://127.0.0.1:8000/api/v1/symbols"
curl.exe "http://127.0.0.1:8000/api/v1/ticker?exchange=simulator&symbol=BTC-USDT"
curl.exe "http://127.0.0.1:8000/api/v1/klines?exchange=simulator&symbol=ETH-USDT&timeframe=1m&limit=20"
curl.exe "http://127.0.0.1:8000/api/v1/trades?exchange=simulator&symbol=BTC-USDT&limit=20"
```

价格在 JSON 里是十进制字符串。`session_*` 从采集进程启动开始累计，进程重启后重新起算。`*_24h` 由 1 分钟 K 线回算。

手工核对 SQL 在 `sql/queries.sql`。

## 测试

单元测试不连接 Doris：

```powershell
python -m pytest
```

## 端口

| 端口 | 用途 |
| --- | --- |
| 9030 | MySQL 协议，建表和查询，用户 `root`，密码为空 |
| 8030 | FE HTTP，Stream Load 入口 |
| 8040 | BE HTTP，Stream Load 被重定向到这里 |
| 8000 | 本 demo 的查询 API |

在 Docker Desktop 上，FE 重定向给出的 BE 地址通常是容器网段。客户端会把它改写成 `DORIS_BE_HOST`（默认 `127.0.0.1`）和 `DORIS_BE_HTTP_PORT`（默认 `8040`）。

换 Doris 版本时：

```powershell
$env:DORIS_IMAGE = "apache/doris:all-in-one-4.1.3"
docker compose up -d
```

`docker compose down` 会丢掉这个容器里的数据。
