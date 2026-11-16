from __future__ import annotations

import argparse
import asyncio
import logging
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="market-data", description="用 Doris 承载的行情 demo")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="创建 market_data 库和三张表")

    collect = sub.add_parser("collect", help="采集成交，聚合成 K 线和最新价，写入 Doris")
    collect.add_argument("--source", choices=["simulator", "binance"], default="simulator")
    collect.add_argument("--symbols", default="BTC-USDT,ETH-USDT", help="计价交易对，逗号分隔")
    collect.add_argument("--rate", type=float, default=5, help="模拟器里每个交易对每秒的成交笔数")
    collect.add_argument("--flush-interval", type=float, default=1.0, help="写入 Doris 的间隔，秒")
    collect.add_argument("--seed", type=int, default=7, help="模拟器随机种子")

    serve = sub.add_parser("serve", help="启动只读查询 API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.command == "init":
        _init()
    elif args.command == "collect":
        _collect(args)
    elif args.command == "serve":
        _serve(args)
    else:
        parser.error(f"未知命令 {args.command}")


def _init() -> None:
    import pymysql

    from market_data.config import Settings
    from market_data.doris import DorisRepository

    settings = Settings.from_env()
    try:
        tables = DorisRepository(settings).init_schema()
    except pymysql.MySQLError as exc:
        raise SystemExit(
            f"无法连接 Doris {settings.doris_host}:{settings.doris_query_port}：{exc}"
        ) from exc
    print("已就绪:", ", ".join(tables))


def _collect(args: argparse.Namespace) -> None:
    from market_data.config import Settings
    from market_data.pipeline import run_collect

    settings = Settings.from_env()
    try:
        asyncio.run(
            run_collect(
                settings,
                source=args.source,
                symbols=args.symbols,
                rate=args.rate,
                flush_interval=args.flush_interval,
                seed=args.seed,
            )
        )
    except KeyboardInterrupt:
        print("\n已停止", file=sys.stderr)


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("market_data.api:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
