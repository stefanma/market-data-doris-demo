from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """连接一个 Doris FE。BE 地址只用于改写 Stream Load 的重定向。"""

    doris_host: str = "127.0.0.1"
    doris_query_port: int = 9030
    doris_http_port: int = 8030
    doris_user: str = "root"
    doris_password: str = ""
    doris_database: str = "market_data"
    doris_be_host: str = "127.0.0.1"
    doris_be_http_port: int = 8040

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv(override=False)
        return cls(
            doris_host=os.getenv("DORIS_HOST", "127.0.0.1"),
            doris_query_port=int(os.getenv("DORIS_QUERY_PORT", "9030")),
            doris_http_port=int(os.getenv("DORIS_HTTP_PORT", "8030")),
            doris_user=os.getenv("DORIS_USER", "root"),
            doris_password=os.getenv("DORIS_PASSWORD", ""),
            doris_database=os.getenv("DORIS_DATABASE", "market_data"),
            doris_be_host=os.getenv("DORIS_BE_HOST", "127.0.0.1"),
            doris_be_http_port=int(os.getenv("DORIS_BE_HTTP_PORT", "8040")),
        )
