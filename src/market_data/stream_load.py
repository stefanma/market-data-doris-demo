from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.auth import HTTPBasicAuth

from market_data.config import Settings

logger = logging.getLogger(__name__)

_REDIRECTS = {301, 302, 303, 307, 308}
_OK_STATUS = {"Success", "Publish Timeout"}
_SCALE = Decimal("1e-18")


class StreamLoadError(RuntimeError):
    pass


def format_datetime(value: datetime) -> str:
    moment = value if value.tzinfo is None else value.astimezone(timezone.utc).replace(tzinfo=None)
    text = moment.strftime("%Y-%m-%d %H:%M:%S.%f")
    return text[:23]


def encode_field(value: object) -> str:
    if isinstance(value, datetime):
        text = format_datetime(value)
    elif isinstance(value, Decimal):
        with localcontext() as ctx:
            ctx.prec = 50
            quantized = value.quantize(_SCALE, rounding=ROUND_HALF_UP)
        text = format(quantized, "f")
    else:
        text = str(value)
    if "," in text or "\n" in text or "\r" in text or '"' in text:
        raise ValueError(f"字段含有 CSV 分隔符，拒绝写入: {text!r}")
    return text


def render_csv(rows: list[list[object]]) -> bytes:
    lines = [",".join(encode_field(field) for field in row) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def rewrite_redirect(location: str, be_host: str, be_port: int) -> str:
    """把 FE 重定向里的 BE 地址换成宿主机能访问的地址。"""
    parts = urlsplit(location)
    scheme = parts.scheme or "http"
    return urlunsplit((scheme, f"{be_host}:{be_port}", parts.path, parts.query, parts.fragment))


class StreamLoader:
    def __init__(self, settings: Settings, timeout: float = 30.0) -> None:
        self.settings = settings
        self.timeout = timeout
        self.session = requests.Session()
        # 重定向到 BE 时仍然带上 FE 的 Basic 认证。
        self.session.should_strip_auth = lambda old_url, new_url: False  # type: ignore[method-assign]

    def load(self, table: str, columns: list[str], rows: list[list[object]]) -> dict:
        if not rows:
            return {"Status": "Success", "NumberLoadedRows": 0}
        body = render_csv(rows)
        label = f"md_{table}_{uuid.uuid4().hex}"
        url = (
            f"http://{self.settings.doris_host}:{self.settings.doris_http_port}"
            f"/api/{self.settings.doris_database}/{table}/_stream_load"
        )
        headers = {
            "Expect": "100-continue",
            "format": "csv",
            "column_separator": ",",
            "columns": ",".join(columns),
            "label": label,
            "max_filter_ratio": "0",
            "strict_mode": "true",
        }
        auth = HTTPBasicAuth(self.settings.doris_user, self.settings.doris_password)
        response = self.session.put(
            url,
            data=body,
            headers=headers,
            auth=auth,
            timeout=self.timeout,
            allow_redirects=False,
        )
        if response.status_code in _REDIRECTS:
            location = response.headers.get("Location")
            if not location:
                raise StreamLoadError(f"重定向没有 Location: HTTP {response.status_code}")
            target = rewrite_redirect(
                location,
                self.settings.doris_be_host,
                self.settings.doris_be_http_port,
            )
            logger.info("Stream Load 重定向 %s -> %s", location, target)
            response = self.session.put(
                target,
                data=body,
                headers=headers,
                auth=auth,
                timeout=self.timeout,
                allow_redirects=False,
            )
        return _ensure_success(response, table, len(rows))


def _ensure_success(response: requests.Response, table: str, rows: int) -> dict:
    try:
        body = response.json()
    except ValueError as exc:
        raise StreamLoadError(
            f"{table} 导入返回的不是 JSON，HTTP {response.status_code}: {response.text[:300]}"
        ) from exc
    status = body.get("Status")
    if response.status_code == 200 and status in _OK_STATUS:
        logger.info(
            "%s 导入 %s 行，过滤 %s 行，耗时 %s ms",
            table,
            body.get("NumberLoadedRows", rows),
            body.get("NumberFilteredRows", 0),
            body.get("LoadTimeMs", "?"),
        )
        return body
    if status == "Label Already Exists":
        logger.info("%s 标签已存在，按成功处理", table)
        return body
    raise StreamLoadError(f"{table} 导入失败 HTTP {response.status_code}: {body}")
