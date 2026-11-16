from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_data.config import Settings
from market_data.stream_load import (
    StreamLoadError,
    StreamLoader,
    encode_field,
    render_csv,
    rewrite_redirect,
)


def test_rewrite_redirect_replaces_container_host() -> None:
    location = "http://172.18.0.2:8040/api/market_data/md_trade/_stream_load?a=1"
    assert rewrite_redirect(location, "127.0.0.1", 8040) == (
        "http://127.0.0.1:8040/api/market_data/md_trade/_stream_load?a=1"
    )


def test_csv_quantizes_to_18_places_and_rejects_commas() -> None:
    moment = datetime(2026, 1, 1, 0, 0, 1, 123456, tzinfo=timezone.utc)
    assert encode_field(moment) == "2026-01-01 00:00:01.123"
    assert encode_field(Decimal("1.2345678901234567894")) == "1.234567890123456789"
    text = render_csv([["binance", Decimal("10"), moment]]).decode()
    assert text == "binance,10.000000000000000000,2026-01-01 00:00:01.123\n"
    with pytest.raises(ValueError):
        encode_field("a,b")


def test_load_rewrites_redirect(monkeypatch) -> None:
    loader = StreamLoader(Settings())
    calls: list[str] = []

    class Response:
        def __init__(self, status_code, body, location=None):
            self.status_code = status_code
            self._body = body
            self.headers = {"Location": location} if location else {}
            self.text = str(body)

        def json(self):
            if not self._body:
                raise ValueError("no json")
            return self._body

    def put(url, data, headers, auth, timeout, allow_redirects):
        calls.append(url)
        assert allow_redirects is False
        if len(calls) == 1:
            return Response(307, {}, "http://172.18.0.2:8040/api/market_data/md_trade/_stream_load")
        return Response(200, {"Status": "Success", "NumberLoadedRows": 1, "NumberFilteredRows": 0, "LoadTimeMs": 4})

    monkeypatch.setattr(loader.session, "put", put)
    result = loader.load("md_trade", ["exchange"], [["binance"]])
    assert calls[1] == "http://127.0.0.1:8040/api/market_data/md_trade/_stream_load"
    assert result["NumberLoadedRows"] == 1


def test_load_failure_raises() -> None:
    loader = StreamLoader(Settings())

    class Response:
        status_code = 200
        text = "fail"
        headers: dict = {}

        def json(self):
            return {"Status": "Fail", "Message": "bad row"}

    loader.session.put = lambda *args, **kwargs: Response()  # type: ignore[method-assign]
    with pytest.raises(StreamLoadError):
        loader.load("md_trade", ["exchange"], [["binance"]])
