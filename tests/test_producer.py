import asyncio
import json
from unittest.mock import MagicMock

import pytest
from tenacity import stop_after_attempt, wait_none
from confluent_kafka import Producer

from producer import main as producer_module
from producer.main import connect_to_binance, parse_kline_message

valid_kline_data = {
    "stream": "btcusdt@kline_1s",  # Stream name
    "data": {
        "e": "kline",  # Event type
        "E": 1795190799024132,  # Event time (us)
        "s": "BTCUSDT",  # Symbol
        "k": {
            "t": 1795190798000000,  # Kline start time (us)
            "T": 1795190798999999,  # Kline close time (us)
            "s": "BTCUSDT",  # Symbol
            "i": "1s",  # Interval
            "f": 6885547436,  # First trade ID
            "L": 6885547653,  # Last trade ID
            "o": "59342.01000000",  # Open price
            "c": "59320.00000000",  # Close price
            "h": "59348.66000000",  # High price
            "l": "59320.00000000",  # Low price
            "v": "0.56613000",  # Base asset volume (BTC)
            "n": 218,  # Number of trades
            "x": True,  # Is this kline closed? (always true for 1s)
            "q": "33589.68808180",  # Quote asset volume (USDT)
            "V": "0.09894000",  # Taker buy base asset volume
            "Q": "5871.23140540",  # Taker buy quote asset volume
            "B": "0",  # Ignore
        },
    },
}


def test_parse_valid_closed_kline():
    raw = json.dumps(valid_kline_data)

    result = parse_kline_message(raw)

    assert result is not None
    symbol, kline = result
    assert symbol == "BTCUSDT"
    assert kline["o"] == "59342.01000000"
    assert kline["c"] == "59320.00000000"
    assert kline["x"] is True


def test_parse_ignores_unclosed_kline():
    raw = json.dumps(
        {
            "stream": "btcusdt@kline_1s",
            "data": {"k": {"s": "BTCUSDT", "c": "90000.00", "x": False}},
        }
    )

    assert parse_kline_message(raw) is None


def test_parse_ignores_subscription_confirmation():
    raw = json.dumps({"result": None, "id": 1})

    assert parse_kline_message(raw) is None


def test_parse_ignores_invalid_json():
    assert parse_kline_message("{not valid json") is None


def test_parse_ignores_server_error_payload():
    raw = json.dumps({"error": {"code": 1, "msg": "Invalid request"}})

    assert parse_kline_message(raw) is None


def test_parse_raises_on_server_shutdown():
    raw = json.dumps({"stream": "!serverShutdown"})

    with pytest.raises(ConnectionError):
        parse_kline_message(raw)


def test_connect_to_binance_reconnects_after_failure(monkeypatch):
    # Skip real backoff sleep and cap attempts so a broken mock fails fast
    monkeypatch.setattr(connect_to_binance.retry, "wait", wait_none())
    monkeypatch.setattr(connect_to_binance.retry, "stop", stop_after_attempt(3))

    valid_kline = json.dumps(valid_kline_data)

    stop_event = asyncio.Event()
    mock_kafka_producer = MagicMock(spec=Producer)
    attempts = 0

    class FakeWebSocket:
        def __init__(self):
            self._messages = iter([valid_kline])

        async def send(self, _msg):
            pass

        async def recv(self):
            try:
                return next(self._messages)
            except StopIteration:
                stop_event.set()
                raise asyncio.TimeoutError()

    class FakeConnectContext:
        async def __aenter__(self):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("Simulated network drop")
            return FakeWebSocket()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        producer_module.websockets, "connect", lambda _url: FakeConnectContext()
    )

    asyncio.run(connect_to_binance(mock_kafka_producer, stop_event))

    assert attempts == 2
    mock_kafka_producer.produce.assert_called_once()
    call_kwargs = mock_kafka_producer.produce.call_args.kwargs
    assert call_kwargs["key"] == b"BTCUSDT"
