"""
This script connects to the Binance WebSocket API and streams real-time kline data for a specified symbol.
The data is then sent to a Kafka topic for further processing.

Important points to handle:

1. Connection Handling: A single connection is only valid for 24 hours, so the script should be able to reconnect after that time.
Solution: The 'tenacity' library is used to implement a retry mechanism with exponential backoff for reconnection attempts.
1.1 Server Shutdowns: The script should handle server shutdowns gracefully and attempt to reconnect.
Solution: The 'tenacity' library will also handle server shutdowns by retrying the connection.

2. Pong Responses: The script should handle ping/pong messages sent by binance every 20s to keep the connection alive.
Solution: The 'websockets' library automatically handles ping/pong messages, so we don't need to implement it manually.

3. Docker SIGTERM Handling: When you run docker compose down or stop kafka-producer, Docker sends SIGTERM. Register loop.add_signal_handler(signal.SIGTERM, stop_event.set)
so your loop exits cleanly and executes producer.flush() before the container terminates.
Solution: Use an asyncio.Event to handle SIGTERM

TODO: historical backfill: we should be able to backfill data that was lost due to downtime or connection issues.
"""

import asyncio
import argparse
import json
import logging
import os
import signal
import sys

import websockets
from confluent_kafka import Producer
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    retry_if_not_exception_type,
    wait_random_exponential,
)
from websockets.exceptions import ConnectionClosed

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "prod.binance.btc_usdt_1s.v1")
STREAM_URL = os.getenv(
    "BINANCE_WS_URL", "wss://stream.binance.com:9443/stream?timeUnit=MICROSECOND"
)
# I will declare symbols as an array to allow future expansion to multiple symbols if needed
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]


# Shutdown timeouts for Kafka producer flush and enqueue operations.
KAFKA_ENQUEUE_SHUTDOWN_TIMEOUT = float(os.getenv("KAFKA_ENQUEUE_SHUTDOWN_TIMEOUT", "5"))
KAFKA_FLUSH_TIMEOUT = float(os.getenv("KAFKA_FLUSH_TIMEOUT", "20"))


class ShutdownEnqueueError(Exception):
    """Raised when an in-flight candle cannot be queued before shutdown deadline."""


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def kafka_delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


async def _enqueue_with_backpressure(
    producer: Producer, topic: str, key: bytes, value: bytes
) -> None:
    attempts = 0
    while True:
        try:
            producer.produce(topic=topic, key=key, value=value)
            producer.poll(0)
            return
        except BufferError:
            attempts += 1
            if attempts == 1 or attempts % 10 == 0:
                logger.warning(
                    f"Producer queue full ({len(producer)} pending). "
                    f"Applying backpressure (attempt {attempts})..."
                )
            # Poll the producer to handle delivery reports and free up space in the queue (skipped in the try)
            producer.poll(0)

            # Yields control back to the event loop so ping/pong messages can be processed and the queue can be drained
            await asyncio.sleep(min(0.1 * attempts, 1))


async def produce_kline(
    producer: Producer,
    topic: str,
    symbol: str,
    payload: dict,
    stop_event: asyncio.Event,
    shutdown_timeout: float = KAFKA_ENQUEUE_SHUTDOWN_TIMEOUT,
) -> None:
    key = symbol.encode("utf-8")
    value = json.dumps(payload).encode("utf-8")

    # 1. Normal path: retry as long as needed until accepted
    if not stop_event.is_set():
        try:
            await _enqueue_with_backpressure(producer, topic, key, value)
            return
        except asyncio.CancelledError:
            # Shutdown interrupted us while holding a candle; fall through to step 2
            stop_event.set()

    # 2. Shutdown path: give this one candle a few seconds to finish queuing
    try:
        await asyncio.wait_for(
            _enqueue_with_backpressure(producer, topic, key, value),
            timeout=shutdown_timeout,
        )
    except TimeoutError:
        logger.error(
            f"Timed out queueing in-flight kline for {symbol} at "
            f"open_time={payload.get('k', {}).get('t')} during shutdown."
        )
        raise ShutdownEnqueueError("In-flight kline dropped on shutdown.")


def parse_kline_message(raw_message: str) -> tuple[str, dict] | None:
    try:
        res = json.loads(raw_message)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return None

    if "error" in res:
        logger.error(f"Error received from server: {res['error']}")
        return None

    if "code" in res and "msg" in res:
        logger.error(f"Error received from server: {res}")
        raise ConnectionError("Server rejected WebSocket request")

    if "result" in res and "id" in res:
        logger.info(f"Subscription confirmed: {res}")
        return None

    if res.get("stream") == "!serverShutdown":
        logger.warning("Server shutdown message received. Attempting to reconnect...")
        raise ConnectionError("Server shutdown, reconnecting...")

    # Safely get 'data' (envelope), then 'k', defaulting to empty dicts if missing
    data = res.get("data", {})
    kline = data.get("k", {})
    if not kline.get("x", False):
        return None  # Ignore messages that are not final (x = False)

    symbol = data.get("s") or kline.get("s", "UNKNOWN")
    return symbol, data


async def receive_messages(
    websocket: websockets.ClientConnection,
    producer: Producer,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=5)
            parsed = parse_kline_message(message)
            if parsed is None:
                continue  # Skip if the message was not a valid kline

            symbol, payload = parsed
            await produce_kline(producer, KAFKA_TOPIC, symbol, payload, stop_event)

        except TimeoutError:
            logger.warning("Timeout waiting for message. Checking connection...")
            continue
        except ConnectionClosed as e:
            logger.error(f"Connection closed: {e}. Attempting to reconnect...")
            raise
        except ShutdownEnqueueError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Attempting to reconnect...")
            raise


@retry(
    wait=wait_random_exponential(multiplier=1, min=1, max=30),
    # we explicitly set Exception as the base class to ensure the retry_if_not doesn't include baseException errors
    retry=retry_if_exception_type(Exception)
    & retry_if_not_exception_type(ShutdownEnqueueError),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def connect_to_binance(producer: Producer, stop_event: asyncio.Event) -> None:
    # connect to the stream, subscribe to the symbols and
    # increase max_queue for better handling of worst-case scenarios
    async with websockets.connect(
        STREAM_URL, max_queue=1024, close_timeout=2
    ) as websocket:
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol.lower()}@kline_1s" for symbol in SYMBOLS],
            "id": 1,
        }

        logger.info(f"Sending subscription message: {subscribe_message}")
        await websocket.send(json.dumps(subscribe_message))

        await receive_messages(websocket, producer, stop_event)


async def main():
    # Asyncio event is basically a thread-safe flag designed for asynchronous code.
    # we use it to handle SIGTERM signals from Docker by setting the event when a SIGTERM is received, which will cause the loop to exit cleanly.
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    delivery_failures = 0
    stream_task: asyncio.Task | None = None

    def on_delivery(err, msg) -> None:
        nonlocal delivery_failures
        kafka_delivery_report(err, msg)

        if err is not None:
            delivery_failures += 1
            # only stop the producer if the stop_event is not already set, to avoid redundant logging and cancellation
            if not stop_event.is_set():
                logger.error(
                    "Permanent Kafka delivery failure detected; "
                    "stopping producer to fail fast."
                )

                stop_event.set()
                if stream_task is not None and not stream_task.done():
                    stream_task.cancel()

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "enable.idempotence": True,
            "on_delivery": on_delivery,
        }
    )

    # Schedule connect_to_binance as a cancellable Task instead of awaiting it directly
    stream_task = asyncio.create_task(connect_to_binance(producer, stop_event))

    def handle_shutdown() -> None:
        logger.info("Shutdown signal received. Stopping producer...")
        stop_event.set()
        if stream_task is not None and not stream_task.done():
            stream_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Register signal handlers to set the stop_event when a SIGINT or SIGTERM is received
        loop.add_signal_handler(sig, handle_shutdown)

    shutdown_failed = False
    try:
        await stream_task
    except asyncio.CancelledError:
        if delivery_failures > 0:
            logger.error("Streaming task cancelled due to Kafka delivery failures.")
        else:
            logger.info("Streaming task cancelled cleanly.")
    except ShutdownEnqueueError:
        shutdown_failed = True
    finally:
        # Ensure the stop_event is set to signal shutdown to avoid further redundant logging
        stop_event.set()
        logger.info("Flushing remaining messages to Kafka...")
        # Flush the producer to ensure all messages are sent before shutdown
        # (20 + 5)s is just below docker's 30s stop_grace_period, so we can still log if the flush failed to complete in time.
        remaining = producer.flush(KAFKA_FLUSH_TIMEOUT)
        if remaining > 0 or delivery_failures > 0 or shutdown_failed:
            logger.error(
                f"Shutdown failed to deliver all Kafka records "
                f"(remaining={remaining}, failed={delivery_failures}, "
                f"enqueue_failed={shutdown_failed})."
            )
            shutdown_failed = True
        else:
            logger.info("All messages delivered successfully.")
            logger.info("Shutdown complete.")

    # putting it inside finally completely erases crashes from the try block.
    if shutdown_failed:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance WebSocket-to-Kafka producer.")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify Kafka topic config and multi-partition roundtrip, then exit.",
    )
    parser.add_argument("--bootstrap-servers", default=KAFKA_BROKER)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    args = parser.parse_args()

    if args.verify:
        if __package__:
            from .verify import run_kafka_verification
        else:
            from verify import run_kafka_verification  # pyright: ignore[reportMissingImports]
        try:
            run_kafka_verification(
                bootstrap_servers=args.bootstrap_servers,
                topic=args.topic,
            )
        except Exception as exc:
            logger.error(f"Kafka verification failed: {exc}")
            sys.exit(1)
    else:
        asyncio.run(main())
