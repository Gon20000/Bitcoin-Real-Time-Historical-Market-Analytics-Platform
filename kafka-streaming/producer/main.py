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
import json
import logging
import os
import signal
import sys

import websockets
from confluent_kafka import Producer
from tenacity import before_sleep_log, retry, wait_random_exponential
from websockets.exceptions import ConnectionClosed

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "prod.binance.btc_usdt_1s.v1")
STREAM_URL = os.getenv(
    "BINANCE_WS_URL", "wss://stream.binance.com:9443/stream?timeUnit=MICROSECOND"
)
# I will declare symbols as an array to allow future expansion to multiple symbols if needed
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]

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


async def produce_kline(
    producer: Producer,
    topic: str,
    symbol: str,
    payload: dict,
    stop_event: asyncio.Event,
) -> None:
    key = symbol.encode("utf-8")
    value = json.dumps(payload).encode("utf-8")
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
            if stop_event.is_set():
                logger.error(
                    f"Shutdown interrupted queuing for {symbol} at "
                    f"open_time={payload.get('k', {}).get('t')} while producer queue was full."
                )
                return

            # Yields control back to the event loop so ping/pong messages can be processed and the queue can be drained
            await asyncio.sleep(min(0.1 * attempts, 1))


def parse_kline_message(raw_message: str) -> tuple[str, dict] | None:
    try:
        res = json.loads(raw_message)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return None

    if "error" in res:
        logger.error(f"Error received from server: {res['error']}")
        return None

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
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Attempting to reconnect...")
            raise


@retry(
    wait=wait_random_exponential(multiplier=1, min=1, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def connect_to_binance(producer: Producer, stop_event: asyncio.Event) -> None:
    # connect to the stream, subscribe to the symbols and
    # increase max_queue for better handling of worst-case scenarios
    async with websockets.connect(STREAM_URL, max_queue=1024) as websocket:
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

    is_shutting_down = False
    shutdown_delivery_failures = 0

    def on_delivery(err, msg) -> None:
        nonlocal shutdown_delivery_failures
        if err is not None and is_shutting_down:
            shutdown_delivery_failures += 1
        kafka_delivery_report(err, msg)

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
        stream_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Register signal handlers to set the stop_event when a SIGINT or SIGTERM is received
        loop.add_signal_handler(sig, handle_shutdown)

    shutdown_failed = False
    try:
        await stream_task
    except asyncio.CancelledError:
        logger.info("Streaming task cancelled cleanly.")
    finally:
        is_shutting_down = True
        logger.info("Flushing remaining messages to Kafka...")
        # Flush the producer to ensure all messages are sent before shutdown
        remaining = producer.flush()
        if remaining > 0 or shutdown_delivery_failures > 0:
            logger.error(
                f"Shutdown failed to deliver all Kafka records "
                f"(remaining={remaining}, failed={shutdown_delivery_failures})."
            )
            shutdown_failed = True
        else:
            logger.info("All messages delivered successfully.")
            logger.info("Shutdown complete.")

    # putting it inside finally completely erases crashes from the try block.
    if shutdown_failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
