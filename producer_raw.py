"""
Always-on producer: polls randomuser.me and publishes the RAW response to
Kafka topic 'users_created' — no field selection, no flattening.

This is a deliberate change from the earlier version of this file. Bronze
layer must store data exactly as the source API delivered it (matching the
Medallion principle your Project 2/3 notebooks already follow) — all parsing,
flattening, and field extraction happens downstream in the Bronze -> Silver
Databricks notebook, not here.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   default: broker:29092
  POLL_INTERVAL_SECONDS     default: 1.0   (delay between API calls)
  API_URL                   default: https://randomuser.me/api/
"""

import json
import logging
import os
import signal
import sys
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092").split(",")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1.0"))
API_URL = os.getenv("API_URL", "https://randomuser.me/api/")
TOPIC = "users_created"

_shutdown_requested = False


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    log.info("Shutdown signal received (%s) — finishing current iteration then exiting", signum)
    _shutdown_requested = True


def get_raw_record():
    """
    Returns the single user record exactly as randomuser.me sent it —
    results[0], completely unmodified. No fields selected, none renamed,
    none dropped. This IS the Bronze-layer payload.
    """
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()["results"][0]


def build_producer():
    log.info("Connecting to Kafka at %s", BOOTSTRAP_SERVERS)
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        max_block_ms=5000,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
    )


def run():
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    producer = build_producer()
    sent_count = 0
    error_count = 0

    log.info("Producer started (RAW mode). Polling every %ss, topic=%s", POLL_INTERVAL_SECONDS, TOPIC)

    while not _shutdown_requested:
        try:
            raw_record = get_raw_record()
            producer.send(TOPIC, value=raw_record)
            sent_count += 1

            if sent_count % 100 == 0:
                log.info("Sent %d raw records so far (%d errors)", sent_count, error_count)

        except requests.RequestException as e:
            error_count += 1
            log.warning("API request failed: %s", e)
        except KafkaError as e:
            error_count += 1
            log.error("Kafka send failed: %s", e)
        except Exception as e:
            error_count += 1
            log.exception("Unexpected error: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)

    log.info("Flushing producer and shutting down. Total sent: %d, errors: %d", sent_count, error_count)
    producer.flush(timeout=10)
    producer.close(timeout=10)
    sys.exit(0)


if __name__ == "__main__":
    run()
