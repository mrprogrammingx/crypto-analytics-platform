"""Ingestion package exports for exchange connectors.

This package currently provides a Binance websocket connector and helpers to
integrate with Kafka. We avoid importing Kafka producer at package import time
because it may attempt to connect to a broker; use `get_kafka_handler()` to
retrieve the handler lazily.
"""
from .binance_websocket import run_binance_socket  # noqa: F401


def get_kafka_handler():
	"""Lazily import and return the kafka_handler from ingestion.kafka_producer.

	This avoids creating a KafkaProducer during package import (which would
	attempt to connect to localhost:9092). Call this when you actually need
	to send messages to Kafka.
	"""
	from . import kafka_producer

	return kafka_producer.kafka_handler


__all__ = ["run_binance_socket", "get_kafka_handler"]
