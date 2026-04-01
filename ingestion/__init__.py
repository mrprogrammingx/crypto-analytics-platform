"""Ingestion package exports for exchange connectors.

This package currently provides a Binance websocket connector.
"""
from .binance_websocket import run_binance_socket, on_message  # noqa: F401

__all__ = ["run_binance_socket", "on_message"]
