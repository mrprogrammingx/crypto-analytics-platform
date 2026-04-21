"""Transformation helpers for trade messages.

This replaces the old `transformations.py` and exposes small, testable
functions for normalizing and converting incoming trade payloads.
"""
from datetime import datetime, timezone
from typing import Optional, Dict, Any


def normalize_symbol(symbol: Any) -> str:
    if symbol is None:
        return "UNKNOWN"
    return str(symbol).upper()


def parse_timestamp(ts: Any) -> Optional[int]:
    """Parse timestamp-like values into an integer milliseconds epoch.

    Accepts integers, floats, or ISO8601-like strings. Returns None if parsing fails.
    """
    if ts is None:
        return None
    try:
        # If it's already numeric, assume it's epoch ms or s
        if isinstance(ts, (int, float)):
            # Heuristic: if ts looks like seconds (10 digits) convert to ms
            if ts < 1e11:
                return int(ts * 1000)
            return int(ts)

        # Try parsing an ISO timestamp
        parsed = datetime.fromisoformat(str(ts))
        # make timezone-aware in UTC
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def clean_trade(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a trade record into the canonical shape used by loaders.

    Returns a dict with keys: symbol (str), price (float), quantity (float), timestamp (int ms),
    or None if the input is invalid or should be dropped.
    """
    if not isinstance(data, dict):
        return None

    # symbol
    symbol = normalize_symbol(data.get("symbol"))

    # price & quantity
    try:
        price = float(data.get("price"))
        quantity = float(data.get("quantity"))
    except Exception:
        return None

    # basic sanity
    if price <= 0 or quantity <= 0:
        return None

    # timestamp
    ts = parse_timestamp(data.get("timestamp"))
    if ts is None:
        return None

    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    
    return {
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "timestamp": dt,
    }
