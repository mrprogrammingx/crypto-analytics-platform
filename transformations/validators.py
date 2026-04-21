from typing import Any


def is_valid_trade(data: Any) -> bool:
    """Quick validation for incoming trade payloads.

    This keeps checks lightweight: presence of required fields and positive numeric values.
    More thorough normalization/validation lives in `ingestion.clean.clean_trade`.
    """
    if data is None:
        return False
    if not isinstance(data, dict):
        return False

    required = ("price", "quantity", "timestamp")
    for key in required:
        if key not in data:
            return False

    try:
        price = float(data["price"])
        qty = float(data["quantity"])
    except Exception:
        return False

    return price > 0 and qty > 0