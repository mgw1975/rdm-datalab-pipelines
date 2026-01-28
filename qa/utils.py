from __future__ import annotations

import math
from typing import Optional


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    """Parse common CLI truthy/falsey strings."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    return default


def safe_divide(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """Return numerator/denominator or None when invalid."""
    if numerator is None or denominator in (None, 0):
        return None
    if isinstance(denominator, float) and math.isnan(denominator):
        return None
    if isinstance(numerator, float) and math.isnan(numerator):
        return None
    return numerator / denominator

