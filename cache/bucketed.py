from __future__ import annotations

import hashlib
import json
import math
from typing import Any


def quantize_value(value: Any, bucket_size: float = 10.0) -> Any:
    """Quantize numeric values to buckets for near-miss deduplication."""
    if isinstance(value, (int, float)) and not math.isnan(value):
        return round(value / bucket_size) * bucket_size
    if isinstance(value, str):
        return value.strip().lower()
    return value


def bucketed_key(step: int, rule_hash: str, row: dict[str, Any]) -> str:
    """Compute a cache key with quantized values for near-miss row matching."""
    bucketed = {k: quantize_value(v) for k, v in sorted(row.items())}
    raw = json.dumps({"step": step, "rule": rule_hash, "row": bucketed}, sort_keys=True, default=str)
    return "bucketed:" + hashlib.sha256(raw.encode()).hexdigest()


def exact_key(step: int, rule_hash: str, row: dict[str, Any]) -> str:
    """Exact cache key for a row (canonical JSON + SHA256)."""
    raw = json.dumps({"step": step, "rule": rule_hash, "row": row}, sort_keys=True, default=str)
    return "exact:" + hashlib.sha256(raw.encode()).hexdigest()
