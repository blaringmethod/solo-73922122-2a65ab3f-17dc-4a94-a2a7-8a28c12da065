"""Opaque continuation cursors for the data-lake read API.

Why a cursor instead of OFFSET
------------------------------
Releases become visible in *decision* order, which differs from acceptance
order: an early-accepted batch of one device can stay PENDING behind a slow
head-of-line batch while a later-accepted batch of another device is released
first.  OFFSET pagination is computed against a changing result set -- once the
early batch is finally released it appears at the head of the list and every
stored offset shifts, so the next page repeats a row and skips another.

The cursor therefore keys on the immutable release position of a batch:
``(decided_at, id)``.  ``id`` (identity primary key) is the unique tie-breaker
for batches decided in the same transaction/microsecond.  Both values are
fixed the moment a batch is released and never change afterwards, so a stored
cursor stays valid forever -- across empty pages, process restarts and new
releases -- and advancing strictly by the returned cursor can neither repeat
nor skip a released batch.

The token is versioned and base64url-encoded JSON; callers must treat it as
opaque.  No instance-specific state is embedded, so a cursor obtained from one
API instance is valid on the other.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

CURSOR_VERSION = 1


class CursorError(ValueError):
    """Raised when a client-supplied cursor cannot be decoded."""


@dataclass(frozen=True)
class ReleaseCursor:
    """Exclusive lower bound: rows with (decided_at, id) greater than this."""

    decided_at: datetime
    batch_id: int

    def encode(self) -> str:
        return encode_cursor(self)


def encode_cursor(cursor: ReleaseCursor) -> str:
    payload: Dict[str, Any] = {
        "v": CURSOR_VERSION,
        # ISO-8601 keeps the value timezone-aware and exact to microseconds.
        "t": cursor.decided_at.isoformat(),
        "id": cursor.batch_id,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_cursor(token: str) -> ReleaseCursor:
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CursorError("游标格式非法 / malformed pagination cursor") from exc

    if not isinstance(payload, dict) or payload.get("v") != CURSOR_VERSION:
        raise CursorError("游标版本不受支持 / unsupported cursor version")
    raw_ts = payload.get("t")
    raw_id = payload.get("id")
    if not isinstance(raw_ts, str) or not isinstance(raw_id, int) \
            or isinstance(raw_id, bool) or raw_id < 0:
        raise CursorError("游标内容非法 / invalid cursor payload")
    try:
        decided_at = datetime.fromisoformat(raw_ts)
    except ValueError as exc:
        raise CursorError("游标时间戳非法 / invalid cursor timestamp") from exc
    if decided_at.tzinfo is None:
        raise CursorError("游标时间戳缺少时区 / cursor timestamp missing timezone")
    return ReleaseCursor(decided_at=decided_at, batch_id=raw_id)
