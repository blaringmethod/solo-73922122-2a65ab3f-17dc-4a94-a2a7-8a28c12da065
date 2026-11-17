"""Cursor (keyset) pagination tests for the data-lake read API.

The motivating scenario (see module docstring of ``app.cursor``): device A's
early-accepted batch stays PENDING behind a slow head-of-line batch while
device B's later-accepted batches are released first; once A is finally
released, OFFSET would repeat B and skip A.  The continuation cursor must
deliver every released batch exactly once, in release order, survive retries
and empty-page polling, and stay valid after an API instance restart.
"""
import asyncio
import time

import pytest

from app.cursor import CursorError, ReleaseCursor, decode_cursor, encode_cursor
from app.service import get_released_data
from tests.conftest import submit, uniform_csv, wait_status


# --------------------------------------------------------------------- unit
def test_cursor_round_trip_preserves_timestamp_and_id():
    from datetime import datetime, timezone

    ts = datetime(2026, 11, 17, 9, 30, 15, 123456, tzinfo=timezone.utc)
    token = encode_cursor(ReleaseCursor(decided_at=ts, batch_id=42))
    assert isinstance(token, str) and "." not in token  # opaque url-safe token
    back = decode_cursor(token)
    assert back.batch_id == 42
    assert back.decided_at == ts


@pytest.mark.parametrize("bad", [
    "not-a-cursor",
    "!!!",
    "eyJ2IjogOX0",                       # base64 JSON, wrong version
    "eyJ0IjogIngifQ",                    # missing fields
    "eyJ2IjogMSwgInQiOiAieCIsICJpZCI6IC0xfQ",  # negative id
])
def test_decode_rejects_malformed_cursors(bad):
    with pytest.raises(CursorError):
        decode_cursor(bad)


def test_decode_rejects_naive_timestamp():
    import base64
    import json

    payload = base64.urlsafe_b64encode(
        json.dumps({"v": 1, "t": "2026-01-01T00:00:00", "id": 1}).encode()
    ).decode()
    with pytest.raises(CursorError):
        decode_cursor(payload)


# --------------------------------------------------------------------- HTTP
async def _drain_with_cursor(ac, *, limit, device_id=None, batch_no=None):
    """Walk every page following next_cursor; return (items, page_count)."""
    seen = []
    pages = 0
    cursor = None
    while True:
        params = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if device_id:
            params["device_id"] = device_id
        if batch_no:
            params["batch_no"] = batch_no
        body = (await ac.get("/data/released", params=params)).json()
        pages += 1
        seen.extend(body["items"])
        if not body["has_more"]:
            return seen, pages, body
        cursor = body["next_cursor"]


@pytest.mark.asyncio
async def test_interleaved_release_cursor_never_skips_or_repeats(
        client, tmp_path, monkeypatch):
    ac = client
    slow_file = tmp_path / "slow_batches.txt"
    from app.config import settings as _cfg
    monkeypatch.setattr(_cfg, "slow_marker_file", str(slow_file))

    from app.db import get_pool
    from app.main import app
    from app.worker import DecisionWorker

    # Deterministic setup: pause the lifespan worker so nothing is decided
    # while the scenario is being staged.
    embedded = app.state.worker
    await embedded.stop()

    # Device A: head batch A1 will be slow; A2 waits behind it.
    a1 = await submit(ac, "A1", "DEV-A", uniform_csv("a1", 4, 500))
    assert a1.status_code == 202
    a1_id = a1.json()["batch_id"]
    await submit(ac, "A2", "DEV-A", uniform_csv("a2", 4, 500))

    # Device B: later accepted, but its batches will be released first.
    await submit(ac, "B1", "DEV-B", uniform_csv("b1", 4, 700))
    await submit(ac, "B2", "DEV-B", uniform_csv("b2", 4, 700))
    await submit(ac, "B3", "DEV-B", uniform_csv("b3", 4, 700))

    # Slow marker in place before any worker looks at the queue: whichever
    # worker picks DEV-A holds its device lock for 4s inside A1.
    slow_file.write_text(f"{a1_id}:4\n")

    # Two workers, exactly like the two API containers behind nginx.
    w1 = DecisionWorker(get_pool(), "pytest-w1")
    w2 = DecisionWorker(get_pool(), "pytest-w2")
    w1.start()
    w2.start()
    try:
        # Interleaving is real: while A1 is still pending, B already released.
        interleaved = False
        deadline = time.time() + 12
        while time.time() < deadline:
            st = (await ac.get("/batches/A1")).json()["status"]
            b1 = (await ac.get("/batches/B1")).json()["status"]
            if st == "PENDING" and b1 == "RELEASED":
                interleaved = True
                break
            await asyncio.sleep(0.1)
        assert interleaved, "B was not released while A's slow batch was pending"

        for name in ("A1", "A2", "B1", "B2", "B3"):
            await wait_status(ac, name, "RELEASED")
    finally:
        await w1.stop()
        await w2.stop()
        # Hand a live worker back for fixture teardown / later assertions.
        embedded.start()

    # Walk the whole stream one batch at a time via the returned cursors.
    items, pages, last = await _drain_with_cursor(ac, limit=1)
    assert pages == 5
    nos = [it["batch_no"] for it in items]
    assert sorted(nos) == ["A1", "A2", "B1", "B2", "B3"]
    assert len(set(nos)) == 5  # no repeats, no misses

    # Release order: B's batches became visible before the slow A batches.
    a1_item = next(it for it in items if it["batch_no"] == "A1")
    b3_item = next(it for it in items if it["batch_no"] == "B3")
    assert b3_item["decided_at"] < a1_item["decided_at"]
    assert nos.index("B3") < nos.index("A1")
    # Within each device FIFO order is preserved.
    assert nos.index("B1") < nos.index("B2") < nos.index("B3")
    assert nos.index("A1") < nos.index("A2")
    assert last["has_more"] is False

    # Retrying the very first cursor-less page, then every stored cursor,
    # reproduces the same batches in the same order (idempotent continuation).
    again, _, _ = await _drain_with_cursor(ac, limit=1)
    assert [it["batch_no"] for it in again] == nos


@pytest.mark.asyncio
async def test_same_cursor_retry_returns_same_page(client):
    ac = client
    for i in range(3):
        await submit(ac, f"R{i}", "DEV-R", uniform_csv(f"r{i}", 3, 500))
        await wait_status(ac, f"R{i}", "RELEASED")

    first = (await ac.get("/data/released", params={"limit": 1})).json()
    cur = first["next_cursor"]
    p1 = (await ac.get("/data/released", params={"limit": 1, "cursor": cur})).json()
    p2 = (await ac.get("/data/released", params={"limit": 1, "cursor": cur})).json()
    assert p1["items"] == p2["items"]
    assert p1["next_cursor"] == p2["next_cursor"]


@pytest.mark.asyncio
async def test_empty_page_keeps_cursor_and_later_poll_finds_new_release(
        client, tmp_path, monkeypatch):
    ac = client
    slow_file = tmp_path / "slow_batches.txt"
    from app.config import settings as _cfg
    monkeypatch.setattr(_cfg, "slow_marker_file", str(slow_file))

    await submit(ac, "E1", "DEV-E", uniform_csv("e1", 3, 500))
    await wait_status(ac, "E1", "RELEASED")

    # Drain to the end of the stream.
    items, _, last = await _drain_with_cursor(ac, limit=1)
    assert [it["batch_no"] for it in items] == ["E1"]
    end_cursor = last["next_cursor"]
    assert last["has_more"] is False

    # Polling past the end yields an empty page but the SAME cursor.
    empty = (await ac.get(
        "/data/released", params={"limit": 1, "cursor": end_cursor})).json()
    assert empty["items"] == []
    assert empty["has_more"] is False
    assert empty["next_cursor"] == end_cursor

    # A newly released batch is discovered by continuing to poll that cursor.
    await submit(ac, "E2", "DEV-E", uniform_csv("e2", 3, 500))
    await wait_status(ac, "E2", "RELEASED")
    found = None
    deadline = time.time() + 10
    while time.time() < deadline:
        body = (await ac.get(
            "/data/released", params={"limit": 1, "cursor": end_cursor})).json()
        if body["items"]:
            found = body
            break
        await asyncio.sleep(0.15)
    assert found is not None
    assert found["items"][0]["batch_no"] == "E2"


@pytest.mark.asyncio
async def test_quarantined_batches_invisible_to_cursor_walk(client):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"Q{i}", "DEV-Q", uniform_csv(f"q{i}", 4, 500))
        await wait_status(ac, f"Q{i}", "RELEASED")
    await submit(ac, "QOUT", "DEV-Q", uniform_csv("qout", 4, 900))
    await wait_status(ac, "QOUT", "QUARANTINED")
    await submit(ac, "Q4", "DEV-Q", uniform_csv("q4", 4, 501))
    await wait_status(ac, "Q4", "RELEASED")

    items, _, _ = await _drain_with_cursor(ac, limit=2)
    nos = [it["batch_no"] for it in items]
    assert sorted(nos) == ["Q1", "Q2", "Q3", "Q4"]
    assert "QOUT" not in nos

    # Explicit filter on the quarantined batch still returns nothing.
    r = await ac.get("/data/released",
                     params={"batch_no": "QOUT", "limit": 10})
    assert r.json()["items"] == []


@pytest.mark.asyncio
async def test_cursor_respects_device_filter_and_filtered_total(client):
    ac = client
    for d in ("DEV-F1", "DEV-F2"):
        for i in range(2):
            await submit(ac, f"{d}-B{i}", d, uniform_csv(f"{d}{i}", 3, 500))
    for d in ("DEV-F1", "DEV-F2"):
        for i in range(2):
            await wait_status(ac, f"{d}-B{i}", "RELEASED")

    items, _, body = await _drain_with_cursor(ac, limit=1, device_id="DEV-F1")
    assert {it["batch_no"] for it in items} == {"DEV-F1-B0", "DEV-F1-B1"}
    # total_released counts the rows matching the current filters.
    assert body["total_released"] == 2

    # The device filter composes with cursor continuation: every page keeps it.
    cursor = None
    page_devices = set()
    for _ in range(5):
        params = {"limit": 1, "device_id": "DEV-F1"}
        if cursor:
            params["cursor"] = cursor
        b = (await ac.get("/data/released", params=params)).json()
        page_devices.update(it["device_id"] for it in b["items"])
        if not b["has_more"]:
            break
        cursor = b["next_cursor"]
    assert page_devices == {"DEV-F1"}


@pytest.mark.asyncio
async def test_malformed_cursor_returns_400(client):
    ac = client
    r = await ac.get("/data/released", params={"cursor": "garbage"})
    assert r.status_code == 400
    assert "error" in r.json()


@pytest.mark.asyncio
async def test_offset_mode_retained_and_also_returns_cursor(client):
    ac = client
    for i in range(4):
        await submit(ac, f"O{i}", "DEV-O", uniform_csv(f"o{i}", 2, 500))
        await wait_status(ac, f"O{i}", "RELEASED")

    p1 = (await ac.get("/data/released",
                       params={"limit": 2, "offset": 0})).json()
    p2 = (await ac.get("/data/released",
                       params={"limit": 2, "offset": 2})).json()
    ids1 = {it["batch_no"] for it in p1["items"]}
    ids2 = {it["batch_no"] for it in p2["items"]}
    assert ids1.isdisjoint(ids2) and len(ids1 | ids2) == 4
    assert p1["offset"] == 0 and p1["next_cursor"]
    assert p2["has_more"] is False

    # In cursor mode offset is echoed back as null and is ignored.
    cur = p1["next_cursor"]
    cp = (await ac.get("/data/released",
                       params={"limit": 2, "offset": 99, "cursor": cur})).json()
    assert cp["offset"] is None
    assert [it["batch_no"] for it in cp["items"]] == sorted(ids2)


@pytest.mark.asyncio
async def test_cursor_survives_instance_restart_and_new_connection(client):
    """A cursor must outlive the process/connection that issued it.

    The token encodes only immutable row values and keeps no instance-local
    state.  A *brand new* database connection (the equivalent of a restarted
    API instance opening its own pool) continues from the stored cursor and
    resumes exactly where the previous instance stopped -- no repeat, no skip.
    """
    import json as _json

    import asyncpg

    from app.config import settings

    ac = client
    for i in range(4):
        await submit(ac, f"Z{i}", "DEV-Z", uniform_csv(f"z{i}", 3, 500))
        await wait_status(ac, f"Z{i}", "RELEASED")

    first = (await ac.get("/data/released", params={"limit": 2})).json()
    assert [it["batch_no"] for it in first["items"]] == ["Z0", "Z1"]
    saved_cursor = first["next_cursor"]
    # The token is self-contained: decode it purely from its string form.
    decoded = decode_cursor(saved_cursor)
    assert decoded.batch_id > 0

    # A fresh, independent pool -- nothing shared with the API instance; this
    # is what a restarted container builds on startup.
    new_pool = await asyncpg.create_pool(
        settings.database_dsn, min_size=1, max_size=2,
        init=lambda c: c.set_type_codec(
            "jsonb", encoder=_json.dumps, decoder=_json.loads,
            schema="pg_catalog"))
    try:
        page = await get_released_data(
            new_pool, device_id=None, batch_no=None,
            limit=10, offset=0, cursor=saved_cursor)
        assert [it["batch_no"] for it in page.items] == ["Z2", "Z3"]
        assert page.has_more is False
        # Re-issuing the same cursor over yet another page read is stable.
        again = await get_released_data(
            new_pool, device_id=None, batch_no=None,
            limit=10, offset=0, cursor=saved_cursor)
        assert [it["batch_no"] for it in again.items] == ["Z2", "Z3"]
    finally:
        await new_pool.close()
