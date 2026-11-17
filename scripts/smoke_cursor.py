"""Live HTTP smoke test of cursor pagination against a running uvicorn.

Reproduces the reported interleaving against a real server:

* device A's early-accepted head batch is held back by a slow marker;
* device B's later-accepted batches are released first;
* draining /data/released strictly via next_cursor returns every released
  batch exactly once -- B... before A -- with no skip and no repeat;
* polling an end-of-stream cursor after an empty page later surfaces a new
  release; a malformed cursor is rejected with HTTP 400.

Usage: python -m scripts.smoke_cursor --base http://127.0.0.1:8201 \
        --marker /tmp/smoke_markers/slow.txt
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import time
import urllib.error
import urllib.request
import uuid


def http(method: str, url: str, data: bytes | None = None,
         content_type: str | None = None):
    req = urllib.request.Request(url, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def upload(base: str, batch_no: str, device: str, thickness: int,
           count: int = 4):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["wafer_no", "thickness_nm"])
    for i in range(1, count + 1):
        w.writerow([f"{batch_no}-W{i:03d}", thickness])
    boundary = f"----smoke{uuid.uuid4().hex}"
    parts = []
    for name, value in (("batch_no", batch_no), ("device_id", device)):
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                     f"name=\"{name}\"\r\n\r\n{value}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{batch_no}.csv\"\r\nContent-Type: text/csv\r\n\r\n"
        .encode() + buf.getvalue().encode() + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return http("POST", f"{base}/batches", b"".join(parts),
                f"multipart/form-data; boundary={boundary}")


def wait_status(base: str, batch_no: str, expected: str, timeout: float = 30):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        code, last = http("GET", f"{base}/batches/{batch_no}")
        if code == 200 and last.get("status") == expected:
            return last
        time.sleep(0.2)
    raise AssertionError(f"{batch_no} never reached {expected}: {last}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8201")
    ap.add_argument("--marker", required=True)
    args = ap.parse_args()
    base = args.base
    run = uuid.uuid4().hex[:6]
    dev_a, dev_b = f"SMK-A-{run}", f"SMK-B-{run}"

    print("== upload A1/A2 (A1 will be slow); mark slow before B arrives ==")
    code, a1 = upload(base, f"{run}-A1", dev_a, 500)
    assert code == 202, a1
    code, _ = upload(base, f"{run}-A2", dev_a, 500)
    assert code == 202
    with open(args.marker, "w") as fh:
        fh.write(f"{a1['batch_id']}:5\n")
    print(f"   slow marker: batch_id={a1['batch_id']} sleeps 5s")

    print("== later-accepted B1..B3 on another device ==")
    for i in (1, 2, 3):
        code, _ = upload(base, f"{run}-B{i}", dev_b, 700)
        assert code == 202

    # Prove the interleaving really happens.
    interleaved = False
    deadline = time.time() + 12
    while time.time() < deadline:
        _, a1row = http("GET", f"{base}/batches/{run}-A1")
        _, b1row = http("GET", f"{base}/batches/{run}-B1")
        if a1row["status"] == "PENDING" and b1row["status"] == "RELEASED":
            interleaved = True
            break
        time.sleep(0.15)
    assert interleaved, "B never overtook the slow A batch"
    print("   interleaving observed: B1 RELEASED while A1 still PENDING")

    for name in (f"{run}-A1", f"{run}-A2", f"{run}-B1",
                 f"{run}-B2", f"{run}-B3"):
        wait_status(base, name, "RELEASED")
    print("   all five batches RELEASED")

    print("== drain strictly via next_cursor, limit=1 ==")
    seen, cursor, pages = [], None, 0
    while True:
        url = f"{base}/data/released?limit=1"
        if cursor:
            url += f"&cursor={cursor}"
        code, body = http("GET", url)
        assert code == 200, body
        pages += 1
        seen.extend(it["batch_no"] for it in body["items"])
        if not body["has_more"]:
            end_cursor = body["next_cursor"]
            break
        cursor = body["next_cursor"]
    mine = [n for n in seen if n.startswith(run)]
    print(f"   pages={pages}, release order of this run: {mine}")
    assert sorted(mine) == sorted(
        [f"{run}-A1", f"{run}-A2", f"{run}-B1", f"{run}-B2", f"{run}-B3"])
    assert len(mine) == len(set(mine)), "duplicate batch in cursor walk"
    assert mine.index(f"{run}-B3") < mine.index(f"{run}-A1"), \
        "slow A batch must appear after B's releases"
    assert mine.index(f"{run}-B1") < mine.index(f"{run}-B2") \
        < mine.index(f"{run}-B3")
    assert mine.index(f"{run}-A1") < mine.index(f"{run}-A2")

    print("== empty page keeps the same cursor; retry is deterministic ==")
    _, empty1 = http("GET", f"{base}/data/released?limit=1&cursor={end_cursor}")
    _, empty2 = http("GET", f"{base}/data/released?limit=1&cursor={end_cursor}")
    assert empty1["items"] == [] and empty2["items"] == []
    assert empty1["next_cursor"] == end_cursor == empty2["next_cursor"]

    print("== new release discovered by continuing to poll that cursor ==")
    code, _ = upload(base, f"{run}-B4", dev_b, 700)
    assert code == 202
    wait_status(base, f"{run}-B4", "RELEASED")
    found = None
    deadline = time.time() + 15
    while time.time() < deadline:
        _, body = http("GET",
                       f"{base}/data/released?limit=1&cursor={end_cursor}")
        if body["items"]:
            found = body["items"][0]["batch_no"]
            break
        time.sleep(0.2)
    assert found == f"{run}-B4", found
    print(f"   poll after empty page discovered new release: {found}")

    print("== malformed cursor -> 400 ==")
    code, body = http("GET", f"{base}/data/released?cursor=not-a-cursor")
    assert code == 400 and "error" in body, body
    print(f"   HTTP 400: {body['error']}")

    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
