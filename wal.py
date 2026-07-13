"""
wal.py -- Write-Ahead Log for durable workflow state.

This is the actual persistence layer the Coordinator uses. Every completed
transition is appended as an immutable JSON line. Current state is never
stored directly -- it is *derived* by replaying the log (event sourcing).
This is what lets a brand new process reconstruct exactly where a workflow
was, with no in-memory state required.

Cross-process safe via flock (multiple worker processes may append
concurrently).
"""
import json
import os
import time
import fcntl

WAL_DIR = os.environ.get("WAL_DIR", os.path.join(os.path.dirname(__file__), "wal_data"))


def _path(workflow_id: str) -> str:
    os.makedirs(WAL_DIR, exist_ok=True)
    return os.path.join(WAL_DIR, f"{workflow_id}.jsonl")


def append_event(workflow_id: str, event_type: str, payload: dict | None = None) -> dict:
    path = _path(workflow_id)
    # Open in append mode, take an exclusive OS-level lock so concurrent
    # worker processes can't interleave/corrupt writes.
    with open(path, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            seq = sum(1 for _ in f)
            event = {
                "seq": seq,
                "ts": time.time(),
                "type": event_type,
                "payload": payload or {},
            }
            f.write(json.dumps(event) + "\n")
            f.flush()
            os.fsync(f.fileno())
            return event
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def read_events(workflow_id: str) -> list[dict]:
    path = _path(workflow_id)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        try:
            return [json.loads(line) for line in f if line.strip()]
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def exists(workflow_id: str) -> bool:
    return os.path.exists(_path(workflow_id))
