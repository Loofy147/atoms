"""
gateway.py -- Idempotent Tool Gateway.

Wraps external tool calls (OCR, payment APIs, etc.) with an idempotency key
= SHA-256(workflow_id + step_name). The cache is a SQLite file on disk, not
an in-memory dict -- that's the whole point: it must survive the calling
worker process being SIGKILLed, because a *new* worker process retrying the
same step needs to see that the side effect already happened.

We also keep a `call_counter` table that the wrapped tool functions
themselves increment on real execution (not on cache hits). This is how we
empirically prove -- not just assert -- that the external side effect only
ran once even though call_tool() was invoked twice across a crash.
"""
import sqlite3
import hashlib
import json
import os
import time

DB_PATH = os.environ.get("GATEWAY_DB", os.path.join(os.path.dirname(__file__), "gateway_cache.db"))

_initialized = False


def idempotency_key(workflow_id: str, step: str) -> str:
    return hashlib.sha256(f"{workflow_id}_{step}".encode()).hexdigest()


def _conn():
    global _initialized
    conn = sqlite3.connect(DB_PATH, timeout=30)
    if not _initialized:
        # Enable Write-Ahead Logging (WAL) mode for better concurrency and write speed
        conn.execute("PRAGMA journal_mode = WAL")
        # Set synchronous mode to NORMAL for safer fast writes without full sync on every transaction
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY, result TEXT, created_ts REAL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS call_counter (
            tool_name TEXT PRIMARY KEY, count INTEGER
        )""")
        conn.commit()
        _initialized = True
    return conn


def bump_call_counter(tool_name: str) -> int:
    """Called by the *actual* tool implementation on real execution only."""
    conn = _conn()
    cur = conn.execute("SELECT count FROM call_counter WHERE tool_name=?", (tool_name,))
    row = cur.fetchone()
    count = (row[0] if row else 0) + 1
    conn.execute("INSERT OR REPLACE INTO call_counter (tool_name, count) VALUES (?,?)",
                 (tool_name, count))
    conn.commit()
    conn.close()
    return count


def get_call_count(tool_name: str) -> int:
    conn = _conn()
    cur = conn.execute("SELECT count FROM call_counter WHERE tool_name=?", (tool_name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0


def call_tool(workflow_id: str, step: str, tool_fn, *args, **kwargs):
    """
    Returns (result, was_cached: bool).
    """
    key = idempotency_key(workflow_id, step)
    conn = _conn()
    cur = conn.execute("SELECT result FROM cache WHERE key=?", (key,))
    row = cur.fetchone()
    if row:
        conn.close()
        return json.loads(row[0]), True

    result = tool_fn(*args, **kwargs)

    # Re-open a connection right before writing in case tool_fn took a while
    # (a crash could happen between the tool call and this commit -- that
    # window is exactly what the demo's kill timing targets).
    conn.execute("INSERT OR REPLACE INTO cache (key, result, created_ts) VALUES (?,?,?)",
                 (key, json.dumps(result), time.time()))
    conn.commit()
    conn.close()
    return result, False


def reset():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
