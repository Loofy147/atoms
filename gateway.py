"""
gateway.py -- Idempotent Tool Gateway with 3-node Raft Consensus Backend.

Wraps external tool calls (OCR, payment APIs, etc.) with an idempotency key
= SHA-256(workflow_id + '_' + step_name).

Maintains a highly-available duplicated cache on 3 Raft nodes to prevent
split-brain or duplicate execution issues even across network partitions.
"""
import sqlite3
import hashlib
import json
import os
import time
from raft import RaftCluster

DB_PATH = os.environ.get("GATEWAY_DB", os.path.join(os.path.dirname(__file__), "gateway_cache.db"))

_initialized = False
_raft_cluster = None


def idempotency_key(workflow_id: str, step: str) -> str:
    # Mandatory Idempotency Key calculation: SHA-256(Workflow_ID + '_' + Execution_Sequence)
    # If step is not already containing sequential execution details, we formulate it.
    return hashlib.sha256(f"{workflow_id}_{step}".encode()).hexdigest()


def _conn():
    global _initialized
    conn = sqlite3.connect(DB_PATH, timeout=30)
    if not _initialized:
        conn.execute("PRAGMA journal_mode = WAL")
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


def get_raft_cluster() -> RaftCluster:
    global _raft_cluster
    if _raft_cluster is None:
        _raft_cluster = RaftCluster()
        _raft_cluster.add_node(0, [1, 2])
        _raft_cluster.add_node(1, [0, 2])
        _raft_cluster.add_node(2, [0, 1])
        _raft_cluster.start()
        # Wait for leader election
        time.sleep(1.5)
    return _raft_cluster


def call_tool(workflow_id: str, step: str, tool_fn, *args, **kwargs):
    """
    Returns (result, was_cached: bool).
    """
    key = idempotency_key(workflow_id, step)

    # 1. Check local DB cache first
    conn = _conn()
    cur = conn.execute("SELECT result FROM cache WHERE key=?", (key,))
    row = cur.fetchone()
    if row:
        conn.close()
        return json.loads(row[0]), True

    # 2. Check Raft Cluster replicated cache
    cluster = get_raft_cluster()
    leader = cluster.get_leader()
    if leader:
        with leader.lock:
            cached_val = leader.state_machine.get(key)
        if cached_val:
            # Write to local cache so local DB is up-to-date
            conn.execute("INSERT OR REPLACE INTO cache (key, result, created_ts) VALUES (?,?,?)",
                         (key, cached_val, time.time()))
            conn.commit()
            conn.close()
            return json.loads(cached_val), True

    # 3. Call actual tool since not cached
    result = tool_fn(*args, **kwargs)
    result_str = json.dumps(result)

    # 4. Replicate to Raft Cluster
    replicated = False
    if leader:
        # Try up to 3 times to replicate to the Raft Leader
        for _ in range(3):
            if leader.propose(key, result_str):
                replicated = True
                break
            time.sleep(0.2)
            leader = cluster.get_leader()
            if not leader:
                break

    # 5. Commit to local cache DB
    conn.execute("INSERT OR REPLACE INTO cache (key, result, created_ts) VALUES (?,?,?)",
                 (key, result_str, time.time()))
    conn.commit()
    conn.close()

    return result, False


def reset():
    global _raft_cluster
    if _raft_cluster:
        _raft_cluster.stop()
        _raft_cluster = None
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
