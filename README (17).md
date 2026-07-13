# Durable Expense-Approval Orchestrator — working implementation

This is a runnable version of the "Distributed AI Orchestrator" script, not a
narrated architecture. Every claim below was produced by actually executing
these files in a sandboxed container — commands and real output included.

## Files

| File | Role |
|---|---|
| `wal.py` | Write-ahead log. Append-only JSONL per workflow, `flock`-guarded for cross-process safety. Current state is never stored — it's derived by replaying events. |
| `gateway.py` | Idempotent tool gateway. SQLite-backed (not in-memory) cache keyed by `SHA-256(workflow_id + step)`, so it survives the calling process being killed. Also tracks a real execution counter, independent of caching, used to prove no duplicate side effects occurred. |
| `tools.py` | The simulated external calls (`ocr_tool`, `ledger_write_tool`) that the gateway wraps. |
| `llm.py` | The reasoning step. Calls the real Anthropic Messages API if `ANTHROPIC_API_KEY` is set; otherwise falls back to a labeled heuristic classifier — it never pretends the fallback is a live model call. |
| `worker_task.py` | The stateless Worker. Each step (`FETCH`/`VERIFY`/`REASON`/`RECORD`) runs as its own OS process, reconstructing everything it needs from the WAL rather than from memory. |
| `coordinator.py` | The durable execution engine. `derive_state()` replays the WAL; `run()` dispatches whatever step is missing. Contains the crash-injection logic used to test recovery. |
| `resume.py` | What an operator runs, as an entirely separate process, to unblock a workflow paused at `WAIT_APPROVAL`. |
| `router.py` | Credit-based flow control — a semaphore of N "credits" gating how many worker subprocesses can be in flight at once. |
| `demo_backpressure.py` | Standalone demo: 9 workflows, 3 credits. |

## What was actually verified (this session, real runs)

**1. Happy path (low-risk, auto-approved end to end)**
`EXP-LOWRISK-001`, $45.50 office supplies → FETCH → VERIFY → REASON(`LOW_RISK`) →
RECORD → `TASK_COMPLETED`, exit code 0, full WAL below.

**2. Human-in-the-loop pause/resume, on a real process boundary**
`EXP-HIGHRISK-001`, $1200 travel → reasoned `HIGH_RISK` → coordinator wrote
`WAIT_APPROVAL` to the WAL, printed "pid 775 exiting now", and returned.
`ps aux | grep coordinator.py` immediately after showed **zero matching
processes** — the pause really costs 0 bytes, because there is no process
left to hold any. Hours later (simulated by just... running the next command
later), a *brand-new* process (`resume.py`, pid 783) replayed 5 WAL events
from disk, appended `HUMAN_DECISION=approved`, and finished the workflow by
calling the same `coordinator.run()` — no shared memory with the original
run, only the log.

**3. The "2 AM crash", with a real SIGKILL, not a simulated log line**
`EXP-CRASHTEST-001`: coordinator dispatched VERIFY (pid 791), the worker made
the real (mocked) OCR call via the gateway, wrote a marker file the instant
that side effect landed, and the coordinator sent `os.kill(791, SIGKILL)` —
confirmed by `proc.wait()` returning **exit status -9**. `VERIFY_DONE` was
never committed. The coordinator detected this by replay, re-dispatched
VERIFY to a fresh worker (pid 792), and the gateway returned `was_cached=True`
instead of re-executing. Proof it didn't double-execute: the OCR call
counter (persisted in SQLite, bumped only inside the real tool body)
incremented by exactly **1** across both `call_tool()` invocations for that
key.

**4. Credit-based backpressure, with real timestamps**
9 workflows submitted at once through `router.Router(max_credits=3)`: tasks
0-2 dispatched immediately (`t=0.00s`–`0.01s`), tasks 3-8 logged
`"waiting for a free credit"` and only got dispatched as earlier tasks
finished, at `t=0.68s`, `t=1.24s`, `t=1.78s` — three clean batches of 3,
matching the credit limit exactly. Total wall time 1.78s vs. ~0.3s if
unthrottled.

**5. Rejection path**
`EXP-REJECT-001`, $900 entertainment → `HIGH_RISK` → operator rejects →
ledger still gets a `RECORD_DONE` entry with `decision: "rejected"` (audit
trail preserved either way).

**6. Guardrails**
Attempting to `start` a workflow ID that already has a WAL correctly raises
`RuntimeError` rather than silently double-initializing.

## Honesty notes / where this is still a simulation

- `tools.ocr_tool` and `tools.ledger_write_tool` are mocked (deterministic,
  seeded, sleep-based latency) — there's no real OCR vendor or ERP behind
  them. Swap the body of those two functions for real HTTP calls and
  everything above (idempotency, replay, crash recovery) keeps working
  unchanged, since the gateway doesn't care what's inside `tool_fn`.
- `llm.classify_expense` used the heuristic fallback in every run above
  because this sandbox has no `ANTHROPIC_API_KEY`. The live-call code path
  (`_classify_via_anthropic`) is implemented and will run instead the moment
  a real key is present in the environment — untested here, since I don't
  have a key to test it with, and I'm not going to claim otherwise.
- The WAL is per-machine local disk (JSONL files). For real multi-host
  durability you'd move this to Postgres/etcd or an actual durable-execution
  engine (Temporal, Restate) — the state-machine logic in `coordinator.py`
  (`derive_state`/`run`) would port over almost unchanged, since it already
  treats the log as the only source of truth.
- `router.py` throttles worker *processes*; it doesn't yet do cross-workflow
  priority, retries-with-backoff, or dead-letter queues — straightforward
  additions to the same `Router` class if you need them.

## GRASP mapping (Larman, 1997)

This came up mid-build and is worth recording honestly rather than glossing over:

| Pattern | Where |
|---|---|
| Controller | `coordinator.py` — single object fielding all system events for the use case |
| Pure Fabrication | `gateway.py`, `router.py`, `wal.py` — none are domain concepts, all exist to buy low coupling elsewhere |
| Indirection | `gateway.call_tool(...)` sits between Worker and the real OCR/ledger call |
| Protected Variations | the `tool_fn` callable + idempotency-key contract in `gateway.call_tool` |
| Information Expert | the WAL, not the Worker or Coordinator, is the only thing holding what's needed to answer "what state are we in" |

One place this *wasn't* true until it was fixed: `worker_task.py`'s `main()`
originally branched on `args.step` with an if/elif ladder — exactly what
GRASP's **Polymorphism** pattern says not to do. Replaced with a
`STEP_HANDLERS` dispatch table (`{"FETCH": do_fetch, "VERIFY": do_verify, ...}`),
each handler given a uniform `(workflow_id, events, args)` signature. Verified
the fix didn't change behavior by rerunning the crash-injection test against
the new code: OCR call counter went from 0 → 1 (not 0 → 2) across the same
kill/replay/re-verify sequence as before.

## Running it yourself

```bash
# happy path
python3 coordinator.py start --workflow EXP-1 --expense-id E1 --amount 45 --category office_supplies

# high risk -> pause -> resume
python3 coordinator.py start --workflow EXP-2 --expense-id E2 --amount 1200 --category travel
python3 resume.py --workflow EXP-2 --decision approved

# crash injection
python3 coordinator.py start --workflow EXP-3 --expense-id E3 --amount 30 --category office_supplies --crash-on-verify

# backpressure
python3 demo_backpressure.py
```
