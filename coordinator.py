"""
coordinator.py -- the durable execution engine.

Holds no long-lived in-memory state of its own. Every call to run() derives
"where are we" purely by replaying the WAL from disk, then dispatches the
next step to a fresh stateless worker subprocess. This is what makes it
safe for the coordinator process itself to die and be restarted, and safe
for it to deliberately exit while waiting on a human.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

import wal

WORKER_SCRIPT = os.path.join(os.path.dirname(__file__), "worker_task.py")
MARKER_DIR = os.path.join(os.path.dirname(__file__), "markers")


def start_workflow(workflow_id: str, expense: dict):
    if wal.exists(workflow_id):
        raise RuntimeError(f"workflow {workflow_id} already has a WAL -- pick a new id")
    wal.append_event(workflow_id, "INIT", {"expense": expense})
    print(f"[coordinator] INIT committed to write-ahead log for {workflow_id}: {expense}")


def derive_state(events: list[dict]) -> dict:
    types = [e["type"] for e in events]
    risk = None
    approver = None
    evaluation_log = None
    for e in events:
        if e["type"] == "CLASSIFY_DONE":
            risk = e["payload"].get("risk")
        elif e["type"] == "ROUTE_DONE":
            approver = e["payload"].get("approver")
        elif e["type"] == "HUMAN_DECISION":
            evaluation_log = e["payload"].get("evaluation_log")
    return {
        "fetched": "FETCH_DONE" in types,
        "verified": "VERIFY_DONE" in types,
        "classified": "CLASSIFY_DONE" in types,
        "routed": "ROUTE_DONE" in types,
        "waiting_approval": "WAIT_APPROVAL" in types and "HUMAN_DECISION" not in types,
        "human_decided": "HUMAN_DECISION" in types,
        "recorded": "RECORD_DONE" in types,
        "completed": "TASK_COMPLETED" in types,
        "risk": risk,
        "approver": approver,
        "evaluation_log": evaluation_log,
    }


def dispatch(workflow_id: str, step: str, delay_before_commit: float = 0.0):
    cmd = [sys.executable, WORKER_SCRIPT, "--workflow", workflow_id, "--step", step]
    if delay_before_commit:
        cmd += ["--delay-before-commit", str(delay_before_commit)]
    return subprocess.Popen(cmd)


def wait_for_event(workflow_id: str, event_type: str, proc: subprocess.Popen, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if any(e["type"] == event_type for e in wal.read_events(workflow_id)):
            return True
        if proc.poll() is not None:
            time.sleep(0.05)
            return any(e["type"] == event_type for e in wal.read_events(workflow_id))
        time.sleep(0.15)
    return False


def run(workflow_id: str, crash_on_verify: bool = False, heartbeat_timeout: float = 8.0) -> str:
    print(f"=== Coordinator pid={os.getpid()}: running workflow {workflow_id} ===")
    events = wal.read_events(workflow_id)
    if not events:
        raise RuntimeError("workflow not initialized -- call start_workflow first")
    state = derive_state(events)
    print(f"[coordinator] replayed {len(events)} event(s) from WAL -> derived state: {state}")

    # Step 1: FETCH
    if not state["fetched"]:
        print("[coordinator] dispatching FETCH")
        proc = dispatch(workflow_id, "FETCH")
        proc.wait()
        state = derive_state(wal.read_events(workflow_id))

    # Step 2: VERIFY
    if not state["verified"]:
        os.makedirs(MARKER_DIR, exist_ok=True)
        marker_path = os.path.join(MARKER_DIR, f"{workflow_id}_verify_gateway_done")
        if os.path.exists(marker_path):
            os.remove(marker_path)

        delay = 4.0 if crash_on_verify else 0.0
        print(f"[coordinator] dispatching VERIFY{' (crash-injection armed)' if crash_on_verify else ''}")
        proc = dispatch(workflow_id, "VERIFY", delay_before_commit=delay)

        if crash_on_verify:
            print(f"[coordinator] supervising pid {proc.pid}; watching for post-side-effect marker...")
            deadline = time.time() + 8
            while time.time() < deadline and not os.path.exists(marker_path):
                time.sleep(0.1)
            if os.path.exists(marker_path):
                time.sleep(0.4)
                print(f"[coordinator] marker observed -- sending SIGKILL to pid {proc.pid} (simulated 2am crash)")
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                ret = proc.wait()
                print(f"[coordinator] worker pid {proc.pid} reaped, exit status {ret}")
            else:
                proc.wait()
        else:
            wait_for_event(workflow_id, "VERIFY_DONE", proc, heartbeat_timeout)
            proc.wait()

        state = derive_state(wal.read_events(workflow_id))

        if not state["verified"]:
            print("[coordinator] VERIFY_DONE missing from WAL after worker exit -> treating as crash/timeout.")
            print("[coordinator] re-reading event history (deterministic replay) to decide next action...")
            print(f"[coordinator]   fetched={state['fetched']} verified={state['verified']} => re-dispatch VERIFY only")
            proc2 = dispatch(workflow_id, "VERIFY")
            wait_for_event(workflow_id, "VERIFY_DONE", proc2, heartbeat_timeout)
            proc2.wait()
            state = derive_state(wal.read_events(workflow_id))
            if not state["verified"]:
                raise RuntimeError("VERIFY failed on retry -- aborting workflow")

    # Step 3: CLASSIFY
    if not state["classified"]:
        print("[coordinator] dispatching CLASSIFY")
        proc = dispatch(workflow_id, "CLASSIFY")
        proc.wait()
        state = derive_state(wal.read_events(workflow_id))
        print(f"[coordinator] CLASSIFY result: risk={state['risk']}")

    # Step 4: ROUTE
    if not state["routed"]:
        print("[coordinator] dispatching ROUTE")
        proc = dispatch(workflow_id, "ROUTE")
        proc.wait()
        state = derive_state(wal.read_events(workflow_id))
        print(f"[coordinator] ROUTE result: approver={state['approver']}")

    # Step 5: WAIT_APPROVAL (Human Gate)
    # Yield control if amount is > 5000 (high-privilege CFO transition) or risk is HIGH_RISK
    init = next(e for e in events if e["type"] == "INIT")
    expense = init["payload"]["expense"]
    amount = expense.get("amount_usd", 0.0)

    if (amount > 5000 or state["risk"] == "HIGH_RISK") and not state["human_decided"]:
        if not state["waiting_approval"]:
            # Serialize execution context to WAL
            context = {
                "workflow_id": workflow_id,
                "expense": expense,
                "risk": state["risk"],
                "approver": state["approver"],
                "state_snapshot": state,
            }
            wal.append_event(workflow_id, "WAIT_APPROVAL", {
                "reason": f"expense of ${amount} (risk={state['risk']}, approver={state['approver']}) requires human sign-off",
                "execution_context_serialized": context,
            })
        print(f"[coordinator] -> WAIT_APPROVAL. Context is fully persisted in the WAL on disk.")
        print(f"[coordinator] pid {os.getpid()} exiting now. No thread, socket, or memory is held during the wait.")
        return "WAITING"

    # Step 6: RECORD
    if not state["recorded"]:
        print("[coordinator] dispatching RECORD")
        proc = dispatch(workflow_id, "RECORD")
        proc.wait()
        state = derive_state(wal.read_events(workflow_id))

    print(f"=== Coordinator: workflow {workflow_id} COMPLETE (state={state}) ===")
    return "DONE"


def _cli():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start")
    p_start.add_argument("--workflow", required=True)
    p_start.add_argument("--expense-id", required=True)
    p_start.add_argument("--amount", type=float, required=True)
    p_start.add_argument("--category", required=True)
    p_start.add_argument("--receipt", default="receipts/default.jpg")
    p_start.add_argument("--crash-on-verify", action="store_true")

    p_run = sub.add_parser("run")
    p_run.add_argument("--workflow", required=True)
    p_run.add_argument("--crash-on-verify", action="store_true")

    args = p.parse_args()

    if args.cmd == "start":
        expense = {
            "expense_id": args.expense_id,
            "amount_usd": args.amount,
            "category": args.category,
            "receipt_path": args.receipt,
        }
        start_workflow(args.workflow, expense)
        run(args.workflow, crash_on_verify=args.crash_on_verify)
    elif args.cmd == "run":
        run(args.workflow, crash_on_verify=args.crash_on_verify)


if __name__ == "__main__":
    _cli()
