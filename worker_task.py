"""
worker_task.py -- a stateless Worker.

Each invocation is a brand new OS process. It knows nothing except the
workflow_id and step it was told to run -- everything else it needs (the
original expense payload, prior step results) it reads back out of the
WAL, exactly the way a real worker would receive its full task payload
from the coordinator. This is what "stateless" means in practice: kill
this process at any point and no in-memory context is lost, because none
of it lived only in memory.
"""
import argparse
import os
import time

import wal
import gateway
import tools
import llm


def do_fetch(workflow_id: str, events: list[dict], args):
    init = next(e for e in events if e["type"] == "INIT")
    expense = init["payload"]["expense"]
    time.sleep(0.3)  # simulate fetching supplementary data
    fetched = {"submitter_history_flags": [], "vendor_known": True}
    wal.append_event(workflow_id, "FETCH_DONE", {"expense": expense, "fetched": fetched})


def do_verify(workflow_id: str, events: list[dict], args):
    init = next(e for e in events if e["type"] == "INIT")
    expense = init["payload"]["expense"]

    result, was_cached = gateway.call_tool(workflow_id, "VERIFY", tools.ocr_tool, expense["receipt_path"])

    # Marker fires the instant the (possibly side-effecting) gateway call
    # returns -- i.e. right after the point of no return, before we've told
    # the coordinator we're done. This is the window the crash demo kills in.
    os.makedirs(args.marker_dir, exist_ok=True)
    marker_path = os.path.join(args.marker_dir, f"{workflow_id}_verify_gateway_done")
    with open(marker_path, "w") as f:
        f.write(str(os.getpid()))

    print(f"[worker pid={os.getpid()}] gateway.call_tool(VERIFY) returned "
          f"(was_cached={was_cached}): {result}", flush=True)

    if args.delay_before_commit > 0:
        time.sleep(args.delay_before_commit)

    wal.append_event(workflow_id, "VERIFY_DONE", {"ocr_result": result, "was_cached": was_cached})


def do_classify(workflow_id: str, events: list[dict], args):
    fetch = next(e for e in events if e["type"] == "FETCH_DONE")
    expense = fetch["payload"]["expense"]
    classification = llm.classify_expense(expense)
    wal.append_event(workflow_id, "CLASSIFY_DONE", classification)


def do_route(workflow_id: str, events: list[dict], args):
    classify_event = next(e for e in events if e["type"] == "CLASSIFY_DONE")
    risk = classify_event["payload"].get("risk", "HIGH_RISK")
    init = next(e for e in events if e["type"] == "INIT")
    expense = init["payload"]["expense"]
    amount = expense.get("amount_usd", 0.0)

    if amount > 5000:
        approver = "cfo"
    elif risk == "HIGH_RISK":
        approver = "finance-manager"
    else:
        approver = "finance-associate"

    wal.append_event(workflow_id, "ROUTE_DONE", {"approver": approver})


def do_record(workflow_id: str, events: list[dict], args):
    init = next(e for e in events if e["type"] == "INIT")
    expense = init["payload"]["expense"]
    decision_events = [e for e in events if e["type"] == "HUMAN_DECISION"]
    decision = decision_events[-1]["payload"]["decision"] if decision_events else "auto-approved"
    result, was_cached = gateway.call_tool(
        workflow_id, "RECORD", tools.ledger_write_tool,
        expense["expense_id"], expense["amount_usd"], decision,
    )
    wal.append_event(workflow_id, "RECORD_DONE", {"ledger_result": result, "was_cached": was_cached})
    wal.append_event(workflow_id, "TASK_COMPLETED", {})


# GRASP: Polymorphism -- one handler per step, uniform (workflow_id, events, args)
# signature, looked up by name instead of branched on by name.
STEP_HANDLERS = {
    "FETCH": do_fetch,
    "VERIFY": do_verify,
    "CLASSIFY": do_classify,
    "ROUTE": do_route,
    "RECORD": do_record,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", required=True)
    ap.add_argument("--step", required=True, choices=["FETCH", "VERIFY", "CLASSIFY", "ROUTE", "RECORD"])
    ap.add_argument("--delay-before-commit", type=float, default=0.0)
    ap.add_argument("--marker-dir", default=os.path.join(os.path.dirname(__file__), "markers"))
    args = ap.parse_args()

    events = wal.read_events(args.workflow)
    print(f"[worker pid={os.getpid()}] dispatched step={args.step} workflow={args.workflow} "
          f"(stateless -- payload reconstructed from {len(events)} WAL events)", flush=True)

    STEP_HANDLERS[args.step](args.workflow, events, args)

    print(f"[worker pid={os.getpid()}] step={args.step} complete", flush=True)


if __name__ == "__main__":
    main()
