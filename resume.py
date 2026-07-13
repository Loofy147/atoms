"""
resume.py -- what a human operator (or an approvals UI backend) runs.

This is invoked as a completely independent process, potentially minutes,
hours, or days after the coordinator that reached WAIT_APPROVAL has fully
exited. It carries no memory of that run. It proves it by replaying the WAL
from scratch, appending the human's decision as a new event, and calling
the exact same coordinator.run() the original process used -- because
correctness here comes from the log, not from any live object.
"""
import argparse
import os

import wal
import coordinator


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", required=True)
    ap.add_argument("--decision", required=True, choices=["approved", "rejected"])
    ap.add_argument("--operator", default="human-operator")
    ap.add_argument("--evaluation-log", default="{}")
    args = ap.parse_args()

    events = wal.read_events(args.workflow)
    if not events:
        raise SystemExit(f"no such workflow: {args.workflow}")

    state = coordinator.derive_state(events)
    print(f"[resume pid={os.getpid()}] fresh process, zero shared memory with the original coordinator run.")
    print(f"[resume] replayed {len(events)} WAL events -> derived state: {state}")

    if not state["waiting_approval"]:
        raise SystemExit(f"workflow {args.workflow} is not waiting for approval right now (state={state})")

    # Appending valid operator evaluation logs via a Continuity Trigger
    wal.append_event(args.workflow, "HUMAN_DECISION", {
        "decision": args.decision,
        "operator": args.operator,
        "evaluation_log": args.evaluation_log
    })
    print(f"[resume] committed HUMAN_DECISION={args.decision} by {args.operator} to WAL with evaluation_log={args.evaluation_log}.")
    print(f"[resume] handing back to coordinator.run() to finish the workflow...")
    coordinator.run(args.workflow)


if __name__ == "__main__":
    main()
