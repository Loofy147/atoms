"""
demo_backpressure.py -- Scene 2/3: credit-based flow control.

Starts 9 independent workflows, then submits all 9 FETCH tasks to the
Router at once with only 3 credits available. Proves (via real wall-clock
timestamps, not narration) that tasks 4-9 sit queued until a credit frees
up, rather than all 9 worker processes forking simultaneously.
"""
import threading
import time

import coordinator
import router

N_WORKFLOWS = 9
MAX_CREDITS = 3

if __name__ == "__main__":
    workflow_ids = [f"EXP-BP-{i:02d}" for i in range(N_WORKFLOWS)]
    for i, wid in enumerate(workflow_ids):
        coordinator.start_workflow(wid, {
            "expense_id": f"EXP-BP-{i:02d}",
            "amount_usd": 10.0,
            "category": "office_supplies",
            "receipt_path": f"receipts/bp-{i}.jpg",
        })

    r = router.Router(max_credits=MAX_CREDITS)
    print(f"\n--- submitting {N_WORKFLOWS} FETCH tasks at once with only {MAX_CREDITS} credits ---\n")

    threads = [threading.Thread(target=r.submit_and_wait, args=(wid, "FETCH")) for wid in workflow_ids]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"\n--- all {N_WORKFLOWS} tasks done in {time.time()-start:.2f}s wall clock "
          f"(would be ~0.3s if unthrottled; throttled to {MAX_CREDITS} concurrent -> ~{N_WORKFLOWS/MAX_CREDITS*0.3:.2f}s expected) ---")
