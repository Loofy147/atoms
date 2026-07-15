"""
demo_backpressure.py -- Scene 3/4: credit-based flow control with Round-Robin.

Starts 9 independent workflows across 3 different agent task sources, then
submits them all to the Router at once with only 3 credits available.
Proves (via real wall-clock timestamps) that the router schedules them
fairly in a Round-Robin fashion across task sources instead of allowing
one high-frequency source to starve others.
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
    print(f"\n--- submitting {N_WORKFLOWS} FETCH tasks across 3 sources with only {MAX_CREDITS} credits ---\n")

    # We submit tasks from 3 different sources: "source_A", "source_B", "source_C"
    # source_A gets tasks 0, 1, 2, 3
    # source_B gets tasks 4, 5, 6
    # source_C gets tasks 7, 8
    threads = []
    for i, wid in enumerate(workflow_ids):
        if i in [0, 1, 2, 3]:
            source = "source_A"
        elif i in [4, 5, 6]:
            source = "source_B"
        else:
            source = "source_C"
        t = threading.Thread(target=r.submit_and_wait, args=(wid, "FETCH", source))
        threads.append(t)

    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"\n--- all {N_WORKFLOWS} tasks done in {time.time()-start:.2f}s wall clock ---")
