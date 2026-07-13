"""
router.py -- credit-based flow control.

A worker only gets a task once it has a "credit". With max_credits < number
of concurrently-submitted tasks, extra tasks queue instead of all spawning
worker processes at once -- this is what protects a real deployment from
memory bloat / fork-bombing itself under a bursty inbound load.
"""
import threading
import time

import coordinator


class Router:
    def __init__(self, max_credits: int):
        self.max_credits = max_credits
        self._sem = threading.Semaphore(max_credits)
        self._lock = threading.Lock()
        self._active = 0
        self.t0 = time.time()

    def _log(self, msg: str):
        print(f"[router t={time.time() - self.t0:6.2f}s] {msg}", flush=True)

    def submit_and_wait(self, workflow_id: str, step: str, **kwargs) -> int:
        with self._lock:
            active_snapshot = self._active
        self._log(f"task({workflow_id},{step}) submitted -- {active_snapshot}/{self.max_credits} "
                   f"credits in use, {'waiting for a free credit' if active_snapshot >= self.max_credits else 'credit available'}")
        wait_start = time.time()
        self._sem.acquire()
        waited = time.time() - wait_start
        with self._lock:
            self._active += 1
        self._log(f"credit GRANTED to task({workflow_id},{step}) after {waited:.2f}s queued -> dispatching worker")
        proc = coordinator.dispatch(workflow_id, step, **kwargs)
        rc = proc.wait()
        with self._lock:
            self._active -= 1
        self._sem.release()
        self._log(f"task({workflow_id},{step}) done (exit={rc}), credit released")
        return rc
