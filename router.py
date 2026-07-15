"""
router.py -- credit-based flow control.

A worker only gets a task once it has a "credit". With max_credits < number
of concurrently-submitted tasks, extra tasks queue instead of all spawning
worker processes at once -- this is what protects a real deployment from
memory bloat / fork-bombing itself under a bursty inbound load.

This router decouples the control signal from the data path using an explicit
pool of 'credits' to authorize work, and implements a Round-robin issuance policy
to ensure fairness across multiple agent task sources.
"""
import threading
import time
from collections import deque

import coordinator


class Router:
    def __init__(self, max_credits: int):
        self.max_credits = max_credits
        self._lock = threading.RLock()

        # Explicit pool of "credits"
        self.credits = max_credits

        # Multi-queue round-robin scheduling
        self.queues: dict[str, deque] = {}
        self.sources_order: list[str] = []
        self.current_source_idx = 0

        self._active = 0
        self.t0 = time.time()

    def _log(self, msg: str):
        print(f"[router t={time.time() - self.t0:6.2f}s] {msg}", flush=True)

    def submit_and_wait(self, workflow_id: str, step: str, source_id: str = "default", **kwargs) -> int:
        cond = threading.Condition(self._lock)
        task = (workflow_id, step, kwargs, cond)

        with self._lock:
            if source_id not in self.queues:
                self.queues[source_id] = deque()
                self.sources_order.append(source_id)
            self.queues[source_id].append(task)

            self._log(f"task({workflow_id},{step}) submitted by source={source_id} -- queue_len={len(self.queues[source_id])}")

            # Trigger dispatcher if there are available credits
            self._dispatch_next_if_possible()

        # Wait until our task is signaled
        with self._lock:
            # We must wait while this task is still in the queue (not yet dispatched)
            while any(t[0] == workflow_id and t[1] == step for t in self.queues[source_id]):
                cond.wait()

            self._active += 1
            self.credits -= 1

        self._log(f"credit GRANTED to task({workflow_id},{step}) from source={source_id} -> dispatching worker")
        proc = coordinator.dispatch(workflow_id, step, **kwargs)
        rc = proc.wait()

        with self._lock:
            self._active -= 1
            self.credits += 1
            self._log(f"task({workflow_id},{step}) done (exit={rc}), credit released. Credits available: {self.credits}")
            self._dispatch_next_if_possible()

        return rc

    def _dispatch_next_if_possible(self):
        """Must be called with self._lock held."""
        while self.credits > 0 and any(len(q) > 0 for q in self.queues.values()):
            # Find the next source in round-robin fashion that has tasks
            num_sources = len(self.sources_order)
            found = False
            for _ in range(num_sources):
                if self.current_source_idx >= num_sources:
                    self.current_source_idx = 0
                source_id = self.sources_order[self.current_source_idx]
                self.current_source_idx += 1

                if self.queues[source_id]:
                    # Dispatch this task
                    workflow_id, step, kwargs, cond = self.queues[source_id].popleft()
                    cond.notify_all()
                    found = True
                    break
            if not found:
                break
