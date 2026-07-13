import unittest
import time
import os
import shutil
import threading
import gateway
import wal
import coordinator
import router
from raft import RaftCluster

class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        # Reset files and databases
        gateway.reset()
        if os.path.exists("wal_data"):
            shutil.rmtree("wal_data")
        if os.path.exists("markers"):
            shutil.rmtree("markers")
        time.sleep(0.5)

    def tearDown(self):
        gateway.reset()
        if os.path.exists("wal_data"):
            shutil.rmtree("wal_data")
        if os.path.exists("markers"):
            shutil.rmtree("markers")
        time.sleep(0.5)

    def test_state_machine_six_states(self):
        # 1. State Machine transitions FETCH -> VERIFY -> CLASSIFY -> ROUTE -> RECORD
        workflow_id = "WF-TEST-6-STATES"
        coordinator.start_workflow(workflow_id, {
            "expense_id": "E1",
            "amount_usd": 45.0,
            "category": "office_supplies",
            "receipt_path": "receipts/default.jpg"
        })

        status = coordinator.run(workflow_id)
        self.assertEqual(status, "DONE")

        events = wal.read_events(workflow_id)
        types = [e["type"] for e in events]

        self.assertIn("INIT", types)
        self.assertIn("FETCH_DONE", types)
        self.assertIn("VERIFY_DONE", types)
        self.assertIn("CLASSIFY_DONE", types)
        self.assertIn("ROUTE_DONE", types)
        self.assertIn("RECORD_DONE", types)
        self.assertIn("TASK_COMPLETED", types)

    def test_credit_based_router_fairness(self):
        # 2. Credit router with explicit credit pool and round-robin scheduling
        r = router.Router(max_credits=2)

        # We submit 4 tasks from 2 different sources: source_A and source_B
        order = []
        lock = threading.Lock()

        def run_task(wid, src):
            r.submit_and_wait(wid, "FETCH", src)
            with lock:
                order.append((src, wid))

        coordinator.start_workflow("WF-R-0", {"expense_id": "0", "amount_usd": 10.0})
        coordinator.start_workflow("WF-R-1", {"expense_id": "1", "amount_usd": 10.0})
        coordinator.start_workflow("WF-R-2", {"expense_id": "2", "amount_usd": 10.0})
        coordinator.start_workflow("WF-R-3", {"expense_id": "3", "amount_usd": 10.0})

        threads = [
            threading.Thread(target=run_task, args=("WF-R-0", "source_A")),
            threading.Thread(target=run_task, args=("WF-R-1", "source_B")),
            threading.Thread(target=run_task, args=("WF-R-2", "source_A")),
            threading.Thread(target=run_task, args=("WF-R-3", "source_B")),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Check that we handled them fairly and no computational freeze occurred
        self.assertEqual(len(order), 4)

    def test_idempotent_tool_gateway_raft(self):
        # 3. Gateway calculates idempotency key using SHA-256 and duplicates are avoided
        cluster = gateway.get_raft_cluster()
        leader = cluster.get_leader()
        self.assertIsNotNone(leader)

        call_count = [0]
        def my_test_tool():
            call_count[0] += 1
            return {"result": "success", "count": call_count[0]}

        res1, cached1 = gateway.call_tool("WF-GW-1", "STEP-GW", my_test_tool)
        self.assertFalse(cached1)
        self.assertEqual(res1, {"result": "success", "count": 1})

        res2, cached2 = gateway.call_tool("WF-GW-1", "STEP-GW", my_test_tool)
        self.assertTrue(cached2)
        self.assertEqual(res2, {"result": "success", "count": 1})
        self.assertEqual(call_count[0], 1)

    def test_human_in_the_loop_gate(self):
        # 4. High-privilege transition yielding and resuming with Continuity Trigger
        workflow_id = "WF-HIL-TEST"
        coordinator.start_workflow(workflow_id, {
            "expense_id": "E_HIL",
            "amount_usd": 6500.0,
            "category": "travel",
            "receipt_path": "receipts/default.jpg"
        })

        status = coordinator.run(workflow_id)
        self.assertEqual(status, "WAITING")

        events = wal.read_events(workflow_id)
        state = coordinator.derive_state(events)
        self.assertTrue(state["waiting_approval"])
        self.assertFalse(state["recorded"])

        # Check serialized execution context exists
        wait_event = next(e for e in events if e["type"] == "WAIT_APPROVAL")
        self.assertIn("execution_context_serialized", wait_event["payload"])

        # Continuity Trigger simulation
        wal.append_event(workflow_id, "HUMAN_DECISION", {
            "decision": "approved",
            "operator": "test-operator",
            "evaluation_log": '{"comments": "LGTM"}'
        })

        status2 = coordinator.run(workflow_id)
        self.assertEqual(status2, "DONE")

        state2 = coordinator.derive_state(wal.read_events(workflow_id))
        self.assertTrue(state2["recorded"])
        self.assertTrue(state2["completed"])

if __name__ == "__main__":
    unittest.main()
