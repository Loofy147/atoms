# Distributed AI Orchestrator

A production-grade, highly scalable, and fault-tolerant Distributed AI Orchestrator designed to coordinate up to one million active agent workflows. This repository implements key patterns of Durable Execution, State Machine Replication (SMR), Consensus-driven Idempotency, Credit-based Flow Control, and Human-in-the-Loop Interrupt Primitives.

---

## 📖 Table of Contents
1. [Core Features & Architecture](#-core-features--architecture)
2. [Structure & Components](#-structure--components)
3. [Running the Demos](#-running-the-demos)
4. [Testing Suite](#-testing-suite)
5. [Guiding Design Rules](#-guiding-design-rules)

---

## 🚀 Core Features & Architecture

The architecture separates highly stateful, transaction-guaranteed persistence elements into the **infrastructure layer**, enabling all upstream business logic components (such as workers and coordinators) to remain entirely **stateless, deterministic, and self-healing**.

### 1. Durable Workflow State Machine (SMR)
Instead of relying on implicit, error-prone chains of LLM prompts, the orchestrator implements six explicit, deterministic states:
- **`FETCH`**: Retrieves relevant records.
- **`VERIFY`**: Runs OCR receipt scanning/validation.
- **`CLASSIFY`**: Uses LLM categorization.
- **`ROUTE`**: Assigns the correct approver based on classification and amount.
- **`WAIT_APPROVAL`**: Yields execution control for high-privilege operations.
- **`RECORD`**: Commits the final transaction to the ledger.

State is reconstructed on demand by replaying the append-only Write-Ahead Log (WAL) per workflow.

### 2. Credit-Based Task Router (Backpressure)
Prevents resource starvation and computational freezes during traffic bursts.
- Decouples the control signal from the worker data path.
- Uses an explicit pool of credit tokens to authorize execution.
- Employs a **Round-Robin** issuance policy across multiple distinct task sources, ensuring fair scheduling and preventing high-frequency sources from starving low-frequency ones.

### 3. Idempotent Tool Gateway (Side-Effect Protection)
Guarantees at-most-once execution of outbound side effects (e.g., calling external payment APIs or OCR tools).
- Automatically calculates a unique SHA-256 key for every outbound call: `SHA-256(Workflow_ID + '_' + Execution_Sequence)`.
- Replicates the idempotency cache across a highly-available **3-node Raft consensus cluster**.
- Successfully handles network partitions, preventing split-brain corruption or duplicate execution.

### 4. Human-in-the-Loop Gate (Interrupt Primitive)
A durable interrupt primitive designed to handle long-running manual approvals:
- For high-privilege transitions (e.g., payments > $5,000), the coordinator serializes the execution context, writes it to the WAL, and exits.
- Releasing active sockets and memory prevents resource starvation.
- Continues only upon receiving a **Continuity Trigger** event containing valid operator evaluation logs.

---

## 📁 Structure & Components

| File | Purpose |
|---|---|
| `coordinator.py` | State machine coordinator. Drives execution and orchestrates re-execution. |
| `worker_task.py` | Stateless worker process. Executes individual steps, reconstructs inputs via WAL. |
| `wal.py` | Append-only write-ahead log. Thread and process-safe with `flock`. |
| `router.py` | Credit-based task router with multi-queue round-robin fairness. |
| `raft.py` | Functional 3-node in-memory Raft consensus implementation. |
| `gateway.py` | Idempotent tool gateway backed by local SQLite cache and Raft replication. |
| `resume.py` | CLI tool to trigger the Continuity Trigger and unblock paused workflows. |
| `llm.py` | Heuristic classifier falling back to the live Anthropic Messages API. |
| `tools.py` | Mock external side-effecting tools. |

---

## 🕹️ Running the Demos

### 1. Run a Standard Auto-Approved Workflow
```bash
python3 coordinator.py start --workflow EXP-AUTO-1 --expense-id E_AUTO_1 --amount 45.0 --category office_supplies
```

### 2. Run a High-Risk/High-Value Workflow (Human-in-the-Loop)
```bash
# This will pause at WAIT_APPROVAL and safely exit
python3 coordinator.py start --workflow EXP-MANUAL-1 --expense-id E_MANUAL_1 --amount 6500.0 --category travel

# Resume the workflow with valid operator evaluation logs
python3 resume.py --workflow EXP-MANUAL-1 --decision approved --operator jules-mgr --evaluation-log '{"comments": "LGTM"}'
```

### 3. Run the Credit-Based Router Backpressure Demo
```bash
python3 demo_backpressure.py
```

---

## 🧪 Testing Suite

Run the complete test suite to verify the state machine, credit router, idempotent Raft gateway, and human gate:
```bash
python3 -m unittest test_orchestrator.py
```

---

## 📏 Guiding Design Rules

- **Isolate State Volatility:** Confine all stateful containers to transaction-guaranteed layers.
- **Enforce Backpressure:** Use implicit bounded-await channels within processes and explicit credit-based flow control across network boundaries.
- **Causal over Temporal Ordering:** Never rely on synchronized physical clocks; use state machine replication and logical consensus to guarantee data integrity.
