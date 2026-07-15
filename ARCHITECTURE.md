# Reference Architecture (Structural Blueprint)

This document outlines the Reference Architecture (Structural Blueprint) of the **Distributed AI Orchestrator** designed to scale up to one million active agent workflows.

---

## 1. C4 Structural Hierarchy & Components

The system consists of five core atomic components structured under the C4 hierarchy:

```
[System Context]
       │
       ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ Container Layer: Distributed AI Orchestrator                           │
 │                                                                        │
 │  ┌──────────────────────────────┐      ┌────────────────────────────┐  │
 │  │  Durable Workflow            │      │  Credit-Based              │  │
 │  │  Coordinator                 ├─────►│  Task Router               │  │
 │  └──────────────┬───────────────┘      └─────────────┬──────────────┘  │
 │                 │                                    │                 │
 │                 ▼                                    ▼                 │
 │  ┌──────────────────────────────┐      ┌────────────────────────────┐  │
 │  │  Stateless Agent             │      │  Idempotent Tool           │  │
 │  │  Worker Pool                 ├─────►│  Gateway                   │  │
 │  └──────────────┬───────────────┘      └─────────────┬──────────────┘  │
 │                 │                                    │                 │
 │                 ▼                                    ▼                 │
 │  ┌──────────────────────────────┐      ┌────────────────────────────┐  │
 │  │  Human-in-the-Loop           │      │  Raft Replicated           │  │
 │  │  Gate                        │      │  Idempotency Cache         │  │
 │  └──────────────────────────────┘      └────────────────────────────┘  │
 └────────────────────────────────────────────────────────────────────────┘
```

### 1.1 Durable Workflow Coordinator (C4 Component)
- **Position:** Container/Component layer. Orchestrates workflow execution flow and replays the immutable append-only Event History log (SMR).
- **Strict Ownership Invariants:**
  - Must not retain any persistent in-memory session state or active TCP socket pools during waiting states.
  - State is fully derived via Event Sourcing (replaying the write-ahead log) rather than cached memory.

### 1.2 Credit-Based Task Router (C4 Component)
- **Position:** Component layer. Decouples the control signal from the worker data path.
- **Strict Ownership Invariants:**
  - Owns and manages credit pool allocations and scheduling queues.
  - Must never own database schemas, external network sockets, or persistence engines.

### 1.3 Stateless Agent Worker Pool (C4 Component/Container)
- **Position:** Component/Container layer. Dispatched on demand to execute individual steps (`FETCH`, `VERIFY`, `CLASSIFY`, `ROUTE`, `RECORD`).
- **Strict Ownership Invariants:**
  - Completely stateless and bound to the duration of a single execution step.
  - Reconstructs its inputs entirely from the serialized WAL payload passed upon invocation.
  - Cannot own long-lived connections or independent database schemas.

### 1.4 Idempotent Tool Gateway (C4 Container)
- **Position:** Container layer. Direct protector of external side-effecting systems (payment rails, OCR APIs, accounting database).
- **Strict Ownership Invariants:**
  - Owns its replicated consensus log and the underlying SQL cache schemas.
  - Must not manage business workflow orchestration; limited strictly to filtering outbound executions based on SHA-256 keys.

### 1.5 Human-in-the-Loop Gate (C4 Component)
- **Position:** Component layer. Manages human approval interrupts for high-privilege transitions.
- **Strict Ownership Invariants:**
  - Must yield control and store context in the WAL.
  - Must never block runtime threads or hold active connection sockets while waiting.

---

## 2. Stateful Volatility Isolation & Layering Pattern

The architecture strictly enforces the **Layer Pattern**:
1. **Stateless Logic Layer (Upstream):** Durable Workflow Coordinator and Stateless Worker Pool. They hold zero local state and are resilient to node crashes. If a worker or coordinator crashes, a new instance is spawned and reconstructs the state using replay.
2. **Transactional State Layer (Infrastructure):** SQLite write-ahead log database and the Raft consensus cluster database. This layer handles transaction guarantees, write serialization, and data redundancy.

This isolation guarantees that all upstream nodes remain completely self-healing.
