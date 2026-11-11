# Module Reference: `engine` (Actor Event Loop & DAG Scheduler)

The `engine` package is the core execution kernel of `GoOrchestrator`. It implements the single-writer actor event loop, structural DAG validation and cycle detection, asynchronous worker task dispatching, status rollups, and the bounded review/retry/escalation remediation loop.

---

## 1. Actor State Machine & Event Loop

The engine strictly adheres to the **single-writer actor pattern**. The event loop in [`engine.Orchestrator.RunEventLoop`](file:///home/jfisher/projects/GoOrchestrator/engine/orchestrator.go#L44) is the sole owner of mutable plan and task state, consuming events from `eventCh chan any`.

```mermaid
sequenceDiagram
    autonumber
    participant Bus as eventCh chan any
    participant Actor as Orchestrator Event Loop
    participant Worker as Background Worker Goroutine
    participant LRM_Rev as LRM Reviewer Hat
    participant LRM_PM as LRM Project Manager Hat

    Bus->>Actor: SeedPlanEvent / Approved Plan
    Actor->>Actor: Validate DAG (Kahn's)
    Actor->>Actor: Find Ready Tasks

    Actor->>Worker: spawn goroutine(TaskInput + UpstreamOutputs)
    Worker->>Bus: TaskExecutionResultEvent

    Bus->>Actor: TaskExecutionResultEvent
    Actor->>Actor: Task status -> Reviewing
    Actor->>LRM_Rev: spawn Review(Task + Output + AcceptanceCriteria)
    LRM_Rev->>Bus: TaskReviewResultEvent

    alt Review Approved
        Bus->>Actor: TaskReviewResultEvent (Approved=true)
        Actor->>Actor: Task status -> Completed
        Actor->>Actor: Evaluate downstream tasks & Dispatch next
    else Tactical Retry (Attempts < MaxRetries)
        Bus->>Actor: TaskReviewResultEvent (Approved=false)
        Actor->>Actor: Append Feedback to task context
        Actor->>Actor: Task status -> Ready
        Actor->>Worker: Re-dispatch with prior feedback
    else Strategic Escalation (Attempts >= MaxRetries or NeedsClarification)
        Bus->>Actor: TaskReviewResultEvent (NeedsClarification=true)
        Actor->>LRM_PM: spawn Triage(Task + Feedback + Error)
        LRM_PM->>Bus: RemediationResultEvent
        alt Action: Split
            Bus->>Actor: RemediationResultEvent (ActionSplit)
            Actor->>Actor: Splice sub-tasks & Join Node
            Actor->>Actor: Re-validate DAG & Dispatch sub-tasks
        else Action: Clarify
            Bus->>Actor: RemediationResultEvent (ActionClarify)
            Actor->>Actor: Task -> AwaitingClarification
            Actor->>Actor: Downstream tasks -> Blocked
        end
    end
```

---

## 2. DAG Scheduling & Topological Validation

All plan structures (initial plans, updates, and spliced sub-graphs) are validated using **Kahn's algorithm** in [`engine/dag.go`](file:///home/jfisher/projects/GoOrchestrator/engine/dag.go):

1. **Unique IDs:** Ensures every task ID is non-empty and unique across the entire plan.
2. **Type Registration:** Verifies that every task specifies a `task_type` registered in [`tasks.Registry`](file:///home/jfisher/projects/GoOrchestrator/tasks/registry.go).
3. **Reference Integrity:** Checks that all IDs in `depends_on` exist in the active plan.
4. **Self-Dependency Check:** Disallows `task.id == dep.id`.
5. **Cycle Detection & Topological Sort:**
   - Computes in-degrees for all nodes and constructs adjacency lists.
   - Enqueues nodes with in-degree 0 into a deterministic queue.
   - Pops nodes, decrements downstream in-degrees, and appends to the sorted list.
   - If the count of processed nodes is less than the total node count, a cycle exists. The engine reports the exact cyclic node IDs in the returned error.

---

## 3. Dynamic Sub-Task Splicing (`ActionSplit`)

When a task repeatedly fails because the scope is too broad for the small worker model in a single shot, the Project Manager Hat can issue an `ActionSplit` remediation.

The engine executes atomic DAG splicing via [`spliceSubTasks`](file:///home/jfisher/projects/GoOrchestrator/engine/orchestrator.go#L446):
1. **Namespacing:** Sub-tasks are prefixed with the original task ID (e.g. `T1.s1`, `T1.s2`).
2. **Dependency Inheritance:** Sub-tasks with no dependencies automatically inherit `T1`'s original upstream dependencies.
3. **Join Node Transformation:** `T1` transforms into a join node (`IsJoinNode = true`) whose `depends_on` points to the terminal sub-tasks.
4. **Transparent Downstream Compatibility:** Downstream dependents of `T1` remain untouched—they continue waiting on `T1`.
5. **Atomic Re-Validation:** The spliced plan is verified via `ValidatePlanSpec`. If validation fails, the splice is rolled back and the task falls back to `ActionClarify`.
6. **Automatic Join Resolution:** When all child sub-tasks complete, the join node automatically marks itself `StatusCompleted` without invoking another worker.

---

## 4. Status Hierarchies & Rollups

The engine manages explicit states at the task level and computes real-time rollups for stories, epics, and plans:

### Task Lifecycle States
- `StatusPending`: Waiting for upstream dependencies to complete.
- `StatusReady`: Dependencies satisfied; queued for dispatch.
- `StatusRunning`: Actively executing inside an asynchronous worker goroutine.
- `StatusReviewing`: Worker execution completed; waiting for LRM Reviewer verdict.
- `StatusAwaitingClarification`: Paused waiting for user clarification via API.
- `StatusCompleted`: Verified and approved by the Reviewer.
- `StatusFailed`: Unrecoverable failure.
- `StatusBlocked`: Downstream dependent of a task that is failed or awaiting clarification.

### Rollup Rules (`computeAggregateStatus`)
1. If **all** child nodes are `Completed` $\rightarrow$ `Completed`.
2. If **any** child node is `Failed` $\rightarrow$ `Failed`.
3. If **any** child node is `AwaitingClarification` $\rightarrow$ `AwaitingClarification`.
4. If **any** child node is `Running` or `Reviewing` $\rightarrow$ `Running`.
5. If **any** child node is `Ready` $\rightarrow$ `Ready`.
6. If **all** child nodes are `Blocked` $\rightarrow$ `Blocked`.
7. Otherwise $\rightarrow$ `Pending`.

---

## 5. Graceful Termination & Drain

Upon receiving `SigEvent` or root context cancellation:
1. Cancels the worker context, signaling in-flight HTTP requests and workers to stop.
2. Invokes the HTTP server `Shutdown(ctx)` hook with a 5-second deadline.
3. Awaits the internal worker `sync.WaitGroup` to drain running goroutines cleanly.
4. Terminates the actor event loop without data corruption.

