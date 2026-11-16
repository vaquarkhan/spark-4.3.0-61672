# Related SPIP: Break the Stage Barrier, Pipelined Spark SQL for Agentic Workloads

**Author:** Ferdinand Xu (ferdinandx@nvidia.com)

**Status:** Draft for community discussion

**JIRA and authoritative discussion record:** [SPARK-61454](https://issues.apache.org/jira/browse/SPARK-61454)

**Draft date supplied with this package:** November 11, 2026

This file is a concise summary of the SPARK-61454 draft supplied alongside SPARK-61672. It is not a replacement for the authoritative SPIP or JIRA discussion. Follow SPARK-61454 for updates.

## What SPARK-61454 proposes

Extend Spark Real-Time Mode (RTM) to eligible batch SQL and DataFrame queries. Instead of requiring every eligible shuffle producer stage to finish and materialize all output before downstream consumers start, selected stages form a connected group and stream intermediate records through RTM's distributed transport as they arrive.

Its north star is to reduce p95 agent-step latency and increase completed agent steps per minute on a fixed cluster, while preserving exact Spark results and bounded resource use.

## Early evidence reported by the draft

| Workload | Reported result |
| --- | --- |
| Distributed TPC-H SF10K Q3 | 239.15 s to 145.53 s, 1.643x; physical shuffle writes 566.71 GiB to 24.85 GiB (95.6 percent lower, with build-side materialization retained) |
| RTM batch CPU microbenchmarks | Positive cases 1.43x to 1.95x; physical writes 52 percent to 99.99 percent lower |
| Kaggle OTTO recommender pipeline | 1.773x; latency about 9.51 s to 5.17 s |
| MS MARCO plus MiniLM cross-encoder | 1.392x; 28.2 percent lower latency |

These are SPARK-61454's reported prototype results, not results produced by the SPARK-61672 work.

## Main mechanisms

1. **Planner selection.** Mark eligible batch `ShuffleExchangeExec` boundaries as pipelined, opt-in and disabled by default. Unsupported exchanges remain materialized. A query may mix pipelined and materialized regions.
2. **Reader-first admission.** Reserve capacity for all resident readers plus a bounded producer window instead of requiring every task in the connected group to fit at once. Producers rotate through the window as they finish.
3. **Bounded transport.** Keep existing per-route credit backpressure and add executor-wide budgets across active routes, receive queues, replay buffers, and in-flight data.
4. **Attempt-scoped lifecycle.** Associate scheduling and transport state with the batch run and stage attempt. Release readers, writers, buffers, replay state, spill, and scheduling state after success, cancellation, or failure. Reject late messages using generation fencing.
5. **Coexistence.** Reservations count against the scheduling pool. Bounded admission waits and concurrent-group limits protect ordinary materialized batch queries.

## Proposed milestones

| Milestone | Scope |
| --- | --- |
| M1 | Batch selection and grouping, explicit opt-in, eligibility, fallback reasons, pipelined exchange, AQE policy |
| M2 | Distributed bounded execution, connected-group submission, RTM transport reuse, reader reservations, producer waves, executor/application budgets |
| M3 | Lifecycle correctness, generation fencing, idempotent cleanup, fault and performance qualification |
| M4 | Follow-up expansion: hybrid region selection, retry across materialization boundaries, broader topologies, AQE replanning, and possible compute-slot yielding |

## Relationship to SPARK-61672

The two SPIPs remove different waits:

| | SPARK-61672: asynchronous functions | SPARK-61454: stage pipelining |
| --- | --- | --- |
| Boundary | Inside a task | Across selected shuffle stages |
| Waiting removed | Task thread waiting on one remote request at a time | Consumer stage waiting for complete shuffle materialization |
| Main state | Provider futures, reorder window, spillable input queue, request limits | Connected group, reader reservations, producer window, route credits, transport buffers |
| Main Spark changes | Function resolution, optimizer extraction/pushdown, one physical operator | Planner marking, DAGScheduler/TaskScheduler admission, shuffle transport, group lifecycle |
| External service semantics | Yes: credentials, retry, cancellation, error forms, at-least-once calls | No provider or remote-function API |
| Dependency | None | None |

SPARK-61672 works with normal materialized shuffle. SPARK-61454 works whether or not a query calls a remote function. Neither proposal should block the other.

## Possible future composition

If Spark later combines them, a query could overlap remote calls within tasks and overlap producer, transfer, and consumer work across selected stage boundaries. That composition has not been built or tested in this package.

A combined qualification must prove:

- Provider in-flight limits, reorder windows, resident-reader slots, producer waves, and transport buffers remain independently bounded.
- A stalled remote call cannot hold all resident-reader or producer capacity needed for progress.
- Transport backpressure cannot prevent cancellation, provider-future cancellation, credit return, or termination messages from being processed.
- Stage, task, and connected-group retries state clearly how many external calls may repeat.
- One run/attempt generation fences both late transport frames and late provider completions.
- Success, cancellation, executor loss, and injected failure release provider clients/futures, input queues, routes, replay state, spill, and scheduling reservations idempotently.
- Metrics separate provider wait, shuffle wait, admission wait, model calls, repeated calls, buffered bytes, and fallback reasons.

Until those tests exist, SPARK-61672 assumes ordinary stage boundaries and makes no performance or liveness claim for the combined execution path.
