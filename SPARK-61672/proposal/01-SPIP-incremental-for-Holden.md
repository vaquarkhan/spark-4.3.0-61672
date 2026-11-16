# SPIP: Provider-neutral asynchronous scalar functions for Spark SQL

**Authors:** Holden Karau, Prashant Singh, Vaquar Khan

**JIRA:** [SPARK-61672](https://issues.apache.org/jira/browse/SPARK-61672)

**Related SPIP:** [[SPIP] Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads](https://issues.apache.org/jira/browse/SPARK-61454) ([SPARK-61454](https://issues.apache.org/jira/browse/SPARK-61454)), authored by Ferdinand Xu. The two SPIPs address different execution boundaries and can be reviewed and delivered independently.

**Status of this document:** an incremental rewrite of the original SPARK-61672 draft. It keeps the original goal and the parts of the draft that hold up against Spark 4.3.0. It replaces the parts that the source, the SPARK-61404 prototype, and a working implementation show cannot be delivered as written. Nothing here has been posted to the dev list or committed.

**Code baseline:** Apache Spark 4.3.0, tag `v4.3.0`, commit `98bc0212c94705434471627068f41e374ccab143`. The feature cannot ship in 4.3.0 because that release already exists. The implementation target is the next feature line that accepts it.

## Q1. What are you trying to do? Articulate your objectives using absolutely no jargon.

Let a Spark SQL query call a remote text or embedding service for each row, with one function call, on any cluster, and against any provider that implements one small interface. Make many slow calls overlap inside one task. Make one bad row fail only that row when the user asks for that behavior. Keep the rules for endpoints, credentials, and spend under the control of the person who runs the cluster, not the person who writes each query.

## Q2. What problem is this proposal NOT designed to solve?

- Model training or fine-tuning.
- Exactly-once delivery of a remote call. Spark can rerun a task. A rerun repeats the call. This proposal says that in the first paragraph of the user docs.
- Changing how Spark schedules, retries, or speculates tasks.
- Rewriting an existing user UDF into a native call. A Python UDF arrives as serialized code. Its meaning cannot be inferred safely.
- A streaming mode where a failed row stays behind while the rest of the micro-batch commits. Spark's offset and commit logs store one position per source, not a set of held rows. A held row needs a durable store. That store is a dead-letter destination, and it is separate later work.
- A quota shared across Spark applications or cloud accounts.
- GPUs, model hosting, multimodal input, structured output schemas, named arguments, and aggregate AI functions. Those are later work.
- [[SPIP] Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads](https://issues.apache.org/jira/browse/SPARK-61454) (SPARK-61454), and task stealing. This SPIP does not depend on either.

### Relationship to SPARK-61454

The two SPIPs are complementary because they remove waiting at different boundaries:

| Question | This SPIP, SPARK-61672 | Break the Stage Barrier, SPARK-61454 |
| --- | --- | --- |
| Where is the waiting? | Inside one Spark task while rows wait for remote model or service calls. | Between eligible Spark SQL stages while a producer stage materializes shuffle output before the consumer stage starts. |
| Main mechanism | A provider-neutral `AsyncScalarFunction` plus `AsyncEvalExec`, with bounded in-task futures and input-order output. | Pipelined Real-Time Mode shuffle for eligible batch queries, with connected-stage scheduling, reader reservations, bounded producer waves, transport credit, and run/attempt cleanup. |
| Spark layers changed | Catalyst function resolution and optimization, plus one SQL physical operator. The scheduler and shuffle transport are unchanged. | SQL plan selection, DAGScheduler/TaskScheduler admission, distributed shuffle transport, lifecycle, and capacity accounting. It does not define an AI function or provider client. |
| User value | One SQL function surface across providers, executor-side credentials, per-row error forms, and fewer idle task threads during remote calls. | Lower query critical-path latency and less shuffle materialization across eligible stage boundaries. |
| Dependency | None. This SPIP works with ordinary materialized shuffle. | None. SPARK-61454 works for eligible batch SQL whether or not a query calls an asynchronous function. |

If both features are enabled in the future, they could overlap two sources of latency in one query: SPARK-61672 overlaps remote calls within a task, while SPARK-61454 overlaps producer, transfer, and consumer work across selected shuffle boundaries. That combined path is not claimed or tested here. It needs explicit qualification for backpressure, resident-reader capacity, task cancellation, at-least-once external calls, and cleanup of both the async-call state and the connected-stage transport state. Until that work exists, each feature keeps its own eligibility and fallback rules, and this SPIP assumes ordinary Spark stage boundaries.

The related SPIP is a draft by Ferdinand Xu, last updated November 11, 2026. Its JIRA and discussion record is [SPARK-61454](https://issues.apache.org/jira/browse/SPARK-61454).

## Q3. How is it done today, and what are the limits of current practice?

Today there is no built-in remote-inference function in Spark 4.3.0. Users write UDFs. A Python iterator UDF or `mapInArrow` can already issue concurrent HTTP calls, so "UDFs cannot do concurrency" is not true. What they lack is a SQL surface, a shared contract, administrator-controlled credentials, and a path that works without Python.

Vendors ship their own functions. Databricks, Snowflake, BigQuery, Microsoft Fabric, and Daft each use a different name, signature, and error behavior. That lock-in is real.

A parallel prototype, SPARK-61404 `ai_query` (PR #60311), shows the demand and also shows the limit of a Python-only path. It is an iterator Pandas UDF. It calls Databricks Model Serving and Amazon Bedrock Converse. It is useful, and several of its tests are stronger than a first sketch. It is not cross-language, it is marked nondeterministic so filters and limits cannot move below the call, and its model registry lives in the Spark session.

Spark's task model has two limits that the original draft names correctly:

- Tasks in one stage are expected to take similar time. Remote calls vary by orders of magnitude, so one slow call can hold a stage.
- A failed call that escapes the function fails the task. `TaskSetManager.handleFailedTask` reruns the partition. `spark.task.maxFailures` defaults to 4. Every successful call in that partition is repeated.

Two claims in the original draft do not survive the source:

- Task time is not `max(latency)`. With N calls and C concurrent calls it is about `ceil(N / C) * latency`. Max applies only when N is at most C.
- Results do not have to come out of order. A bounded reorder window emits rows in input order. The working implementation does this, and the tests show it.

## Q4. What is new in your approach, and why will it be successful?

Two layers, with a hard boundary.

**1. Spark core gets a provider-neutral asynchronous scalar function.** A new `@Evolving` DataSource V2 interface, `AsyncScalarFunction`, returns a `CompletableFuture` for one row. It carries no cloud SDK and no model API. Catalyst resolves it to an unevaluable expression, the optimizer lifts it into one logical node, and one physical operator keeps a bounded number of calls in flight while emitting rows in input order. The same operator serves an OpenAI-compatible endpoint, Amazon Bedrock, Databricks Model Serving, or a private HTTP service.

**2. Providers are separate modules.** Two are built and packaged: `connector/ai-openai` (OpenAI-compatible HTTP, which also covers Databricks chat completions) and `connector/ai-bedrock` (AWS SDK v2 Converse). Each implements `AsyncScalarFunction`, builds the provider request, and returns a future; neither is a special case inside Catalyst. Endpoint, model, and region allowlists, every numeric limit, the credential source, and the circuit-breaker policy are cluster configuration; a session, including a Spark Connect session, can tighten a limit but cannot widen one, cannot name an unlisted endpoint or model, and cannot choose which executor secret is sent. The modules passed a 71-case conformance harness (7 cases live against real services), ran end to end through real Spark SQL in one process, and went through three adversarial review rounds.

This will work because every core piece already has a precedent: `PythonUDF` for an unevaluable expression, `ExtractPythonUDFs` for extraction, `BatchEvalPython` for a logical node the optimizer can see, `HybridRowQueue` for spillable input, and `FunctionCatalog` for a plugin function. The core path is not a sketch. It compiles in the 4.3.0 tree and the tests below pass.

## Q5. Who cares?

- Data engineers who enrich, classify, summarize, or embed text in SQL and need the same query to move between providers.
- Platform teams who need one place to set the endpoint allowlist, credential source, concurrency ceiling, and rate limit.
- Connector authors who want to expose any slow remote call as SQL without waiting for an AI-specific API.
- The authors of SPARK-61404, because their `ai_query(endpoint, request)` surface can sit on this engine instead of a Pandas UDF.

## Q6. What are the risks?

| Risk | What this design does |
| --- | --- |
| Provider APIs differ. | The public surface is the SPI and a small set of functions. Each provider module owns its wire format. |
| Spark repeats external calls. | Documented as at-least-once. Speculation is documented as something to turn off for these jobs. |
| A SQL function can send table data to a URL. | The function does nothing unless its module is installed and the base URL or model is on a static allowlist. Credentials come from the executor environment or the cloud SDK's default chain. They are not in SQL text, plans, or logs. |
| A query can generate a large bill. | Deterministic predicates and `LIMIT` move below the call. Identical deterministic calls run once. A nondeterministic model call is not collapsed. Static ceilings bound in-flight calls, attempts, and elapsed time. |
| A session-level model registry is not safe on a shared cluster. | Registration that changes the endpoint, model, or credential is an administrator setting, not a `spark.conf.set` from a Connect session. |
| Future composition with SPARK-61454 could couple two backpressure and cleanup domains. | No dependency or composition is included in v1. A later integration must qualify resident-reader capacity, provider in-flight limits, cancellation, retries, and idempotent cleanup together before a planner may combine them. |
| The original one-month Phase 0 is too small for the safe design. | Four pull requests with gates, below. |

## Q7. How long will it take?

| PR | Scope | Estimate |
| --- | --- | --- |
| 1 | Catalyst SPI, expression, resolution, analysis checks, logical node, limit and predicate pushdown | 3 to 4 weeks. The core of this PR already compiles and runs. |
| 2 | Extraction, physical operator, engine tests | 4 to 6 weeks. The operator, extraction, and the tests in this document already run. |
| 3 | First provider module, plus a second provider to prove the SPI is not tied to one vendor | 4 to 6 weeks. |
| 4 | Benchmark against `mapInArrow` and against the SPARK-61404 Pandas UDF, user docs, security review | 2 weeks. |

The original Phase 2 (a new task-failure percentage) and Phase 3 (UDF transpilation) are removed, not re-estimated. Phase 4 streaming is rejected in v1 and becomes a separate proposal if a durable retry destination is designed.

## Q8. How do we know it worked?

### Mid-term: the generic engine

Pass when all of the following hold without a paid API:

1. SQL resolves `AsyncScalarFunction` through a test catalog and plans `AsyncEvalExec`.
2. A predicate on an ordinary column and `LIMIT` run below the call. A limited query issues no more calls than the limit.
3. Out-of-order completion still returns one row per input, in input order.
4. A stalled first call cannot grow past the reorder window, and input rows spill through `HybridRowQueue`.
5. Cancelling the task cancels the source future, not only a dependent future.
6. A zero or negative in-flight setting fails at query start. It does not spin.
7. A nested call is evaluated inside-out. Two identical nondeterministic calls are not collapsed. Two identical deterministic calls are collapsed.
8. A provider that throws, or returns a null future, fails the query promptly and does not leak the in-flight counter.

### Final v1: two providers

Pass when one OpenAI-compatible module and one Bedrock module:

1. Use the same SQL shape and the same operator.
2. Keep credentials on the executor.
3. Retry only the documented transient failures, with a deadline.
4. Return null or a status row for a row-level error, and still fail the task for authentication and permission errors.
5. Put no prompt, token, or response body in the plan, the error message, or the event log.
6. Beat or match `mapInArrow` plus `asyncio` on throughput, or win on a non-performance axis that the benchmark states: SQL, a JVM-only cluster, or credential isolation.

### Already demonstrated on Spark 4.3.0

| Suite | Result |
| --- | --- |
| `AsyncFunctionEndToEndSuite` | 6 passed. Real async HTTP, in-order output, forced reverse-completion order, null passthrough, strict failure, deterministic dedup. |
| `AiQueryParitySuite` | 7 passed. Databricks chat-completions envelope and Bedrock Converse envelope, null policy, 429 retry, 120-row order. |
| `AsyncFunctionSafetySuite` | 12 passed in the final combined run. Limit and child-filter call count, nested and aggregate extraction, nondeterministic non-dedup, bounded config, static ceiling, synchronous failure, null future, global plus per-provider-identity limits, shared close, source-future cancellation. |
| Final combined core run | 25 passed, 0 failed, BUILD SUCCESS (`../poc/final-core-test4.log`). |
| Amazon Bedrock CLI smoke | `amazon.nova-micro-v1:0` in `us-east-1` returned `OK` for the prompt `Reply only: OK` (4 input tokens, 2 output tokens, 223 ms). |
| Live Spark SQL Bedrock suite | 1 passed. The query planned to `EvalAsyncFunctions` and `AsyncEvalExec`, then made two typed AWS SDK v2 Converse calls to `amazon.nova-micro-v1:0`; the NULL input stayed NULL. Log: `../poc/final-bedrock-live2.log`. |
| Provider module conformance | 71 passed, 0 failed, against the packaged `ai-openai` and `ai-bedrock` jars through their catalog path: allowlists and cluster ceilings, gzip, streaming size caps, `Retry-After` seconds and HTTP-date, deadlines, body-phase timeout with exchange abort, server-observed cancellation, half-open breaker, pacer isolation, 32-caller concurrency, sanitization across 7 error paths and 3 forms, 4 live calls through the OpenAI-compatible module against Amazon Bedrock's OpenAI-compatible API, and 3 live calls through `BedrockFunctionCatalog`. Log: `../spark-implementation/conformance.log`. |
| One-process end to end, both modules | Real `SparkSession`, both provider jars loaded only through `spark.sql.catalog.*` configuration, native planner (`EvalAsyncFunctions`, `AsyncEval`), real services. 25 of 25 checks: answers, NULL passthrough, service request ids, filter and `LIMIT` below the async node, and real six-word summaries over the example reviews. Log: `../spark-implementation/live-spark-sql-proof.log`. |
| Adversarial review loop on the modules | Three automated adversarial rounds found 17, 12, and 6 issues. All 35 were subsequently fixed and covered by passing tests; round 3 contained 0 high, 3 medium, and 3 low findings. Reviews: `../semantic-review/`. |
| Live throughput benchmark on real Bedrock | Same Spark SQL over 24 prompts in one task, in-flight 1 versus 8: 8,200 ms versus 1,416 ms, 5.79x faster, 24 of 24 answers correct, input order preserved in both runs. Log: `../spark-implementation/live-throughput-benchmark.log`. A full Pandas/Arrow comparison remains a vote gate. |

---

# Appendix A. Design

## A.1 Shape

```text
SELECT catalog.fn(review) FROM reviews WHERE stars = 5 LIMIT 10
  -> FunctionResolution sees AsyncScalarFunction
  -> ApplyAsyncFunctionExpression (unevaluable)
  -> ExtractAsyncFunctions
  -> EvalAsyncFunctions
  -> AsyncEvalExec
  -> provider produceResultAsync returns CompletableFuture
```

The provider may be OpenAI-compatible HTTP, Databricks Model Serving, Amazon Bedrock Converse, or a private endpoint. Catalyst does not know which.

## A.2 SPI

`AsyncScalarFunction[R]` extends `BoundFunction` and `Serializable`.

- `produceResultAsync(InternalRow): CompletableFuture[R]`. It must not block. It must not return null. It must not retain the engine's row after it returns.
- `maxInFlightPerTask` defaults to 16 and must be positive. The engine also enforces a positive ceiling.
- `canonicalName` is mandatory and stable.
- `isDeterministic` is a real promise. A generative model returns false. The engine will not collapse two nondeterministic calls into one result.
- `close` runs after the task asks outstanding calls to stop.

## A.3 Validation-driven hardening already completed

This is a record of resolved implementation work, not a list of current proposal defects. The tests changed the code before this document was finalized:

| Validated condition | Current implementation |
| --- | --- |
| Cancellation cancelled a dependent future, so the HTTP call kept running. | The operator tracks and cancels the source future. |
| The interrupt listener and the task thread both touched a `TreeMap` and the spill queue. | The interrupt listener only cancels source futures. The task thread closes the queue. |
| A zero ceiling made the task wait forever for a head row it never submitted. | Settings must be inside a documented range or the query fails. |
| Several functions on one row could exceed the in-flight ceiling. | The operator enforces one global ceiling plus each function's own ceiling. |
| Two limit-one functions were treated as one global limit-one operator. | Per-function counters let both functions run while each stays at one active call. |
| A nested call was extracted outside-in, so the inner call was still unevaluable. | The collector returns the innermost call first. |
| A function consuming `sum(v)` was moved below the aggregate. | An aggregate-aware rule places that function above the aggregate. |
| `LIMIT` and ordinary filters did not move below the call. | `LimitPushDown` and `PushPredicateThroughNonJoin` now know `EvalAsyncFunctions`. |
| `InterruptedException` was swallowed. | It is restored and becomes a task kill. |
| Concurrent callbacks updated a non-thread-safe SQL metric. | Failures accumulate atomically and publish from the task thread. |
| One shared bound-function object could be closed twice. | Cleanup uses identity deduplication and calls `close` once per distinct object. |
| A session could raise the cost ceiling. | The active-call ceiling is a static `SQLConf` entry bounded at 1 to 1,024. |
| A generative function was treated as deterministic and deduplicated. | Every implementation must declare `isDeterministic()`; generative providers return false. |

## A.4 Ordering, retry, and failure

Output is one row per input row, in input order. Per-call retry, backoff, and `Retry-After` belong to the provider future. The Spark task is unchanged.

Row-level tolerance is a function choice, not `spark.task.maxFailurePercentage`:

| Form | On a terminal row error |
| --- | --- |
| strict | the future fails and the task fails |
| `try_` | SQL NULL |
| `_status` | a struct with a status code |

NULL is not the failure signal by itself, because NULL is a valid input and a valid model output. Authentication and permission errors stay fatal under every form.

## A.5 Security and cloud independence

- No cloud SDK in `sql/core`.
- A provider module depends on its own client. Bedrock uses AWS SDK v2. OpenAI-compatible HTTP uses the JDK HTTP client.
- Credentials are resolved on the executor. For AWS this is the default credential chain, which covers instance roles, workload identity, and a named profile. Access keys are not Spark configuration.
- Endpoint and model allowlists are static. A session cannot add a destination.
- Errors returned to SQL contain a category and, where useful, a request id. They do not contain the prompt, the response body, or a credential.

## A.6 What is deliberately not in v1

Opaque UDF transpilation. Streaming "stay behind" without a durable destination. Cluster-wide quota. Out-of-order output as the default. A task-percentage failure policy. Per-call model selection that bypasses the allowlist.

---

# Appendix B. Relationship to the original draft

| Original phase | Decision |
| --- | --- |
| Phase 0, native functions | Kept, but the function is generic. AI is the first provider, not a Catalyst type. |
| Phase 1, in-task concurrency and per-call retry | Kept. Ordering is preserved. The scheduler is not changed. The latency formula is corrected. |
| Phase 2, poison pills | Kept as `try_` and `_status`. The task-percentage setting is rejected. |
| Phase 3, transpilation | Removed. |
| Phase 4, streaming stay-behind | Removed from v1. A later proposal can add a durable retry destination. |

The original draft is right that remote inference is a real workload, that per-call retry is the right unit, that in-task concurrency is the right performance lever, and that vendor-specific functions create lock-in. This rewrite keeps those points and drops the points the 4.3.0 source cannot support.
