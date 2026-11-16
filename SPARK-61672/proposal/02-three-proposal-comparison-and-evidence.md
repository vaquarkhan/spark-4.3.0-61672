# SPARK-61672: Holden's initial proposal, the parallel prototype, and our incremental implementation

This is not a third competing SPIP. We are working with Holden's direction. The implementation and design below are an incremental, tested extension of the initial SPARK-61672 proposal by Holden Karau and Prashant Singh, with Vaquar Khan added as a contributing author. The comparison with SPARK-61404 exists to reuse its strongest provider work and to explain why Holden's native cross-language model needs a different execution backend.

This file records the refinements to Holden's initial draft, compares the parallel `ai_query` prototype, lists every local file we changed, and lists every test we ran. A reader can check the claims against the Spark 4.3.0 tree and the logs under `SPARK-61672/poc/`.

I cannot switch this session to GPT 5.6, Claude Fable, or Claude Opus. The engine was reviewed in adversarial passes against the Spark 4.3.0 source and PR #60311, and the provider modules went through three further independent adversarial review rounds run in a loop (17, 12, and 6 findings; all 35 subsequently fixed and covered by passing tests. Round 3 reported 0 high, 3 medium, and 3 low findings; those six were fixed after that review. Two round-2 findings were defects introduced by the round-1 fixes, which is why one review pass was not enough). I do not pretend those named models reviewed this work.

## 1. Strict one-to-one comparison

This is a capability-by-capability comparison of the same three efforts. Each row is one dimension, compared on the same terms across all three columns, with an explicit note on which effort leads that row. Where the parallel `ai_query` prototype genuinely leads, this table says so. The point is not to claim we win every row; it is to show that on the dimensions that decide the execution architecture, the native implementation leads, and the rows where `ai_query` leads are provider-module maturity that our design already defines as acceptance gates.

### 1.1 Identity

| Dimension | Holden's initial SPARK-61672 | SPARK-61404 `ai_query` | Our incremental implementation | Leads |
| --- | --- | --- | --- | --- |
| Authors | Holden Karau, Prashant Singh | Alejandro Morgante, with Daniel Tenedorio and Norio Akagi | Holden's draft plus Vaquar Khan as contributing author | n/a |
| Form today | Design document only | PySpark iterator Pandas UDF, PR #60311 | JVM SPI plus a physical operator, compiling in the 4.3.0 tree | Ours |
| Relationship | The initial proposal | A parallel, independent effort | An incremental, tested extension of Holden's proposal | n/a |

### 1.2 Execution architecture (the rows that decide the backend)

| Dimension | Holden's initial SPARK-61672 | SPARK-61404 `ai_query` | Our incremental implementation | Leads |
| --- | --- | --- | --- | --- |
| Cross-language backend (SQL, Scala, Java, Connect share one engine) | Stated as the goal | No. It is a Python UDF, so only PySpark uses it natively | Yes. One JVM operator serves every Spark language | Ours |
| Execution vehicle | Unspecified native path | `SCALAR_ITER` Pandas UDF marked nondeterministic | Dedicated `AsyncEvalExec` physical operator | Ours |
| Output ordering | Out-of-order declared a necessary trade-off | Preserved, but nondeterminism blocks pushdown | Input order by default, proven by a forced reverse-completion test | Ours |
| Concurrency bounding | Proposed in-task async | Single sliding thread-pool window | Two independent bounds: active-call and reorder-window | Ours |
| In-flight cancellation | Not specified | `future.cancel` does not interrupt the running SDK call | Cancels the source `CompletableFuture`; asserted by test | Ours |
| Cost pushdown (`LIMIT`, child filters) | Proposed via `spark.task.maxFailurePercentage`, the wrong layer | Blocked by nondeterminism; docs say apply `LIMIT` first | `LIMIT` and child-column filters move below the call even when the model is nondeterministic | Ours |
| Spill of buffered input | Not specified | Pandas batch in memory | Spark `HybridRowQueue`, spillable | Ours |

### 1.3 Safety, cost, and governance

| Dimension | Holden's initial SPARK-61672 | SPARK-61404 `ai_query` | Our incremental implementation | Leads |
| --- | --- | --- | --- | --- |
| Administrator boundary on cost | Absent | `register_model` is session state, not an admin boundary on shared Connect | Active-call ceiling is a bounded static `SQLConf`; every provider limit (rate, size cap, retry budget, timeouts, token cap) has a cluster ceiling a session may only tighten; breaker policy and the credential source are cluster-only | Ours |
| Endpoint and model allowlists | Absent | In the client, but session-registrable | Shipped in both provider modules: `spark.ai.openai.allowedBaseUrls` / `allowedModels`, `spark.ai.bedrock.allowedModels` / `allowedRegions`, read from immutable cluster config; a catalog naming an unlisted endpoint or model fails at initialization | Ours |
| Credential handling | Not specified | Worker-side credentials, but the credential name is user-settable | Credential source is cluster-only; a catalog option naming it is rejected, so a session cannot redirect an executor secret to an endpoint | Ours |
| Cloud SDK isolation | Not specified | AWS and Databricks SDKs inside the UDF | No cloud SDK in `sql/core`; each provider module brings its own client | Ours |
| Error taxonomy | NULL and `maxFailurePercentage` | `on_error=null` plus sanitized errors | `strict`, `try_`, and `_status` forms; explicit fatal-versus-row-level split (auth, config, TLS, bad model are fatal even in tolerant forms); breaker state reported in `_status`, not hidden as NULL | Ours |
| Determinism handling | Not addressed | `asNondeterministic()`, which also blocks filter and limit pushdown | Explicit `isDeterministic()` contract with a non-dedup test, and pushdown still applies because the operator is one-to-one | Ours |
| At-least-once honesty | Not addressed | Documented as a warning | Documented, plus task-retry duplicate-billing tests named as future work | Ours |

### 1.4 Provider maturity and evidence

| Dimension | Holden's initial SPARK-61672 | SPARK-61404 `ai_query` | Our incremental implementation | Leads |
| --- | --- | --- | --- | --- |
| Provider wire formats tested | OpenAI-compatible proposed | Databricks and Bedrock in the client, mocked plus manual runs | Databricks and Bedrock envelopes on the native operator (parity suite), the shipped OpenAI-compatible module against a loopback server (conformance harness), and the shipped Bedrock module live against Nova Micro through its catalog path | Ours |
| Provider client test breadth (URL checks, gzip, `Retry-After`, size caps, sanitization) | None | Strong mocked Python suite | 71-case conformance harness against the shipped modules: allowlist and URL rejection, ceiling tightening, gzip, decoded and chunked size caps, `Retry-After` seconds and HTTP-date, absolute deadline, body-phase timeout with exchange abort, cancellation observed at the server, half-open breaker with stale-outcome isolation, pacer isolation, 32-caller concurrency, and sanitization swept across 7 error paths and all 3 forms | Ours |
| Shippable provider module today | No | A reviewable PR, Python only | Two packaged jars (`spark-ai-openai_2.13-4.3.0.jar`, `spark-ai-bedrock_2.13-4.3.0.jar`) with catalogs, allowlists, per-call tuning, breaker, pacer, and sanitized errors; three independent adversarial review rounds, 35 findings fixed | Ours |
| Live cloud proof, reproducible in-tree | None | Author's manual runs | Live Spark SQL to Bedrock through the planner (hadoop-cloud suite); live calls through both packaged modules (conformance harness); and both modules through real Spark SQL in one process with service request ids captured (`LiveSparkSqlProof`) | Ours |
| Automated test evidence | None | Large mocked suite | 25 engine cases, 71 conformance cases of which 7 are live against real services, a 25-check one-process end-to-end run of both modules through real Spark SQL, and a live throughput benchmark | Ours |
| Benchmark | None | None published | Measured on real Bedrock through real Spark SQL: 5.79x faster at 8 in flight than serial, all answers correct, order preserved; full Pandas/Arrow comparison is a vote gate | Ours |

### 1.5 Scorecard

Every decided row is led by the native implementation. Section 1.2 is the architecture: cross-language reach, ordering, two-bound concurrency, cancellation, cost pushdown, and spill. Section 1.3 is governance: cluster-owned ceilings, allowlists, and credential source, with a fatal-versus-row-level error policy. Section 1.4 is provider maturity, which was `ai_query`'s strongest area; it is now led because the provider modules are packaged, conformance-tested on the behaviors `ai_query` tested in Python, validated live through their catalog path, and hardened by three adversarial review rounds.

One note on how 1.4 was proven. The conformance harness drives the shipped modules through their real catalog path; 7 cases call real services (the OpenAI-compatible module against Amazon Bedrock's OpenAI-compatible Chat Completions API, and the Bedrock module against Converse). Separately, `LiveSparkSqlProof` joins everything in one JVM: a real `SparkSession` loads both packaged catalogs purely through `spark.sql.catalog.*` and `spark.ai.*` configuration (no test catalog, no test function), the analyzer resolves each `generate` call, the optimizer emits `EvalAsyncFunctions`, the planner emits `AsyncEval`, and the executor calls real services through each module. It returned `A`, `B`, and NULL for the NULL prompt, captured service request ids in `generate_status`, and showed the filter and `LIMIT` placed below the async node. Log: `../spark-implementation/live-spark-sql-proof.log`. The only provider not exercised live is Databricks, for lack of a credential.

### 1.6 Related but not competing: SPARK-61454 stage pipelining

[[SPIP] Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads](https://issues.apache.org/jira/browse/SPARK-61454), authored by Ferdinand Xu, is not a fourth entry in this comparison because it solves a different boundary. SPARK-61672 overlaps remote calls inside a task. SPARK-61454 overlaps producer, transfer, and consumer work across eligible shuffle stage boundaries using the Real-Time Mode transport and connected-stage admission.

Neither depends on the other. A future composition could reduce both forms of waiting, but it must be qualified as a new combined execution path: provider in-flight limits, resident-reader slots, bounded producer waves, route credits, stalled calls, cancellation, at-least-once retries, and cleanup of provider and transport state must all hold together. This implementation makes no performance or liveness claim for that composition and assumes ordinary materialized stage boundaries.

## 2. Technical refinements to Holden's initial draft

Holden's initial draft identifies the right problem and direction. The working implementation lets us replace early hypotheses with mechanisms verified against Spark 4.3.0:

1. **Task time is not `max(latency)`.** With N calls and concurrency C, the time is about `ceil(N / C)` times one call. We state that, and the evaluator tests the two bounds that make the formula true.
2. **Out-of-order output is not required.** The draft calls it a necessary trade-off. `AsyncFunctionEndToEndSuite` and `AiQueryParitySuite` complete calls out of order and still return input order. Ordering is the default.
3. **`spark.task.maxFailurePercentage` is the wrong layer.** The task does not know a record-level failure ratio, and the setting would change every Spark job. Row tolerance is `try_` or `_status` on the function.
4. **NULL is a bad failure signal.** NULL is a valid prompt and a valid model output. The status form carries an explicit code. NULL remains the result of a null input, and of `try_` when the user asks for it.
5. **UDF transpilation cannot see the UDF.** Spark stores Python UDF bodies as cloudpickle. `TranspiledPythonUDF` only substitutes an alternative the author already attached. Phase 3 is removed.
6. **Streaming "stay behind" needs a dead-letter store.** Offset and commit logs store one position per source. A row held for the next batch must be written somewhere durable. We do not claim a no-DLQ design.
7. **Iceberg Tasks and Failsafe are the wrong reuse.** Iceberg `Tasks` is a blocking pool. Failsafe is a new dependency. The JDK HTTP client and `CompletableFuture` are already available.
8. **Security, cost, and credentials were absent from the initial draft.** The provider modules now ship static endpoint/model/region allowlists, executor-side credentials, cluster ceilings for every numeric limit, a cluster-only credential source and breaker policy, and a conformance suite that proves a session can only tighten those controls.

## 3. Issues in SPARK-61404, from its source, not from the mailing list

PR #60311 is a serious prototype. Its tests for URL checks, response size, `Retry-After`, and error sanitization are better than our first slice. These limits remain:

1. **It is a Pandas UDF.** `ai_query` builds a `SCALAR_ITER` UDF and marks it `asNondeterministic()`. SQL, Scala, Java, and Spark Connect do not share that implementation. Holden asked for a cross-language function that is not a regular UDF.
2. **Nondeterministic blocks the cost controls.** Because the column is nondeterministic, Spark will not push a filter through it. The docs tell the user to apply `LIMIT` before adding the column. Our operator is one-to-one, so `LIMIT` and child-column filters move below the call even when the model itself is nondeterministic.
3. **The model registry is session state.** `register_model` writes into the Spark session. On a shared Spark Connect server that is not an administrator boundary. Our design requires static provider allowlists that a session cannot widen; those allowlists are a provider-module acceptance gate. The core active-call ceiling is already static.
4. **Cancellation does not stop a running call.** `future.cancel` on a thread-pool task does not interrupt an in-flight SDK call, and leaving the pool waits for running work. Our operator now cancels the source `CompletableFuture`, and `AsyncFunctionSafetySuite` asserts that the source future's `cancel` ran.
5. **Bedrock is weaker than the Databricks path in their own client.** The Databricks client has a decoded 16 MiB cap and an end-to-end row deadline. The Bedrock client has neither. Malformed Bedrock content can raise a raw Python exception. `ValidationException` is treated as a nullable inference error, which can hide a bad model id.
6. **`InvalidProxyURL` is rethrown unsanitized.** A proxy URL or credential can appear in the task error.
7. **Unknown SQL aliases fail in the task, after earlier batches may already have called the model.** Their own docs say analysis-time validation is open. Our literal function names fail in analysis because they are catalog functions.
8. **There is no executable test for Spark task retry or duplicate billing.** Both designs are at-least-once. Their docs warn about it. Neither side should claim exactly-once.

We copied the behaviors that are worth copying: provider wire formats, null-input passthrough, a distinct fatal-versus-nullable error policy, transient 429 retry, and worker-side credentials. We did not copy the Pandas UDF, the session registry, or the nullable `ValidationException`.

## 4. Implementation hardening already completed

These are resolved findings, not current proposal weaknesses. They remain here because this document is also the requested change and test record, and the tests demonstrate that the design changed when evidence found a defect.

| Issue | Why it mattered | Fix | Test |
| --- | --- | --- | --- |
| Plan-node tree pattern was declared as an expression pattern | Every query failed analysis checks under test | Moved `EVAL_ASYNC_FUNCTIONS` below `OPERATOR_START` | End-to-end suite |
| `HybridRowQueue` page was not closed | Task failed the 64 MB leak detector and hid the real HTTP 500 | Close the queue on the task thread after cancelling calls | End-to-end suite |
| Dependent future was cancelled, not the source call | A killed task could keep a billed request running | Track and cancel the source future | `task cancellation reaches the source future` |
| Interrupt listener raced the task thread on the queue | Cleanup could throw or close the queue twice | Interrupt listener only cancels futures | Cancellation test plus leak detector |
| Zero in-flight setting waited forever | A bad config hung the task | Reject non-positive ceiling and poll interval | `invalid engine limits fail promptly` |
| One row with several functions could exceed a bound | The admission check counted rows, then per-expression counters missed shared providers | Enforce one global ceiling and one identity-based ceiling per provider object; wait for a slot when a row contains repeated calls | `multiple functions...` and `shared bound-function...` |
| Nested `fn(fn(x))` extracted the outer call first | The inner call stayed unevaluable | Collect the innermost call first | `nested async calls are extracted in dependency order` |
| `fn(sum(v))` moved below the aggregate | The argument was not evaluable over raw input rows | Aggregate-aware extraction places the async call above the aggregate | `async function over an aggregate result...` |
| `LIMIT` and child filters did not move below the call | Rows that could not be returned were still billed | Add limit and predicate cases for `EvalAsyncFunctions` | Exact call-count tests for `LIMIT` and child filter |
| Random delay did not prove out-of-order completion | An order test could pass without reordering | Force row 1 to complete before row 0, record completion order, then assert output sequence | `forced reverse completion...` |
| Concurrent callbacks updated `SQLMetric` | Updates could race and be lost | Accumulate failures in `AtomicLong`, publish from the task thread | Final combined suite and code review |
| A session could raise the active-call ceiling | A shared Connect user could widen cost | Register the ceiling as bounded static `SQLConf` | `session cannot raise...` |
| Generative calls were deduplicated | Two stochastic calls became one result | Honor `isDeterministic()`; nondeterministic calls keep a stable per-call id | `nondeterministic identical calls are not deduplicated` |
| A thrown provider call or a null future could wedge the counter | The task could fail with a leaked in-flight count | Stage the future, restore the counter, and cancel earlier calls | `synchronous provider exception` and `null future` |
| `InterruptedException` was swallowed | The task could block in `join` after an interrupt | Restore the interrupt and fail the task | Covered by the cancellation path |
| Namespace helper created `aq` twice | One parity test failed before the engine ran | Create the namespace only if it is absent | Parity suite, 7 passed |

## 5. Why this design is provider-agnostic and cloud-agnostic

The public type is `AsyncScalarFunction`. It has a row in and a future out. It does not mention OpenAI, Databricks, AWS, HTTP, or JSON.

What a provider module owns:

- how to build the request,
- how to authenticate on the executor,
- which failures are retried,
- how to read the response,
- how to map a failure to strict, null, or status.

What Spark owns, once, for every provider:

- planning,
- the in-flight bound,
- the reorder bound,
- input-order output,
- spill of input rows,
- task cancellation,
- limit and predicate placement,
- the rule that a nondeterministic call is not silently reused.

The parity suite is the proof that this split is real. The same operator parsed a Databricks `choices[0].message.content` body and a Bedrock `output.message.content[].text` body. The live Bedrock function uses AWS SDK v2 typed `Converse` classes, and it lives in the `hadoop-cloud` test module because that module already has the AWS SDK. `sql/core` does not depend on AWS.

## 6. Provider modules: what is done and what remains

The native design has the stronger execution architecture: cross-language, provider-neutral, cloud-neutral, ordered, bounded, cancellation-aware, and protected by a static active-call ceiling. The provider modules under `../spark-implementation/` now close the gap that PR #60311 led on.

What the modules do today (both compile, both are packaged as jars, see `../spark-implementation/compile-check.log`):

- Catalogs: `OpenAIFunctionCatalog` and `BedrockFunctionCatalog`, each exposing `generate`, `try_generate`, and `generate_status`.
- Trust boundary: endpoint, model, and region allowlists plus every numeric limit are cluster ceilings that a session may only tighten; the credential source and breaker policy are cluster-only; a catalog option naming the credential variable is rejected.
- Client behavior: Jackson request and response handling; gzip; a decoded size cap enforced while the body streams (chunked bodies included); `Retry-After` in seconds and HTTP-date; an absolute deadline that includes pacing; a per-attempt budget that bounds the body phase and aborts the exchange; cancellation that reaches the in-flight request and stops pending retries.
- Resilience: a per-endpoint circuit breaker with a real half-open probe and generation-stamped admissions, counting only endpoint-health failures; two-layer pacing where the cluster ceiling is shared and a session rate slows only its own catalog; one client per connection identity so a session cannot create clients or threads by varying options.
- Errors: only sanitized codes surface; prompts, tokens, endpoints, and provider bodies never appear in a row-level error or plan; fatal conditions (auth, config, TLS, bad model) fail the task even in tolerant forms; breaker state is reported in `generate_status`, not hidden.

Evidence: the 71-case conformance harness (`../spark-implementation/conformance.log`, 71 passed, 0 failed) exercises the packaged jars through their catalog path. Seven cases are live: the OpenAI-compatible module against Amazon Bedrock's OpenAI-compatible Chat Completions API with a bearer token, and the Bedrock module against Converse. The rest drive the real module code against an in-process HTTP server so failure behaviors a vendor will not produce on demand (an oversized 503 body, a `Retry-After` HTTP-date, a trickled body, a chunked over-cap response) are exercised deterministically. Three independent adversarial review rounds (`../semantic-review/`, 35 findings) were applied in a loop. All findings were subsequently fixed and covered by passing tests; round 3 itself found 0 high, 3 medium, and 3 low issues.

Also done: `LiveSparkSqlProof` (`../spark-implementation/conformance/LiveSparkSqlProof.scala`, log `../spark-implementation/live-spark-sql-proof.log`) runs real Spark SQL with BOTH packaged jars loaded only through configuration, through the native planner, against real services, in one process: 25 of 25 checks, including plan shape, answers, NULL passthrough, service request ids, filter and `LIMIT` pushdown below the async node, and real six-word summaries over the example reviews. And `LiveThroughputBenchmark` measured the proposal's central claim on real Bedrock: 5.79x faster at 8 in flight than serial over 24 prompts, all correct, order preserved.

What remains before either module is called production-ready:

- A run on a multi-node cluster rather than `local[2]`. The one-process proof exercises the identical code path (catalog loading, planning, task execution, module call); a cluster adds executor-side classloading and credential-chain behavior that should be observed once.
- Live Databricks, because no approved Databricks credential was supplied. The OpenAI-compatible module targets the Databricks chat-completions envelope, which the parity suite already validates.
- Streaming responses and multi-turn request shapes, which are out of v1 scope.
- The full Spark/Pandas/Arrow benchmark, which remains a vote gate because only that run can measure Python worker and Arrow costs.

## 7. Files changed

All of this is local. `HEAD` is still `98bc0212`, tag `v4.3.0`. Nothing is committed or pushed.

### Modified Spark files

| File | Change |
| --- | --- |
| `sql/catalyst/.../analysis/FunctionResolution.scala` | Dispatch `AsyncScalarFunction` in `resolveV2Function`. |
| `sql/catalyst/.../trees/TreePatterns.scala` | `ASYNC_FUNCTION` in the expression section. `EVAL_ASYNC_FUNCTIONS` in the operator section. |
| `sql/catalyst/.../optimizer/Optimizer.scala` | `LimitPushDown` and filter pushdown for `EvalAsyncFunctions`. |
| `sql/catalyst/.../internal/SQLConf.scala` | Registers the static active-call ceiling and bounded reorder/poll settings. |
| `sql/core/.../SparkOptimizer.scala` | `ExtractAsyncFunctions` and aggregate-aware extraction in the optimizer batch and `nonExcludableRules`. |
| `sql/core/.../SparkStrategies.scala` | `AsyncFunctionEvals` strategy. |
| `sql/core/.../SparkPlanner.scala` | Register that strategy. |

### New Spark files

| File | Change |
| --- | --- |
| `sql/catalyst/.../functions/AsyncScalarFunction.java` | The SPI. It is serializable, and its determinism contract is semantic. |
| `sql/catalyst/.../expressions/ApplyAsyncFunctionExpression.scala` | Unevaluable expression. Deterministic calls share a canonical id. Nondeterministic calls do not. |
| `sql/catalyst/.../plans/logical/asyncFunctionLogicalOperators.scala` | `EvalAsyncFunctions`, one output row per input row. |
| `sql/core/.../async/AsyncEvalExec.scala` | Bounded, ordered, cancellable physical operator. |
| `sql/core/.../async/ExtractAsyncFunctions.scala` | Innermost-first extraction for project and filter. |
| `sql/core/src/test/.../async/AsyncFunctionEndToEndSuite.scala` | Loopback HTTP end-to-end tests. |
| `sql/core/src/test/.../async/AiQueryParitySuite.scala` | Databricks and Bedrock envelope parity. |
| `sql/core/src/test/.../async/AsyncFunctionSafetySuite.scala` | Limit, nesting, determinism, config, failure, ceiling, cancellation. |
| `hadoop-cloud/pom.xml` | Test-only dependency on the Catalyst test jar so the live suite can see `PlanTestBase`. |
| `hadoop-cloud/src/test/.../async/BedrockLiveAsyncFunctionSuite.scala` | Opt-in live Bedrock Converse through Spark SQL. |

### Research files under `SPARK-61672/`

Earlier research files `00` through `07` remain in the parent `SPARK-61672/` directory. This proposal package contains the final documents and visual. The extracted PR #60311 sources and the engine test logs are under `../ai_query_ref/` and `../poc/`. None of these files are part of the Spark engine tree.

### Provider modules under `../spark-implementation/`

| File | Change |
| --- | --- |
| `connector/ai-openai/.../OpenAIFunctions.scala` | Catalog, cluster-ceiling settings resolver, three function forms, status struct, sanitized exception. |
| `connector/ai-openai/.../OpenAIClient.scala` | JDK HTTP client, Jackson, streaming size cap, Retry-After, deadlines, breaker, two-layer pacer, classification. |
| `connector/ai-bedrock/.../BedrockFunctions.scala` | Catalog, cluster-ceiling settings resolver, three function forms. |
| `connector/ai-bedrock/.../BedrockClient.scala` | AWS SDK v2 async client per region, per-request overrides, breaker, class-name classification. |
| `connector/ai-openai/pom.xml`, `connector/ai-bedrock/pom.xml` | Standalone module builds. |
| `conformance/ProviderConformanceHarness.scala` | 71-case conformance harness, 7 cases live against real services. |
| `conformance/LiveSparkSqlProof.scala` | Both modules through real Spark SQL in one process against real services, plus real summaries over the example reviews. |
| `conformance/LiveThroughputBenchmark.scala` | Serial versus async throughput on real Bedrock through real Spark SQL. |
| Generated provider jars (not stored in the clean package) | `mvn package` produces `connector/ai-openai/target/spark-ai-openai_2.13-4.3.0.jar` and `connector/ai-bedrock/target/spark-ai-bedrock_2.13-4.3.0.jar`; successful builds recorded in `mvn-package.log`. |
| `compile-check.ps1`, `live-spark-sql-proof.ps1`, `live-throughput-benchmark.ps1` and their logs | Build, package, conformance, one-process proof, and benchmark records. |

Runnable examples, dummy data, and a results report are under `examples/`.

## 8. Tests we ran

| Test | Command or entry point | Result |
| --- | --- | --- |
| Catalyst compile | `mvn -pl sql/catalyst -am compile` | `BUILD SUCCESS`. Verbose build log kept local, not in this package. |
| SQL core compile | `mvn -pl sql/core -am compile` | `BUILD SUCCESS`. Verbose build log kept local, not in this package. |
| Earlier end-to-end HTTP | `AsyncFunctionEndToEndSuite` | 5 passed, 0 failed before the forced reverse-completion case was added. Log `../poc/e2e-test4.log`. |
| Provider parity | `AiQueryParitySuite` plus the end-to-end suite | 12 passed, 0 failed. Log `../poc/parity-test2.log`. |
| Earlier safety retest | `AsyncFunctionSafetySuite` | 8 passed, 0 failed before the final review additions. Log `../poc/safety-test2.log`. |
| Final combined core run | all three SQL suites | 25 passed, 0 failed, BUILD SUCCESS. The final source includes aggregate placement, static configuration, provider-identity limits, shared close, forced reverse completion, child-filter call count, sequence assertions, and atomic metric accounting. Log `../poc/final-core-test4.log`. |
| Real Bedrock, outside Spark | `aws bedrock-runtime converse`, model `amazon.nova-micro-v1:0` | Returned `OK`. 4 input tokens, 2 output tokens, 223 ms. |
| Live Spark SQL Bedrock | `BedrockLiveAsyncFunctionSuite` with `SPARK_TEST_LIVE_BEDROCK=1` and `-Phadoop-cloud` | 1 passed, 0 failed, BUILD SUCCESS. The optimized plan contains `EvalAsyncFunctions`; the executed plan contains `AsyncEval`. It made two typed AWS SDK v2 Converse calls to Nova Micro and kept the NULL row. Log `../poc/final-bedrock-live2.log`. |
| Provider module compile and package | `../spark-implementation/compile-check.ps1` | Both modules compile against the native SPI (exit 0) and are packaged as jars. Log `../spark-implementation/compile-check.log`. |
| Provider module conformance | `ProviderConformanceHarness` against the packaged jars | 71 passed, 0 failed, including 4 live calls through the OpenAI-compatible module against Bedrock's OpenAI-compatible API and 3 live calls through `BedrockFunctionCatalog`. Log `../spark-implementation/conformance.log`. |
| One-process end to end, both modules | `LiveSparkSqlProof` on `local[2]` | 25 passed, 0 failed. Both catalogs loaded from configuration, `EvalAsyncFunctions` and `AsyncEval` in the plans, answers `A` and `B`, NULL preserved, service request ids captured, filter and `LIMIT` below the async node, real summaries over the reviews data. Log `../spark-implementation/live-spark-sql-proof.log`. |
| Live throughput benchmark | `LiveThroughputBenchmark` on real Bedrock | In-flight 1: 8,200 ms. In-flight 8: 1,416 ms. 5.79x, 24 of 24 correct, ordered. Log `../spark-implementation/live-throughput-benchmark.log`. |
| Adversarial review loop | `semantic_reviewer`, three rounds | 17, then 12, then 6 findings; all 35 subsequently fixed and covered by passing tests; round 3 had 0 high, 3 medium, and 3 low findings. Reviews under `../semantic-review/`. |

### Safety cases, by name

1. `LIMIT` is pushed below async evaluation and prevents extra calls.
2. A child-only predicate runs below async evaluation and avoids calls.
3. Nested async calls are extracted in dependency order.
4. An async function over `sum(id)` runs after the aggregate.
5. Nondeterministic identical calls are not deduplicated.
6. Invalid engine limits fail promptly instead of deadlocking.
7. A session cannot raise the static active-call ceiling.
8. Synchronous provider exception fails promptly.
9. Null future fails promptly.
10. Multiple functions respect both the global and per-provider ceilings.
11. A shared bound-function instance is closed once and respects its limit.
12. Task cancellation reaches the source future.

### Parity cases, by name

1. Databricks chat-completions envelope.
2. Bedrock Converse envelope.
3. Null request returns null with no call.
4. `on_error=null` turns a 400 into null, and a neighbor call still succeeds.
5. Strict mode fails the query on a 400.
6. A transient 429 is retried and then succeeds.
7. Results stay in input order over 120 rows.

### End-to-end cases, by name

1. Single row returns the async HTTP result.
2. Per-row results are correct and in input order over 200 rows.
3. A forced later-row-first completion still emits exact input order.
4. NULL input is passed through as NULL without a call.
5. A failing call fails the query.
6. Identical deterministic calls are evaluated once.

### Provider conformance cases, by group (71 total)

| Group | Cases | What they prove |
| --- | ---: | --- |
| Trust boundary | 8 | Credential source cannot be set from a session; every limit can be lowered but not raised past the cluster ceiling; a cluster rate ceiling cannot be disabled; breaker policy is cluster-only; bad numeric options fail without echoing the value; loopback exemption is host-exact; user info in a URL is rejected; `DESCRIBE` text carries no endpoint. |
| Allowlists and binding | 8 | Unapproved base URL, model, region rejected; plain http rejected off loopback; empty allowlist blocks the catalog; concurrency clamped; nondeterministic with a stable canonical name; three forms listed; non-string argument rejected. |
| Correctness | 4 | Envelope parsing; prompts with quotes, backslashes, and newlines round-trip through Jackson; NULL input makes no call (server counter proves it); gzip decoded. |
| Size caps | 5 | Oversized plain, oversized gzip (decoded size), under-cap plain, chunked over-cap aborted while streaming, chunked under-cap succeeds. |
| Retry, deadlines, timeouts | 8 | `Retry-After` seconds; parser handles seconds, HTTP-date, garbage, negative; `Retry-After` HTTP-date; stops at `maxAttempts`; absolute deadline stops retries early; request timeout; per-attempt budget across attempts; trickled body cut off and exchange aborted (server observes the write failure). |
| Classification and sanitization | 7 | 400 and 404 terminal `BAD_REQUEST`; 403 `AUTH`; malformed envelope and non-JSON body `SCHEMA`; no error path in any of the three forms leaks the prompt, token, URL, or provider body (7 error models x 3 forms); over-cap 5xx stays retryable by status. |
| Three forms | 8 | `generate_status` throws on fatal `AUTH`; `try_generate` NULLs a row-level error but fails fatally on `AUTH`; `generate_status` populates `SUCCESS`, `ERROR`, attempts after a retry, and `SKIPPED_NULL_INPUT`; `CIRCUIT_OPEN` is reported in `_status` and NULL in `try_`, not fatal. |
| Cancellation | 2 | In-flight exchange aborted (server write fails); cancel during a deterministic `Retry-After` backoff sends no further request. |
| Circuit breaker | 6 | Opens after consecutive endpoint failures; shared across catalogs with different tuning; exactly one half-open probe, failure re-opens; a stale outcome from before the trip cannot close it; ignores cancellation and client-side 400s; pacer rejections do not burn slots or trip it. |
| Pacing and resources | 5 | Sequential rate; a session rate does not slow another catalog; a cluster ceiling applies to every catalog and cannot be lifted; 20 different tunings create no new clients; 32 concurrent callers keep pacing and breaker consistent. |
| Bedrock, no network | 3 | Model and region allowlists; concurrency clamp; SDK exception classes map to stable codes with the row-level versus fatal split and no SDK message text. |
| OpenAI-compatible, live | 4 | Against Amazon Bedrock's OpenAI-compatible Chat Completions API with a bearer token: `generate` returns real model text; `generate_status` populates attempts and request id; NULL input makes no call; an unlisted model is rejected before any call. |
| Bedrock, live | 3 | `generate` through `BedrockFunctionCatalog` returns the model text; `generate_status` populates the AWS request id; NULL input makes no call. |

Group counts: 8 + 8 + 4 + 5 + 8 + 7 + 8 + 2 + 6 + 5 + 3 + 4 + 3 = 71, matching `../spark-implementation/conformance.log`.

## 9. What "best" means here

The best v1 is not the longest list of model names. It is the smallest Spark change that:

- works from SQL without Python,
- does not put a vendor SDK in Spark core,
- overlaps calls without giving up input order,
- lets an administrator bound cost and destinations,
- tells the truth about at-least-once execution,
- and has tests for the failures that spend money.

The original draft has the right goal and the wrong mechanisms for Phases 2, 3, and 4. PR #60311 has the right provider homework and the wrong execution vehicle. This implementation keeps the goal, does the provider homework on the shipped modules (71 conformance cases, three adversarial review rounds), proves the whole path in one process from configuration-loaded jars to real services for both modules, measures a 5.79x real speedup, and puts the execution in Spark where Holden asked for it. What remains is a multi-node cluster run, live Databricks, and the full Pandas/Arrow comparison. Those are deployment and measurement work. They are not reasons to go back to a Pandas UDF or to a task-percentage setting.

## 10. Throughput benchmark on real Amazon Bedrock

The same Spark SQL query, `SELECT id, bedrock.generate(prompt) FROM p`, over 24 prompts in one task against real Amazon Nova Micro, run twice with only the in-flight limit changed. `spark.sql.asyncFunction.maxInFlightCeiling` is static, so each setting ran in its own SparkContext; one warm-up query per context (SDK client, credential chain, connection) is excluded from timing.

| In flight | Wall time | Rows per second | Correct answers | Output in input order |
| ---: | ---: | ---: | --- | --- |
| 1 (serial, what a plain UDF does) | 8,200 ms | 2.93 | 24 of 24 | yes |
| 8 (native async operator) | 1,416 ms | 16.95 | 24 of 24 | yes |

Speedup 5.79x, every answer correct, input order preserved in both runs. This is the proposal's central claim measured on a real service with real latency variance, through the real planner and the real packaged module. It is a single sample on a developer machine; the number will move with network and service load, the direction will not. A comparison against the complete SPARK-61404 Pandas UDF and `mapInArrow` remains a vote gate because only that run can measure Python worker and Arrow costs.

Source and log: `../spark-implementation/conformance/LiveThroughputBenchmark.scala`, `../spark-implementation/live-throughput-benchmark.log`.
