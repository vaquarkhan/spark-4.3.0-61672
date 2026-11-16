# SPARK-61672: native asynchronous AI functions

This folder is the shareable review package for the incremental implementation of Holden Karau and
Prashant Singh's SPARK-61672 proposal, with Vaquar Khan as a contributing author. It sits inside
the Spark 4.3.0 tree alongside the code changes so a single commit captures both the engine and its
documentation.

## Start here

| Document | What it is |
| --- | --- |
| [`START-HERE.md`](START-HERE.md) | First-time, copy-paste path: choose offline/live mode, run automated preflight, build, validate, see expected output, and fix common errors. Start here if you did not build Spark before. |
| [`GUIDE-FOR-HOLDEN.md`](GUIDE-FOR-HOLDEN.md) | A plain-language map of what we added on top of the initial proposal, folder by folder and file by file, including the Spark engine changes outside this folder. |
| [`REVIEW-CHECKLIST.md`](REVIEW-CHECKLIST.md) | Every request mapped to what was delivered and where to verify it, plus every defect the review loop found. |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | The architecture: layering, query lifecycle, concurrency and ordering, error model, configuration trust boundary. |
| [`proposal/01-SPIP-incremental-for-Holden.md`](proposal/01-SPIP-incremental-for-Holden.md) | The proposed SPIP. |
| [`proposal/02-three-proposal-comparison-and-evidence.md`](proposal/02-three-proposal-comparison-and-evidence.md) | Strict one-to-one comparison, defects found and fixed, full change and test record, benchmark. |
| [`proposal/03-layman-tutorial.md`](proposal/03-layman-tutorial.md) | Plain-language walkthrough. |
| [`proposal/examples/`](proposal/examples) | Input data, per-provider SQL, and a results report from a real run, including real model summaries of the example reviews. |

## What is in this package

| Path | Contents |
| --- | --- |
| `START-HERE.md`, `GUIDE-FOR-HOLDEN.md`, `ARCHITECTURE.md`, `BUILD-AND-RUN.md`, `REVIEW-CHECKLIST.md` | First-time run path, folder map, architecture, detailed run guide, and review map. |
| `SOURCE-ZIP-MANIFEST.md`, `make-zip.py`, `make-full-source-zip.py`, `verify-zips.py` | Exact source-archive contents/exclusions plus reproducible clean packaging and secret/binary verification. |
| `proposal/` | The three proposal documents, the architecture diagram, and the examples. |
| `spark-implementation/` | Provider source and poms, plus three proof programs under `conformance/`: the 71-case conformance harness (7 cases live), `LiveSparkSqlProof` (both modules through real Spark SQL against real services), and `LiveThroughputBenchmark` (5.79x on real Bedrock). Build and evidence logs are alongside. Generated jars are intentionally excluded from the clean package. |
| `semantic-review/` | The three adversarial review rounds applied to the provider modules (17, 12, and 6 findings; all fixed). |
| `related-work/` | A concise local summary of the SPARK-61454 draft and how it relates to SPARK-61672; the JIRA remains authoritative. |
| `presentation/` | **Start with** `SPARK-61672-Feature-Overview.pptx` or `.pdf` (15 slides): feature story, user SQL, execution, providers, controls, failure forms, real proof, output, performance, SPARK-61454, and decision. `SPARK-61672-POC-Review.pptx`/`.pdf` (27 slides) is the technical file/class/configuration appendix. Generators and PowerPoint/PDF verifiers are included. |
| `ai_query_ref/` | Extracted SPARK-61404 PR source, for reference. |
| `poc/` | The five engine test-result logs behind the evidence claims. |

The engine code changes themselves live in the normal Spark locations (`sql/catalyst`, `sql/core`,
`hadoop-cloud`). `REVIEW-CHECKLIST.md` lists exactly which files changed.

## Archives

- `../SPARK-61672-review-package.zip`: this POC/review folder only, source and evidence, no generated jars/classes/targets or machine-specific classpaths.
- `../../SPARK-61672-full-spark-source.zip`: the complete cleaned Spark 4.3.0 source tree with all engine changes and this POC folder, so a recipient can build and validate without separately overlaying files. See `SOURCE-ZIP-MANIFEST.md` for exact exclusions.

Both archives include the PPTX and PDF. Neither archive contains a credential value, local AWS account identifier, private-key fixture, or generated provider jar.

## Related SPIP

[[SPIP] Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads](https://issues.apache.org/jira/browse/SPARK-61454) (SPARK-61454, Ferdinand Xu) is complementary, not a dependency. SPARK-61672 overlaps remote calls inside a task; SPARK-61454 pipelines records across eligible shuffle stage boundaries. The proposed future composition and its qualification requirements are explained in `proposal/01-SPIP-incremental-for-Holden.md` and `ARCHITECTURE.md`.

## One-line summary

A provider-neutral `AsyncScalarFunction` SPI plus an `AsyncEvalExec` operator let SQL call any model
provider asynchronously, in input order, with bounded concurrency and administrator cost controls,
while cloud code stays in swappable modules outside Spark core. Validated with 25 engine tests, a
live Amazon Bedrock run through the planner, two packaged provider modules that pass a 71-case
conformance harness with seven cases live against real services, a one-process run of both modules
through real Spark SQL against real services with real summaries of the example reviews, a real
throughput benchmark (5.79x at eight in flight versus serial), and three adversarial review rounds.
Everything here is local on the `v4.3.0` tag; nothing was committed or pushed.

## Quick start: build and run independently

Use the full source archive. It already contains the Spark source and every SPARK-61672 engine
change in the correct source folder. Do not copy this POC folder into a stock Spark binary.

```text
SPARK-61672-full-spark-source.zip
  -> spark-4.3.0-SPARK-61672/
       -> sql/catalyst/       modified engine source
       -> sql/core/           modified engine source and tests
       -> hadoop-cloud/       live Bedrock test
       -> SPARK-61672/        provider source, docs, evidence, PPTX/PDF
```

Prerequisites:

- JDK 17
- Maven 3.9 or later
- For live Bedrock: an AWS identity selected by the caller's default credential chain with
  `bedrock:InvokeModel` for the chosen model
- On Windows: a compatible `winutils.exe` and `hadoop.dll` under `HADOOP_HOME/bin`

### 1. Build and install the modified Spark artifacts

From the extracted Spark root:

```bash
./build/mvn -pl sql/catalyst,sql/core -am install -DskipTests
```

On Windows with Maven on `PATH`:

```powershell
mvn -pl sql/catalyst,sql/core -am install -DskipTests
```

### 2. Run all SPARK-61672 engine tests

```bash
./build/mvn -pl sql/core -am test \
  -DwildcardSuites=org.apache.spark.sql.execution.async \
  -Dtest=none
```

Expected result from the validated source: 25 passed, 0 failed. Evidence:
`SPARK-61672/poc/final-core-test4.log`.

### 3. Build both provider modules

```bash
mvn -f SPARK-61672/spark-implementation/connector/ai-openai/pom.xml package
mvn -f SPARK-61672/spark-implementation/connector/ai-bedrock/pom.xml package
```

Generated jars:

```text
SPARK-61672/spark-implementation/connector/ai-openai/target/
  spark-ai-openai_2.13-4.3.0.jar
SPARK-61672/spark-implementation/connector/ai-bedrock/target/
  spark-ai-bedrock_2.13-4.3.0.jar
```

Generated jars are deliberately excluded from both clean ZIPs. Successful Maven builds are
recorded in `spark-implementation/mvn-package.log`.

### 4. Run the real one-process Spark SQL proof

The script derives the Spark checkout from its own location. Override machine paths through
`SPARK_61672_*` environment variables; it contains no account, profile, or credential value.

```powershell
$env:SPARK_61672_JAVA_HOME = 'C:\path\to\jdk-17'
$env:SPARK_61672_MAVEN = 'C:\path\to\mvn.cmd'
$env:HADOOP_HOME = 'C:\path\to\hadoop'        # Windows only
$env:AWS_PROFILE = '<profile-selected-by-you>' # optional; caller controlled
powershell -File SPARK-61672\spark-implementation\live-spark-sql-proof.ps1
```

The validated run loaded both provider catalogs through `spark.sql.catalog.*`, planned
`EvalAsyncFunctions` and `AsyncEval`, called real services, captured service request IDs, preserved
NULL, verified filter/limit placement, and wrote real review summaries. Result: 25 passed.
Evidence: `spark-implementation/live-spark-sql-proof.log`.

For the OpenAI-compatible live path, also supply the endpoint, model, and token selected by your
administrator:

```powershell
$env:SPARK_TEST_LIVE_OPENAI_BASE_URL = 'https://approved.example/v1'
$env:SPARK_TEST_LIVE_OPENAI_MODEL = 'approved-model'
$env:AI_PROVIDER_TOKEN = '<short-lived-token>'
```

Full platform notes and a `spark-sql` command are in [`BUILD-AND-RUN.md`](BUILD-AND-RUN.md).

## Complete Spark source change inventory

### Modified existing Spark files

| File | Class or object changed | Change |
| --- | --- | --- |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionResolution.scala` | `class FunctionResolution` | Resolve `AsyncScalarFunction` returned by a V2 function catalog. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala` | `object LimitPushDown` | Push `LocalLimit` below `EvalAsyncFunctions`. |
| Same file | `object PushPredicateThroughNonJoin` | Push predicates that use only child columns below async evaluation. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreePatterns.scala` | `object TreePattern` | Add `ASYNC_FUNCTION` and `EVAL_ASYNC_FUNCTIONS`. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala` | `object SQLConf`, `class SQLConf` | Static active-call ceiling plus reorder-window and polling settings. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala` | `class SparkOptimizer` | Register normal and aggregate-aware extraction as non-excludable rules. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala` | `abstract class SparkStrategies`, `object AsyncFunctionEvals` | Plan `EvalAsyncFunctions` as `AsyncEvalExec`. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala` | `class SparkPlanner` | Register `AsyncFunctionEvals`. |
| `hadoop-cloud/pom.xml` | Maven module | Add the test-only Catalyst test-jar dependency for the live suite. |

### Added production Spark files and classes

| File | Classes, interfaces, or objects added | Purpose |
| --- | --- | --- |
| `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/functions/AsyncScalarFunction.java` | `interface AsyncScalarFunction<R>` | Public provider-neutral row-to-future SPI. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ApplyAsyncFunctionExpression.scala` | `ApplyAsyncFunctionExpression`, `AsyncFunctionIdentity` | Unevaluable call expression and canonical identity. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/asyncFunctionLogicalOperators.scala` | `EvalAsyncFunctions` | One-to-one logical operator that appends async results. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/async/AsyncEvalExec.scala` | `case class AsyncEvalExec`, `object AsyncEvalExec` | Physical execution, bounds, order, spill, cancellation, cleanup, metrics. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/async/ExtractAsyncFunctions.scala` | `object ExtractAsyncFunctions`, `object ExtractAsyncFunctionsFromAggregate` | Innermost-first extraction and aggregate-aware placement. |

### Added Spark test and live-integration files/classes

| File | Classes or objects added |
| --- | --- |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/async/AsyncFunctionEndToEndSuite.scala` | `AsyncFunctionEndToEndSuite`, `GenerateUnbound`, `HttpGenerateFunction` |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/async/AiQueryParitySuite.scala` | `AiQueryParitySuite`, `AiQueryUnbound`, `AiQueryFunction` |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/async/AsyncFunctionSafetySuite.scala` | `AsyncFunctionSafetySuite`, `SafetyState`, `LongToStringUnbound`, `LongToStringFunction`, `SafetyUnbound`, `CountingFunction`, `SynchronousFailureFunction`, `SlowCountingFunction`, `CloseCountingFunction`, `CancellableFunction` |
| `hadoop-cloud/src/test/scala/org/apache/spark/sql/execution/async/BedrockLiveAsyncFunctionSuite.scala` | `BedrockLiveAsyncFunctionSuite`, `BedrockConverseUnbound`, `BedrockConverseFunction` |

## Complete provider and proof class inventory

### OpenAI-compatible module

Folder: `SPARK-61672/spark-implementation/connector/ai-openai/src/main/scala/org/apache/spark/sql/ai/openai/`

| File | Classes and objects |
| --- | --- |
| `OpenAIFunctions.scala` | `ErrorMode` plus `Fail`, `Null`, `Status`; `OpenAISettings`; `OpenAIConf`; `OpenAIFunctionCatalog`; `GenerateUnbound`; `GenerateBound`; `OpenAIFunctions`; `ChatResult`; `OpenAIException` |
| `OpenAIClient.scala` | `OpenAIClient`; companion `OpenAIClient`; nested `Pacer`; nested `CircuitBreaker`; `Classification`; `Ok`; `Retryable`; `Terminal` |

### Amazon Bedrock module

Folder: `SPARK-61672/spark-implementation/connector/ai-bedrock/src/main/scala/org/apache/spark/sql/ai/bedrock/`

| File | Classes and objects |
| --- | --- |
| `BedrockFunctions.scala` | `ErrorMode` plus `Fail`, `Null`, `Status`; `BedrockSettings`; `BedrockConf`; `BedrockFunctionCatalog`; `GenerateUnbound`; `GenerateBound`; `BedrockFunctions`; `ConverseResult`; `BedrockException` |
| `BedrockClient.scala` | `BedrockClient`; companion `BedrockClient`; nested `CircuitBreaker` |

### Validation and real-run programs

Folder: `SPARK-61672/spark-implementation/conformance/`

| File | Object | Purpose |
| --- | --- | --- |
| `ProviderConformanceHarness.scala` | `ProviderConformanceHarness` | 71 catalog/client conformance checks; 7 call real services. |
| `LiveSparkSqlProof.scala` | `LiveSparkSqlProof` | Both provider jars through real Spark SQL and real services in one JVM. |
| `LiveThroughputBenchmark.scala` | `LiveThroughputBenchmark` | Real Bedrock throughput at one versus eight active calls. |

## Complete engine test inventory: 25 cases

### `AsyncFunctionEndToEndSuite`: 6

1. Single row returns the async HTTP result.
2. Per-row results are correct and in input order over 200 rows.
3. Forced reverse completion still emits input order.
4. NULL input passes through as NULL without a call.
5. A failing call fails the query in strict mode.
6. Identical deterministic calls are evaluated once.

### `AiQueryParitySuite`: 7

1. Databricks chat-completions response envelope.
2. Amazon Bedrock Converse response envelope.
3. NULL request returns NULL with no call.
4. `on_error=null` converts HTTP 400 to NULL while a neighboring row succeeds.
5. Strict mode fails the query on HTTP 400.
6. A transient HTTP 429 is retried and succeeds.
7. Results remain in input order over 120 rows.

### `AsyncFunctionSafetySuite`: 12

1. `LIMIT` moves below async evaluation and prevents extra calls.
2. A child-only predicate moves below async evaluation and avoids calls.
3. Nested async calls are extracted in dependency order.
4. An async function consuming `sum(id)` runs after the aggregate.
5. Identical nondeterministic calls are not deduplicated.
6. Invalid engine limits fail promptly instead of deadlocking.
7. A session cannot raise the static active-call ceiling.
8. A synchronous provider exception fails promptly.
9. A null future fails promptly.
10. Multiple functions respect the global and per-provider ceilings.
11. A shared bound-function instance closes once and respects its identity limit.
12. Task cancellation reaches the source future.

## Provider conformance inventory: 71 cases

The exact PASS line for every case is in `spark-implementation/conformance.log`.

<details>
<summary>Show all 71 case names</summary>

1. `credentialEnv` cannot be set from a catalog option.
2. A session can lower but not raise `maxResponseBytes` above its cluster ceiling.
3. A session cannot disable or raise a cluster rate ceiling.
4. Breaker policy options supplied by a session are ignored.
5. A garbage numeric option fails without echoing its value.
6. The loopback HTTP exemption is host-exact, not prefix-matched.
7. A base URL containing user information is rejected.
8. Function-description text does not expose the endpoint URL.
9. An unapproved base URL is rejected.
10. An unapproved model is rejected.
11. Plain HTTP is rejected outside loopback.
12. An empty allowlist blocks the catalog.
13. Requested concurrency is clamped to the static ceiling.
14. The bound generative function is nondeterministic and has a stable canonical name.
15. The catalog lists exactly `generate`, `try_generate`, and `generate_status`.
16. Binding rejects a non-string argument.
17. Success parses `choices[0].message.content`.
18. Quotes, backslashes, and newlines round-trip through Jackson.
19. NULL input returns NULL without a call, proved by the server request counter.
20. A gzip response is decoded.
21. An oversized plain response is rejected by the decoded size cap.
22. An oversized gzip response is rejected by decoded, not compressed, size.
23. A response below the cap succeeds.
24. A chunked response without `Content-Length` is aborted at the cap while streaming.
25. A chunked response below the cap succeeds.
26. HTTP 429 with `Retry-After` seconds retries and succeeds.
27. The `Retry-After` parser handles seconds, HTTP-date, invalid, and negative values.
28. HTTP 429 with `Retry-After` HTTP-date retries and succeeds.
29. Retries stop at `maxAttempts`.
30. The absolute deadline stops retries before `maxAttempts`.
31. Request timeout surfaces `TIMEOUT` and does not hang.
32. HTTP 400 is terminal `BAD_REQUEST` and is not retried.
33. HTTP 404 is terminal `BAD_REQUEST`.
34. HTTP 403 is terminal `AUTH`.
35. A malformed response envelope is `SCHEMA`.
36. A non-JSON body is `SCHEMA`.
37. No error path in any form leaks prompt, token, URL, or provider body.
38. `generate_status` throws instead of returning a struct on fatal `AUTH`.
39. `try_generate` converts a row-level error to NULL.
40. `try_generate` still fails fatally on `AUTH`.
41. `generate_status` returns a populated `SUCCESS` struct.
42. `generate_status` returns `ERROR` with safe code/message, attempts, and request ID.
43. `generate_status` reports attempts after retry.
44. `generate_status` on NULL returns `SKIPPED_NULL_INPUT` with zero attempts.
45. Cancelling a result aborts the in-flight HTTP exchange, observed at the server.
46. Cancellation during deterministic retry backoff sends no further request.
47. Each attempt is bounded end to end.
48. A trickled body is cut off at the attempt budget and its exchange is aborted.
49. Rejected pacer callers do not burn slots or trip the breaker.
50. The breaker opens after consecutive endpoint failures and reports `CIRCUIT_OPEN`.
51. A second catalog with different tuning shares the open breaker.
52. Tolerant forms report or NULL `CIRCUIT_OPEN` rather than failing fatally.
53. Exactly one half-open probe is admitted after reset, and failure reopens the breaker.
54. A late outcome admitted before a trip cannot close the breaker.
55. Cancellation and client-side HTTP 400 do not open the breaker.
56. An oversized HTTP 5xx body is classified by status, not size.
57. The pacer spaces sequential requests at the configured rate.
58. One session's rate does not slow a different catalog on the same endpoint.
59. A cluster rate ceiling applies to every catalog and cannot be lifted by a session.
60. Varying per-call tuning does not create new clients or threads.
61. Pacer and breaker remain correct under 32 concurrent callers.
62. The Bedrock model and region allowlists reject unapproved values.
63. Bedrock concurrency is clamped to the static ceiling.
64. Bedrock SDK exception classes map to stable row-level versus fatal codes.
65. Live OpenAI-compatible `generate` returns real model text.
66. Live OpenAI-compatible `generate_status` populates attempts and request ID.
67. Live OpenAI-compatible NULL input makes no call.
68. Live OpenAI-compatible unlisted model is rejected before any call.
69. Live Bedrock `generate` through `BedrockFunctionCatalog` returns real model text.
70. Live Bedrock `generate_status` populates the service request ID.
71. Live Bedrock NULL input makes no call.

</details>

## Additional validation and real evidence

| Validation | Result | Evidence |
| --- | --- | --- |
| Existing Catalyst optimizer regression suites | 314 passed, 0 failed, 1 ignored | `poc/regression-targeted.log` (Catalyst section; the later broad SQL portion was stopped and is not claimed) |
| Standalone provider Maven builds | 2 of 2 passed | `spark-implementation/mvn-package.log` |
| Provider conformance | 71 of 71 passed | `spark-implementation/conformance.log` |
| Live cases within conformance | 7 passed | Same log: 4 OpenAI-compatible, 3 Bedrock Converse |
| Both providers through real Spark SQL | 25 of 25 passed | `spark-implementation/live-spark-sql-proof.log` |
| Live Spark planner to Bedrock suite | 1 passed | `poc/final-bedrock-live2.log` |
| Real review summaries | Six summaries; two NULL rows made no call; two low-star rows filtered before billing | `proposal/examples/report/live-bedrock-summaries.csv` |
| Real Bedrock throughput | 8,200 ms serial versus 1,416 ms async; 5.79x; 24 of 24 correct; order preserved | `spark-implementation/live-throughput-benchmark.log` |
| Automated adversarial review | Rounds found 17, 12, and 6 issues; all subsequently fixed and covered by passing tests | `semantic-review/` |

## POC boundaries

- Built against this modified Spark 4.3.0 source tree. Stock Spark 4.3.x binaries do not contain
  `AsyncScalarFunction` or `AsyncEvalExec`.
- Batch SQL only. Streaming is outside this POC and not validated.
- Real end-to-end execution used `local[2]`; multi-node qualification remains open.
- Databricks was not exercised live because no approved credential was available. The
  OpenAI-compatible provider was exercised against a real compatible service.
- External calls are at-least-once. Task/stage retry, speculation, executor loss, or repeating an
  action can repeat paid calls.
- The full Pandas/Arrow comparison against the SPARK-61404 UDF remains open.
