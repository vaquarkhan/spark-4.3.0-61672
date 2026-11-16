# SPARK-61672: a guide to what is here and where

Holden, this is the map. It explains what we added on top of your initial proposal, which folder
each piece lives in, and what each file is for, so you can go straight to the part you care about.
Everything described here is local on the `v4.3.0` tag; nothing has been committed or pushed. We
would like to agree on the direction with you before anything goes to a branch.

If you only have ten minutes, read in this order: this file, then `REVIEW-CHECKLIST.md`, then
`proposal/01-SPIP-incremental-for-Holden.md`.

## The one-paragraph version

Your proposal asked for native AI functions with asynchronous calls inside a task. We built that
as a provider-neutral `AsyncScalarFunction` SPI plus an `AsyncEvalExec` physical operator in Spark
core, with no cloud SDK in core. On top of it we built two swappable provider modules (an
OpenAI-compatible one, which also covers Databricks chat completions, and an Amazon Bedrock one),
packaged them as jars, and tested them hard: 25 engine tests, a live Bedrock run through the Spark
planner, a 71-case conformance harness that drives the packaged jars through their real catalog
path with seven cases live against real services, a one-process run of both modules through real
Spark SQL against real services (configuration-loaded jars, native planner, service request ids
captured, real model summaries over the example reviews), a real throughput benchmark showing 5.79x
at eight calls in flight versus serial, and three adversarial review rounds that found and fixed
35 defects. Some of your original mechanisms changed after we checked them against Spark 4.3.0, and
the SPIP says exactly which and why.

## Related draft: SPARK-61454, Break the Stage Barrier

Ferdinand Xu's [[SPIP] Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads](https://issues.apache.org/jira/browse/SPARK-61454) is related but separate. Our work overlaps slow external calls inside a task. SPARK-61454 overlaps work across eligible shuffle stage boundaries. Neither depends on the other, and neither replaces the other.

The combined idea is attractive: while one stage streams records to the next, an `AsyncEvalExec` task could overlap model calls for those records. We do not claim that combination here. It would need joint tests for resident-reader slots, producer waves, provider in-flight calls, transport buffers, stalled calls, cancellation, retry duplication, and cleanup across both execution lifecycles. The current implementation assumes ordinary Spark stage boundaries.

## Two kinds of files

There are two kinds of things in this work and they live in different places.

1. **Spark engine changes** live in the normal Spark source locations (`sql/catalyst`, `sql/core`,
   `hadoop-cloud`). These are the files a Spark committer would review as a PR.
2. **Everything else** lives in this folder, `SPARK-61672/`: the proposal documents, the provider
   modules, the tests that are not Spark suites, the logs, and the reviews. This folder sits inside
   the Spark tree so one commit captures both, but none of it is Spark engine code.

## Part 1: the Spark engine changes (outside this folder)

These are the only files that touch Spark itself. Eight existing files were modified and seven new
files were added.

### What we added to Spark core

| File | What it is | Why |
| --- | --- | --- |
| `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/functions/AsyncScalarFunction.java` | The public SPI. One row in, one `CompletableFuture` out. | This is the whole contract between Spark and any provider. It knows nothing about AWS, OpenAI, HTTP, or JSON. It requires an explicit `isDeterministic()` because generative models are not deterministic and Spark must not deduplicate their calls. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ApplyAsyncFunctionExpression.scala` | An unevaluable Catalyst expression wrapping a call. | Prevents normal expression codegen from ever making a remote call by accident. Carries a stable per-call id. |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/asyncFunctionLogicalOperators.scala` | The `EvalAsyncFunctions` logical plan node. | One output row per input row, so the optimizer can reason about it and push `LIMIT` and filters below it. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/async/ExtractAsyncFunctions.scala` | Optimizer rules that lift async calls out of projections and filters into the logical node. | Innermost call first for nested calls; aggregate-aware so `fn(sum(v))` runs above the aggregate. Mirrors `ExtractPythonUDFs`. |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/async/AsyncEvalExec.scala` | The physical operator. | Keeps a bounded number of calls in flight, buffers input in a spillable `HybridRowQueue`, emits rows in input order no matter the completion order, cancels the source future when a task is killed, closes provider resources once per task. |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/async/` | Three test suites: `AsyncFunctionEndToEndSuite` (6), `AiQueryParitySuite` (7), `AsyncFunctionSafetySuite` (12). | End-to-end over a loopback HTTP server; both the Databricks and Bedrock response envelopes on the native operator; and the safety cases (limit and filter pushdown call counts, nested and aggregate extraction, non-dedup of nondeterministic calls, static ceiling, cancellation reaching the source future). 25 pass. |
| `hadoop-cloud/src/test/scala/org/apache/spark/sql/execution/async/BedrockLiveAsyncFunctionSuite.scala` | Opt-in live test: real Spark SQL to real Amazon Bedrock. | Proves the planner path end to end with the AWS SDK. Lives in `hadoop-cloud` because that module already has the SDK; `sql/core` never does. |

### What we modified in Spark core

| File | Change |
| --- | --- |
| `sql/catalyst/.../analysis/FunctionResolution.scala` | Recognize an `AsyncScalarFunction` when a catalog returns one. |
| `sql/catalyst/.../trees/TreePatterns.scala` | Tree patterns for the expression and the plan node. |
| `sql/catalyst/.../optimizer/Optimizer.scala` | Push `LIMIT` and safe child-column filters below `EvalAsyncFunctions`, so filtered and limited rows are never sent to a provider even though the model is nondeterministic. |
| `sql/catalyst/.../internal/SQLConf.scala` | The active-call ceiling as a bounded static config a session cannot raise, plus bounded reorder-window and poll-interval settings. |
| `sql/core/.../execution/SparkOptimizer.scala` | Register extraction as a non-excludable correctness rule. |
| `sql/core/.../execution/SparkStrategies.scala`, `SparkPlanner.scala` | Map the logical node to `AsyncEvalExec`. |
| `hadoop-cloud/pom.xml` | Test-only dependency on the Catalyst test jar so the live suite can use the plan test base. |

## Part 2: this folder, piece by piece

### Root files

| File | What it is |
| --- | --- |
| `README.md` | Index of the package. |
| `GUIDE-FOR-HOLDEN.md` | This file. |
| `REVIEW-CHECKLIST.md` | Every request made during this work, its status, and where to verify it. Also lists every defect the review loop found, in plain words. |
| `ARCHITECTURE.md` | Layering, query lifecycle, concurrency and ordering model, error model, configuration trust boundary, and provider module internals. Three rendered diagrams. |
| `make-zip.py` | Rebuilds `../SPARK-61672-review-package.zip` from this folder. |

### `proposal/`: the documents

This is the part to read.

| File | What it is |
| --- | --- |
| `01-SPIP-incremental-for-Holden.md` | The proposed SPIP. It keeps your objective and your central idea (native inference, asynchronous calls inside a task, per-call retries), and it changes four mechanisms after checking them against Spark 4.3.0: we preserve row order instead of requiring out-of-order output; we use strict, `try_`, and status function forms instead of `spark.task.maxFailurePercentage`; we removed automatic UDF transpilation because Spark cannot see inside a pickled function; and we removed "failed rows stay behind" without a durable dead-letter store. Each change has the reasoning next to it. |
| `02-three-proposal-comparison-and-evidence.md` | A strict one-to-one comparison of your initial draft, the parallel SPARK-61404 `ai_query` prototype, and this implementation, row by row, with a "Leads" column. Then the technical refinements, the issues found in SPARK-61404's own source, the defects our own tests found and fixed, every file changed, every test run, and the benchmark. The comparison with SPARK-61404 is there to reuse their strongest provider work and to explain why your cross-language model needs a native backend, not to compete. |
| `03-layman-tutorial.md` | A plain-language walkthrough for someone who is not a Spark internals person: the problem, what we built, how a query flows, how to connect Bedrock, OpenAI, or Claude, what an administrator controls versus what a user controls, and how to run everything. |
| `README.md` | Reading order and an evidence-location table. |
| `diagrams/provider-neutral-ai-functions.svg` and `.png` | The architecture and provider-switching visual used in the tutorial. `render-svg.js` regenerates the PNG. |

### `proposal/examples/`: runnable examples with dummy data

| Path | What it is |
| --- | --- |
| `data/reviews.csv`, `data/reviews.jsonl` | Ten short product reviews used as input, two with a NULL body. |
| `sql/01-bedrock.sql` | The full `spark-sql` launch configuration and SQL for the Bedrock provider. |
| `sql/02-openai.sql` | The same for an OpenAI-compatible endpoint. The SQL is identical; only the launch configuration changes. |
| `sql/03-claude-via-bedrock.sql` | Claude through Bedrock: a one-line model change, identical SQL. |
| `report/RESULTS.md`, `report/live-bedrock-summaries.csv` | What ran and what came out, including the real model summaries of the reviews produced by Spark SQL calling Bedrock through the packaged module. |

### `spark-implementation/`: the provider modules

These are the swappable modules that sit outside Spark core. They are the answer to "where does the
cloud code go."

| Path | What it is |
| --- | --- |
| `connector/ai-openai/` | The OpenAI-compatible module. `OpenAIFunctions.scala` is the catalog, the settings resolver (allowlists and cluster ceilings), and the three function forms. `OpenAIClient.scala` is the JDK HTTP client with Jackson, gzip, a streaming size cap, `Retry-After`, deadlines, a circuit breaker, and a two-layer pacer. `pom.xml` builds it standalone. |
| `connector/ai-bedrock/` | The Amazon Bedrock module, same shape. `BedrockClient.scala` wraps the AWS SDK v2 async Converse client. |
| Generated jars | Deliberately removed from the clean share package. Run each module's `mvn package`; the jar appears under that module's `target/`. The successful builds are recorded in `mvn-package.log`. |
| `conformance/ProviderConformanceHarness.scala` | The 71-case conformance harness. It drives the packaged jars through the real catalog path (`initialize`, `loadFunction`, `bind`, `produceResultAsync`). Seven cases are live: the OpenAI-compatible module against Amazon Bedrock's OpenAI-compatible Chat Completions API with a bearer token, and the Bedrock module against Converse. The rest drive the real module code over real HTTP to an in-process server, for failure behaviors a vendor will not produce on demand. |
| `conformance/LiveSparkSqlProof.scala` | Both modules through a real `SparkSession`, configured only with `spark.sql.catalog.*` and `spark.ai.*`, through the native planner, against real services. Prints the plans, the rows, and the service request ids, and writes real summaries of the example reviews. |
| `conformance/LiveThroughputBenchmark.scala` | The same Spark SQL over the same prompts against real Bedrock with 1 call in flight, then 8. |
| `conformance.log`, `live-spark-sql-proof.log`, `live-throughput-benchmark.log` | 71 passed; 25 passed; 5.79x with 24 of 24 correct and ordered. |
| `env.ps1`, `compile-check.ps1`, `live-spark-sql-proof.ps1`, `live-throughput-benchmark.ps1` | Portable run scripts. `env.ps1` derives the Spark checkout from the package location and accepts `SPARK_61672_*` overrides for Java, Maven, Spark, Maven repository, and Windows Hadoop paths. |
| `README.md` | The trust model and the full configuration reference for both modules. |

The trust model is the thing to understand about these modules. Catalog options
(`spark.sql.catalog.ai.*`) are what a session may set, and they can only make a limit tighter.
Cluster settings (`spark.ai.<provider>.*`) are set once at launch: the endpoint, model, and region
allowlists, every ceiling, the credential source, and the breaker policy. A session cannot widen a
limit, name an unlisted endpoint, or choose which executor secret is sent.

### `semantic-review/`: the adversarial review rounds

We asked an independent reviewer to try to break the provider modules, then fixed what it found,
then asked again after each fix set. Three rounds:

| File | Round | Findings |
| --- | --- | --- |
| `2026-11-15-154400-pr-SPARK-61672.md` | 1 | 17, five high. The worst: a session could choose which executor environment variable was sent as a bearer token. Also session-overridable limits, a breaker that tripped on cancellation, a size cap applied after full buffering, and two harness tests that could not fail. |
| `2026-11-15-161323-pr-SPARK-61672.md` | 2 | 12, two high, both introduced by the round-1 fixes. This is why we continued reviewing rather than stopping after one pass. |
| `2026-11-15-163455-pr-SPARK-61672.md` | 3 | 6, none high. Verified 11 of 12 round-2 fixes resolved and found the last subtle one (a JDK `orTimeout` detail that made a cancel a no-op). |

All 35 are fixed, and each one became a conformance case. `REVIEW-CHECKLIST.md` lists them in
plain terms grouped by what they would have cost in production.

### `poc/`: the engine test logs

The five Spark test-result logs behind the evidence claims. `final-core-test4.log` is the 25-case
engine run; `final-bedrock-live2.log` is the live Bedrock run through the planner. The others are
earlier runs kept for the record. Verbose multi-megabyte build logs were left out.

### `presentation/`

Start with `SPARK-61672-Feature-Overview.pptx` or `.pdf`. It is the 15-slide feature story with large text and one idea per slide: user SQL, problem, architecture, bounds/order, providers, governance, error forms, real proof/output, performance, SPARK-61454, and the decision requested.

`SPARK-61672-POC-Review.pptx` and `.pdf` are the 27-slide technical appendix. They include every changed Spark and provider folder/file/class, all test helper classes, configuration keys, evidence, package map, run path, and boundaries. The Python generators, PowerPoint converters, and verifier reproduce both.

### `related-work/`

`SPARK-61454-break-stage-barrier.md` is a concise local summary of Ferdinand Xu's supplied draft, its reported prototype evidence and milestones, and the boundary and possible future composition with SPARK-61672. The authoritative discussion record is [SPARK-61454](https://issues.apache.org/jira/browse/SPARK-61454).

### `ai_query_ref/`

The SPARK-61404 PR source extracted for reference, so the comparison can be checked against what they actually wrote.

### `diagrams/`

The three architecture diagrams embedded in `ARCHITECTURE.md`, as PNG with their Mermaid `.mmd`
sources so they stay editable.

## What is honestly still open

- A run on a multi-node cluster rather than `local[2]`. The one-process run exercises the identical
  code path; a cluster adds executor-side classloading and credential-chain behavior worth observing.
- Live Databricks Model Serving, because no approved credential was available. The OpenAI-compatible
  module is proven against a real OpenAI-compatible service with the same response envelope.
- The full Pandas/Arrow comparison against the SPARK-61404 UDF.
- Spark external calls are at-least-once. A task retry, speculation, or executor loss can repeat
  billable calls. The documents say this plainly and do not claim otherwise.

## What is real and what is not

Everything that claims to have called a model called a real model, and the service-minted request
ids are in the logs. The example summaries in `proposal/examples/report/` came from Amazon Bedrock.
The benchmark ran on Amazon Bedrock. The only in-process servers are inside unit tests, where Spark
convention requires them, and inside the part of the conformance harness that exercises failure
behaviors (oversized error bodies, `Retry-After` dates, trickled bodies) that a vendor cannot be
asked to produce; those cases still run the real module code over real HTTP.

## How to reproduce the key results

From the Spark repo root, the engine suites:

```powershell
mvn -pl sql/core -am test "-DwildcardSuites=org.apache.spark.sql.execution.async" "-Dtest=none"
```

Both modules through real Spark SQL in one process, with real summaries of the example reviews
(paid, a few short calls; adjust the absolute paths in the script for your machine):

```powershell
$env:AWS_PROFILE = '<approved-profile>'
powershell -File SPARK-61672/spark-implementation/live-spark-sql-proof.ps1
```

The full command set, including the opt-in live Bedrock runs, is in `REVIEW-CHECKLIST.md`.
