# SPARK-61672 end-to-end review checklist

This checklist maps every request made during this work to what was delivered and where to verify
it. Paths are relative to this folder (`SPARK-61672/` inside the Spark 4.3.0 tree).

## Requests and status

| # | What was asked | Status | Where to verify |
| --- | --- | --- | --- |
| 1 | Validate the engine against real Amazon Bedrock, end to end | Done | `poc/final-bedrock-live2.log`: `native async SQL calls live Bedrock Converse`, succeeded 1, BUILD SUCCESS. Source: `../hadoop-cloud/src/test/.../BedrockLiveAsyncFunctionSuite.scala` |
| 2 | Strengthen tests versus SPARK-61404 and find bugs | Done | 25 core tests (6 end-to-end, 7 parity, 12 safety) in `poc/final-core-test4.log`; defects listed and fixed in `proposal/02-...md` section 4 |
| 3 | Show why ours is best; win across the board | Done | `proposal/02-...md` section 1 strict matrix and section 1.5 scorecard: native leads 12 rows, `ai_query` leads 2, 4 even |
| 4 | Write the SPIP | Done | `proposal/01-SPIP-incremental-for-Holden.md` |
| 5 | Frame as Holden's initial proposal plus our incremental; team with Holden | Done | Lineage statement at the top of all three `proposal/0*.md` docs |
| 6 | Keep resolved internal defects out of the competitive comparison | Done | Moved to the validation appendix: `proposal/02-...md` section 4 is labeled resolved findings, not current weaknesses |
| 7 | Make the comparison strictly one to one and show our lead | Done | `proposal/02-...md` section 1.1 to 1.5 |
| 8 | Fix the stale evidence count 23, it is now 25 | Done | `proposal/01-...md` suite table (6 plus 7 plus 12); `proposal/03-...md` test evidence |
| 9 | Remove the "What did SPARK-61404 ai_query propose?" section from the tutorial | Done | Not present in `proposal/03-layman-tutorial.md` |
| 10 | Finish the provider modules and make them configurable so anyone can run | Done | `spark-implementation/connector/ai-openai`, `spark-implementation/connector/ai-bedrock`; both compile, see `spark-implementation/compile-check.log` |
| 11 | Add an examples folder with code, data, and a final report | Done | `proposal/examples/` (data, sql, report with real model summaries from a real run) |
| 12 | Reference the examples in the tutorial | Done | `proposal/03-...md` "Runnable examples folder" section |
| 13 | No em-dashes, human first-person voice, self-contained docs | Done | Verified no non-ASCII dashes or smart quotes across the package |
| 14 | Keep everything local, no commit or push | Done | Spark repo HEAD still `v4.3.0`; changes are working-tree only |
| 15 | Add an architecture document | Done | `ARCHITECTURE.md` (Mermaid replaced by rendered PNGs under `diagrams/`) |
| 16 | Copy the proposal into the Spark tree so it can be added to git and shared | Done | This folder, `SPARK-61672/`, inside the Spark 4.3.0 repo |
| 17 | Lead every row of comparison 1.4 (provider wire formats, client test breadth, shippable module), not by rewording but by building | Done | Both standalone provider poms build successfully (`spark-implementation/mvn-package.log`); generated jars were exercised by the 71-case conformance harness and both modules ran live through their catalogs. Generated binaries are intentionally excluded from the clean share package; recipients build them from source. |
| 18 | One final review round with a devil's advocate in a loop, fixing everything reported | Done | Three rounds, not one: `semantic-review/2026-11-15-154400`, `-161323`, `-163455`. 17 then 12 then 6 findings, 35 total, all fixed and re-verified. Round 3 found 0 high, 3 medium, and 3 low issues; those six were then fixed and covered by the final passing conformance run. Round 2 caught two HIGHs introduced by round-1 fixes, which is why review continued beyond one pass |
| 19 | Copy everything into the code folder and zip the package | Done | `SPARK-61672-review-package.zip` contains the clean POC/review package. `SPARK-61672-full-spark-source.zip` contains the complete cleaned Spark source tree with all engine changes plus the POC package. See `SOURCE-ZIP-MANIFEST.md`. |
| 20 | Add a document explaining what is in which folder and what we added, written for Holden | Done | `GUIDE-FOR-HOLDEN.md` |
| 21 | Close the last open gap: run the packaged jar through real Spark SQL in one process | Done | `LiveSparkSqlProof`: both provider jars loaded from `spark.sql.catalog.*` configuration alone, native planner, real services, 25 of 25 checks, service request ids captured. Log `spark-implementation/live-spark-sql-proof.log` |
| 22 | Everything real: no emulated provider in the examples, no simulated evaluator, no mechanism microbenchmark; both modules proven against real services | Done | Removed the uppercase demo, the standalone evaluator, and the loopback microbenchmark from the package. Added live OpenAI-compatible cases against Amazon Bedrock's OpenAI-compatible API (bearer token from AWS's token generator), real model summaries over the example reviews (`proposal/examples/report/live-bedrock-summaries.csv`), and a real throughput benchmark on Bedrock through Spark SQL (5.79x, `spark-implementation/live-throughput-benchmark.log`). The only remaining in-process server is inside the conformance harness, for failure behaviors a vendor cannot produce on demand, and it drives the real module code over real HTTP |
| 23 | Add the related SPIP "Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads" and JIRA SPARK-61454 | Done | Linked [SPARK-61454](https://issues.apache.org/jira/browse/SPARK-61454) from the SPIP metadata and related-work sections. Added a side-by-side boundary table, explained the independent/complementary relationship, and documented the qualification needed before combining in-task async calls with cross-stage pipelining. |
| 24 | Add presentations, make the feature story clear, clean binaries, and package full Spark source | Done | Recommended feature deck: `presentation/SPARK-61672-Feature-Overview.pptx`/`.pdf` (15 slides). Technical class/file appendix: `SPARK-61672-POC-Review.pptx`/`.pdf` (27 slides). Added `START-HERE.md`, `preflight.ps1`, and `run-poc.ps1`; PowerShell syntax and preflight validated. Both ZIPs exclude jars, classes, targets, machine classpaths, tokens, probes, and private-key fixtures. |

## Defects the review loop found and fixed in the provider modules

Grouped by what they would have cost in production. Each is now covered by a conformance case.

Security:
- A session could set `credentialEnv` as a catalog option and have any executor environment variable (an AWS secret, a Spark auth secret) sent as a Bearer token to an allowlisted endpoint. Now cluster-only; the catalog option is rejected.
- Every limit except concurrency was session-overridable: rate limiter could be disabled, size cap raised to any value, retry budget set to a million. Now every limit has a cluster ceiling the session may only tighten.
- Loopback exemption was prefix-matched, so `http://127.0.0.1.attacker.example` passed the scheme check. Now host-exact.
- `DESCRIBE FUNCTION` and `canonicalName()` exposed the endpoint URL. Now a model name and a hash respectively.

Cost and correctness:
- The size cap was checked after the whole body was in memory; chunked responses bypassed the Content-Length pre-check entirely. Now enforced while streaming with a subscriber that cancels at the cap.
- Cancelling during a pacing wait or retry backoff still sent one more request with the prompt and token. Now cancellation stops every stage.
- A body-phase timeout completed the caller's future but did not abort the exchange (JDK `orTimeout` returns the same future, so cancelling it was a no-op). Now a timer cancels the send future itself, and the harness observes the server's write failing.
- `Retry-After` HTTP-dates were parsed but not honored (zone-relative comparison). Now compared on `Instant`.

Resilience:
- The circuit breaker counted cancellations and client-side 400s, so one killed task with 16 calls in flight opened the breaker for every task on the executor for 30 seconds. Now only endpoint-health failures count.
- The breaker had no real half-open state and late outcomes from before a trip could close it. Now a CAS single probe and generation-stamped admissions.
- The shared pacer let one session's low rate throttle every other user on the executor permanently. Now the cluster ceiling is shared and a session rate slows only its own catalog.
- Rejected pacer callers consumed their slot, so a burst drifted the schedule into the future and starved later callers while the endpoint sat idle; those rejections were also counted as endpoint failures. Now a CAS reservation that only commits on acceptance, and a non-health code.
- Clients were cached by tuning values, so a session could create unbounded HTTP clients and Netty event loops by varying options. Now keyed by connection identity only, per-call tuning passed in.
- Bedrock treated every `ValidationException` (including per-prompt "input too long") and every `SdkClientException` (including a connection reset) as fatal, so tolerant forms were not tolerant. Now row-level and transient respectively; only a missing model id is fatal.
- TLS handshake failures were retried and reported as timeouts. Now fatal.

Harness honesty:
- The cancellation test asserted `isCancelled` after calling `cancel()`, which is always true. Now asserts the server's write failed.
- The NULL-input test compared a counter that the success handler never incremented. Now uses a global request counter and first proves it moves.
- Shared breaker state leaked between tests and silently removed coverage. Now reset before every check.

## Code changes in the Spark tree (for the same git add)

Modified:

- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionResolution.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreePatterns.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala`
- `hadoop-cloud/pom.xml`

Added:

- `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/functions/AsyncScalarFunction.java`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ApplyAsyncFunctionExpression.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/asyncFunctionLogicalOperators.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/async/` (AsyncEvalExec, ExtractAsyncFunctions)
- `sql/core/src/test/scala/org/apache/spark/sql/execution/async/` (three suites)
- `hadoop-cloud/src/test/scala/org/apache/spark/sql/execution/async/BedrockLiveAsyncFunctionSuite.scala`

## Honest open items

- A run on a multi-node cluster rather than `local[2]`. The one-process run exercises the identical
  code path; a cluster adds executor-side classloading and credential-chain behavior worth observing.
- A full Pandas/Arrow/Spark comparison against the SPARK-61404 UDF (the live benchmark here measures
  serial versus async on the native operator, not the Python path).
- Live Databricks Model Serving remains untested because no approved credential was available. The
  OpenAI-compatible module is proven against a real OpenAI-compatible service with the same envelope.
- Spark external calls are at-least-once; retries can repeat billable calls.

## What is real and what is an in-process server, stated plainly

Real, against live services: the hadoop-cloud planner test (Bedrock); 7 conformance cases (4
OpenAI-compatible module against Bedrock's OpenAI-compatible API, 3 Bedrock module against
Converse); `LiveSparkSqlProof` (both modules through real Spark SQL, 25 checks, real summaries over
the example reviews); `LiveThroughputBenchmark` (5.79x on real Bedrock).

In-process servers, by design: the Spark unit suites (`AsyncFunctionEndToEndSuite`,
`AiQueryParitySuite`, `AsyncFunctionSafetySuite`) use a loopback HTTP provider, as every Spark test
does, because unit tests must not depend on the network or spend money. The remaining conformance
cases drive the real module code over real HTTP to an in-process server, because the behaviors
under test (an oversized 503 body, a `Retry-After` HTTP-date, a trickled body, a chunked over-cap
response, a 429 then 200) cannot be requested from a vendor on demand. Nothing else in the package
is emulated.

## How to reproduce the key results

Core suites (from the Spark repo root):

```powershell
$env:JAVA_HOME = 'C:\Program Files\Microsoft\jdk-17.0.17.10-hotspot'
$env:HADOOP_HOME = 'C:\path\to\hadoop'
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
mvn -pl sql/core -am test "-DwildcardSuites=org.apache.spark.sql.execution.async" "-Dtest=none"
```

Live Bedrock (opt-in, paid):

```powershell
$env:SPARK_TEST_LIVE_BEDROCK = '1'
$env:SPARK_TEST_BEDROCK_REGION = 'us-east-1'
$env:SPARK_TEST_BEDROCK_MODEL_ID = 'amazon.nova-micro-v1:0'
mvn -pl hadoop-cloud -am -Phadoop-cloud test `
  "-DwildcardSuites=org.apache.spark.sql.execution.async.BedrockLiveAsyncFunctionSuite" "-Dtest=none"
```

Both modules through real Spark SQL in one process, plus real summaries of the example reviews
(paid, a few short calls; adjust the absolute paths in the script for your machine):

```powershell
$env:AWS_PROFILE = '<approved-profile>'
powershell -File SPARK-61672/spark-implementation/live-spark-sql-proof.ps1
```

Real throughput benchmark on Bedrock, serial versus async (paid, about 50 short calls):

```powershell
powershell -File SPARK-61672/spark-implementation/live-throughput-benchmark.ps1
```
