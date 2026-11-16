# SPARK-61672 final proposal package

Read these files in order:

1. [`01-SPIP-incremental-for-Holden.md`](01-SPIP-incremental-for-Holden.md)  
   The proposed SPIP. It keeps Holden Karau and Prashant Singh's objective, replaces mechanisms that do not hold against Spark 4.3.0, and defines the provider-neutral design, risks, milestones, and acceptance tests.

2. [`02-three-proposal-comparison-and-evidence.md`](02-three-proposal-comparison-and-evidence.md)  
   The technical evidence. It compares the original SPARK-61672 draft, SPARK-61404 `ai_query`, and the native async implementation; lists defects found and fixed; records every changed file and test; and reports the real throughput benchmark on Amazon Bedrock.

3. [`03-layman-tutorial.md`](03-layman-tutorial.md)  
   A plain-language walkthrough: the problem, what the proposal solves, how the code works, how Bedrock/OpenAI/Claude adapters will plug in, what users configure, the value, tests, and limits.

4. [`examples/`](examples/)  
   Runnable examples: input data, per-provider SQL (Bedrock, OpenAI-compatible, Claude through Bedrock), and a results report from a real run including real model summaries of the example reviews.

5. [`../spark-implementation/`](../spark-implementation/)  
   The provider modules. `connector/ai-openai` and `connector/ai-bedrock` implement the native SPI with cluster-owned allowlists, ceilings, credential source, and breaker policy. Both poms build successfully; generated jars are intentionally excluded from this clean share package. A 71-case conformance harness (`conformance/`, log in `conformance.log`) drives them through their catalog path, 7 cases live against real services, and `LiveSparkSqlProof` runs both through real Spark SQL in one process.

6. [`diagrams/provider-neutral-ai-functions.svg`](diagrams/provider-neutral-ai-functions.svg)  
   The editable architecture and provider-switching visual. A PNG rendering is included for quick preview.

7. [`../presentation/SPARK-61672-Feature-Overview.pptx`](../presentation/SPARK-61672-Feature-Overview.pptx) and [`PDF`](../presentation/SPARK-61672-Feature-Overview.pdf)  
   The recommended 15-slide feature story: user experience, execution model, provider portability, controls, errors, real proof/output, performance, SPARK-61454, and the decision requested.

8. [`../presentation/SPARK-61672-POC-Review.pptx`](../presentation/SPARK-61672-POC-Review.pptx) and [`PDF`](../presentation/SPARK-61672-POC-Review.pdf)  
   The 27-slide technical appendix covering every changed folder/file/class, test helper, configuration key, evidence layer, package map, and build path.

## Related work

- [[SPIP] Break the Stage Barrier: Pipelined Spark SQL for Agentic Workloads](https://issues.apache.org/jira/browse/SPARK-61454) (SPARK-61454, Ferdinand Xu) is complementary and independent. It pipelines records across eligible shuffle stage boundaries; SPARK-61672 overlaps external calls inside a task. The relationship and future integration risks are documented in `01-SPIP-incremental-for-Holden.md`, `03-layman-tutorial.md`, `../ARCHITECTURE.md`, and the local summary `../related-work/SPARK-61454-break-stage-barrier.md`.
- SPARK-61404 `ai_query` is the parallel Python prototype compared in `02-three-proposal-comparison-and-evidence.md`.

## Evidence locations

| Evidence | Location |
| --- | --- |
| Final core test run | `../poc/final-core-test4.log` (25 passed, 0 failed, BUILD SUCCESS) |
| Live Spark SQL to Bedrock | `../poc/final-bedrock-live2.log` (1 passed, BUILD SUCCESS) |
| Earlier provider parity | `../poc/parity-test2.log` (12 passed) |
| Focused safety retest | `../poc/safety-test2.log` (8 passed) |
| Live throughput benchmark on real Bedrock | `../spark-implementation/live-throughput-benchmark.log` (5.79x, 24 of 24 correct, ordered) |
| Both modules through real Spark SQL in one process | `../spark-implementation/live-spark-sql-proof.log` (25 passed) |
| Provider module compile and jars | `../spark-implementation/mvn-package.log` (both standalone poms exit 0; generated jars intentionally excluded from the clean package) |
| Provider module conformance | `../spark-implementation/conformance.log` (71 passed, 0 failed, 7 live against real services) |
| Adversarial review rounds | `../semantic-review/2026-11-15-154400-*.md`, `-161323-*.md`, `-163455-*.md` (17, 12, 6 findings; all fixed) |
| Runnable examples and report | `examples/` (`examples/report/RESULTS.md`) |
| Original draft and research | Parent directory `../` |
| SPARK-61404 extracted PR reference | `../ai_query_ref/` |

## Important limits

- The provider modules (`../spark-implementation/`) are packaged, pass a 71-case conformance harness that drives their real catalog path (7 cases live against real services), run end to end through real Spark SQL in one process, and went through three adversarial review rounds. Two things remain before calling them production-certified: a run on a multi-node cluster, and a live Databricks endpoint run, which needs a credential that was not available.
- The throughput benchmark is a single sample on a developer machine against a real service; the number will move with load, the direction will not. A full Pandas/Arrow comparison remains a vote gate.
- Spark external calls are at-least-once. Task retry, stage retry, speculation, executor loss, and repeated actions can repeat billable calls.
- Endpoint/model allowlists remain a provider-module acceptance gate. The engine active-call ceiling is now a static SQL configuration that a session cannot raise.
- All source changes are local on the `v4.3.0` tag. Nothing was committed or pushed.
