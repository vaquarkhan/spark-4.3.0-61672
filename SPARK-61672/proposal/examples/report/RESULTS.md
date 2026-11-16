# SPARK-61672 examples: results report

Every result below came from a real run: real Spark SQL in a real `SparkSession`, the packaged
provider jars loaded through configuration alone, the native async planner path, and real model
services (Amazon Bedrock Converse, and Amazon Bedrock's OpenAI-compatible Chat Completions API).
Nothing in this report was emulated. All runs are local on the Spark 4.3.0 tag `v4.3.0`; nothing
was committed or pushed.

## 1. Both provider modules through real Spark SQL in one process

Program: `../../spark-implementation/conformance/LiveSparkSqlProof.scala`.
Log: `../../spark-implementation/live-spark-sql-proof.log`. 25 checks, 25 passed.

Spark was configured only with what an administrator would pass to `spark-sql --conf`:

```text
spark.sql.catalog.bedrock = org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog
spark.sql.catalog.bedrock.region = us-east-1
spark.sql.catalog.bedrock.modelId = amazon.nova-micro-v1:0
spark.ai.bedrock.allowedModels = amazon.nova-micro-v1:0
spark.ai.bedrock.allowedRegions = us-east-1

spark.sql.catalog.oai = org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog
spark.sql.catalog.oai.baseUrl = https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1
spark.sql.catalog.oai.chatModel = qwen.qwen3-32b-v1:0
spark.ai.openai.allowedBaseUrls = https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1
spark.ai.openai.allowedModels = qwen.qwen3-32b-v1:0
spark.ai.openai.credentialEnv = AI_PROVIDER_TOKEN

spark.sql.asyncFunction.maxInFlightCeiling = 2
```

For each catalog the program proved the same seven things:

| Check | Bedrock module | OpenAI-compatible module |
| --- | --- | --- |
| Optimized plan contains `EvalAsyncFunctions` | yes | yes |
| Executed plan contains `AsyncEval` | yes | yes |
| `generate('Reply with exactly A')` returned | `A` | `A` |
| `generate('Reply with exactly B')` returned | `B` | `B` |
| NULL prompt stayed NULL with no call | yes | yes |
| `generate_status` request id (service-minted) | `191c242a-11e5-41c2-964f-289f4ef6a5f5` | `ff08cf3c-fc68-4712-b9b5-af075bec46a7` |
| `WHERE ordinal = 1 LIMIT 1` placed below the async node, returned only `B` | yes | yes |

The executed plan, for the record:

```text
AdaptiveSparkPlan isFinalPlan=false
+- Sort [ordinal#5 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(ordinal#5 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS
      +- Project [ordinal#5, asyncFunc0#8 AS answer#2]
         +- AsyncEval [generate(prompt#6)], [asyncFunc0#8]
            +- LocalTableScan [ordinal#5, prompt#6]
```

## 2. Real summaries over the example reviews

Same program, same session. `bedrock.try_generate(CONCAT('Summarize in at most six words: ',
review))` over `data/reviews.csv` with `WHERE stars >= 3`. Output in `live-bedrock-summaries.csv`.

```text
  101  5  Long battery, easy setup.             | Battery lasted all day and setup was simple.
  103  5  Fast, clear guidance.                 | Clear instructions and fast performance.
  104  5  NULL                                  | (NULL review, no call)
  105  3  Loud screen, quiet speaker.           | The screen is bright but the speaker is weak.
  106  5  Great value, exceeded expectations.   | Exceeded my expectations for the price.
  108  4  Easy-grip, easy-clean design          | Comfortable to hold and easy to clean.
  109  4  NULL                                  | (NULL review, no call)
  110  5  Compatible with my current gear.      | Works well with my existing accessories.
```

Eight rows returned, six real summaries, two NULLs with no call, two rows (102, 107) filtered below
the async node and never billed.

## 3. Real throughput benchmark: serial versus async on real Bedrock

Program: `../../spark-implementation/conformance/LiveThroughputBenchmark.scala`.
Log: `../../spark-implementation/live-throughput-benchmark.log`.

The same query, `SELECT id, bedrock.generate(prompt) FROM p`, over 24 prompts in one task against
real Amazon Nova Micro, run twice with only the in-flight limit changed. One warm-up query per
SparkContext is excluded from timing.

| In flight | Wall time | Rows per second | Correct answers | Output in input order |
| ---: | ---: | ---: | --- | --- |
| 1 (serial, what a plain UDF does) | 8,200 ms | 2.93 | 24 of 24 | yes |
| 8 (native async operator) | 1,416 ms | 16.95 | 24 of 24 | yes |

**Speedup: 5.79x**, with every answer correct and input order preserved in both runs. This is the
proposal's central claim measured on a real service with real latency variance, not a mechanism
microbenchmark. A single sample on a developer machine; the number will move with network and
service load, the direction will not.

## 4. Provider module conformance

Harness: `../../spark-implementation/conformance/ProviderConformanceHarness.scala`.
Log: `../../spark-implementation/conformance.log`. 71 passed, 0 failed.

The harness exercises the packaged jars through their real catalog path. Seven of its cases are
live against real services (four through the OpenAI-compatible module against Bedrock's
OpenAI-compatible API, three through the Bedrock module against Converse). The remaining cases
drive the modules against an in-process HTTP server so that failure behaviors a real vendor will
not produce on demand (a 70 KB 503 body, a `Retry-After` HTTP-date, a trickled body, a chunked
response over the size cap, a 429 then 200) can be exercised deterministically. Those cases test
the real module code against real HTTP; only the far end is local.

## 5. Adversarial review

The modules went through three independent adversarial review rounds in a loop. Round one found 17
issues (five high, including a session-settable credential name that could have sent an executor
secret to an endpoint). Round two found 12, two high, both introduced by round-one fixes. Round
three found 6, none high. All 35 were fixed and re-verified; the conformance suite grew from 42 to
71 cases as each gap became a test. Reviews: `../semantic-review/`.

## 6. What is still open

- A run on a multi-node cluster rather than `local[2]`. The one-process run exercises the identical
  code path; a cluster adds executor-side classloading and credential-chain behavior worth observing
  once.
- A live Databricks Model Serving endpoint. The OpenAI-compatible module is proven against a real
  OpenAI-compatible service (Bedrock's), and the Databricks response envelope is the same shape, but
  no Databricks credential was available.
- Spark external calls are at-least-once. A task retry, stage retry, speculation, or executor loss
  can repeat billable calls.
