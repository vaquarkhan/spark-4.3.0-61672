# SPARK-61672 provider modules

Two provider modules built on the SPARK-61672 `AsyncScalarFunction` SPI. They live outside Spark
core: each is a separate artifact that an administrator drops onto the classpath and wires with
configuration. Spark core never depends on any cloud SDK.

| Module | Catalog class | Generated jar (after `mvn package`) | Brings |
| --- | --- | --- | --- |
| `connector/ai-openai` | `org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog` | `connector/ai-openai/target/spark-ai-openai_2.13-4.3.0.jar` | JDK HTTP client, Jackson, streaming size cap, pacer, circuit breaker |
| `connector/ai-bedrock` | `org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog` | `connector/ai-bedrock/target/spark-ai-bedrock_2.13-4.3.0.jar` | AWS SDK v2 `BedrockRuntimeAsyncClient`, circuit breaker |

Each module exposes the same three functions, so the business SQL is identical across providers:

- `ai.generate(prompt)` strict: a terminal provider error fails the task.
- `ai.try_generate(prompt)` tolerant: a row-level provider error returns SQL NULL.
- `ai.generate_status(prompt)` observable: returns a struct with status, result, error code, safe
  message, attempts (NULL for Bedrock, whose SDK retries internally), and request id.

## Trust model

There are two kinds of settings and the difference is the whole security story.

**Catalog options** (`spark.sql.catalog.<name>.*`) are session-settable. They choose the endpoint
and model from the allowlist and may TIGHTEN any limit. They can never raise a limit, name an
unlisted endpoint or model, choose the credential source, or change the breaker policy.

**Cluster settings** (`spark.ai.<provider>.*` and `spark.sql.asyncFunction.maxInFlightCeiling`)
are read from the immutable `SparkEnv` configuration set at launch. They are the allowlists, the
ceilings, the credential source, and the breaker policy. When the administrator sets no ceiling
for a limit, the module default acts as the ceiling.

Fatal conditions (authentication, configuration, TLS failure, a model id that does not exist)
fail the task even in the tolerant forms. Row-level conditions (bad request, schema, oversized
response, timeout after retries) follow the form: fail, NULL, or status row. A tripped circuit
breaker is reported in `generate_status`, returns NULL in `try_generate`, and fails `generate`.

### OpenAI-compatible

```text
# catalog options
spark.sql.catalog.ai = org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog
spark.sql.catalog.ai.baseUrl = https://approved.example.com/v1
spark.sql.catalog.ai.chatModel = approved-model

# cluster settings
spark.ai.openai.allowedBaseUrls = https://approved.example.com/v1
spark.ai.openai.allowedModels = approved-model
spark.ai.openai.credentialEnv = AI_PROVIDER_TOKEN
spark.ai.openai.maxResponseBytes = 16777216
spark.ai.openai.ratePermitsPerSec = 50
spark.ai.openai.maxAttempts = 4
spark.ai.openai.maxElapsedMs = 60000
spark.ai.openai.requestTimeoutMs = 30000
spark.ai.openai.breakerFailureThreshold = 10
spark.ai.openai.breakerResetMs = 30000
spark.sql.asyncFunction.maxInFlightCeiling = 16
```

Catalog options a session may set to tighten: `maxInFlight`, `maxAttempts`, `maxElapsedMs`,
`requestTimeoutMs`, `maxResponseBytes`, `ratePermitsPerSec`. A session rate slows only that
catalog; the cluster rate is shared per endpoint and cannot be lifted. `credentialEnv` as a catalog
option is rejected.

### Amazon Bedrock

```text
# catalog options
spark.sql.catalog.ai = org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog
spark.sql.catalog.ai.region = us-east-1
spark.sql.catalog.ai.modelId = amazon.nova-micro-v1:0

# cluster settings
spark.ai.bedrock.allowedModels = amazon.nova-micro-v1:0,anthropic.claude-sonnet-4-5
spark.ai.bedrock.allowedRegions = us-east-1
spark.ai.bedrock.maxTokens = 1024
spark.ai.bedrock.apiCallTimeoutMs = 25000
spark.ai.bedrock.apiCallAttemptTimeoutMs = 20000
spark.ai.bedrock.breakerFailureThreshold = 10
spark.ai.bedrock.breakerResetMs = 30000
spark.sql.asyncFunction.maxInFlightCeiling = 16
```

Catalog options a session may set to tighten: `maxInFlight`, `maxTokens`, `temperature`,
`apiCallTimeoutMs`, `apiCallAttemptTimeoutMs`. Credentials come from the AWS default chain on each
executor. Switching to Claude is a one-line model change; the SQL does not change.

## Client behavior

- Requests and responses go through Jackson, so any prompt is escaped correctly.
- Responses may be gzip-encoded. The decoded size cap is enforced while the body streams in, so an
  oversized or chunked response is aborted at the cap, not buffered.
- `Retry-After` is honored in delay-seconds and HTTP-date forms. Retries stop at `maxAttempts` or
  at the absolute `maxElapsedMs` deadline, which includes any pacing wait.
- Each attempt is bounded end to end (headers and body) and the exchange is aborted when the
  budget expires.
- Cancelling the engine future aborts the in-flight request and stops a pending retry or pacing
  wait from sending another one.
- One client per connection identity (base URL plus credential source; region for Bedrock), so a
  session cannot create clients or threads by varying options.
- A per-endpoint circuit breaker with a real half-open probe and generation-stamped admissions
  counts only endpoint-health failures (timeouts, 5xx, throttling). Cancellation and per-row errors
  never open it, and a late outcome from before a trip cannot close it.
- Only sanitized error codes surface. Prompts, tokens, endpoints, and provider bodies never appear
  in a row-level error, log line, or plan produced by these modules.

## Build

The modules compile against the SPARK-61672 `AsyncScalarFunction` SPI, which exists only in this
Spark 4.3.0 build. From the Spark repo root, install those artifacts once:

```bash
build/mvn -pl sql/catalyst,sql/core -am install -DskipTests
```

Then build a module jar:

```bash
cd SPARK-61672/spark-implementation/connector/ai-openai
mvn -q package
```

`compile-check.ps1` records how the modules were compiled and conformance-tested on the author's
machine. `mvn-package.log` records the real standalone Maven builds. Generated jars are deliberately
excluded from the clean share package; run `mvn package` in each connector folder to recreate them.

## Validation

| Check | Result | Log |
| --- | --- | --- |
| Compile both modules against the native SPI | exit 0 | `compile-check.log` |
| Package jars | Both module poms completed with exit 0; jars regenerated under each connector's `target/` and then removed from this clean share package | `mvn-package.log` |
| Conformance harness against the packaged jars | 71 passed, 0 failed | `conformance.log` |
| Live, OpenAI-compatible module against Amazon Bedrock's OpenAI-compatible API | 4 of the 71, all passed | `conformance.log` |
| Live, Bedrock module against Converse through `BedrockFunctionCatalog` | 3 of the 71, all passed | `conformance.log` |
| Both modules through real Spark SQL in one process, real services | 25 passed, 0 failed | `live-spark-sql-proof.log` |
| Throughput on real Bedrock through real Spark SQL, in-flight 1 versus 8 | 8,200 ms versus 1,416 ms, 5.79x, 24 of 24 correct, ordered | `live-throughput-benchmark.log` |
| Adversarial review loop | 3 automated rounds found 35 issues; all subsequently fixed and covered by passing tests (round 3: 0 high, 3 medium, 3 low) | `../semantic-review/` |

### The three programs under `conformance/`

`ProviderConformanceHarness.scala` drives the packaged jars through the real catalog path
(`initialize` with cluster allowlists, `loadFunction`, `bind`, `produceResultAsync`). Seven cases
are live against real services. The remaining cases drive the real module code over real HTTP to
an in-process server, because failure behaviors a vendor will not produce on demand (an oversized
503 body, a `Retry-After` HTTP-date, a trickled body, a chunked response over the cap, a 429 then
200) have to be exercised deterministically; only the far end is local. Set
`SPARK_TEST_LIVE_BEDROCK=1`, and `SPARK_TEST_LIVE_OPENAI_BASE_URL` plus `AI_PROVIDER_TOKEN`, to
include the live cases.

`LiveSparkSqlProof.scala` starts a real `SparkSession` with both provider jars configured only
through `spark.sql.catalog.*` and `spark.ai.*`, runs SQL through the native planner against real
services, prints the plans and the service request ids, and summarizes the example reviews with a
real model. `live-spark-sql-proof.ps1` runs it.

`LiveThroughputBenchmark.scala` runs the same Spark SQL over the same prompts against real Bedrock
with 1 call in flight and then 8, and reports wall time, correctness, and ordering.
`live-throughput-benchmark.ps1` runs it.

For the live OpenAI-compatible cases the run here used Amazon Bedrock's OpenAI-compatible Chat
Completions API (`https://bedrock-runtime.<region>.amazonaws.com/openai/v1`) with a 12-hour bearer
key minted by AWS's `aws-bedrock-token-generator`. No IAM resource was created and the key is read
from a local file at run time; it never appears in source or configuration.

Still open before calling either module production-certified: a run on a multi-node cluster rather
than `local[2]`, and a live Databricks Model Serving endpoint run (no approved credential was
available; the module is proven against a real OpenAI-compatible service with the same envelope).
