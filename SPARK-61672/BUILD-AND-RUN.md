# Build and run this independently

This is the detailed path for someone who wants to build this Spark checkout with the SPARK-61672
changes and run an AI function against a real model. If this is your first time, begin with
[`START-HERE.md`](START-HERE.md), which adds automated preflight, expected output, and
troubleshooting.

You need: JDK 17, Maven 3.9 or newer (or the bundled `build/mvn`), an AWS identity with
`bedrock:InvokeModel` on at least one model for the optional paid run, and roughly 1 to 2.5 hours
for the first Spark build. The validated Windows build took 2 hours 8 minutes. On Windows you also
need `winutils.exe` and `hadoop.dll` for your Hadoop version under `HADOOP_HOME\bin`.

## 1. Build Spark with the engine changes

From the repository root (this checkout, tag `v4.3.0` plus the SPARK-61672 working-tree changes):

```bash
./build/mvn -pl sql/catalyst,sql/core -am install -DskipTests
```

This compiles the engine (the new `AsyncScalarFunction` SPI, `AsyncEvalExec`, the extraction
rules, the optimizer and config changes) and installs the Spark artifacts into your local Maven
repository so the provider modules can depend on them. Status: executed on the author's machine;
see `poc/install-for-modules.log`.

To run the engine's own tests for the feature (no network, no cost):

```bash
./build/mvn -pl sql/core -am test -DwildcardSuites=org.apache.spark.sql.execution.async -Dtest=none
```

Expected: `Tests: succeeded 25, failed 0`. Status: executed; see `poc/final-core-test4.log`.

## 2. Build the provider modules

```bash
cd SPARK-61672/spark-implementation/connector/ai-bedrock && mvn -q package
cd ../ai-openai && mvn -q package
```

Each produces `target/spark-ai-<provider>_2.13-4.3.0.jar`. The Bedrock jar is thin; at run time
the AWS SDK must also be on the classpath (`software.amazon.awssdk:bedrockruntime` and
`netty-nio-client`, version 2.35.4, or the single `bundle` artifact). The OpenAI jar needs nothing
beyond Spark itself. Status: executed; see `spark-implementation/mvn-package.log`.

Generated jars are deliberately not included in the clean share package. Build them from the poms
above so the bytecode matches your Spark and Scala environment.

## 3. Run a query from spark-sql

Build a distribution or use an existing Spark 4.3.0 launcher with this checkout's jars on the
classpath. The simplest form, from the repository root after step 1:

```bash
bin/spark-sql \
  --jars SPARK-61672/spark-implementation/connector/ai-bedrock/target/spark-ai-bedrock_2.13-4.3.0.jar \
  --packages software.amazon.awssdk:bedrockruntime:2.35.4,software.amazon.awssdk:netty-nio-client:2.35.4 \
  --conf spark.sql.catalog.ai=org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog \
  --conf spark.sql.catalog.ai.region=us-east-1 \
  --conf spark.sql.catalog.ai.modelId=amazon.nova-micro-v1:0 \
  --conf spark.ai.bedrock.allowedModels=amazon.nova-micro-v1:0 \
  --conf spark.ai.bedrock.allowedRegions=us-east-1 \
  --conf spark.sql.asyncFunction.maxInFlightCeiling=16
```

Then:

```sql
SELECT ai.generate('Reply with exactly the word READY') AS answer;
```

Status: the `bin/spark-sql` launcher itself was NOT run on the author's Windows machine (the
launcher scripts need a built distribution and a POSIX shell). The identical configuration was run
through a `SparkSession` in one JVM by `spark-implementation/conformance/LiveSparkSqlProof.scala`,
which is the same code path Spark takes from `spark-sql`: `Catalogs.load` instantiates the catalog
from `spark.sql.catalog.ai`, the analyzer resolves `ai.generate`, the planner emits `AsyncEval`,
the executor calls the module. Log: `spark-implementation/live-spark-sql-proof.log`. The plans and
the service request ids in that log are what you will see from `spark-sql`.

For the OpenAI-compatible module, replace the catalog class and settings per
`spark-implementation/README.md`, and put the bearer token in the executor environment variable
named by `spark.ai.openai.credentialEnv`.

## 4. Run the full example

`proposal/examples/sql/01-bedrock.sql` has a complete session: a view over sample reviews, the
strict form, the tolerant form, and the status form. `proposal/examples/report/RESULTS.md` shows
what it produced on the author's run, including the real summaries.

## 5. What to expect

- `ai.generate` fails the task on an authentication, configuration, TLS, or unknown-model error, and
  on a terminal provider error. `ai.try_generate` returns NULL for row-level errors but still fails
  the task on the fatal ones. `ai.generate_status` returns a struct with the code instead.
- A `WHERE` on ordinary columns and a `LIMIT` are applied below the AI call, so filtered rows are
  never sent to the model.
- Streaming is out of v1 scope and has not been validated. Do not use an AI function in a
  streaming query in this POC.
- Calls are at-least-once. A task retry, speculation, or executor loss repeats the partition's calls.

## 6. Regression status of the engine changes

The engine changes touch shared optimizer and planner code. Beyond the 25 feature tests, the author ran 314 existing Catalyst optimizer regression tests,
all of which passed. A broader SQL regression run was stopped because `SQLQuerySuite` made it much
larger than intended; it is not claimed as passed. See `poc/regression-targeted.log` and
`REVIEW-CHECKLIST.md`.
