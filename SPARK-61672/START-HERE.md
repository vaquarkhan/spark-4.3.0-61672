# Start here: run the SPARK-61672 POC

This guide assumes you are new to this code. Follow it in order. Do not start with the proposal or
the test logs.

## What you will see

The POC adds a real Spark SQL function engine for slow external calls. When it runs successfully,
Spark SQL will load a provider catalog from configuration, plan an `AsyncEval` physical operator,
call a real model (if you select the live path), preserve input order, skip NULL and filtered rows,
and print the service request ID.

Use the full source archive:

```text
SPARK-61672-full-spark-source.zip
```

The smaller review ZIP does not contain the Spark engine source and cannot run independently.

## Choose your path

| Path | AWS required? | Makes paid calls? | What it proves |
| --- | --- | --- | --- |
| A. Build + engine tests | No | No | The modified Spark engine compiles and all 25 feature cases pass. |
| B. Real Bedrock run | Yes | Yes, a few short calls | Real Spark SQL loads the packaged provider, plans `AsyncEval`, and calls a real model. |
| C. OpenAI-compatible live run too | Yes, plus a compatible bearer token | Yes | The second provider module calls a real OpenAI-compatible endpoint through the same engine. |

Start with A. Add B or C only after A passes.

## Before you begin

1. Extract the full source ZIP. It creates:

   ```text
   spark-4.3.0-SPARK-61672/
   ```

2. Install JDK 17. Confirm:

   ```powershell
   java -version
   ```

   The first line must contain `17`.

3. Install Maven 3.9 or later. Confirm:

   ```powershell
   mvn -version
   ```

4. On Windows, put a compatible `winutils.exe` and `hadoop.dll` under a Hadoop directory such as
   `C:\tools\hadoop\bin`, then set `HADOOP_HOME` to `C:\tools\hadoop`.

5. For a live Bedrock run, choose an AWS profile, role, workload identity, or instance role that has
   only `bedrock:InvokeModel` for the model you plan to call. This POC never selects an account or
   profile for you and contains no key.

## Windows: copy-paste path

Open PowerShell in `spark-4.3.0-SPARK-61672`.

Set your machine paths (examples only):

```powershell
$env:SPARK_61672_JAVA_HOME = 'C:\path\to\jdk-17'
$env:SPARK_61672_MAVEN = 'C:\path\to\mvn.cmd'
$env:SPARK_61672_HADOOP_HOME = 'C:\path\to\hadoop'
```

Run the read-only preflight:

```powershell
powershell -ExecutionPolicy Bypass -File SPARK-61672\preflight.ps1
```

Expected last line:

```text
RESULT: READY FOR THE NEXT STEP
```

Warnings that Spark and provider classes are not built yet are normal immediately after extracting
the clean ZIP.

### Path A: build and run all engine tests, no AWS and no cost

```powershell
powershell -ExecutionPolicy Bypass -File SPARK-61672\run-poc.ps1
```

The first Spark build is large. It took 2 hours 8 minutes on the author's machine; allow roughly 1
to 2.5 hours. It compiles Spark, builds both provider modules, and runs all 25 engine cases. Later
runs reuse Maven/compiler output.

Expected final line:

```text
RESULT: SPARK-61672 POC BUILD AND SELECTED VALIDATION PASSED
```

### Path B: include the real Bedrock proof

First select your AWS identity. This is your choice, not hardcoded by the POC:

```powershell
$env:AWS_PROFILE = '<profile-selected-by-you>'
$env:SPARK_TEST_BEDROCK_REGION = 'us-east-1'
$env:SPARK_TEST_BEDROCK_MODEL_ID = 'amazon.nova-micro-v1:0'
```

Read-only AWS preflight:

```powershell
powershell -ExecutionPolicy Bypass -File SPARK-61672\preflight.ps1 -CheckAws
```

Then run without rebuilding Spark:

```powershell
powershell -ExecutionPolicy Bypass -File SPARK-61672\run-poc.ps1 `
  -SkipSparkBuild -SkipEngineTests -Live
```

This makes paid calls. The proof uses short prompts and small output limits. Expected evidence:

```text
optimized plan contains EvalAsyncFunctions
executed plan contains AsyncEval
answer=A
answer=B
NULL prompt stays NULL
service request id present
RESULT: ALL CHECKS PASSED
```

Output files:

```text
SPARK-61672\spark-implementation\live-spark-sql-proof.log
SPARK-61672\proposal\examples\report\live-bedrock-summaries.csv
```

### Path C: include the OpenAI-compatible provider

The Bedrock provider uses the AWS default credential chain. The OpenAI-compatible provider also
needs an approved endpoint, model, and short-lived bearer token:

```powershell
$env:SPARK_TEST_LIVE_OPENAI_BASE_URL = 'https://approved.example/v1'
$env:SPARK_TEST_LIVE_OPENAI_MODEL = 'approved-model'
$env:AI_PROVIDER_TOKEN = '<short-lived-token>'
```

Then rerun Path B. If those variables are absent, the OpenAI-compatible half is skipped explicitly;
the Bedrock half still runs.

## Linux or macOS

The source and poms are portable. Build Spark and providers:

```bash
./build/mvn -pl sql/catalyst,sql/core -am install -DskipTests
mvn -f SPARK-61672/spark-implementation/connector/ai-openai/pom.xml package
mvn -f SPARK-61672/spark-implementation/connector/ai-bedrock/pom.xml package
./build/mvn -pl sql/core test \
  -DwildcardSuites=org.apache.spark.sql.execution.async -Dtest=none
```

Then use the `spark-sql` command in `BUILD-AND-RUN.md`. The convenience proof scripts are
PowerShell; the SQL/catalog configuration itself is platform-independent.

## Common errors and exact fixes

| Error or symptom | Cause | Fix |
| --- | --- | --- |
| `java` not found or wrong version | JDK missing/wrong | Set `SPARK_61672_JAVA_HOME` to JDK 17. |
| `mvn` not found | Maven not on `PATH` | Set `SPARK_61672_MAVEN` to the full `mvn.cmd`/`mvn` path. |
| `Could not find artifact spark-catalyst_2.13:4.3.0` | Modified Spark artifacts not installed | Run Path A step 1; do not skip the Spark build. |
| `winutils.exe` or `HADOOP_HOME` error on Windows | Native Hadoop helper missing | Put `winutils.exe` and `hadoop.dll` in `HADOOP_HOME\bin`; rerun preflight. |
| `AccessDeniedException` | Identity cannot invoke model | Grant only `bedrock:InvokeModel` for the chosen model, or select the correct role/profile. |
| `ResourceNotFoundException` / `BAD_MODEL` | Model unavailable in region/account | Set an allowed model available to your identity and update the allowlist. |
| OpenAI-compatible catalog skipped | Endpoint/token variables absent | Set all three variables listed in Path C. |
| PowerShell treats a `-D...` as a Maven phase | Unquoted dotted Maven property | Quote each argument: `'-Dfoo.bar=value'`. |
| Script says provider jar missing | Clean ZIP intentionally has no jars | Run both provider `mvn package` commands. |
| Build seems idle | First Spark compile is large | Check the Maven process/log; the first validated build took 2 h 8 min. Do not run two Spark builds concurrently. |

## What is and is not real

Real: the engine source, both provider implementations, standalone Maven builds, real Spark SQL
plans, real model calls, service request IDs, real review summaries, and the 5.79x real Bedrock
benchmark.

Local test endpoints remain only where a deterministic failure must be created (429 then success,
trickled body, oversized response). They drive the real provider client over real HTTP; they are not
used as evidence that a model was called. The seven live conformance cases and the one-process proof
are separate and call real services.

## More detail

- `README.md`: full change/class/test inventory.
- `BUILD-AND-RUN.md`: detailed commands and `spark-sql` launch.
- `proposal/03-layman-tutorial.md`: how the feature works in plain language.
- `presentation/SPARK-61672-Feature-Overview.pdf`: short feature story.
- `presentation/SPARK-61672-POC-Review.pdf`: technical file/class appendix.
