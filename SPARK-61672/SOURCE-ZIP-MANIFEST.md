# Full source ZIP manifest

The full source archive is `SPARK-61672-full-spark-source.zip`, created next to the
`spark-4.3.0` checkout. It contains the Apache Spark 4.3.0 source tree, all SPARK-61672 engine
changes in their normal Spark folders, and the full `SPARK-61672/` review/POC package.

## Included

- The complete Spark source tree needed to build Spark.
- All eight modified Spark files (`FunctionResolution.scala`, `Optimizer.scala`,
  `TreePatterns.scala`, `SQLConf.scala`, `SparkOptimizer.scala`, `SparkPlanner.scala`,
  `SparkStrategies.scala`, `hadoop-cloud/pom.xml`).
- All new engine files (`AsyncScalarFunction.java`, `ApplyAsyncFunctionExpression.scala`,
  `asyncFunctionLogicalOperators.scala`, `AsyncEvalExec.scala`, `ExtractAsyncFunctions.scala`).
- All new engine and live test suites.
- Provider module source and Maven poms.
- Conformance, live Spark SQL, and live throughput benchmark source.
- Proposal documents, architecture, related work, examples and real outputs, evidence logs,
  review records, PowerPoint, PDF, and generation/verification scripts.

## Excluded to keep the archive clean and small

- `.git/` and editor metadata.
- Every `target/`, `out/`, `node_modules/`, Python cache, and test cache directory.
- Every generated `.jar`, `.class`, `.zip`, `.args`, `.pyc`, `.pyo`, and temporary file.
- Machine-specific runtime classpath files.
- Token, probe, credential, and Office lock files.
- Private-key test fixtures from the upstream Apache Spark tree, including source files that embed
  a `BEGIN PRIVATE KEY` block. These are upstream synthetic test fixtures, not user credentials,
  but they are excluded because this archive promises no key material. Main Spark and SPARK-61672
  build/run paths do not use them. A few unrelated SSL/Kubernetes tests that require those fixtures
  will not be runnable from this sanitized archive.
- Logs outside `SPARK-61672/`. The package evidence logs are retained.

## Credential scan

Before packaging, the SPARK-61672 source/package was scanned for:

- AWS access-key prefixes (`AKIA`, `ASIA` patterns),
- Bedrock bearer-token patterns,
- PEM/private-key headers,
- hardcoded AWS profile/account defaults,
- the local AWS account identifier used during validation.

The clean SPARK-61672 package contained no match. The full source packager independently rejects
any file containing a private-key header or a credential-value pattern and records the skipped path
without writing its contents.

Credentials are always caller-supplied at runtime through the AWS default credential chain or the
`AI_PROVIDER_TOKEN` environment variable. No credential value is present in source, docs, logs,
PPTX, PDF, or either ZIP.
