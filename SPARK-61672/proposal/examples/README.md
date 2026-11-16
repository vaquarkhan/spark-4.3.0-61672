# SPARK-61672 examples

Runnable examples for the native AI functions proposal. Everything here was run for real: real
Spark SQL, the packaged provider jars loaded only through configuration, the native async planner,
and real model services. There is no emulated provider in this folder.

## What is here

| Path | What it is |
| --- | --- |
| `data/reviews.csv`, `data/reviews.jsonl` | Ten short product reviews used as input, two with a NULL review body. |
| `sql/01-bedrock.sql` | Full `spark-sql` launch configuration and SQL for the Amazon Bedrock provider. |
| `sql/02-openai.sql` | Same SQL for an OpenAI-compatible endpoint; only the launch configuration differs. |
| `sql/03-claude-via-bedrock.sql` | Claude through Bedrock: a one-line model change, identical SQL. |
| `report/RESULTS.md` | What ran, the evidence, and the measured results. |
| `report/live-bedrock-summaries.csv` | Real model summaries of the reviews, produced by Spark SQL calling Amazon Bedrock through the packaged module. |

## The real run

`../../spark-implementation/conformance/LiveSparkSqlProof.scala` is the program that produced the
results. In one JVM it:

1. Starts a `SparkSession` with `spark.sql.catalog.bedrock=...BedrockFunctionCatalog` and
   `spark.sql.catalog.oai=...OpenAIFunctionCatalog`, plus the `spark.ai.*` cluster allowlists and
   ceilings. Nothing else: no test catalog, no test function, the jars are found on the classpath
   the way `--jars` would place them.
2. Runs `SELECT ordinal, bedrock.generate(prompt) ...` and `SELECT ordinal, oai.generate(prompt) ...`
   and prints the optimized and executed plans (`EvalAsyncFunctions`, `AsyncEval`), the rows, and
   the service-minted request ids.
3. Shows `WHERE` and `LIMIT` placed below the async node, so filtered rows never reach a model.
4. Reads `data/reviews.csv`, runs `bedrock.try_generate(CONCAT('Summarize in at most six words: ',
   review))` over it, and writes the real summaries to `report/live-bedrock-summaries.csv`.

Run it yourself (paid, a few short calls) with an AWS identity that has `bedrock:InvokeModel`:

```powershell
# from the Spark repo root, after building sql/core
$env:AWS_PROFILE = '<approved-profile>'
powershell -File SPARK-61672/spark-implementation/live-spark-sql-proof.ps1
```

The script's absolute paths are the author's; adjust `$sparkBase`, `$m2`, and `$root` for your
machine. For the `oai` catalog it also expects a bearer token in `AI_PROVIDER_TOKEN`; the run here
used Amazon Bedrock's OpenAI-compatible Chat Completions API with a short-lived key from AWS's
`aws-bedrock-token-generator`, so both modules were proven against real services with one account.

## What the reviews run produced

| id | stars | Review | Real model summary |
| ---: | ---: | --- | --- |
| 101 | 5 | Battery lasted all day and setup was simple. | Long battery, easy setup. |
| 103 | 5 | Clear instructions and fast performance. | Fast, clear guidance. |
| 104 | 5 | NULL | NULL, no call made |
| 105 | 3 | The screen is bright but the speaker is weak. | Loud screen, quiet speaker. |
| 106 | 5 | Exceeded my expectations for the price. | Great value, exceeded expectations. |
| 108 | 4 | Comfortable to hold and easy to clean. | Easy-grip, easy-clean design |
| 109 | 4 | NULL | NULL, no call made |
| 110 | 5 | Works well with my existing accessories. | Compatible with my current gear. |

Rows 102 and 107 (stars below 3) were removed by the `WHERE stars >= 3` filter below the async
node and were never sent to the model. The summary text will differ run to run; the row count, the
NULL behavior, and which rows are billed will not.
