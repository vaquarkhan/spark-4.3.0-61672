# SPARK-61672: real Spark SQL + both packaged provider jars (configured only through
# spark.sql.catalog.* and spark.ai.*) + the native async planner + real model services, in ONE JVM.
# Mirrors a spark-sql --jars ... --conf ... launch. Paid: a few short prompts.
# Paths come from env.ps1 (override with SPARK_61672_* environment variables).
# Writes live-spark-sql-proof.log next to this script and the real summaries into
# proposal/examples/report/live-bedrock-summaries.csv.

$ErrorActionPreference = 'Continue'
. (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'env.ps1')
# Credentials are caller-controlled through the AWS default chain. This script never selects a
# profile or account.

$log = Join-Path $Root 'live-spark-sql-proof.log'
Remove-Item $log -ErrorAction SilentlyContinue

# OpenAI-compatible catalog against Bedrock's OpenAI-compatible API, if a bearer key is present.
$bearerFile = Join-Path $Root 'conformance\target\bedrock-bearer.txt'
if ((Test-Path $bearerFile) -and -not $env:SPARK_TEST_LIVE_OPENAI_BASE_URL) {
  $env:AI_PROVIDER_TOKEN = (Get-Content $bearerFile -Raw).Trim()
  $env:SPARK_TEST_LIVE_OPENAI_BASE_URL = "https://bedrock-runtime.$($env:SPARK_TEST_BEDROCK_REGION).amazonaws.com/openai/v1"
  if (-not $env:SPARK_TEST_LIVE_OPENAI_MODEL) { $env:SPARK_TEST_LIVE_OPENAI_MODEL = 'qwen.qwen3-32b-v1:0' }
}
# Real summaries over the example reviews.
$examples = Join-Path (Split-Path -Parent $Root) 'proposal\examples'
$env:SPARK_61672_REVIEWS_CSV = Join-Path $examples 'data\reviews.csv'
$env:SPARK_61672_REVIEWS_OUT = Join-Path $examples 'report\live-bedrock-summaries.csv'

$out = Join-Path $Root 'conformance\target\live-spark-sql'
$cp = (@($ResolvedCp) + $SparkDirs + @($AwsBundle, $BedrockJar, $OpenAIJar)) -join ';'

Add-Content $log "=== compile LiveSparkSqlProof ==="
Invoke-Scalac @((Join-Path $Root 'conformance\LiveSparkSqlProof.scala')) $cp $out | Add-Content $log
Add-Content $log "COMPILE_EXIT=$LASTEXITCODE"
Add-Content $log "=== run ==="
Invoke-JavaMain ($cp + ';' + $out) 'org.apache.spark.sql.ai.conformance.LiveSparkSqlProof' $out |
  Where-Object { $_ -notmatch '^SLF4J|WARN |^\s*at |NativeCodeLoader|ProcfsMetricsGetter' } | Add-Content $log
Add-Content $log "RUN_EXIT=$LASTEXITCODE"
