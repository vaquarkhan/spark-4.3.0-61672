# SPARK-61672: real throughput benchmark. Same Spark SQL over the same prompts against real
# Amazon Bedrock with 1 call in flight (serial) versus 8 (async). Paid: about 2N+2 short calls.
# Paths come from env.ps1 (override with SPARK_61672_* environment variables).
# Writes live-throughput-benchmark.log next to this script.

$ErrorActionPreference = 'Continue'
. (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'env.ps1')
# Credentials are caller-controlled through the AWS default chain. This script never selects a
# profile or account.

$log = Join-Path $Root 'live-throughput-benchmark.log'
Remove-Item $log -ErrorAction SilentlyContinue

$out = Join-Path $Root 'conformance\target\live-bench'
$cp = (@($ResolvedCp) + $SparkDirs + @($AwsBundle, $BedrockJar)) -join ';'

Invoke-Scalac @((Join-Path $Root 'conformance\LiveThroughputBenchmark.scala')) $cp $out | Add-Content $log
Add-Content $log "COMPILE_EXIT=$LASTEXITCODE"
Invoke-JavaMain ($cp + ';' + $out) 'org.apache.spark.sql.ai.conformance.LiveThroughputBenchmark' $out |
  Where-Object { $_ -notmatch '^SLF4J|WARN |^\s*at |NativeCodeLoader|ProcfsMetricsGetter' } | Add-Content $log
Add-Content $log "RUN_EXIT=$LASTEXITCODE"
