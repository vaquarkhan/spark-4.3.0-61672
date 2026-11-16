# SPARK-61672 provider modules: compile, package, and run the conformance harness.
# Paths come from env.ps1 (override with SPARK_61672_* environment variables).
# Writes compile-check.log and conformance.log next to this script.
#
# Live cases (opt-in):
#   SPARK_TEST_LIVE_BEDROCK=1                       Bedrock module against Converse (AWS default chain)
#   SPARK_TEST_LIVE_OPENAI_BASE_URL + AI_PROVIDER_TOKEN   OpenAI-compatible module against a real endpoint
# If a file conformance\target\bedrock-bearer.txt exists it is used as AI_PROVIDER_TOKEN against
# Amazon Bedrock's OpenAI-compatible API (see README for how the key is minted). Never commit it.

$ErrorActionPreference = 'Continue'
. (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'env.ps1')

$log = Join-Path $Root 'compile-check.log'
$confLog = Join-Path $Root 'conformance.log'
Remove-Item $log, $confLog -ErrorAction SilentlyContinue

$jackson = @('com\fasterxml\jackson\core\jackson-databind\2.21.2\jackson-databind-2.21.2.jar',
  'com\fasterxml\jackson\core\jackson-core\2.21.2\jackson-core-2.21.2.jar',
  'com\fasterxml\jackson\core\jackson-annotations\2.21\jackson-annotations-2.21.jar') | ForEach-Object { Join-Path $M2 $_ }
$baseCp = @($ScalaLib, $Kryo) + $jackson + $SparkDirs

function Compile-Into($name, $srcDir, $out, $extraCp) {
  Get-ChildItem -Recurse $out -Filter *.class -EA SilentlyContinue | Remove-Item -Force
  $sources = Get-ChildItem -Recurse -Path $srcDir -Filter *.scala | ForEach-Object { $_.FullName }
  Add-Content $log "=== compiling $name ==="
  Invoke-Scalac $sources (($baseCp + $extraCp) -join ';') $out | Add-Content $log
  Add-Content $log "EXIT($name)=$LASTEXITCODE"
  $cnt = (Get-ChildItem -Recurse $out -Filter *.class -EA SilentlyContinue | Measure-Object).Count
  Add-Content $log "CLASSFILES($name)=$cnt"
}

$openaiOut = Join-Path $Root 'connector\ai-openai\target\classes'
$bedrockOut = Join-Path $Root 'connector\ai-bedrock\target\classes'
$confOut = Join-Path $Root 'conformance\target\classes'

Compile-Into 'ai-openai' (Join-Path $Root 'connector\ai-openai\src\main\scala') $openaiOut @()
Compile-Into 'ai-bedrock' (Join-Path $Root 'connector\ai-bedrock\src\main\scala') $bedrockOut @($AwsBundle)
Compile-Into 'conformance' (Join-Path $Root 'conformance\ProviderConformanceHarness.scala') $confOut @($AwsBundle, $openaiOut, $bedrockOut)

# --- jars -------------------------------------------------------------------------------------
# Use the Maven-generated jars under each connector's target/. On a fresh source package, build
# them with `mvn package` first. As a fallback for this low-level compile checker, package the
# classes compiled above into the same target paths.
if (-not (Test-Path $OpenAIJar)) {
  & $Jar --create --file $OpenAIJar -C $openaiOut . 2>&1 | Add-Content $log
  Add-Content $log "JAR(ai-openai) built from compile-check classes (Maven target jar was absent)"
}
if (-not (Test-Path $BedrockJar)) {
  & $Jar --create --file $BedrockJar -C $bedrockOut . 2>&1 | Add-Content $log
  Add-Content $log "JAR(ai-bedrock) built from compile-check classes (Maven target jar was absent)"
}
Add-Content $log "JAR(ai-openai)=$((Get-Item $OpenAIJar).Length) bytes  $((Get-Item $OpenAIJar).LastWriteTime)"
Add-Content $log "JAR(ai-bedrock)=$((Get-Item $BedrockJar).Length) bytes  $((Get-Item $BedrockJar).LastWriteTime)"

# --- conformance against the packaged jars ----------------------------------------------------
$env:AI_TEST_TOKEN = 'SECRET_TOKEN_MARKER_123'   # fake token for the in-process server only
$bearerFile = Join-Path $Root 'conformance\target\bedrock-bearer.txt'
if ((Test-Path $bearerFile) -and -not $env:SPARK_TEST_LIVE_OPENAI_BASE_URL) {
  $env:AI_PROVIDER_TOKEN = (Get-Content $bearerFile -Raw).Trim()
  $env:SPARK_TEST_LIVE_OPENAI_BASE_URL = "https://bedrock-runtime.$($env:SPARK_TEST_BEDROCK_REGION).amazonaws.com/openai/v1"
  if (-not $env:SPARK_TEST_LIVE_OPENAI_MODEL) { $env:SPARK_TEST_LIVE_OPENAI_MODEL = 'qwen.qwen3-32b-v1:0' }
}
$runCp = (@($ResolvedCp) + $SparkDirs + @($AwsBundle, $OpenAIJar, $BedrockJar, $confOut)) -join ';'
Invoke-JavaMain $runCp 'org.apache.spark.sql.ai.conformance.ProviderConformanceHarness' $confOut | Add-Content $confLog
Add-Content $log "CONFORMANCE_EXIT=$LASTEXITCODE (see conformance.log)"
Add-Content $log "=== DONE ==="
