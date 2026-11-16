# Build and run SPARK-61672 on Windows from a clean full-source ZIP extraction.
#
# Offline engine demonstration (no AWS calls):
#   powershell -ExecutionPolicy Bypass -File SPARK-61672\run-poc.ps1
#
# Real provider demonstration (paid, a few short Bedrock calls):
#   $env:AWS_PROFILE = '<profile-selected-by-you>'
#   powershell -ExecutionPolicy Bypass -File SPARK-61672\run-poc.ps1 -Live
#
# The first Spark build is large. On the author's machine it took 2 h 8 min; another machine may
# take less or more. Later runs reuse Maven and compiler output.

param(
  [switch]$Live,
  [switch]$SkipSparkBuild,
  [switch]$SkipEngineTests
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$spark = Split-Path -Parent $here
$preflight = Join-Path $here 'preflight.ps1'

function Stage($name) {
  Write-Host ''
  Write-Host ('=' * 78) -ForegroundColor DarkCyan
  Write-Host $name -ForegroundColor Cyan
  Write-Host ('=' * 78) -ForegroundColor DarkCyan
}

# Use preflight to discover/validate caller paths. Dot source env.ps1 afterward for shared values.
Stage '0. Preflight'
& powershell -ExecutionPolicy Bypass -File $preflight $(if ($Live) { '-CheckAws' })
if ($LASTEXITCODE -ne 0) { throw 'Preflight failed. Fix the listed prerequisites and run again.' }
. (Join-Path $here 'spark-implementation\env.ps1')
$maven = $env:SPARK_61672_MAVEN
if (-not $maven) { throw 'Set SPARK_61672_MAVEN to mvn or mvn.cmd.' }

if (-not $SkipSparkBuild) {
  Stage '1. Build and install modified Spark artifacts'
  Write-Host 'This is the long first step. Expect roughly 1 to 2.5 hours on a developer machine.'
  & $maven -f (Join-Path $spark 'pom.xml') -pl sql/catalyst,sql/core -am install `
    '-DskipTests=true' '-Dscalastyle.skip=true' '-Dcheckstyle.skip=true' `
    '-Dspotless.check.skip=true' '-Dmaven.javadoc.skip=true'
  if ($LASTEXITCODE -ne 0) { throw 'Modified Spark build failed.' }
} else {
  Stage '1. Spark build skipped by caller'
  & powershell -ExecutionPolicy Bypass -File $preflight -RequireBuiltSpark
  if ($LASTEXITCODE -ne 0) { throw 'SkipSparkBuild was set, but the modified Spark classes are missing.' }
}

Stage '2. Build provider modules from their poms'
& $maven -f (Join-Path $here 'spark-implementation\connector\ai-openai\pom.xml') package
if ($LASTEXITCODE -ne 0) { throw 'OpenAI-compatible provider build failed.' }
& $maven -f (Join-Path $here 'spark-implementation\connector\ai-bedrock\pom.xml') package
if ($LASTEXITCODE -ne 0) { throw 'Bedrock provider build failed.' }
& powershell -ExecutionPolicy Bypass -File $preflight -RequireBuiltSpark -RequireProviderJars
if ($LASTEXITCODE -ne 0) { throw 'Post-build preflight failed.' }

if (-not $SkipEngineTests) {
  Stage '3. Run all 25 SPARK-61672 engine cases (no network, no cost)'
  & $maven -f (Join-Path $spark 'pom.xml') -pl sql/core test `
    '-DwildcardSuites=org.apache.spark.sql.execution.async' '-Dtest=none' `
    '-Dscalastyle.skip=true' '-Dcheckstyle.skip=true' '-Dspotless.check.skip=true' `
    '-Dmaven.javadoc.skip=true'
  if ($LASTEXITCODE -ne 0) { throw 'SPARK-61672 engine tests failed.' }
} else {
  Stage '3. Engine tests skipped by caller'
}

if ($Live) {
  Stage '4. Run real Spark SQL against caller-authorized model services (paid)'
  & powershell -ExecutionPolicy Bypass -File `
    (Join-Path $here 'spark-implementation\live-spark-sql-proof.ps1')
  if ($LASTEXITCODE -ne 0) { throw 'Live Spark SQL proof failed.' }
  Write-Host 'Real output: SPARK-61672\spark-implementation\live-spark-sql-proof.log'
  Write-Host 'Real summaries: SPARK-61672\proposal\examples\report\live-bedrock-summaries.csv'
} else {
  Stage '4. Live model calls not requested'
  Write-Host 'To see real model calls, rerun with -Live after selecting your AWS identity.'
}

Stage 'DONE'
Write-Host 'RESULT: SPARK-61672 POC BUILD AND SELECTED VALIDATION PASSED' -ForegroundColor Green
