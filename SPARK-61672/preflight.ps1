# SPARK-61672 preflight. Read-only: does not build, modify AWS, or call a model.
#
# Basic local checks:
#   powershell -ExecutionPolicy Bypass -File SPARK-61672\preflight.ps1
#
# Also verify the caller-selected AWS identity and Bedrock model (read-only AWS calls):
#   $env:AWS_PROFILE = '<your-profile>'
#   powershell -ExecutionPolicy Bypass -File SPARK-61672\preflight.ps1 -CheckAws

param(
  [switch]$CheckAws,
  [switch]$RequireBuiltSpark,
  [switch]$RequireProviderJars
)

$ErrorActionPreference = 'Continue'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$spark = Split-Path -Parent $here
$failures = 0
$warnings = 0

function Pass($message) { Write-Host "PASS  $message" -ForegroundColor Green }
function Fail($message) { Write-Host "FAIL  $message" -ForegroundColor Red; $script:failures++ }
function Warn($message) { Write-Host "WARN  $message" -ForegroundColor Yellow; $script:warnings++ }

Write-Host 'SPARK-61672 preflight' -ForegroundColor Cyan
Write-Host ('=' * 78)

# Package and source layout
if (Test-Path (Join-Path $spark 'pom.xml')) { Pass "Spark root found: $spark" }
else { Fail 'SPARK-61672 must be directly inside the extracted Spark source root.' }
$requiredSource = @(
  'sql\catalyst\src\main\java\org\apache\spark\sql\connector\catalog\functions\AsyncScalarFunction.java',
  'sql\catalyst\src\main\scala\org\apache\spark\sql\catalyst\expressions\ApplyAsyncFunctionExpression.scala',
  'sql\core\src\main\scala\org\apache\spark\sql\execution\async\AsyncEvalExec.scala',
  'sql\core\src\main\scala\org\apache\spark\sql\execution\async\ExtractAsyncFunctions.scala',
  'SPARK-61672\spark-implementation\connector\ai-openai\pom.xml',
  'SPARK-61672\spark-implementation\connector\ai-bedrock\pom.xml'
)
$missing = $requiredSource | Where-Object { -not (Test-Path (Join-Path $spark $_)) }
if ($missing.Count -eq 0) { Pass 'All required engine and provider source files are present.' }
else { $missing | ForEach-Object { Fail "Missing source file: $_" } }

# Java 17
$javaHome = if ($env:SPARK_61672_JAVA_HOME) { $env:SPARK_61672_JAVA_HOME } else { $env:JAVA_HOME }
$java = if ($javaHome) { Join-Path $javaHome 'bin\java.exe' } else { $null }
if (-not $java -or -not (Test-Path $java)) {
  $cmd = Get-Command java.exe -ErrorAction SilentlyContinue
  if ($cmd) { $java = $cmd.Source }
}
if (-not $java) { Fail 'JDK 17 not found. Set JAVA_HOME or SPARK_61672_JAVA_HOME.' }
else {
  $version = (& $java -version 2>&1 | Select-Object -First 1).ToString()
  if ($version -match '"17\.') { Pass "JDK 17 found: $version" }
  else { Fail "This POC requires JDK 17; found: $version" }
}

# Maven 3.9+
$maven = if ($env:SPARK_61672_MAVEN) { $env:SPARK_61672_MAVEN } else { $null }
if (-not $maven) {
  $cmd = Get-Command mvn.cmd -ErrorAction SilentlyContinue
  if (-not $cmd) { $cmd = Get-Command mvn -ErrorAction SilentlyContinue }
  if ($cmd) { $maven = $cmd.Source }
}
if (-not $maven -or -not (Get-Command $maven -ErrorAction SilentlyContinue)) {
  Fail 'Maven not found. Install Maven 3.9+ and set SPARK_61672_MAVEN to mvn.cmd (Windows) or mvn.'
} else {
  $mv = (& $maven -version 2>&1 | Select-Object -First 1).ToString()
  if ($mv -match 'Apache Maven (3\.(9|[1-9][0-9])|[4-9]\.)') { Pass "Maven found: $mv" }
  else { Warn "Maven 3.9+ is recommended; found: $mv" }
}

# Windows native Hadoop helper (needed to execute Spark SQL tests/local run, not to read source)
if ($IsWindows -or $env:OS -eq 'Windows_NT') {
  $hadoop = if ($env:SPARK_61672_HADOOP_HOME) { $env:SPARK_61672_HADOOP_HOME } else { $env:HADOOP_HOME }
  if (-not $hadoop) { Fail 'Windows run requires HADOOP_HOME or SPARK_61672_HADOOP_HOME.' }
  else {
    $winutils = Join-Path $hadoop 'bin\winutils.exe'
    $dll = Join-Path $hadoop 'bin\hadoop.dll'
    if (Test-Path $winutils) { Pass "winutils.exe found under $hadoop" } else { Fail "Missing $winutils" }
    if (Test-Path $dll) { Pass "hadoop.dll found under $hadoop" } else { Fail "Missing $dll" }
  }
}

# Build outputs, only when requested
$catalystClass = Join-Path $spark 'sql\catalyst\target\scala-2.13\classes\org\apache\spark\sql\connector\catalog\functions\AsyncScalarFunction.class'
$coreClass = Join-Path $spark 'sql\core\target\scala-2.13\classes\org\apache\spark\sql\execution\async\AsyncEvalExec.class'
if ($RequireBuiltSpark) {
  if ((Test-Path $catalystClass) -and (Test-Path $coreClass)) { Pass 'Modified Spark engine classes are built.' }
  else { Fail 'Spark is not built. Run step 1 in START-HERE.md.' }
} elseif (-not ((Test-Path $catalystClass) -and (Test-Path $coreClass))) {
  Warn 'Spark classes are not built yet. This is expected immediately after extracting the clean ZIP.'
}
$openaiJar = Join-Path $here 'spark-implementation\connector\ai-openai\target\spark-ai-openai_2.13-4.3.0.jar'
$bedrockJar = Join-Path $here 'spark-implementation\connector\ai-bedrock\target\spark-ai-bedrock_2.13-4.3.0.jar'
if ($RequireProviderJars) {
  if ((Test-Path $openaiJar) -and (Test-Path $bedrockJar)) { Pass 'Both provider jars are built.' }
  else { Fail 'Provider jars are not built. Run step 2 in START-HERE.md.' }
} elseif (-not ((Test-Path $openaiJar) -and (Test-Path $bedrockJar))) {
  Warn 'Provider jars are not built yet. They are deliberately excluded from the clean ZIP.'
}

# Caller-selected AWS identity. Read-only; do not print credential values.
if ($CheckAws) {
  $aws = Get-Command aws.exe -ErrorAction SilentlyContinue
  if (-not $aws) { $aws = Get-Command aws -ErrorAction SilentlyContinue }
  if (-not $aws) { Fail 'AWS CLI not found. Install it or use an executor role on a cluster.' }
  else {
    $identity = & $aws.Source sts get-caller-identity --output json 2>&1
    if ($LASTEXITCODE -eq 0) {
      Pass 'AWS caller identity resolved through the caller-selected default credential chain.'
      # Print only the ARN role/user suffix, not the account field.
      try {
        $obj = $identity | ConvertFrom-Json
        $suffix = ($obj.Arn -split ':')[-1]
        Write-Host "      principal: $suffix" -ForegroundColor DarkGray
      } catch {}
    } else { Fail 'AWS caller identity did not resolve. Configure a profile, role, or workload identity.' }
    $region = if ($env:SPARK_TEST_BEDROCK_REGION) { $env:SPARK_TEST_BEDROCK_REGION } else { 'us-east-1' }
    $model = if ($env:SPARK_TEST_BEDROCK_MODEL_ID) { $env:SPARK_TEST_BEDROCK_MODEL_ID } else { 'amazon.nova-micro-v1:0' }
    $modelInfo = & $aws.Source bedrock get-foundation-model --region $region --model-identifier $model --output json 2>&1
    if ($LASTEXITCODE -eq 0) { Pass "Bedrock model is discoverable: $model in $region" }
    else { Fail "Bedrock model is not discoverable: $model in $region. Check model id, region, and permission." }
  }
}

Write-Host ('=' * 78)
Write-Host "Failures: $failures   Warnings: $warnings"
if ($failures -eq 0) {
  Write-Host 'RESULT: READY FOR THE NEXT STEP' -ForegroundColor Green
  exit 0
} else {
  Write-Host 'RESULT: FIX THE FAILURES ABOVE BEFORE RUNNING' -ForegroundColor Red
  exit 1
}
