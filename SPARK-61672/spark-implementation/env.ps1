# SPARK-61672 shared environment for the run scripts.
#
# Defaults are derived from this package and the caller's environment. Override when needed:
#
#   $env:SPARK_61672_SPARK_HOME = 'D:\src\spark'
#   $env:SPARK_61672_JAVA_HOME  = 'C:\jdk-17'
#   $env:SPARK_61672_MAVEN      = 'C:\tools\maven\bin\mvn.cmd'
#
# Nothing here contains an account id, profile name, key, or secret. Credentials come from the AWS
# default chain selected by the caller (environment, profile, workload identity, or instance role).

function Default($name, $value) {
  if (-not (Get-Item -Path "Env:$name" -ErrorAction SilentlyContinue) -and $value) {
    Set-Item -Path "Env:$name" -Value $value
  }
}

$Script:Root = Split-Path -Parent $MyInvocation.MyCommand.Path   # spark-implementation/
$packageRoot = Split-Path -Parent $Root                          # SPARK-61672/
$repoRoot = Split-Path -Parent $packageRoot                      # Spark checkout
$javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
$defaultJava = if ($env:JAVA_HOME) { $env:JAVA_HOME }
  elseif ($javaCommand) { Split-Path -Parent (Split-Path -Parent $javaCommand.Source) }
  else { $null }
$mavenCommand = Get-Command mvn.cmd -ErrorAction SilentlyContinue
if (-not $mavenCommand) { $mavenCommand = Get-Command mvn -ErrorAction SilentlyContinue }

Default 'SPARK_61672_JAVA_HOME' $defaultJava
Default 'SPARK_61672_SPARK_HOME' $repoRoot
Default 'SPARK_61672_M2' (Join-Path $env:USERPROFILE '.m2\repository')
Default 'SPARK_61672_HADOOP_HOME' $env:HADOOP_HOME
Default 'SPARK_61672_MAVEN' $(if ($mavenCommand) { $mavenCommand.Source } else { $null })
Default 'SPARK_61672_SCALA_VER' '2.13.18'
Default 'SPARK_61672_AWS_SDK_VER' '2.35.4'
Default 'SPARK_TEST_BEDROCK_REGION' 'us-east-1'
Default 'SPARK_TEST_BEDROCK_MODEL_ID' 'amazon.nova-micro-v1:0'

if (-not $env:SPARK_61672_JAVA_HOME) {
  throw 'Set JAVA_HOME or SPARK_61672_JAVA_HOME to a JDK 17 installation.'
}
$env:JAVA_HOME = $env:SPARK_61672_JAVA_HOME
$env:JAVA_TOOL_OPTIONS = ''
if ($env:SPARK_61672_HADOOP_HOME -and (Test-Path $env:SPARK_61672_HADOOP_HOME)) {
  $env:HADOOP_HOME = $env:SPARK_61672_HADOOP_HOME
  $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
}

$Script:Java = Join-Path $env:JAVA_HOME 'bin\java.exe'
$Script:Jar = Join-Path $env:JAVA_HOME 'bin\jar.exe'
$Script:M2 = $env:SPARK_61672_M2
$Script:SparkBase = $env:SPARK_61672_SPARK_HOME

$sv = $env:SPARK_61672_SCALA_VER
$Script:ScalaLib = Join-Path $M2 "org\scala-lang\scala-library\$sv\scala-library-$sv.jar"
$Script:ScalaComp = Join-Path $M2 "org\scala-lang\scala-compiler\$sv\scala-compiler-$sv.jar"
$Script:ScalaRefl = Join-Path $M2 "org\scala-lang\scala-reflect\$sv\scala-reflect-$sv.jar"
$av = $env:SPARK_61672_AWS_SDK_VER
$Script:AwsBundle = Join-Path $M2 "software\amazon\awssdk\bundle\$av\bundle-$av.jar"
$Script:Kryo = Join-Path $M2 'com\esotericsoftware\kryo-shaded\4.0.3\kryo-shaded-4.0.3.jar'

# Built Spark class directories (the engine with the SPARK-61672 changes).
$Script:SparkDirs = @('sql\catalyst','sql\api','core','common\unsafe','common\utils','common\utils-java',
  'common\config','common\tags','common\network-common','common\network-shuffle','common\kvstore',
  'common\sketch','common\variant','launcher','sql\core') |
  ForEach-Object { Join-Path $SparkBase "$_\target\scala-2.13\classes" } |
  Where-Object { Test-Path $_ }

# Resolve the Spark runtime/test dependencies into target/ on this machine. The clean share package
# intentionally excludes this machine-specific file and every target directory.
$targetDir = Join-Path $Root 'conformance\target'
New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
$Script:ResolvedCpFile = Join-Path $targetDir 'spark-runtime-classpath.txt'
if (-not (Test-Path $ResolvedCpFile)) {
  if (-not $env:SPARK_61672_MAVEN) {
    throw 'Set SPARK_61672_MAVEN to mvn or mvn.cmd so the runtime classpath can be resolved.'
  }
  & $env:SPARK_61672_MAVEN -q -f (Join-Path $SparkBase 'sql\core\pom.xml') `
    dependency:build-classpath "-DincludeScope=test" "-Dmdep.outputFile=$ResolvedCpFile"
  if ($LASTEXITCODE -ne 0 -or -not (Test-Path $ResolvedCpFile)) {
    throw 'Could not resolve the Spark runtime classpath. Build sql/core and check Maven output.'
  }
}
$Script:ResolvedCp = (Get-Content $ResolvedCpFile -Raw).Trim()

# Jars are generated under connector/*/target by Maven. They are not stored in the clean package.
$Script:OpenAIJar = Join-Path $Root 'connector\ai-openai\target\spark-ai-openai_2.13-4.3.0.jar'
$Script:BedrockJar = Join-Path $Root 'connector\ai-bedrock\target\spark-ai-bedrock_2.13-4.3.0.jar'

# Compile Scala with @files to stay under Windows command-line limits.
function Invoke-Scalac($sources, $classpath, $outDir) {
  New-Item -ItemType Directory -Force -Path $outDir | Out-Null
  $cargs = Join-Path $outDir 'scalac-compiler.args'
  [IO.File]::WriteAllText($cargs,
    "-cp `"" + (@($ScalaComp,$ScalaLib,$ScalaRefl) -join ';').Replace('\','\\') + "`"`n")
  $opts = Join-Path $outDir 'scalac.opts'
  [IO.File]::WriteAllText($opts,
    "-deprecation`n-feature`n-classpath`n$classpath`n-d`n$outDir`n" + ($sources -join "`n") + "`n")
  & $Java "@$cargs" scala.tools.nsc.Main "@$opts" 2>&1
  return $LASTEXITCODE
}

# Run a main class with a long classpath through a JVM @argfile.
function Invoke-JavaMain($classpath, $mainClass, $outDir) {
  $rargs = Join-Path $outDir 'run.args'
  [IO.File]::WriteAllText($rargs, "-cp `"" + ($classpath -replace '\\','\\') + "`"`n")
  & $Java "@$rargs" $mainClass 2>&1
  return $LASTEXITCODE
}
