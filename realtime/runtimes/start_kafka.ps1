$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

Set-RealtimeJavaEnvironment
$KafkaHome = Get-KafkaHome
$KafkaConfig = Join-Path $script:RuntimeRoot "kafka-server.properties"
if (-not (Test-Path $KafkaConfig)) {
    throw "Kafka config missing. Run bootstrap.ps1 first."
}

$pidFile = Join-Path $script:PidRoot "kafka.pid"
if (Test-Path $pidFile) {
    $existing = [int](Get-Content $pidFile -Raw).Trim()
    if (Get-Process -Id $existing -ErrorAction SilentlyContinue) {
        Write-Output "Kafka already running pid=$existing"
        exit 0
    }
}

$JavaExecutable = Join-Path $env:JAVA_HOME "bin\java.exe"
$KafkaClassPath = Join-Path $KafkaHome "libs\*"
$Log4jConfig = (Join-Path $KafkaHome "config\log4j.properties").Replace("\", "/")
$process = Start-HiddenCommand `
    -FilePath $JavaExecutable `
    -ArgumentList @(
        "-Xms512m",
        "-Xmx1024m",
        "-Dlog4j.configuration=file:$Log4jConfig",
        "-cp",
        $KafkaClassPath,
        "kafka.Kafka",
        $KafkaConfig
    ) `
    -WorkingDirectory $KafkaHome `
    -Stdout (Join-Path $script:LogsRoot "kafka.stdout.log") `
    -Stderr (Join-Path $script:LogsRoot "kafka.stderr.log")

Save-TrackedProcess "kafka" $process
Wait-TcpPort "127.0.0.1" 19092 60
Write-Output "Kafka started pid=$($process.Id) address=127.0.0.1:19092"

