$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

Set-RealtimeJavaEnvironment
$FlinkHome = Get-FlinkHome
$KafkaHome = Get-KafkaHome
$JavaExecutable = Join-Path $env:JAVA_HOME "bin\java.exe"
$ConfDir = Join-Path $FlinkHome "conf"
$ClassPath = @(
    (Join-Path $FlinkHome "lib\*"),
    (Join-Path $FlinkHome "opt\flink-sql-client-1.18.1.jar"),
    (Join-Path $FlinkHome "opt\flink-sql-gateway-1.18.1.jar"),
    (Join-Path $FlinkHome "opt\flink-python-1.18.1.jar")
) -join ";"
$env:FLINK_CONF_DIR = $ConfDir
$env:FLINK_LIB_DIR = Join-Path $FlinkHome "lib"
$env:FLINK_PLUGINS_DIR = Join-Path $FlinkHome "plugins"
$SqlFile = Join-Path $script:RealtimeRoot "flink_jobs\trade_metrics.sql"
if (-not (Test-Path $SqlFile)) {
    throw "SQL job not found: $SqlFile"
}

$KafkaClassPath = Join-Path $KafkaHome "libs\*"
foreach ($topic in @("olist_order_events", "olist_realtime_metrics")) {
    & $JavaExecutable `
        -cp $KafkaClassPath `
        kafka.admin.TopicCommand `
        --bootstrap-server 127.0.0.1:19092 `
        --create --if-not-exists `
        --topic $topic --partitions 3 --replication-factor 1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create Kafka topic: $topic"
    }
}

Stop-TrackedProcess "flink-sql-client"

$SqlLog = (Join-Path $script:LogsRoot "flink-sql-client.log").Replace("\", "/")
$Log4jConfig = (Join-Path $ConfDir "log4j.properties").Replace("\", "/")
$process = Start-HiddenCommand `
    -FilePath $JavaExecutable `
    -ArgumentList @(
        "-Xms256m",
        "-Xmx512m",
        "-Dlog.file=$SqlLog",
        "-Dlog4j.configurationFile=file:$Log4jConfig",
        "-cp",
        $ClassPath,
        "org.apache.flink.table.client.SqlClient",
        "embedded",
        "-Dexecution.target=remote",
        "-Drest.address=127.0.0.1",
        "-Drest.port=18081",
        "-f",
        $SqlFile
    ) `
    -WorkingDirectory $FlinkHome `
    -Stdout (Join-Path $script:LogsRoot "flink-sql-client.stdout.log") `
    -Stderr (Join-Path $script:LogsRoot "flink-sql-client.stderr.log")

Save-TrackedProcess "flink-sql-client" $process
Start-Sleep -Seconds 8

if ($process.HasExited) {
    Write-Output "SQL client exited code=$($process.ExitCode)"
    if (Test-Path (Join-Path $script:LogsRoot "flink-sql-client.stdout.log")) {
        Get-Content (Join-Path $script:LogsRoot "flink-sql-client.stdout.log") -Tail 60
    }
    if (Test-Path (Join-Path $script:LogsRoot "flink-sql-client.stderr.log")) {
        Get-Content (Join-Path $script:LogsRoot "flink-sql-client.stderr.log") -Tail 60
    }
    if ($process.ExitCode -ne 0) {
        throw "Flink SQL job failed. See logs."
    }
    Write-Output "SQL statements submitted; streaming job remains on the cluster."
    exit 0
}

Write-Output "Flink SQL job submitted pid=$($process.Id)"
Write-Output "Job UI: http://127.0.0.1:18081"

