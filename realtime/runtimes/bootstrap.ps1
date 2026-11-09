param(
    [switch]$Force,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$DownloadRoot = Join-Path $script:RuntimeRoot "downloads"
New-Item -ItemType Directory -Force -Path $DownloadRoot | Out-Null

$JdkUrl = "https://mirrors.tuna.tsinghua.edu.cn/Adoptium/17/jdk/x64/windows/OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1.zip"
$KafkaUrl = "https://archive.apache.org/dist/kafka/3.6.2/kafka_2.13-3.6.2.tgz"
$FlinkUrl = "https://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz"
$ConnectorUrl = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar"

if (-not $PythonExe) {
    $PythonExe = (Get-Command python.exe -ErrorAction Stop).Source
}
$DownloadScript = Join-Path $PSScriptRoot "download_file.py"

function Get-File {
    param([string]$Url, [string]$Target)
    if (
        (Test-Path $Target) -and
        -not $Force -and
        ((Get-Item $Target).Length -gt 0)
    ) {
        return
    }
    Write-Output "downloading $Url"
    if (Test-Path $Target) {
        Remove-Item -LiteralPath $Target -Force
    }
    & $PythonExe $DownloadScript $Url $Target
    if (
        $LASTEXITCODE -ne 0 -or
        -not (Test-Path $Target) -or
        ((Get-Item $Target).Length -le 0)
    ) {
        throw "download failed: $Url"
    }
}

$JdkArchive = Join-Path $DownloadRoot "temurin-jdk17.zip"
$KafkaArchive = Join-Path $DownloadRoot "kafka_2.13-3.6.2.tgz"
$FlinkArchive = Join-Path $DownloadRoot "flink-1.18.1.tgz"
$ConnectorArchive = Join-Path $DownloadRoot "flink-sql-connector-kafka-3.0.2-1.18.jar"

Get-File $JdkUrl $JdkArchive
Get-File $KafkaUrl $KafkaArchive
Get-File $FlinkUrl $FlinkArchive
Get-File $ConnectorUrl $ConnectorArchive

$jdkCandidate = Get-ChildItem $script:RuntimeRoot -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue |
    Where-Object { Test-Path (Join-Path $_.FullName "bin\java.exe") } |
    Select-Object -First 1
if (-not $jdkCandidate) {
    Write-Output "extracting JDK 17"
    Expand-Archive -LiteralPath $JdkArchive -DestinationPath $script:RuntimeRoot -Force
}

$kafkaCandidate = Get-ChildItem $script:RuntimeRoot -Directory -Filter "kafka_2.13-*" -ErrorAction SilentlyContinue |
    Select-Object -First 1
if (-not $kafkaCandidate) {
    Write-Output "extracting Kafka"
    & tar.exe -xzf $KafkaArchive -C $script:RuntimeRoot
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka extraction failed"
    }
}

$flinkCandidate = Get-ChildItem $script:RuntimeRoot -Directory -Filter "flink-1.18.1" -ErrorAction SilentlyContinue |
    Select-Object -First 1
if (-not $flinkCandidate) {
    Write-Output "extracting Flink"
    & tar.exe -xzf $FlinkArchive -C $script:RuntimeRoot
    if ($LASTEXITCODE -ne 0) {
        throw "Flink extraction failed"
    }
}

$JavaHome = Get-RuntimeJavaHome
$KafkaHome = Get-KafkaHome
$FlinkHome = Get-FlinkHome

$TargetJar = Join-Path $FlinkHome "lib\flink-sql-connector-kafka-3.0.2-1.18.jar"
Copy-Item -LiteralPath $ConnectorArchive -Destination $TargetJar -Force

$KafkaTemplate = Join-Path $KafkaHome "config\kraft\server.properties"
$KafkaConfig = Join-Path $script:RuntimeRoot "kafka-server.properties"
$LogDirs = (Join-Path $script:RuntimeRoot "kafka-data-19092").Replace("\", "/")
$ConfigText = Get-Content $KafkaTemplate -Raw
$ConfigText = $ConfigText -replace "(?m)^controller\.quorum\.voters=.*$", "controller.quorum.voters=1@127.0.0.1:19093"
$ConfigText = $ConfigText -replace "(?m)^listeners=.*$", "listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093"
$ConfigText = $ConfigText -replace "(?m)^advertised\.listeners=.*$", "advertised.listeners=PLAINTEXT://127.0.0.1:19092"
$ConfigText = $ConfigText -replace "(?m)^log\.dirs=.*$", "log.dirs=$LogDirs"
$ConfigText = $ConfigText -replace "(?m)^num\.partitions=.*$", "num.partitions=3"
if ($ConfigText -notmatch "(?m)^auto\.create\.topics\.enable=") {
    $ConfigText += "`nauto.create.topics.enable=true`n"
}
Set-Content -LiteralPath $KafkaConfig -Value $ConfigText -Encoding utf8

$FormattedMarker = Join-Path $script:RuntimeRoot "kafka-formatted-19092.marker"
if (-not (Test-Path $FormattedMarker)) {
    Set-RealtimeJavaEnvironment
    $JavaExecutable = Join-Path $env:JAVA_HOME "bin\java.exe"
    $KafkaClassPath = Join-Path $KafkaHome "libs\*"
    $ToolsLog4j = (Join-Path $KafkaHome "config\tools-log4j.properties").Replace("\", "/")
    $ClusterId = (
        & $JavaExecutable `
            "-Dlog4j.configuration=file:$ToolsLog4j" `
            -cp $KafkaClassPath `
            kafka.tools.StorageTool random-uuid
    ).Trim()
    & $JavaExecutable `
        "-Dlog4j.configuration=file:$ToolsLog4j" `
        -cp $KafkaClassPath `
        kafka.tools.StorageTool format -t $ClusterId -c $KafkaConfig
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka storage format failed"
    }
    Set-Content -LiteralPath $FormattedMarker -Value $ClusterId -Encoding ascii
}

$FlinkConf = Join-Path $FlinkHome "conf\flink-conf.yaml"
$FlinkConfigLines = @(
    "",
    "taskmanager.numberOfTaskSlots: 4",
    "parallelism.default: 2",
    "execution.checkpointing.interval: 10s",
    "state.backend.type: hashmap",
    "jobmanager.bind-host: 127.0.0.1",
    "taskmanager.bind-host: 127.0.0.1",
    "taskmanager.host: 127.0.0.1",
    "rest.address: 127.0.0.1",
    "rest.port: 8081"
) -join "`n"
$FlinkMarker = Join-Path $script:RuntimeRoot "flink-configured.marker"
if (-not (Test-Path $FlinkMarker)) {
    Add-Content -LiteralPath $FlinkConf -Value $FlinkConfigLines -Encoding utf8
    Set-Content -LiteralPath $FlinkMarker -Value "configured" -Encoding ascii
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
if ($FlinkConfigText -notmatch "(?m)^taskmanager\.resource-id=") {
    Add-Content `
        -LiteralPath $FlinkConf `
        -Value "taskmanager.resource-id: olist-taskmanager-1" `
        -Encoding utf8
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
if ($FlinkConfigText -notmatch "(?m)^taskmanager\.cpu\.cores=") {
    Add-Content `
        -LiteralPath $FlinkConf `
        -Value "taskmanager.cpu.cores: 2" `
        -Encoding utf8
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
if ($FlinkConfigText -notmatch "(?m)^taskmanager\.memory\.task\.heap\.size=") {
    Add-Content `
        -LiteralPath $FlinkConf `
        -Value "taskmanager.memory.task.heap.size: 1024m" `
        -Encoding utf8
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
if ($FlinkConfigText -notmatch "(?m)^taskmanager\.memory\.managed\.size=") {
    Add-Content `
        -LiteralPath $FlinkConf `
        -Value "taskmanager.memory.managed.size: 128m" `
        -Encoding utf8
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
if ($FlinkConfigText -notmatch "(?m)^taskmanager\.memory\.network\.min=") {
    Add-Content `
        -LiteralPath $FlinkConf `
        -Value "taskmanager.memory.network.min: 64m" `
        -Encoding utf8
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
if ($FlinkConfigText -notmatch "(?m)^taskmanager\.memory\.network\.max=") {
    Add-Content `
        -LiteralPath $FlinkConf `
        -Value "taskmanager.memory.network.max: 64m" `
        -Encoding utf8
}
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
$FlinkConfigText = $FlinkConfigText -replace "(?m)^taskmanager\.memory\.process\.size:.*$", "taskmanager.memory.process.size: 2048m"
foreach ($memoryLine in @(
    "taskmanager.memory.task.off-heap.size: 0m",
    "taskmanager.memory.framework.heap.size: 128m",
    "taskmanager.memory.framework.off-heap.size: 128m",
    "taskmanager.memory.jvm-metaspace.size: 256m",
    "taskmanager.memory.jvm-overhead.min: 128m",
    "taskmanager.memory.jvm-overhead.max: 128m"
)) {
    $key = ($memoryLine -split ":")[0]
    if ($FlinkConfigText -notmatch "(?m)^$([regex]::Escape($key))=") {
        $FlinkConfigText += "`n$memoryLine"
    }
}
Set-Content -LiteralPath $FlinkConf -Value $FlinkConfigText -Encoding utf8
$FlinkConfigText = Get-Content -LiteralPath $FlinkConf -Raw
$FlinkConfigText = $FlinkConfigText -replace "(?m)^jobmanager\.rpc\.port:.*$", "jobmanager.rpc.port: 16123"
$FlinkConfigText = $FlinkConfigText -replace "(?m)^rest\.port:.*$", "rest.port: 18081"
if ($FlinkConfigText -notmatch "(?m)^rest\.bind-address=") {
    $FlinkConfigText += "`nrest.bind-address: 127.0.0.1`n"
}
Set-Content -LiteralPath $FlinkConf -Value $FlinkConfigText -Encoding utf8

Write-Output ""
Write-Output "Realtime runtime is ready."
Write-Output "JAVA_HOME=$JavaHome"
Write-Output "KAFKA_HOME=$KafkaHome"
Write-Output "FLINK_HOME=$FlinkHome"
Write-Output "KAFKA_CONFIG=$KafkaConfig"

