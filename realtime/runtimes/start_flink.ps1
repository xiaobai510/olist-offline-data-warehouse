$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

Set-RealtimeJavaEnvironment
$FlinkHome = Get-FlinkHome
$JavaExecutable = Join-Path $env:JAVA_HOME "bin\java.exe"
$ClassPath = Join-Path $FlinkHome "lib\*"
$ConfDir = Join-Path $FlinkHome "conf"
$Log4jConfig = (Join-Path $ConfDir "log4j.properties").Replace("\", "/")
$env:FLINK_CONF_DIR = $ConfDir
$env:FLINK_LIB_DIR = Join-Path $FlinkHome "lib"
$env:FLINK_PLUGINS_DIR = Join-Path $FlinkHome "plugins"

$jmPidFile = Join-Path $script:PidRoot "flink-jobmanager.pid"
$tmPidFile = Join-Path $script:PidRoot "flink-taskmanager.pid"
$jmActive = $false
if (Test-Path $jmPidFile) {
    $jmPid = [int](Get-Content $jmPidFile -Raw).Trim()
    $jmActive = [bool](Get-Process -Id $jmPid -ErrorAction SilentlyContinue)
}
$tmActive = $false
if (Test-Path $tmPidFile) {
    $tmPid = [int](Get-Content $tmPidFile -Raw).Trim()
    $tmActive = [bool](Get-Process -Id $tmPid -ErrorAction SilentlyContinue)
}

$JobManagerLog = (Join-Path $script:LogsRoot "flink-jobmanager.log").Replace("\", "/")
$TaskManagerLog = (Join-Path $script:LogsRoot "flink-taskmanager.log").Replace("\", "/")
if (-not $jmActive) {
    $jobManager = Start-HiddenCommand `
        -FilePath $JavaExecutable `
        -ArgumentList @(
            "-Xms512m",
            "-Xmx1024m",
            "-Dlog.file=$JobManagerLog",
            "-Dlog4j.configurationFile=file:$Log4jConfig",
            "-cp",
            $ClassPath,
            "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint",
            "-c",
            $ConfDir
        ) `
        -WorkingDirectory $FlinkHome `
        -Stdout (Join-Path $script:LogsRoot "flink-jobmanager.stdout.log") `
        -Stderr (Join-Path $script:LogsRoot "flink-jobmanager.stderr.log")
    Save-TrackedProcess "flink-jobmanager" $jobManager
    Wait-TcpPort "127.0.0.1" 18081 60
}

if (-not $tmActive) {
    $taskManager = Start-HiddenCommand `
        -FilePath $JavaExecutable `
        -ArgumentList @(
            "-Xms512m",
            "-Xmx1024m",
            "-Dlog.file=$TaskManagerLog",
            "-Dlog4j.configurationFile=file:$Log4jConfig",
            "-cp",
            $ClassPath,
            "org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
            "-c",
            $ConfDir
        ) `
        -WorkingDirectory $FlinkHome `
        -Stdout (Join-Path $script:LogsRoot "flink-taskmanager.stdout.log") `
        -Stderr (Join-Path $script:LogsRoot "flink-taskmanager.stderr.log")
    Save-TrackedProcess "flink-taskmanager" $taskManager
    Start-Sleep -Seconds 5
}

Write-Output "Flink started"
Write-Output "Web UI: http://127.0.0.1:18081"

