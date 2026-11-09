$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

foreach ($name in @(
    "flink-sql-client",
    "flink-taskmanager",
    "flink-jobmanager",
    "kafka"
)) {
    Stop-TrackedProcess $name
}

Write-Output "Kafka and Flink processes stopped."

