param(
    [string]$ProjectRoot = "D:\pycharm\PythonProject"
)

$ErrorActionPreference = "Stop"
$Source = $PSScriptRoot
$Destination = Join-Path $ProjectRoot "realtime"

if (-not (Test-Path $ProjectRoot)) {
    throw "Project root not found: $ProjectRoot"
}

New-Item -ItemType Directory -Force -Path $Destination | Out-Null

& robocopy.exe `
    $Source `
    $Destination `
    /E `
    /XD ".runtime" "logs" "metrics_output" "__pycache__" `
    /XF "*.pyc" `
    /NFL /NDL /NJH /NJS /NP

if ($LASTEXITCODE -ge 8) {
    throw "robocopy failed with exit code $LASTEXITCODE"
}

Write-Output "Realtime source copied to $Destination"
Write-Output "Next:"
Write-Output "  pip install -r `"$Destination\requirements.txt`""
Write-Output "  powershell -ExecutionPolicy Bypass -File `"$Destination\runtimes\bootstrap.ps1`""

