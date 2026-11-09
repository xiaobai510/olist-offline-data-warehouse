$ErrorActionPreference = "Stop"

$script:RealtimeRoot = Split-Path $PSScriptRoot -Parent
$script:ProjectRoot = Split-Path $script:RealtimeRoot -Parent
$script:RuntimeRoot = Join-Path $script:RealtimeRoot ".runtime"
$script:LogsRoot = Join-Path $script:RealtimeRoot "logs"
$script:PidRoot = Join-Path $script:RuntimeRoot "pids"

New-Item -ItemType Directory -Force -Path $script:RuntimeRoot | Out-Null
New-Item -ItemType Directory -Force -Path $script:LogsRoot | Out-Null
New-Item -ItemType Directory -Force -Path $script:PidRoot | Out-Null

function Get-RuntimeJavaHome {
    $candidate = Get-ChildItem $script:RuntimeRoot -Directory -Filter "jdk-17*" |
        Where-Object { Test-Path (Join-Path $_.FullName "bin\java.exe") } |
        Select-Object -First 1
    if (-not $candidate) {
        throw "JDK 17 not found. Run runtimes\bootstrap.ps1 first."
    }
    return $candidate.FullName
}

function Get-KafkaHome {
    $candidate = Get-ChildItem $script:RuntimeRoot -Directory -Filter "kafka_2.13-*" |
        Select-Object -First 1
    if (-not $candidate) {
        throw "Kafka not found. Run runtimes\bootstrap.ps1 first."
    }
    return $candidate.FullName
}

function Get-FlinkHome {
    $candidate = Get-ChildItem $script:RuntimeRoot -Directory -Filter "flink-1.18.1" |
        Select-Object -First 1
    if (-not $candidate) {
        throw "Flink not found. Run runtimes\bootstrap.ps1 first."
    }
    return $candidate.FullName
}

function Get-GitBash {
    $candidates = @(
        "D:\git\Git\bin\bash.exe",
        "C:\Program Files\Git\bin\bash.exe",
        "C:\Program Files (x86)\Git\bin\bash.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }
    $command = Get-Command bash.exe -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    throw "Git Bash not found. Install Git for Windows first."
}

function Set-RealtimeJavaEnvironment {
    $env:JAVA_HOME = Get-RuntimeJavaHome
    $env:PATH = (Join-Path $env:JAVA_HOME "bin") + ";" + $env:PATH
}

function Start-HiddenCommand {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory,
        [string]$Stdout,
        [string]$Stderr
    )
    return Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $WorkingDirectory `
        -WindowStyle Hidden `
        -PassThru `
        -RedirectStandardOutput $Stdout `
        -RedirectStandardError $Stderr
}

function Save-TrackedProcess {
    param([string]$Name, [System.Diagnostics.Process]$Process)
    Set-Content `
        -LiteralPath (Join-Path $script:PidRoot "$Name.pid") `
        -Value $Process.Id `
        -Encoding ascii
}

function Stop-TrackedProcess {
    param([string]$Name)
    $pidFile = Join-Path $script:PidRoot "$Name.pid"
    if (-not (Test-Path $pidFile)) {
        return
    }
    $processId = [int](Get-Content $pidFile -Raw).Trim()
    if (Get-Process -Id $processId -ErrorAction SilentlyContinue) {
        & taskkill.exe /PID $processId /T /F | Out-Null
        Write-Output "stopped $Name pid=$processId"
    }
    Remove-Item -LiteralPath $pidFile -Force
}

function Wait-TcpPort {
    param(
        [string]$HostName,
        [int]$Port,
        [int]$TimeoutSeconds = 60
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $task = $client.ConnectAsync($HostName, $Port)
            if ($task.Wait(1000) -and $client.Connected) {
                return
            }
        } catch {
        } finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $HostName`:$Port"
}

