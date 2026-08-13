# Requires -RunAsAdministrator
<#
.SYNOPSIS
    Automated Installation & Packaging Script for Hermes Windows Service
.DESCRIPTION
    Deploys Hermes as a production background Windows Service using WinSW or NSSM.
    Configures log rotation, graceful shutdown hooks, and Windows Defender Firewall rules.
.EXAMPLE
    .\install-service.ps1 -InstallDir "C:\Program Files\Hermes" -ServiceType WinSW
#>

param(
    [string]$InstallDir = "C:\Program Files\Hermes",
    [ValidateSet("WinSW", "NSSM")]
    [string]$ServiceType = "WinSW",
    [int]$BrokerPort = 9092,
    [int]$MetricsPort = 10092
)

$ErrorActionPreference = "Stop"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "    HERMES WINDOWS SERVICE INSTALLATION & PACKAGING        " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Administrator Rights Check
$currentPrincipal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Error "This script requires Administrator privileges. Please run PowerShell as Administrator."
    exit 1
}

# 2. Directory Creation
Write-Host "[1/5] Creating installation directory structure at '$InstallDir'..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "$InstallDir" | Out-Null
New-Item -ItemType Directory -Force -Path "$InstallDir\data" | Out-Null
New-Item -ItemType Directory -Force -Path "$InstallDir\logs" | Out-Null

# 3. Copy Binary and Configuration
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Resolve-Path "$ScriptDir\..\.."
$BinPath = "$ProjectRoot\target\release\hermes.exe"
if (-not (Test-Path $BinPath)) {
    $BinPath = "$ProjectRoot\target\debug\hermes.exe"
}

if (Test-Path $BinPath) {
    Write-Host "[2/5] Copying binary from '$BinPath'..." -ForegroundColor Yellow
    Copy-Item -Path $BinPath -Destination "$InstallDir\hermes.exe" -Force
} else {
    Write-Warning "Hermes binary not found at target build path. Please compile with 'cargo build --release' first."
}

$PropertiesPath = "$ProjectRoot\server.properties"
if (Test-Path $PropertiesPath) {
    Write-Host "[3/5] Copying configuration 'server.properties'..." -ForegroundColor Yellow
    Copy-Item -Path $PropertiesPath -Destination "$InstallDir\server.properties" -Force
} else {
    Write-Host "[3/5] Generating default server.properties..." -ForegroundColor Yellow
    @"
node.id=1
bind.address=0.0.0.0:$BrokerPort
metrics.bind.address=0.0.0.0:$MetricsPort
data.dir=$InstallDir\data
log.file.dir=$InstallDir\logs
cleanup.policy=delete
retention.bytes=10737418240
"@ | Out-File -FilePath "$InstallDir\server.properties" -Encoding UTF8
}

# 4. Windows Defender Firewall Configuration
Write-Host "[4/5] Configuring Windows Defender Firewall rules..." -ForegroundColor Yellow
Remove-NetFirewallRule -DisplayName "Hermes Event Store (Broker)" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "Hermes Event Store (Broker)" -Direction Inbound -Protocol TCP -LocalPort $BrokerPort -Action Allow -ErrorAction SilentlyContinue | Out-Null

Remove-NetFirewallRule -DisplayName "Hermes Metrics (Prometheus)" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "Hermes Metrics (Prometheus)" -Direction Inbound -Protocol TCP -LocalPort $MetricsPort -Action Allow -ErrorAction SilentlyContinue | Out-Null

# 5. Service Installation (WinSW or NSSM)
Write-Host "[5/5] Installing Service '$ServiceType'..." -ForegroundColor Yellow

if ($ServiceType -eq "WinSW") {
    Copy-Item -Path "$ScriptDir\winsw.xml" -Destination "$InstallDir\HermesEventStore.xml" -Force
    Write-Host "WinSW configuration manifest copied to '$InstallDir\HermesEventStore.xml'." -ForegroundColor Green
    Write-Host "To register WinSW wrapper service:" -ForegroundColor Green
    Write-Host "  Download WinSW.exe, rename to HermesEventStore.exe in '$InstallDir', and run:" -ForegroundColor White
    Write-Host "  '$InstallDir\HermesEventStore.exe install'" -ForegroundColor White
    Write-Host "  '$InstallDir\HermesEventStore.exe start'" -ForegroundColor White
} elseif ($ServiceType -eq "NSSM") {
    $nssm = Get-Command "nssm.exe" -ErrorAction SilentlyContinue
    if ($nssm) {
        nssm stop HermesEventStore 2>$null
        nssm remove HermesEventStore confirm 2>$null
        nssm install HermesEventStore "$InstallDir\hermes.exe" "--config $InstallDir\server.properties"
        nssm set HermesEventStore AppDirectory "$InstallDir"
        nssm set HermesEventStore AppStdout "$InstallDir\logs\hermes-service.log"
        nssm set HermesEventStore AppStderr "$InstallDir\logs\hermes-service-err.log"
        nssm set HermesEventStore AppRotateFiles 1
        nssm set HermesEventStore AppRotateBytes 20971520
        nssm set HermesEventStore AppStopMethodSkip 0
        nssm set HermesEventStore AppStopMethodConsole 1500
        nssm start HermesEventStore
        Write-Host "Hermes service successfully registered and started via NSSM." -ForegroundColor Green
    } else {
        Write-Warning "NSSM is not installed in PATH. Download nssm.exe or use WinSW."
    }
}

Write-Host "`nInstallation completed successfully!" -ForegroundColor Green
