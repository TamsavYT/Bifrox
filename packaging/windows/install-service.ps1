# Requires -RunAsAdministrator
<#
.SYNOPSIS
    Automated Installation & Packaging Script for Bifrox Windows Service
.DESCRIPTION
    Deploys Bifrox as a production background Windows Service using WinSW or NSSM.
    Configures log rotation, graceful shutdown hooks, and Windows Defender Firewall rules.
.EXAMPLE
    .\install-service.ps1 -InstallDir "C:\Program Files\Bifrox" -ServiceType WinSW
#>

param(
    [string]$InstallDir = "C:\Program Files\Bifrox",
    [ValidateSet("WinSW", "NSSM")]
    [string]$ServiceType = "WinSW",
    [int]$BrokerPort = 9092,
    [int]$MetricsPort = 10092,
    [string]$AdvertisedHost = $env:COMPUTERNAME
)

$ErrorActionPreference = "Stop"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "    BIFROX WINDOWS SERVICE INSTALLATION & PACKAGING        " -ForegroundColor Cyan
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
$BinPath = "$ProjectRoot\target\release\bifrox.exe"
if (-not (Test-Path $BinPath)) {
    $BinPath = "$ProjectRoot\target\debug\bifrox.exe"
}

if (Test-Path $BinPath) {
    Write-Host "[2/5] Copying binary from '$BinPath'..." -ForegroundColor Yellow
    Copy-Item -Path $BinPath -Destination "$InstallDir\bifrox.exe" -Force
} else {
    Write-Warning "Bifrox binary not found at target build path. Please compile with 'cargo build --release' first."
}

$PropertiesPath = "$ProjectRoot\server.properties"
if (Test-Path $PropertiesPath) {
    Write-Host "[3/5] Copying configuration 'server.properties'..." -ForegroundColor Yellow
    Copy-Item -Path $PropertiesPath -Destination "$InstallDir\server.properties" -Force
} else {
    Write-Host "[3/5] Generating default server.properties..." -ForegroundColor Yellow
    @"
node.id=1
# Bind to all interfaces, but advertise a specific dialable address to peers
# (the broker requires an explicit advertised address, not a wildcard)
bind.address=0.0.0.0:$BrokerPort
advertised.listeners=$AdvertisedHost:$BrokerPort
metrics.bind.address=0.0.0.0:$MetricsPort
data.dir=$InstallDir\data
log.file.dir=$InstallDir\logs
cleanup.policy=delete
retention.bytes=10737418240
"@ | Out-File -FilePath "$InstallDir\server.properties" -Encoding UTF8
}

# 4. Windows Defender Firewall Configuration
Write-Host "[4/5] Configuring Windows Defender Firewall rules..." -ForegroundColor Yellow
Remove-NetFirewallRule -DisplayName "Bifrox Event Store (Broker)" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "Bifrox Event Store (Broker)" -Direction Inbound -Protocol TCP -LocalPort $BrokerPort -Action Allow -ErrorAction SilentlyContinue | Out-Null

Remove-NetFirewallRule -DisplayName "Bifrox Metrics (Prometheus)" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "Bifrox Metrics (Prometheus)" -Direction Inbound -Protocol TCP -LocalPort $MetricsPort -Action Allow -ErrorAction SilentlyContinue | Out-Null

# 5. Service Installation (WinSW or NSSM)
Write-Host "[5/5] Installing Service '$ServiceType'..." -ForegroundColor Yellow

if ($ServiceType -eq "WinSW") {
    Copy-Item -Path "$ScriptDir\winsw.xml" -Destination "$InstallDir\BifroxEventStore.xml" -Force
    Write-Host "WinSW configuration manifest copied to '$InstallDir\BifroxEventStore.xml'." -ForegroundColor Green
    Write-Host "To register WinSW wrapper service:" -ForegroundColor Green
    Write-Host "  Download WinSW.exe, rename to BifroxEventStore.exe in '$InstallDir', and run:" -ForegroundColor White
    Write-Host "  '$InstallDir\BifroxEventStore.exe install'" -ForegroundColor White
    Write-Host "  '$InstallDir\BifroxEventStore.exe start'" -ForegroundColor White
} elseif ($ServiceType -eq "NSSM") {
    $nssm = Get-Command "nssm.exe" -ErrorAction SilentlyContinue
    if ($nssm) {
        nssm stop BifroxEventStore 2>$null
        nssm remove BifroxEventStore confirm 2>$null
        nssm install BifroxEventStore "$InstallDir\bifrox.exe" "--config $InstallDir\server.properties"
        nssm set BifroxEventStore AppDirectory "$InstallDir"
        nssm set BifroxEventStore AppStdout "$InstallDir\logs\bifrox-service.log"
        nssm set BifroxEventStore AppStderr "$InstallDir\logs\bifrox-service-err.log"
        nssm set BifroxEventStore AppRotateFiles 1
        nssm set BifroxEventStore AppRotateBytes 20971520
        nssm set BifroxEventStore AppStopMethodSkip 0
        nssm set BifroxEventStore AppStopMethodConsole 1500
        nssm start BifroxEventStore
        Write-Host "Bifrox service successfully registered and started via NSSM." -ForegroundColor Green
    } else {
        Write-Warning "NSSM is not installed in PATH. Download nssm.exe or use WinSW."
    }
}

Write-Host "`nInstallation completed successfully!" -ForegroundColor Green
