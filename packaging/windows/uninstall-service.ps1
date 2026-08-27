# Requires -RunAsAdministrator
<#
.SYNOPSIS
    Uninstallation & Cleanup Script for Bifrox Windows Service
.DESCRIPTION
    Stops and unregisters the Bifrox background Windows Service and cleans up firewall rules.
#>

param(
    [string]$ServiceName = "BifroxEventStore"
)

$ErrorActionPreference = "Stop"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "    BIFROX WINDOWS SERVICE UNINSTALLATION & CLEANUP        " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Administrator Rights Check
$currentPrincipal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Error "This script requires Administrator privileges. Please run PowerShell as Administrator."
    exit 1
}

# 2. Stop and Remove Service
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($service) {
    Write-Host "[1/2] Stopping service '$ServiceName'..." -ForegroundColor Yellow
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue

    Write-Host "[2/2] Removing service '$ServiceName'..." -ForegroundColor Yellow
    $nssm = Get-Command "nssm.exe" -ErrorAction SilentlyContinue
    if ($nssm) {
        nssm remove $ServiceName confirm
    } else {
        sc.exe delete $ServiceName
    }
    Write-Host "Service '$ServiceName' uninstalled successfully." -ForegroundColor Green
} else {
    Write-Host "Service '$ServiceName' is not registered." -ForegroundColor Yellow
}

# 3. Clean up Firewall Rules
Remove-NetFirewallRule -DisplayName "Bifrox Event Store (Broker)" -ErrorAction SilentlyContinue
Remove-NetFirewallRule -DisplayName "Bifrox Metrics (Prometheus)" -ErrorAction SilentlyContinue
Write-Host "Firewall rules cleaned up." -ForegroundColor Green
