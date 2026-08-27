# Bifrox Windows Service Packaging & Deployment Guide

This directory contains the production Windows Service packaging tools for **Bifrox Event Streaming Store**.

---

## Features & Capabilities

- **Service Managers Supported**: Both [WinSW (Windows Service Wrapper)](https://github.com/winsw/winsw) and [NSSM (Non-Sucking Service Manager)](https://nssm.cc/).
- **Graceful Shutdown Hooks**: Sends `CTRL_C_EVENT` / `CTRL_CLOSE_EVENT` / `SIGTERM` console signals to `bifrox.exe`, giving the storage engine up to 30 seconds to flush dirty log segments and sync indexes before termination.
- **Log Rotation & Retention**: Integrated log rotation via `tracing-appender` and service wrappers (`roll-by-size-time`, 20 MB size threshold, automatic ZIP archiving, 30 days history).
- **Configurable Metrics Exporter**: Supports custom bind address (`metrics.bind.address=0.0.0.0:10092`), Bearer token authentication (`metrics.auth.token`), and IP whitelist restrictions (`metrics.allowed.ips`).
- **Automated Firewall Rules**: Configures Windows Defender Firewall inbound rules for broker port (`9092`) and metrics port (`10092`).

---

## Deployment Instructions

### Method 1: WinSW (Recommended for Production)

1. Build the release binary:
   ```cmd
   cargo build --release
   ```

2. Download `WinSW-x64.exe` from [WinSW Releases](https://github.com/winsw/winsw/releases).

3. Run the automated PowerShell installer as Administrator:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\install-service.ps1 -InstallDir "C:\Program Files\Bifrox" -ServiceType WinSW
   ```

4. Copy `WinSW-x64.exe` to `C:\Program Files\Bifrox\BifroxEventStore.exe` and register:
   ```cmd
   "C:\Program Files\Bifrox\BifroxEventStore.exe" install
   "C:\Program Files\Bifrox\BifroxEventStore.exe" start
   ```

---

### Method 2: NSSM (Non-Sucking Service Manager)

1. Build the release binary:
   ```cmd
   cargo build --release
   ```

2. Ensure `nssm.exe` is in your system `PATH`.

3. Run the batch wrapper or PowerShell installer:
   ```cmd
   .\nssm-install.bat
   ```

---

## Configuration Settings (`server.properties`)

Add or customize Prometheus metrics listener & security settings:

```properties
# Server Listener & Storage
node.id=1
bind.addr=0.0.0.0:9092
advertised.listeners=myhost.example.com:9092
data.dir=C:\Program Files\Bifrox\data
log.file.dir=C:\Program Files\Bifrox\logs

# Prometheus Metrics Exporter Configuration
metrics.bind.address=0.0.0.0:10092
metrics.auth.token=SecretScrapeToken123
metrics.allowed.ips=127.0.0.1,10.0.0.0/8
```

**Important:** When using a wildcard bind address like `0.0.0.0`, you must provide an explicit `advertised.listeners` value that peers can use to dial back to this broker. The broker will refuse to start if the advertised address is a wildcard or unspecified. Use the machine's hostname, FQDN, or a specific LAN IP address.

---

## Service Management Commands

- **Check Service Status**:
  ```powershell
  Get-Service -Name BifroxEventStore
  ```
- **Stop Service Gracefully**:
  ```powershell
  Stop-Service -Name BifroxEventStore
  ```
- **Uninstall Service**:
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\uninstall-service.ps1
  ```
