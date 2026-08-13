@echo off
REM Automated NSSM Installation Script for Hermes Windows Service
echo ============================================================
echo      HERMES SERVICE INSTALLATION (NSSM BATCH WRAPPER)      
echo ============================================================

set SERVICE_NAME=HermesEventStore
set APP_DIR=%~dp0..
set BIN_PATH=%APP_DIR%\target\release\hermes.exe
set CONFIG_PATH=%APP_DIR%\server.properties
set LOG_DIR=%APP_DIR%\logs

if not exist "%BIN_PATH%" (
    echo [ERROR] Binary not found at %BIN_PATH%. Please run 'cargo build --release' first.
    exit /b 1
)

if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

echo [1/4] Installing %SERVICE_NAME% service via NSSM...
nssm stop %SERVICE_NAME% >nul 2>&1
nssm remove %SERVICE_NAME% confirm >nul 2>&1
nssm install %SERVICE_NAME% "%BIN_PATH%" "--config %CONFIG_PATH%"

echo [2/4] Configuring service parameters and working directory...
nssm set %SERVICE_NAME% AppDirectory "%APP_DIR%"
nssm set %SERVICE_NAME% AppStdout "%LOG_DIR%\hermes-nssm.log"
nssm set %SERVICE_NAME% AppStderr "%LOG_DIR%\hermes-nssm-err.log"

echo [3/4] Configuring log rotation (20MB threshold) and graceful stop signals...
nssm set %SERVICE_NAME% AppRotateFiles 1
nssm set %SERVICE_NAME% AppRotateBytes 20971520
nssm set %SERVICE_NAME% AppStopMethodSkip 0
nssm set %SERVICE_NAME% AppStopMethodConsole 1500
nssm set %SERVICE_NAME% AppStopMethodWindow 1500
nssm set %SERVICE_NAME% AppStopMethodThreads 1500

echo [4/4] Starting service %SERVICE_NAME%...
nssm start %SERVICE_NAME%
echo Service %SERVICE_NAME% started successfully!
pause
