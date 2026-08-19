@echo off
setlocal
cd /d "%~dp0"
title Obour Youth Club Management
set "DATA_DIR=%LOCALAPPDATA%\ObourYouthClub\data"
set "BACKUP_DIR=%LOCALAPPDATA%\ObourYouthClub\backups"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
echo ==========================================
echo   Obour Youth Club Management System
echo ==========================================
echo.
echo Runtime data folder:
echo %DATA_DIR%
echo.
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed or not in PATH.
  pause
  exit /b 1
)
if not exist "node_modules\express" (
  echo Installing dependencies...
  call npm install
  if errorlevel 1 (
    echo Failed to install dependencies.
    pause
    exit /b 1
  )
)
echo Starting server on all network interfaces...
echo Open on this PC: http://localhost:3000
echo Other devices on same network: http://YOUR-PC-IP:3000
echo.
start "" "http://localhost:3000"
npm start
pause
