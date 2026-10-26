@echo off
REM Ativa os recursos do Windows necessarios para o WSL 2 / Docker
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Pedindo permissao de administrador...
    powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)
echo ===== Ativando Virtual Machine Platform =====
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
echo ===== Ativando Subsistema do Windows para Linux =====
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
wsl --update
wsl --set-default-version 2
echo.
echo Pronto. REINICIE o computador (Reiniciar, nao Desligar) e abra o Docker Desktop.
pause
