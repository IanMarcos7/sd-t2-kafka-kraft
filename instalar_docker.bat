@echo off
REM Instala WSL 2, Docker Desktop e Git (Trabalho 2 - Sistemas Distribuidos)

REM --- Pede permissao de administrador se ainda nao tiver ---
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Pedindo permissao de administrador...
    powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

echo.
echo ===== 1/4 Instalando WSL 2 =====
wsl --install --no-distribution
wsl --update
wsl --set-default-version 2

echo.
echo ===== 2/4 Instalando Docker Desktop =====
winget install -e --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements

echo.
echo ===== 3/4 Instalando Git =====
winget install -e --id Git.Git --accept-source-agreements --accept-package-agreements

echo.
echo ===== 4/4 Pronto =====
echo REINICIE o computador agora.
echo Depois abra o Docker Desktop, aceite os termos e espere "Engine running".
echo Para testar, abra um terminal novo e rode:  docker run hello-world
echo.
pause
