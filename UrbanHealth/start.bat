@echo off
title Edge IoT Control Panel - UrbanHealth
color 0B

:MENU
cls
echo =======================================================
echo                      CONTROL PANEL
echo =======================================================
echo.
echo   --- INICIAR ---
echo   1. Ligar TUDO (Orquestracao Completa)
echo   2. Ligar apenas Cloud Server
echo   3. Ligar apenas Edge Gateways
echo   4. Ligar apenas IoT Sensors
echo.
echo   --- PARAR ---
echo   5. Desligar TUDO
echo   6. Desligar apenas Cloud Server
echo   7. Desligar apenas Edge Gateways
echo   8. Desligar apenas IoT Sensors
echo.
echo   --- GESTAO ---
echo   9. Ver Estado da Rede (Docker PS)
echo   0. Limpeza Total (O "Botao de Panico")
echo   S. Sair
echo.
echo =======================================================
set /p choice="Escolhe uma opcao: "

if "%choice%"=="1" goto START_ALL
if "%choice%"=="2" goto START_SERVER
if "%choice%"=="3" goto START_GATEWAY
if "%choice%"=="4" goto START_SENSORS
if "%choice%"=="5" goto STOP_ALL
if "%choice%"=="6" goto STOP_SERVER
if "%choice%"=="7" goto STOP_GATEWAY
if "%choice%"=="8" goto STOP_SENSORS
if "%choice%"=="9" goto SHOW_STATUS
if "%choice%"=="0" goto NUKE_ALL
if /I "%choice%"=="S" goto EOF

goto MENU

:START_ALL
cls
echo A iniciar orquestracao completa...
echo [1/3] Cloud Server...
docker-compose -f docker-compose.server.yml up -d --build
timeout /t 3 /nobreak > NUL
echo [2/3] Edge Gateways...
docker-compose -f docker-compose.gateway.yml up -d --build
timeout /t 3 /nobreak > NUL
echo [3/3] IoT Sensors...
docker-compose -f docker-compose.sensor.yml up -d --build
echo.
echo [SUCESSO] Toda a infraestrutura esta online!
pause
goto MENU

:START_SERVER
cls
echo A iniciar apenas a Cloud...
docker-compose -f docker-compose.server.yml up -d --build
echo [SUCESSO] Servidor Central online (Dashboard em localhost:8081).
pause
goto MENU

:START_GATEWAY
cls
echo A iniciar apenas os Gateways Edge...
docker-compose -f docker-compose.gateway.yml up -d --build
echo [SUCESSO] Gateways online. A aguardar conexoes...
pause
goto MENU

:START_SENSORS
cls
echo A iniciar apenas os Sensores IoT...
docker-compose -f docker-compose.sensor.yml up -d --build
echo [SUCESSO] Sensores online. A emitir telemetria...
pause
goto MENU

:STOP_ALL
cls
echo A desligar infraestrutura...
docker-compose -f docker-compose.sensor.yml down
docker-compose -f docker-compose.gateway.yml down
docker-compose -f docker-compose.server.yml down
echo [SUCESSO] Tudo desligado.
pause
goto MENU

:STOP_SERVER
cls
echo A desligar a Cloud...
docker-compose -f docker-compose.server.yml down
echo [SUCESSO] Servidor Central desligado.
pause
goto MENU

:STOP_GATEWAY
cls
echo A desligar Gateways Edge...
docker-compose -f docker-compose.gateway.yml down
echo [SUCESSO] Gateways desligados.
pause
goto MENU

:STOP_SENSORS
cls
echo A desligar Sensores IoT...
docker-compose -f docker-compose.sensor.yml down
echo [SUCESSO] Sensores desligados.
pause
goto MENU

:SHOW_STATUS
cls
echo ================= CONTENTORES ATIVOS =================
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo =======================================================
pause
goto MENU

:NUKE_ALL
cls
echo AVISO: Isto vai forcar a paragem e apagar TODOS os contentores da tua maquina!
set /p confirm="Tens a certeza? (S/N): "
if /I "%confirm%" neq "S" goto MENU
FOR /f "tokens=*" %%i IN ('docker ps -aq') DO docker rm -f %%i
docker network prune -f
echo.
echo [SUCESSO] Ambiente completamente limpo.
pause
goto MENU

:EOF
exit