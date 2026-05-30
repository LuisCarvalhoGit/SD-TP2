@echo off
title Edge IoT Control Panel - UrbanHealth
color 0B

:MENU
cls
echo =======================================================
echo          URBANHEALTH - EDGE IOT CONTROL PANEL
echo =======================================================
echo.
echo   --- INICIAR ---
echo   1. Ligar TUDO (Orquestracao Completa Sequencial)
echo   2. Ligar apenas Cloud Server
echo   3. Ligar apenas Broker [RabbitMQ]
echo   4. Ligar apenas Edge Gateways
echo   5. Ligar apenas IoT Sensors
echo.
echo   --- PARAR ---
echo   6. Desligar TUDO
echo   7. Desligar apenas Cloud Server
echo   8. Desligar apenas Broker [RabbitMQ]
echo   9. Desligar apenas Edge Gateways
echo   A. Desligar apenas IoT Sensors
echo.
echo   --- GESTAO ---
echo   B. Ver Estado da Rede (Docker PS)
echo   L. Ver Logs Globais (Em tempo real)
echo   0. Limpeza Total (O "Botao de Panico")
echo   S. Sair
echo.
echo =======================================================
set /p choice="Escolhe uma opcao: "

if "%choice%"=="1" goto START_ALL
if "%choice%"=="2" goto START_SERVER
if "%choice%"=="3" goto START_BROKER
if "%choice%"=="4" goto START_GATEWAY
if "%choice%"=="5" goto START_SENSORS
if "%choice%"=="6" goto STOP_ALL
if "%choice%"=="7" goto STOP_SERVER
if "%choice%"=="8" goto STOP_BROKER
if "%choice%"=="9" goto STOP_GATEWAY
if /I "%choice%"=="A" goto STOP_SENSORS
if /I "%choice%"=="B" goto SHOW_STATUS
if /I "%choice%"=="L" goto SHOW_LOGS
if "%choice%"=="0" goto NUKE_ALL
if /I "%choice%"=="S" goto EOF

goto MENU

:START_ALL
cls
echo A iniciar orquestracao completa com verificacao de estado...
echo.

echo [1/5] A iniciar Cloud Server...
docker-compose -f docker-compose.server.yml up -d --build
:WAIT_SERVER
timeout /t 3 /nobreak > NUL
SET STATUS=starting
FOR /F "tokens=*" %%g IN ('docker inspect --format="{{.State.Status}}" csharp-server 2^>NUL') DO (SET STATUS=%%g)
if /I "%STATUS%" == "running" (
    echo [SUCESSO] Cloud Server esta ONLINE!
) else (
    echo ... a aguardar Cloud Server...
    goto WAIT_SERVER
)
echo.

echo [2/5] A iniciar o Broker de Mensagens [RabbitMQ]...
docker-compose -f docker-compose.broker.yml up -d --build
echo [!] A aguardar que o RabbitMQ fique HEALTHY [pode demorar ate 60 segundos]...
:WAIT_RABBITMQ
timeout /t 5 /nobreak > NUL
SET STATUS=starting
FOR /F "tokens=*" %%g IN ('docker inspect --format="{{.State.Health.Status}}" rabbitmq 2^>NUL') DO (SET STATUS=%%g)
if /I "%STATUS%" == "healthy" (
    echo [SUCESSO] RabbitMQ esta HEALTHY!
) else (
    echo ... estado atual: [%STATUS%]. A verificar novamente em 5s...
    goto WAIT_RABBITMQ
)
echo.

echo [3/5] A criar rede de comunicacaoo do Broker...
docker network create urban-broker 2>NUL
echo.

echo [4/5] A iniciar Edge Gateways...
docker-compose -f docker-compose.gateway.yml up -d --build
:WAIT_GATEWAY
timeout /t 3 /nobreak > NUL
SET STATUS=starting
FOR /F "tokens=*" %%g IN ('docker inspect --format="{{.State.Status}}" gateway-g101 2^>NUL') DO (SET STATUS=%%g)
if /I "%STATUS%" == "running" (
    echo [SUCESSO] Gateway G101 esta ONLINE e ligado ao RabbitMQ!
) else (
    echo ... a aguardar Gateway...
    goto WAIT_GATEWAY
)
echo.

echo [5/5] A iniciar IoT Sensors...
docker-compose -f docker-compose.sensor.yml up -d --build
echo.
echo [SUCESSO] Toda a infraestrutura esta online na sequencia perfeita!
pause
goto MENU

:START_SERVER
cls
echo A iniciar apenas a Cloud...
docker-compose -f docker-compose.server.yml up -d --build
echo [SUCESSO] Servidor Central online.
pause
goto MENU

:START_BROKER
cls
echo A iniciar apenas o Broker de Mensagens [RabbitMQ]...
docker network create urban-broker 2>NUL
docker-compose -f docker-compose.broker.yml up -d --build
echo [!] A aguardar que o RabbitMQ fique HEALTHY...
:WAIT_BROKER
timeout /t 5 /nobreak > NUL
SET STATUS=starting
FOR /F "tokens=*" %%g IN ('docker inspect --format="{{.State.Health.Status}}" rabbitmq 2^>NUL') DO (SET STATUS=%%g)
if /I "%STATUS%" == "healthy" (
    echo [SUCESSO] RabbitMQ esta HEALTHY!
) else (
    echo ... estado atual: [%STATUS%]. A verificar novamente...
    goto WAIT_BROKER
)
echo [SUCESSO] Broker online.
pause
goto MENU

:START_GATEWAY
cls
echo A iniciar Edge Gateways...
echo [NOTA] Certifica-te que o Broker [RabbitMQ] esta a correr primeiro!
docker-compose -f docker-compose.gateway.yml up -d --build
echo [SUCESSO] Gateways online.
pause
goto MENU

:START_SENSORS
cls
echo A iniciar apenas os Sensores IoT...
docker-compose -f docker-compose.sensor.yml up -d --build
echo [SUCESSO] Sensores online.
pause
goto MENU

:STOP_ALL
cls
echo A desligar infraestrutura completa...
echo.
echo [1/4] A desligar Sensores...
docker-compose -f docker-compose.sensor.yml down
echo [SUCESSO] Sensores desligados.
echo.
echo [2/4] A desligar Gateways...
docker-compose -f docker-compose.gateway.yml down
echo [SUCESSO] Gateways desligados.
echo.
echo [3/4] A desligar Servidor Central...
docker-compose -f docker-compose.server.yml down
echo [SUCESSO] Servidor Central desligado.
echo.
echo [4/4] A desligar Broker...
docker-compose -f docker-compose.broker.yml down
echo [SUCESSO] Broker desligado.
echo.
echo [SUCESSO] Tudo desligado corretamente!
pause
goto MENU

:STOP_SERVER
cls
echo A desligar a Cloud...
docker-compose -f docker-compose.server.yml down
echo [SUCESSO] Servidor Central desligado.
pause
goto MENU

:STOP_BROKER
cls
echo A desligar o Broker de Mensagens...
docker-compose -f docker-compose.broker.yml down
echo [SUCESSO] Broker desligado.
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

:SHOW_LOGS
cls
echo =======================================================
echo               CENTRAL DE LOGS GLOBAIS
echo =======================================================
echo [!] Pressiona CTRL+C para parar de seguir os logs.
echo Se o Windows perguntar "Terminar o ficheiro batch (S/N)?", escreve N para voltar ao menu.
echo.
docker-compose -f docker-compose.broker.yml -f docker-compose.server.yml -f docker-compose.gateway.yml -f docker-compose.sensor.yml logs -f --tail=30
pause
goto MENU

:NUKE_ALL
cls
echo AVISO: Isto vai forcar a paragem e apagar TODOS os contentores da tua maquina!
set /p confirm="Tens a certeza? [S/N]: "
if /I "%confirm%" neq "S" goto MENU
FOR /f "tokens=*" %%i IN ('docker ps -aq') DO docker rm -f %%i
docker network prune -f
echo.
echo [SUCESSO] Ambiente completamente limpo.
pause
goto MENU

:EOF
exit