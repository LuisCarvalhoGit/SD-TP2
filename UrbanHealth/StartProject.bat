@echo off
setlocal

set DC=docker-compose
set SERVER=%DC% -f docker-compose.server.yml
set GATEWAY=%DC% -f docker-compose.gateway.yml
set SENSOR=%DC% -f docker-compose.sensor.yml

if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="restart" goto restart
if "%1"=="status" goto status
if "%1"=="logs-server" goto logs_server
if "%1"=="logs-gateway" goto logs_gateway
if "%1"=="logs-sensor" goto logs_sensor

:help
echo Available commands:
echo   .\StartProject.bat up             Starts server, gateway and sensor compose files
echo   .\StartProject.bat down           Stops all compose files
echo   .\StartProject.bat restart        Restarts all compose files
echo   .\StartProject.bat status         Shows container status
echo   .\StartProject.bat logs-server    Follows server logs
echo   .\StartProject.bat logs-gateway   Follows gateway logs
echo   .\StartProject.bat logs-sensor    Follows sensor logs
goto:eof

:up
echo Starting SERVER stack...
%SERVER% up -d --build

echo Starting GATEWAY stack...
%GATEWAY% up -d --build

echo Starting SENSOR stack...
%SENSOR% up -d --build

echo All stacks started. Services use retry/backoff while dependencies become ready.
goto:eof

:down
echo Stopping SENSOR stack...
%SENSOR% down
echo Stopping GATEWAY stack...
%GATEWAY% down
echo Stopping SERVER stack...
%SERVER% down
goto:eof

:restart
call :down
call :up
goto:eof

:status
echo SERVER:
%SERVER% ps
echo GATEWAY:
%GATEWAY% ps
echo SENSOR:
%SENSOR% ps
goto:eof

:logs_server
%SERVER% logs -f
goto:eof

:logs_gateway
%GATEWAY% logs -f
goto:eof

:logs_sensor
%SENSOR% logs -f
goto:eof
