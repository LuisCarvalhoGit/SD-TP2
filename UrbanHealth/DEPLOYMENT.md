# UrbanHealth Deployment

## Modelo atual

A infraestrutura esta preparada para correr de forma hibrida:

- tudo localmente, com Docker Compose;
- componentes em hosts diferentes, usando IPs reais nos ficheiros JSON;
- comunicacao local no mesmo host atraves das portas publicadas no host;
- rede/DNS Docker apenas para `gateway -> python-preprocess` e `server -> python-analysis`.

Para detalhes de IPs, portas e cenarios, ver `IP_CONFIGURATION_GUIDE.md`.

## Ficheiros principais

| Ficheiro | Funcao |
| --- | --- |
| `docker-compose.yml` | Simulacao local completa |
| `docker-compose.broker.yml` | RabbitMQ isolado |
| `docker-compose.gateway.yml` | Gateways + PreProcessing |
| `docker-compose.server.yml` | Server + Analysis |
| `docker-compose.sensor.yml` | Sensores |
| `SensorConfigs/sensor-config-S*.json` | Destino do gateway e RabbitMQ por sensor |
| `GatewayConfigs/gateway-config-G*.json` | Destino do server, RabbitMQ e PreProcessing por gateway |
| `ServerConfigs/server-config.json` | Portas do server e destino do Analysis |

## Arranque local completo

```powershell
docker compose up --build
```

Tambem podes usar:

```powershell
.\start.bat
```

No modo local, mantem `local` nos JSONs. Dentro de containers, `local` e resolvido para `host.docker.internal`; fora de Docker, e resolvido para `127.0.0.1`.

## Arranque separado

Broker:

```powershell
docker compose --env-file .env.distributed.broker -f docker-compose.broker.yml up -d --build
```

Server + Analysis:

```powershell
docker compose --env-file .env.distributed.server -f docker-compose.server.yml up -d --build
```

Gateways + PreProcessing:

```powershell
docker compose --env-file .env.distributed.gateway -f docker-compose.gateway.yml up -d --build
```

Sensores:

```powershell
docker compose --env-file .env.distributed.sensor -f docker-compose.sensor.yml up -d --build
```

## Distribuir por hosts

Se um destino estiver na mesma maquina fisica, usa `local` no JSON.

Se estiver noutra maquina, usa o IP real dessa maquina:

```json
"TargetGatewayIp": "192.168.1.100",
"TargetRabbitMqHost": "192.168.1.100"
```

ou, no gateway:

```json
"ServerIp": "192.168.1.100",
"RabbitMqHost": "192.168.1.100"
```

Mantem estes endpoints com nomes Docker quando esses pares correm juntos por compose:

```json
"PreprocessRpcUrl": "http://python-preprocess:50051"
```

```json
"AnalysisRpcUrl": "http://python-analysis:50052"
```

## Verificacao

```powershell
docker ps
docker logs -f rabbitmq
docker logs -f gateway-g101
docker logs -f csharp-server
```

Health RabbitMQ:

```powershell
docker exec rabbitmq rabbitmq-diagnostics check_running
```

Dashboard:

```text
http://localhost:8081
```

RabbitMQ management:

```text
http://localhost:15672
```

## Troubleshooting rapido

- `Connection refused` para RabbitMQ: confirma que o host configurado em `RabbitMqHost` ou `TargetRabbitMqHost` esta correto e que a porta `5672` esta aberta.
- Gateway nao chega ao server: confirma `ServerIp`, `ServerPort=5001` e `ServerUdpPort=5003`.
- Sensor nao envia video: confirma `TargetGatewayIp` e `TargetGatewayUdpPort`; para G102, a porta publicada no host e `5005`.
- Erro de DNS em `python-preprocess` ou `python-analysis`: confirma que o gateway/server esta no compose correto com o microservico Python correspondente.
