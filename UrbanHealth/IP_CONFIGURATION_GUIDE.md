# Configuracao Hibrida de Comunicacao

Este projeto usa uma configuracao hibrida:

- Sensores -> Gateway: via IP/host configurado no JSON, passando pela porta exposta do host.
- Sensores/Gateways -> RabbitMQ: via IP/host configurado no JSON, passando pela porta exposta do host.
- Gateways -> Server: via IP/host configurado no JSON, passando pelas portas expostas do host.
- Gateway -> PreProcessing: via rede Docker, usando `http://python-preprocess:50051`.
- Server -> Analysis: via rede Docker, usando `http://python-analysis:50052`.

Ou seja, so os pares gRPC `gateway -> preprocess` e `server -> analysis` dependem de DNS/rede Docker.

## Alias `local`

Nos JSONs podes usar:

```json
"TargetGatewayIp": "local"
```

O codigo resolve automaticamente:

- fora de Docker: `local` vira `127.0.0.1`;
- dentro de Docker: `local` vira `host.docker.internal`.

Isto permite simular tudo na mesma maquina sem trocar configs. Quando quiseres separar hosts, troca `local` pelo IP real da maquina de destino.

## Onde configurar

Sensores:

- `SensorConfigs/sensor-config-S101.json`
- `SensorConfigs/sensor-config-S102.json`

Campos importantes:

```json
"Networking": {
  "TargetGatewayIp": "local",
  "TargetGatewayUdpPort": 5004,
  "TargetRabbitMqHost": "local"
}
```

Gateways:

- `GatewayConfigs/gateway-config-G101.json`
- `GatewayConfigs/gateway-config-G102.json`

Campos importantes:

```json
"Networking": {
  "ServerIp": "local",
  "ServerPort": 5001,
  "ServerUdpPort": 5003,
  "PreprocessRpcUrl": "http://python-preprocess:50051",
  "RabbitMqHost": "local"
}
```

Server:

- `ServerConfigs/server-config.json`

Campo importante:

```json
"AnalysisRpcUrl": "http://python-analysis:50052"
```

## Cenarios

### Tudo local em Docker

```powershell
.\start.bat
```

Mantem `local` nos JSONs. Os containers isolados falam entre si atraves das portas publicadas no host, exceto os dois pares gRPC autorizados.

### Sensores num host e infraestrutura noutro

Exemplo:

- Host A, sensores: `192.168.1.50`
- Host B, broker/gateways/server: `192.168.1.100`

No Host A, nos `SensorConfigs/sensor-config-S*.json`:

```json
"TargetGatewayIp": "192.168.1.100",
"TargetRabbitMqHost": "192.168.1.100"
```

No Host B, se gateway e server estiverem na mesma maquina, mantem nos `GatewayConfigs/gateway-config-G*.json`:

```json
"ServerIp": "local",
"RabbitMqHost": "local"
```

### Gateway num host e server noutro

No JSON do gateway:

```json
"ServerIp": "192.168.1.100"
```

Se o RabbitMQ tambem estiver nesse host:

```json
"RabbitMqHost": "192.168.1.100"
```

## Portas

- RabbitMQ AMQP: `5672`
- RabbitMQ Web: `15672`
- Gateway G101 UDP: `5004`
- Gateway G102 UDP: `5005` no host, `5004` dentro do container
- Server TCP: `5001`
- Server UDP video: `5003`
- Dashboard server: `8081`
- Preview gateway G101: `8080`
- Preview gateway G102: `8082`

## Checklist rapido

- Abre as portas na firewall do host que recebe ligacoes.
- Usa `local` apenas quando o destino esta na mesma maquina fisica.
- Usa IP real quando o destino esta noutra maquina.
- Mantem `python-preprocess` e `python-analysis` nos URLs gRPC quando estiveres a correr esses pares por compose.
