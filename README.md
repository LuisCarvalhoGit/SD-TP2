# Sistema Distribuído de Monitorização Urbana

UrbanHealth é uma plataforma de monitorização ambiental urbana baseada numa arquitetura **Edge-to-Cloud** distribuída. Sensores IoT recolhem dados ambientais em tempo real (temperatura, humidade, qualidade do ar, vídeo, etc.), que são processados por gateways de borda e enviados para um servidor central, onde ficam disponíveis num dashboard web.

O projeto foi desenvolvido no âmbito da cadeira de **Sistemas Distribuídos** e implementa conceitos como comunicação TCP/UDP, mensageria assíncrona com RabbitMQ, microserviços gRPC em Python, orquestração com Docker Compose e persistência em SQLite.

---

## Arquitetura

```
                                                         ┌──────────────────┐
                                                         │  Dashboard Web   │
                                                         │  http://...:8081 │
                                                         └──────────────────┘
                                                                  │ 
                                                                  │ API do Servidor
                                                                  │
 ┌─────────────┐    UDP     ┌──────────────┐   TCP/UDP   ┌──────────────────┐  
 │  Sensores   │ ─────────► │  Gateways    │ ──────────► │ Servidor Central │
 │             │  RabbitMQ  │              │             │ (csharp-server)  │
 └─────────────┘ ─────────► └──────┬───────┘             └────────┬─────────┘
                                   │ gRPC                         │ gRPC
                           ┌───────▼─────────┐           ┌────────▼─────────┐
                           │python-preprocess│           │  python-analysis │
                           │   (gRPC :50051) │           │   (gRPC :50052)  │
                           └─────────────────┘           └──────────────────┘
```

O sistema divide-se em quatro camadas principais:

**Sensores** — Simulam dispositivos IoT que recolhem dados ambientais (TEMP, HUM, CO2, NOISE, PM2, UV, VIDEO) e os enviam via RabbitMQ para o gateways, com a exceção do streaming de video, que é feita diretamente por UDP. Cada sensor tem a sua própria configuração JSON.

**Gateways** — Fazem a agregação e pré-processamento dos dados. Cada gateway recebe leituras dos sensores, invoca o microserviço `python-preprocess` via gRPC para normalização/validação, e reencaminha os dados para o servido. Implementam **Store-and-Forward** para lidar com falhas de conectividade.

**Servidor** — Recebe os dados dos gateways, invoca o microserviço `python-analysis` via gRPC para análise estatística, persiste tudo numa base de dados SQLite e serve um dashboard web em tempo real.

**Microserviços Python** — Dois serviços gRPC independentes: `python-preprocess` (co-localizado com o gateway) e `python-analysis` (co-localizado com o servidor), responsáveis pelo processamento e análise dos dados.

---

## Tecnologias

| Camada | Tecnologia |
|---|---|
| Sensores & Gateway & Servidor | C# / .NET |
| Microserviços de processamento | Python 3 |
| Comunicação Sensor → Gateway | RabbitMQ + UDP (vídeo) |
| Comunicação Gateway → Servidor | TCP + UDP (vídeo) |
| Comunicação com microserviços | gRPC / Protocol Buffers |
| Base de dados | SQLite |
| Containerização | Docker + Docker Compose |
| Dashboard web | HTML/JS (servido pelo servidor) |

---

## Estrutura do Repositório

```
UrbanHealth/
├── Sensor/                  # Projeto C# do simulador de sensores
├── Gateway/                 # Projeto C# do gateway de borda
├── Gateway.Tests/           # Projeto C# de testes de funcionalidades do gateway
├── Server/                  # Projeto C# do servidor central + dashboard (index.html)
├── Server.Tests/            # Projeto C# de testes de funcionalidades do Server
├── Services/
│   ├── PreProcessing/       # Microserviço Python (gRPC) para pré-processamento
│   └── Analysis/            # Microserviço Python (gRPC) para análise estatística
├── Shared/                  # Biblioteca C# com tipos partilhados (Message, etc.)
├── Shared.Tests/            # Projeto C# de testes de funcionalidades dos recursos partilhados
├── Contracts/               # Definições .proto para os serviços gRPC
├── SensorConfigs/           # Configurações JSON por sensor (S101, S102)
├── GatewayConfigs/          # Configurações JSON por gateway (G101, G102)
├── ServerConfigs/           # Configuração JSON do servidor
├── docker-compose.broker.yml
├── docker-compose.sensor.yml
├── docker-compose.gateway.yml
├── docker-compose.server.yml
├── .env.distributed.sensor
├── .env.distributed.gateway
├── .env.distributed.server
├── .env.distributed.broker
├── start.bat                # Painel de controlo interativo (Windows)
├── DEPLOYMENT.md            # Guia de deployment detalhado
└── IP_CONFIGURATION_GUIDE.md  # Guia de configuração de IPs e portas
```

---

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (com Docker Compose)
- Git

Não é necessário ter .NET SDK nem Python instalados localmente — tudo corre dentro de containers Docker.

---

## Início Rápido (Tudo Local)

### Windows — Painel de Controlo Interativo

A forma mais simples de arrancar o sistema é usar o script `start.bat`, que apresenta um menu interativo:

```powershell
cd UrbanHealth
.\start.bat
```

No menu, escolhe a opção **1** para ligar tudo de forma sequencial e automática (Server → RabbitMQ → Gateways → Sensores).

### Arranque Manual por Componente

Se preferires controlar cada componente individualmente, podes usar os comandos abaixo a partir da pasta `UrbanHealth/`:

```powershell
# 1. Servidor Central (inclui python-analysis)
docker compose -f docker-compose.server.yml up -d --build

# 2. Broker de Mensagens (RabbitMQ)
docker compose -f docker-compose.broker.yml up -d --build

# 3. Gateways (inclui python-preprocess)
docker compose -f docker-compose.gateway.yml up -d --build

# 4. Sensores
docker compose -f docker-compose.sensor.yml up -d --build
```

> **Importante:** Respeita a ordem de arranque acima. Os gateways dependem do RabbitMQ estar saudável antes de se ligarem.

---

## Interfaces Disponíveis

Após o arranque completo, os seguintes endpoints ficam acessíveis:

| Interface | URL |
|---|---|
| Dashboard Web (dados em tempo real) | `http://localhost:8081` |
| Preview de vídeo — Gateway G101 | `http://localhost:8080` |
| Preview de vídeo — Gateway G102 | `http://localhost:8082` |
| RabbitMQ Management | `http://localhost:15672` (guest/guest) |

---

## Configuração

### Sensores (`SensorConfigs/sensor-config-S*.json`)

```json
{
  "SupportedTypes": ["TEMP", "HUM", "CO2", "NOISE", "PM2", "UV", "VIDEO"],
  "FrequencySeconds": 5,
  "Networking": {
    "TargetGatewayIp": "local",
    "TargetGatewayUdpPort": 5004,
    "TargetRabbitMqHost": "local"
  }
}
```

### Gateways (`GatewayConfigs/gateway-config-G*.json`)

```json
{
  "Networking": {
    "ServerIp": "local",
    "ServerPort": 5001,
    "ServerUdpPort": 5003,
    "PreprocessRpcUrl": "http://python-preprocess:50051",
    "RabbitMqHost": "local"
  }
}
```

O alias `local` é resolvido automaticamente:
- **Fora de Docker** → `127.0.0.1`
- **Dentro de Docker** → `host.docker.internal`

Para correr componentes em máquinas diferentes, substitui `local` pelo IP real da máquina de destino. Consulta o [`IP_CONFIGURATION_GUIDE.md`](UrbanHealth/IP_CONFIGURATION_GUIDE.md) para cenários detalhados.

---

## Portas do Sistema

| Serviço | Porta | Protocolo |
|---|---|---|
| Servidor Central (TCP) | 5001 | TCP |
| Servidor Central (Vídeo UDP) | 5003 | UDP |
| Dashboard Web | 8081 | HTTP |
| Gateway G101 (UDP) | 5004 | UDP |
| Gateway G101 (Preview) | 8080 | HTTP |
| Gateway G102 (UDP) | 5005 (host) | UDP |
| Gateway G102 (Preview) | 8082 | HTTP |
| RabbitMQ (AMQP) | 5672 | TCP |
| RabbitMQ (Web) | 15672 | HTTP |
| python-preprocess (gRPC) | 50051 | gRPC |
| python-analysis (gRPC) | 50052 | gRPC |

---

## Testes

O projeto inclui testes unitários para os componentes C#:

```bash
cd UrbanHealth
dotnet test
```

Os projetos de teste estão em `Gateway.Tests/`, `Server.Tests/` e `Shared.Tests/`.

---

## Gestão dos Containers

```powershell
# Ver estado de todos os containers
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Ver logs em tempo real de todos os serviços
docker compose -f docker-compose.broker.yml -f docker-compose.server.yml \
               -f docker-compose.gateway.yml -f docker-compose.sensor.yml logs -f

# Logs de um serviço específico
docker logs -f csharp-server
docker logs -f gateway-g101
docker logs -f rabbitmq

# Verificar saúde do RabbitMQ
docker exec rabbitmq rabbitmq-diagnostics check_running

# Parar tudo
docker compose -f docker-compose.sensor.yml down
docker compose -f docker-compose.gateway.yml down
docker compose -f docker-compose.server.yml down
docker compose -f docker-compose.broker.yml down
```

---

## Resolução de Problemas

**`Connection refused` para o RabbitMQ**
Verifica se o `RabbitMqHost` / `TargetRabbitMqHost` nos ficheiros JSON está correto e se a porta `5672` está acessível. O RabbitMQ pode demorar até 60 segundos a ficar healthy.

**Gateway não consegue chegar ao servidor**
Confirma os campos `ServerIp`, `ServerPort` (5001) e `ServerUdpPort` (5003) no ficheiro de configuração do gateway.

**Sensor não envia vídeo**
Verifica `TargetGatewayIp` e `TargetGatewayUdpPort` no config do sensor. Para o G102, a porta publicada no host é `5005` (mapeada internamente para `5004`).

**Erro de DNS em `python-preprocess` ou `python-analysis`**
Estes serviços são resolvidos por DNS interno do Docker. Garante que o gateway e o seu `python-preprocess` fazem parte do mesmo `docker-compose.gateway.yml`, e que o servidor e o `python-analysis` fazem parte do mesmo `docker-compose.server.yml`.

---

## Documentação Adicional

- [`DEPLOYMENT.md`](UrbanHealth/DEPLOYMENT.md) — Guia completo de deployment (local, híbrido e multi-host)
- [`IP_CONFIGURATION_GUIDE.md`](UrbanHealth/IP_CONFIGURATION_GUIDE.md) — Configuração de IPs, portas e cenários de rede