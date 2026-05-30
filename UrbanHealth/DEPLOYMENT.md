# UrbanHealth - Deployment Guide

## Arquitetura Separada: Broker & Gateway

A partir desta versão, o **Broker (RabbitMQ)** foi separado do **Gateway** para permitir deployments distribuídos e escaláveis.

### Estrutura de Deployment

```
┌─────────────────────────────────────────────────────────────┐
│                     CLOUD SERVER                             │
│  (docker-compose.server.yml)                                │
│  ├── Python Analysis Service                                │
│  └── C# Central Server                                      │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ (TCP/UDP)
                              │
┌─────────────────────────────────────────────────────────────┐
│                  BROKER (RabbitMQ)                           │
│  (docker-compose.broker.yml)                                │
│  └── RabbitMQ (urban-broker network)                        │
└─────────────────────────────────────────────────────────────┘
            ▲                                   ▲
            │ (AMQP)                           │ (AMQP)
            │                                   │
    ┌───────────────┐                    ┌───────────────┐
    │  EDGE ZONE 1  │                    │  EDGE ZONE 2  │
    │               │                    │               │
    │ (docker-comp) │                    │ (docker-comp) │
    │ .gateway.yml  │                    │ .gateway.yml  │
    │               │                    │               │
    │ ├─ Gateway    │                    │ ├─ Gateway    │
    │ │  G101       │                    │ │  G102       │
    │ └─ Preprocess │                    │ └─ Preprocess │
    │   Service     │                    │   Service     │
    └───────────────┘                    └───────────────┘
            ▲                                   ▲
            │ (UDP)                            │ (UDP)
            │                                   │
    ┌───────────────┐                    ┌───────────────┐
    │ Sensors (S1) │                    │ Sensors (S2) │
    │ (docker-comp)│                    │ (docker-comp)│
    │ .sensor.yml  │                    │ .sensor.yml  │
    └───────────────┘                    └───────────────┘
```

---

## Ficheiros de Configuração

### Docker Compose Files

| Ficheiro | Descrição | Rede |
|----------|-----------|------|
| `docker-compose.broker.yml` | RabbitMQ Central | `urban-broker` |
| `docker-compose.gateway.yml` | Gateways + Preprocess Service | `urban-edge` |
| `docker-compose.server.yml` | Servidor Central + Analysis | `urban-cloud` |
| `docker-compose.sensor.yml` | Sensores IoT | `urban-sensor` |

### Environment Files

| Ficheiro | Propósito |
|----------|-----------|
| `.env.local` | LOCAL mode - todos os serviços no mesmo compose |
| `.env.distributed.broker` | Broker (RabbitMQ) - configurações do broker |
| `.env.distributed.gateway` | Gateway - aponta para broker externo |
| `.env.distributed.server` | Server - análise e servidor central |
| `.env.distributed.sensor` | Sensor - configurações dos sensores |

---

## Como Usar

### 1️⃣ Local Development (Tudo integrado)

```bash
# Ligar tudo na mesma rede
docker-compose up --build

# Parar tudo
docker-compose down
```

**Usa**: `.env.local`  
**Para**: Testes rápidos, debug no mesmo PC

---

### 2️⃣ Distributed Mode (Separado por camadas)

#### **Opção A: Tudo automático (recomendado)**

```bash
# Menu interativo
start.bat
# Escolher opção 1 para ligar tudo, ou opções individuais
```

#### **Opção B: Manual passo a passo**

##### **Passo 1: Broker**
```bash
# Criar rede do broker
docker network create urban-broker

# Ligar RabbitMQ
docker-compose -f docker-compose.broker.yml up -d --build

# Verificar se está saudável
docker-compose -f docker-compose.broker.yml logs rabbitmq
```

**Variáveis**: `.env.distributed.broker`

##### **Passo 2: Cloud Server**
```bash
# Ligar servidor central
docker-compose -f docker-compose.server.yml up -d --build

# Verificar
docker-compose -f docker-compose.server.yml logs csharp-server
```

**Variáveis**: `.env.distributed.server`

##### **Passo 3: Gateways (Edge)**
```bash
# Ligar os gateways (comunicam com RabbitMQ externo)
docker-compose -f docker-compose.gateway.yml up -d --build

# Verificar
docker-compose -f docker-compose.gateway.yml logs gateway-g101
```

**Variáveis**: `.env.distributed.gateway`

##### **Passo 4: Sensores**
```bash
# Ligar os sensores
docker-compose -f docker-compose.sensor.yml up -d --build

# Verificar
docker-compose -f docker-compose.sensor.yml logs sensor-s101
```

**Variáveis**: `.env.distributed.sensor`

---

## Configuração para Deployments em Hosts Diferentes

Se o **Broker** estiver em outro host (IP diferente):

### 1. No Host do Broker
```bash
# Criar rede compartilhada
docker network create urban-broker --driver bridge

# Ligar RabbitMQ
docker-compose -f docker-compose.broker.yml up -d --build
```

### 2. No Host dos Gateways

Editar `.env.distributed.gateway`:
```env
# Mudar RABBITMQ_HOST de host.docker.internal para o IP real
RABBITMQ_HOST=192.168.1.100    # IP do host do broker
```

Depois ligar:
```bash
docker-compose -f docker-compose.gateway.yml up -d --build
```

### 3. No Host dos Sensores

Editar `.env.distributed.sensor`:
```env
# Mudar GATEWAY_IP para apontar para o gateway correto
GATEWAY_IP=192.168.1.101       # IP do gateway na sua zona
RABBITMQ_HOST=192.168.1.100    # IP do broker (opcional para sensores)
```

---

## Verificações Rápidas

### Status dos Contentores
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### Redes Existentes
```bash
docker network ls
```

### Logs em Tempo Real
```bash
# Todos
docker-compose -f docker-compose.broker.yml -f docker-compose.gateway.yml \
                -f docker-compose.server.yml -f docker-compose.sensor.yml logs -f

# Específico
docker-compose -f docker-compose.gateway.yml logs -f gateway-g101
```

### Verificar Saúde do RabbitMQ
```bash
docker exec rabbitmq rabbitmq-diagnostics check_running
docker exec rabbitmq rabbitmq-diagnostics -q status
```

---

## Troubleshooting

### Problema: Gateway não conecta ao RabbitMQ

**Verificar**:
1. RabbitMQ está `healthy`?
   ```bash
   docker inspect --format="{{.State.Health.Status}}" rabbitmq
   ```

2. RABBITMQ_HOST está correto em `.env.distributed.gateway`?
   ```bash
   docker logs gateway-g101 | grep "rabbitmq"
   ```

3. Firewall permite comunicação na porta 5672?
   ```bash
   docker exec gateway-g101 nc -zv rabbitmq 5672
   ```

### Problema: Redes isoladas

**Verificar se as redes existem**:
```bash
docker network ls
```

**Conectar contentores a redes**:
```bash
# Se precisar reconectar manualmente
docker network connect urban-broker gateway-g101
```

### Problema: Porta já em uso

```bash
# Encontrar o processo
netstat -ano | findstr :5672

# Ou forçar kill
docker-compose down -v  # Remove volumes também
```

---

## Ports por Defecto

| Serviço | Porta | Protocolo |
|---------|-------|-----------|
| RabbitMQ AMQP | 5672 | TCP |
| RabbitMQ Web | 15672 | HTTP |
| Server HTTP | 5001 | TCP |
| Server UDP | 5003 | UDP |
| Gateway UDP | 5004 | UDP |
| Gateway Web | 8080 | HTTP |
| Python Preprocess | 50051 | gRPC |
| Python Analysis | 50052 | gRPC |
| Dashboard | 8081 | HTTP |

---

## Exemplo Prático: 2 Zonas em Hosts Separados

```
Host A (Broker + Server)
├─ docker-compose.broker.yml  (RabbitMQ → 192.168.1.100:5672)
└─ docker-compose.server.yml  (Server → 192.168.1.100:5001)

Host B (Gateway Zone 1)
└─ docker-compose.gateway.yml (.env.distributed.gateway)
    RABBITMQ_HOST=192.168.1.100

Host C (Gateway Zone 2)
└─ docker-compose.gateway.yml (.env.distributed.gateway)
    RABBITMQ_HOST=192.168.1.100
```

---

## Notas Importantes

✅ **Recomendações**:
- Usar `start.bat` para deployment local/teste
- Usar scripts Python para automação em produção
- Manter `.env.distributed.*` versionados (com valores default)
- Criar `.env.distributed.*.local` para valores específicos

⚠️ **Cuidados**:
- RabbitMQ demora ~200s para ficar `healthy` na primeira vez
- Não esquecer de criar a rede `urban-broker` antes de ligar o broker
- As redes de composição são independentes (não precisam ser roteadas)
- Gateways em hosts diferentes precisam de IP explícito do broker

🔒 **Segurança**:
- Alterar `RABBITMQ_DEFAULT_PASS` em produção
- Usar firewall para limitar acesso às portas
- Configurar autenticação entre serviços

---

**Última atualização**: 2026-05-30
**Autor**: UrbanHealth Team
