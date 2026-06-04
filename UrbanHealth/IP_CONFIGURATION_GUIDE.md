# Configuração de Comunicação com IPs (Distributed Mode)

## Como Funciona

Os ficheiros de configuração usam placeholders (`$VARIABLE_NAME$`) que são substituídos automaticamente pelos valores definidos nos ficheiros `.env.distributed.sensor` e `.env.distributed.gateway`.

### Exemplo

**sensor-config-S101.json** (com placeholders):
```json
{
  "Networking": {
    "TargetGatewayIp": "$GATEWAY_IP$",
    "TargetRabbitMqHost": "$RABBITMQ_IP$",
    ...
  }
}
```

**.env.distributed.sensor** (valores):
```bash
GATEWAY_IP=127.0.0.1
RABBITMQ_IP=127.0.0.1
```

**Resultado**: Os placeholders são substituídos antes de desserializar o JSON, ficando:
```json
{
  "Networking": {
    "TargetGatewayIp": "127.0.0.1",
    "TargetRabbitMqHost": "127.0.0.1",
    ...
  }
}
```

---

## Cenários de Uso

### Cenário 1: Desenvolvimento Local (Tudo no mesmo PC)

```bash
# .env.distributed.sensor
GATEWAY_IP=127.0.0.1
RABBITMQ_IP=127.0.0.1

# .env.distributed.gateway  
SERVER_IP=127.0.0.1
RABBITMQ_IP=127.0.0.1
```

Todos os serviços rodam com `docker-compose`:
```bash
# Terminal 1: Broker
docker-compose -f docker-compose.broker.yml up --build

# Terminal 2: Server
docker-compose -f docker-compose.server.yml up --build

# Terminal 3: Gateway
docker-compose -f docker-compose.gateway.yml up --build

# Terminal 4: Sensors
docker-compose -f docker-compose.sensor.yml up --build
```

---

### Cenário 2: Sensors Remotos (Ex: Casa do Amigo)

**Casa do Amigo (2 Sensores)**:
- IP da máquina: `192.168.1.50`
- Sensores S101 e S102 rodando

**Minha Casa (Gateway + Broker + Server)**:
- IP da máquina: `192.168.1.100`

**Minha configuração (.env.distributed.gateway)**:
```bash
SERVER_IP=127.0.0.1  # Server local
RABBITMQ_IP=127.0.0.1  # Broker local
```

**Configuração do Amigo (.env.distributed.sensor)**:
```bash
GATEWAY_IP=192.168.1.100  # IP do meu Gateway
RABBITMQ_IP=192.168.1.100  # IP do meu Broker
```

**No PC do Amigo**:
```bash
docker-compose -f docker-compose.sensor.yml up --build
```

Os sensores conectam-se automaticamente ao Gateway e Broker no IP 192.168.1.100.

---

### Cenário 3: Múltiplos Gateways (Diferentes Zonas)

**Zona 1 (Gateway G101)**:
```bash
# .env.distributed.gateway
SERVER_IP=192.168.1.100
RABBITMQ_IP=192.168.1.100
```

**Zona 2 (Gateway G102)**:
```bash
# Mesmo ficheiro, mesmos valores
SERVER_IP=192.168.1.100
RABBITMQ_IP=192.168.1.100
```

Ambos os gateways em máquinas diferentes, mas conectando ao mesmo Broker/Server.

---

## IPs Padrão

Quando o ficheiro `.env` não tem a variável definida, usa-se `localhost`:

```csharp
// ConfigManager.cs (Gateway)
json = json.Replace("$SERVER_IP$", Environment.GetEnvironmentVariable("SERVER_IP") ?? "localhost");
json = json.Replace("$RABBITMQ_IP$", Environment.GetEnvironmentVariable("RABBITMQ_IP") ?? "localhost");
```

---

## Checklist de Configuração

- [ ] Verificar IP da máquina Gateway/Broker/Server (use `ipconfig` no Windows ou `ifconfig` no Linux)
- [ ] Editar `.env.distributed.sensor` com os IPs corretos
- [ ] Editar `.env.distributed.gateway` com os IPs corretos
- [ ] Testar conectividade de rede entre máquinas (ping)
- [ ] Iniciar os containers
- [ ] Verificar logs para ver se as connexões foram bem-sucedidas

---

## Comandos Úteis

### Ver IPs da máquina (Windows)
```powershell
ipconfig
```

### Ver IPs da máquina (Linux)
```bash
ifconfig
# ou
hostname -I
```

### Testar conectividade
```bash
# Do container do Sensor para o Gateway
docker exec sensor-s101 ping 192.168.1.100

# Do container do Gateway para o Server
docker exec gateway-g101 ping 192.168.1.100
```

### Ver logs do container
```bash
docker logs -f gateway-g101
docker logs -f sensor-s101
```

---

## Troubleshooting

### "Connection refused" ao conectar ao Gateway
- Verificar se o Gateway está a correr
- Verificar se o IP está correto no `.env.distributed.sensor`
- Verificar firewall (pode estar a bloquear a porta 5004 UDP)

### "Connection refused" ao conectar ao RabbitMQ
- Verificar se o RabbitMQ está a correr
- Verificar se o IP está correto no `.env.distributed.sensor` e `.env.distributed.gateway`
- RabbitMQ usa porta 5672 (AMQP) e 15672 (Web Management)

### "Name or service not known"
- Não acontece mais! Agora estamos a usar IPs em vez de nomes

---

