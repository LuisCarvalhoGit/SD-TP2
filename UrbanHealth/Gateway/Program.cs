using Gateway;
using Grpc.Net.Client;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Urbanhealth; 

public class SensorPayload {
    public string SID { get; set; }
    public string Timestamp { get; set; }
    public string Type { get; set; }
    public string Value { get; set; }
    public string Action { get; set; }
}

class Program {
    private static string GID;
    private static ConfigManager _config = new ConfigManager();
    private static LocalCacheManager _cache = new LocalCacheManager();

    private static IModel _rmqChannel;
    private static IConnection _rmqConnection;
    private static TcpClient _serverClient;
    private static readonly SemaphoreSlim _serverTxLock = new SemaphoreSlim(1, 1);
    private static readonly Random _rnd = new Random();

    private static string RawServerIp;
    private static int ServerPort;
    private static int UdpPort;
    private static int ServerUdpPort;
    private static bool EnableLocalVideoPreview;
    private static bool VideoDebugPackets;

    // UDP Video Routing
    private static UdpClient _udpListener;
    private static UdpClient _serverUdpClient = new UdpClient();
    private static VideoFrameAssembler _videoAssembler;
    private static IPEndPoint? _serverUdpEndpoint;
    private static DateTime _nextServerUdpResolveUtc = DateTime.MinValue;
    private static long _forwardedVideoPackets = 0;
    private static long _assembledPreviewFrames = 0;

    private static ConcurrentDictionary<string, byte[]> _latestFrames = new();
    private static ConcurrentDictionary<string, bool> _activeStreams = new();
    private static ConcurrentDictionary<string, DateTime> _activeSensors = new();
    private static readonly object _bufferLock = new object();
    private static Dictionary<(string, string), List<(DateTime, double)>> _valuesToForward = new();
    private static PreProcessingService.PreProcessingServiceClient _rpcClient;

    private static bool TryResolveAddress(string hostname, out IPAddress? address) {
        address = null;
        try {
            var addresses = System.Net.Dns.GetHostAddresses(hostname);
            if (addresses.Length == 0) {
                Console.WriteLine($"[DNS] No addresses resolved for hostname: {hostname}");
                return false;
            }
            address = addresses[0];
            return true;
        } catch (Exception ex) {
            Console.WriteLine($"[DNS] Failed to resolve '{hostname}': {ex.Message}");
            return false;
        }
    }

    static async Task Main(string[] args) {
        _config.LoadConfig();
        GID = _config.GatewayInfo.GatewayId;

        RawServerIp = EndpointResolver.ResolveHost(_config.GatewayInfo.Networking.ServerIp);
        ServerPort = _config.GatewayInfo.Networking.ServerPort;
        UdpPort = _config.GatewayInfo.Networking.UdpListenPort;
        ServerUdpPort = _config.GatewayInfo.Networking.ServerUdpPort;
        EnableLocalVideoPreview = _config.GatewayInfo.Streaming.GatewayEnableLocalVideoPreview;
        VideoDebugPackets = _config.GatewayInfo.Streaming.VideoDebugPackets;

        _videoAssembler = new VideoFrameAssembler(
            TimeSpan.FromMilliseconds(_config.GatewayInfo.Streaming.VideoFrameTtlMs),
            _config.GatewayInfo.Streaming.VideoMaxPendingFramesPerSensor,
            _config.GatewayInfo.Streaming.VideoMaxFrameBytes,
            _config.GatewayInfo.Streaming.VideoMaxPartsPerFrame);

        Console.WriteLine($"[SYSTEM] Starting Gateway {GID}...");
        Console.WriteLine($"[DEBUG] Target Cloud TCP: {RawServerIp}:{ServerPort}");
        Console.WriteLine($"[DEBUG] Target Cloud UDP: {RawServerIp}:{ServerUdpPort}");
        Console.WriteLine($"[DEBUG] Listening UDP from Sensors on port: {UdpPort}");
        Console.WriteLine($"[DEBUG] Local video preview: {(EnableLocalVideoPreview ? "enabled" : "disabled")}");

        string rpcUrl = EndpointResolver.ResolveHttpUrl(
            _config.GatewayInfo.Networking.PreprocessRpcUrl ?? Environment.GetEnvironmentVariable("PREPROCESS_RPC_URL"),
            "http://local:50051");
        Console.WriteLine($"[RPC] PreProcessing endpoint: {rpcUrl}");
        var grpcChannel = GrpcChannel.ForAddress(rpcUrl);
        _rpcClient = new PreProcessingService.PreProcessingServiceClient(grpcChannel);

        _udpListener = new UdpClient(UdpPort);

        _ = Task.Run(ConnectToServerLoopAsync);
        _ = Task.Run(GatewayHeartbeatRoutineAsync);
        _ = Task.Run(BatchDataRoutineAsync);
        _ = Task.Run(StartWebServerAsync);
        _ = Task.Run(SensorTimeoutMonitorRoutineAsync);
        _ = Task.Run(ListenForVideoUdpAsync);
        _ = Task.Run(VideoGarbageCollectorRoutineAsync);

        StartRabbitMQConsumer();

        Console.WriteLine("==================================================");
        Console.WriteLine(" Gateway Menu. Available commands:");
        Console.WriteLine(" -> DISCONN (to shutdown gateway gracefully)");
        Console.WriteLine("==================================================\n");

        while (true) {
            var input = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(input)) continue;
            if (input.Trim().ToUpper() == "DISCONN") {
                await PerformGracefulShutdownAsync();
                break;
            }
        }
    }

    private static void StartRabbitMQConsumer() {
        var factory = new ConnectionFactory() {
            HostName = EndpointResolver.ResolveHost(_config.GatewayInfo.Networking.RabbitMqHost ?? Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "local"),
            UserName = _config.GatewayInfo.Networking.RabbitMqUser ?? Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
            Password = _config.GatewayInfo.Networking.RabbitMqPassword ?? Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest",
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            RequestedHeartbeat = TimeSpan.FromSeconds(15)
        };

        int attempt = 0;
        int delayMs = 1000;

        while (true) {
            attempt++;
            try {
                _rmqConnection = factory.CreateConnection();
                _rmqChannel = _rmqConnection.CreateModel();

                string exchange = _config.GatewayInfo.Rabbitmq.Exchange ?? "urbanhealth_exchange";
                List<string> routingKeys = _config.GatewayInfo.Rabbitmq.RoutingKeys;

                if (routingKeys == null || routingKeys.Count <= 0)
                {
                    routingKeys = new List<string> { "sensor.#" };
                }

                _rmqChannel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic);

                var queueName = _rmqChannel.QueueDeclare().QueueName;

                foreach (var routingKey in routingKeys)
                {
                    _rmqChannel.QueueBind(queue: queueName, exchange: exchange, routingKey: routingKey);
                }

                _rmqChannel.BasicQos(prefetchSize: 0, prefetchCount: 200, global: false);

                var consumer = new EventingBasicConsumer(_rmqChannel);
                consumer.Received += (model, ea) => {
                    var body = ea.Body.ToArray();
                    var messageJson = Encoding.UTF8.GetString(body);
                    try {
                        var payload = JsonSerializer.Deserialize<SensorPayload>(messageJson);
                        ProcessRabbitMQMessage(payload);
                    } catch { Console.WriteLine("[DEBUG RMQ] Falha a desserializar mensagem."); }
                };
                _rmqChannel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine("[RMQ] Gateway consumer connected and ready.");
                return;
            } catch (Exception ex) {
                int sleepMs = delayMs + Random.Shared.Next(0, 750);
                Console.WriteLine($"[RMQ] Attempt {attempt} failed: {ex.Message}. Retrying in {sleepMs}ms.");
                Thread.Sleep(sleepMs);
                delayMs = Math.Min(delayMs * 2, 30000);
            }
        }
    }

    private static void SendCommandToSensor(string sid, string type, string value) {
        if (_rmqChannel == null || !_rmqChannel.IsOpen) return;
        try {
            var payload = new { Type = type, Value = value };
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
            string exchange = _config.GatewayInfo.Rabbitmq.Exchange ?? "urbanhealth_exchange";
            _rmqChannel.BasicPublish(exchange: exchange, routingKey: $"cmd.{sid}", basicProperties: null, body: body);
        } catch (Exception ex) {
            Console.WriteLine($"[RMQ] Failed to send command to {sid}: {ex.Message}");
        }
    }

    private static void ProcessRabbitMQMessage(SensorPayload payload) {
        if (payload == null) return;

        // O Gateway pergunta ao ConfigManager se este sensor pertence à sua zona
        var (exists, _, _, supportedTypes, _) = _config.ValidateSensor(payload.SID);
        
        // Se o sensor não estiver no ficheiro JSON DESTE gateway, ignora a mensagem
        if (!exists) return;

        _activeSensors[payload.SID] = DateTime.Now;

        if (payload.Type != "HB") {
            Console.WriteLine($"[DEBUG RMQ RX] Sens: {payload.SID} | Type: {payload.Type} | Val: {payload.Value}");
        }

        if (payload.Type == "STRM_REQ") {
            if (exists && supportedTypes.Contains("VIDEO")) {
                _activeStreams[payload.SID] = true;
                Console.WriteLine($"[GATEWAY] Stream authorized for {payload.SID}");
                SendCommandToSensor(payload.SID, "STRM_GRANT", "OK");

                if (_serverClient != null && _serverClient.Connected) {
                    var fwdStrm = new Message { CMD = "FWD_STRM", SID = payload.SID, GID = GID };
                    fwdStrm.Data["ACTION"] = "START";
                    
                    _ = Task.Run(async () => {
                        await _serverTxLock.WaitAsync();
                        try { 
                            if (_serverClient != null && _serverClient.Connected) {
                                await Message.SendMessageAsync(_serverClient, fwdStrm); 
                            }
                        } catch { } 
                        finally { _serverTxLock.Release(); }
                    });
                }
            } else {
                Console.WriteLine($"[GATEWAY] Stream DENIED for {payload.SID}");
                SendCommandToSensor(payload.SID, "STRM_DENIED", "UNAUTHORIZED");
            }
            return;
        }

        if (payload.Type == "STRM" && payload.Action == "STOP") {
            _activeStreams.TryRemove(payload.SID, out _);
            Console.WriteLine($"[GATEWAY] Stream stopped for {payload.SID}");
            return;
        }

        if (payload.Type == "STS" || payload.Type == "HB" || payload.Type == "STRM") return;

        if (!supportedTypes.Contains(payload.Type)) {
            var newTypesList = supportedTypes.ToList();
            newTypesList.Add(payload.Type);
            
            // Reconstrói a string separada por vírgulas para gravar no JSON
            string newTypesString = string.Join(", ", newTypesList);
            
            _config.UpdateSensorDataTypes(payload.SID, newTypesString);
            Console.WriteLine($"[CONFIG] O sensor {payload.SID} começou a transmitir {payload.Type}. Configuração atualizada!");
        }

        try {
            if (!double.TryParse(payload.Value, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double numericValue)) return;

            var rpcRequest = new DataRequest { SensorId = payload.SID, DataType = payload.Type, RawValue = numericValue };
            
            double finalValue = numericValue;
            bool addedToBuffer = false;

            try {
                var rpcResponse = _rpcClient.ProcessData(rpcRequest);
                if (rpcResponse.Success) {
                    finalValue = rpcResponse.ProcessedValue;
                    Console.WriteLine($"[DEBUG RPC] Python aprovou o valor: {finalValue}");
                    addedToBuffer = true;
                } else {
                    Console.WriteLine($"[DEBUG RPC] Python REJEITOU o valor por estar fora das regras.");
                }
            } catch (Exception ex) {
                // REDE DE SEGURANÇA: Se o python não estiver ligado, guardamos à mesma!
                Console.WriteLine($"[DEBUG RPC WARNING] Python Service Inacessível! A contornar e guardar valor original: {numericValue}. Erro: {ex.Message}");
                addedToBuffer = true;
            }

            if (addedToBuffer) {
                var bufferKey = (payload.SID, payload.Type);
                
                // O lock bloqueia a thread durante microsegundos só para garantir a inserção segura
                lock (_bufferLock) {
                    if (!_valuesToForward.ContainsKey(bufferKey)) {
                        _valuesToForward[bufferKey] = new List<(DateTime, double)>();
                    }
                    _valuesToForward[bufferKey].Add((DateTime.Now, finalValue));
                }
                Console.WriteLine($"[DEBUG MEMORY] Adicionado ao buffer para envio à Cloud.");
            }

        } catch (Exception fatalEx) { Console.WriteLine($"[DEBUG FATAL] Erro em ProcessRabbitMQMessage: {fatalEx.Message}"); }
    }

    private static async Task ListenForVideoUdpAsync() {
        Console.WriteLine($"[UDP RELAY] Escuta UDP ativa na porta {UdpPort}...");
        while (true) {
            try {
                var result = await _udpListener.ReceiveAsync();
                var msg = Message.FromUdpBytes(result.Buffer);

                if (msg == null || msg.CMD != "STRM") {
                    if (VideoDebugPackets) Console.WriteLine("[UDP] Dropped invalid video packet.");
                    continue;
                }

                if (!_activeStreams.ContainsKey(msg.SID)) {
                    if (VideoDebugPackets) Console.WriteLine($"[UDP] Dropped unauthorized stream packet from {msg.SID}.");
                    continue;
                }

                if (TryGetServerUdpEndpoint(out var serverEndpoint) && serverEndpoint != null) {
                    await _serverUdpClient.SendAsync(result.Buffer, result.Buffer.Length, serverEndpoint);
                    long forwarded = Interlocked.Increment(ref _forwardedVideoPackets);
                    if (VideoDebugPackets && forwarded % 100 == 0) {
                        Console.WriteLine($"[UDP RELAY] Forwarded {forwarded} video packets to {serverEndpoint}.");
                    }
                }

                if (!EnableLocalVideoPreview) continue;

                if (_videoAssembler.TryAddPacket(msg, out byte[]? fullImage, out string reason) && fullImage != null) {
                    _latestFrames[msg.SID] = fullImage;
                    long assembled = Interlocked.Increment(ref _assembledPreviewFrames);
                    if (VideoDebugPackets || assembled % 30 == 0) {
                        Console.WriteLine($"[UDP PREVIEW] Assembled {assembled} local frames. Latest sensor={msg.SID}.");
                    }
                } else if (VideoDebugPackets && reason != "pending" && reason != "duplicate-packet") {
                    Console.WriteLine($"[UDP PREVIEW] Dropped packet for {msg.SID}: {reason}");
                }
            } catch (Exception ex) { Console.WriteLine($"[DEBUG UDP LOOP ERRO] {ex.Message}"); }
        }
    }

    private static async Task VideoGarbageCollectorRoutineAsync() {
        while (true) {
            await Task.Delay(2000);
            int removed = _videoAssembler.GarbageCollect();
            if (VideoDebugPackets && removed > 0) {
                Console.WriteLine($"[UDP PREVIEW] Garbage collector removed {removed} stale frames.");
            }
        }
    }

    private static async Task BatchDataRoutineAsync() {
        int batchWindowMs = _config.GatewayInfo.Timings.BatchIntervalMs;
        while (true) {
            await Task.Delay(batchWindowMs);

            Dictionary<(string, string), List<(DateTime, double)>> snapshot;

            // Fazemos o Swap em milissegundos. Quem está a receber do RMQ nunca fica bloqueado muito tempo.
            lock (_bufferLock) {
                if (_valuesToForward.Count == 0) continue;
                snapshot = _valuesToForward;
                // Reinicia o buffer original para estar pronto para novas leituras imediatamente
                _valuesToForward = new Dictionary<(string, string), List<(DateTime, double)>>(); 
            }

            // Agora iteramos o snapshot com calma (sem medo de concorrência)
            foreach (var kvp in snapshot) {
                var key = kvp.Key;
                var snapshotList = kvp.Value;
                
                if (snapshotList.Count == 0) continue;

                string sensorId = key.Item1;
                string dataType = key.Item2;
                var (_, zone, _, _, _) = _config.ValidateSensor(sensorId);

                var payloadList = snapshotList.Select(item => new Dictionary<string, string> {
                    { "Timestamp", item.Item1.ToUniversalTime().ToString("o") },
                    { "Value", item.Item2.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                }).ToList();

                var batchMsg = new Message { CMD = "FWD", SID = sensorId, GID = GID };
                batchMsg.Data["TYPE"] = dataType;
                batchMsg.Data["ZONE"] = !string.IsNullOrWhiteSpace(zone) ? zone : "DESCONHECIDA";
                batchMsg.Data["BATCH_COUNT"] = snapshotList.Count.ToString();
                batchMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                Console.WriteLine($"[DEBUG TCP BATCH] Preparando para enviar {snapshotList.Count} leituras de {sensorId}...");

                try {
                    await _serverTxLock.WaitAsync();
                    try { 
                        // Verificamos e enviamos TUDO dentro da proteção
                        if (_serverClient == null || !_serverClient.Connected) throw new Exception("Cloud down");
                        await Message.SendMessageAsync(_serverClient, batchMsg); 
                    } finally { 
                        _serverTxLock.Release(); 
                    }
                    Console.WriteLine($"[DEBUG TCP BATCH] Enviado com sucesso para a Cloud!");
                } catch (Exception netEx) {
                    Console.WriteLine($"[DEBUG TCP ERROR] Cloud inacessível ({netEx.Message}). Guardando na Base de Dados Edge (SQLite).");
                    foreach (var reading in snapshotList) _cache.SaveReading(sensorId, dataType, reading.Item2, reading.Item1);
                }
            }
        }
    }

    private static async Task ConnectToServerLoopAsync() {
        int baseDelayMs = 2000;
        int maxDelayMs = 60000;
        int currentDelayMs = baseDelayMs;

        while (true) {
            try {
                // Cria a ligação de forma isolada numa variável local
                var newClient = new TcpClient();
                await newClient.ConnectAsync(RawServerIp, ServerPort);
                Console.WriteLine("[DEBUG TCP] Conectado ao Server com sucesso!");
                currentDelayMs = baseDelayMs;

                var statusMsg = new Message { CMD = "STS", GID = GID };
                statusMsg.Data["STATUS"] = "ONLINE";

                // Bloqueia as threads de envio APENAS para fazer a substituição segura
                await _serverTxLock.WaitAsync();
                try {
                    _serverClient = newClient;
                    await Message.SendMessageAsync(_serverClient, statusMsg); 

                    // Dispara a recuperação de dados offline em background
                    _ = Task.Run(async () => {
                        var pendentes = _cache.GetPendingReadings();
                        if (pendentes.Count > 0) {
                            Console.WriteLine($"[EDGE CACHE] O Server voltou! A recuperar {pendentes.Count} leituras offline...");
                            
                            // Agrupar as leituras por Sensor e Tipo de Dados (ex: todas as TEMP do S101 juntas)
                            var agrupados = pendentes.GroupBy(p => new { p.Sid, p.Type });

                            bool erroNoEnvio = false;

                            foreach (var grupo in agrupados) {
                                string sensorId = grupo.Key.Sid;
                                string dataType = grupo.Key.Type;
                                var (_, zone, _, _, _) = _config.ValidateSensor(sensorId);

                                // Formatar tal como no BatchDataRoutineAsync
                                var payloadList = grupo.Select(item => new Dictionary<string, string> {
                                    { "Timestamp", item.Ts.ToUniversalTime().ToString("o") },
                                    { "Value", item.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                                }).ToList();

                                var batchMsg = new Message { CMD = "FWD", SID = sensorId, GID = GID };
                                batchMsg.Data["TYPE"] = dataType;
                                batchMsg.Data["ZONE"] = !string.IsNullOrWhiteSpace(zone) ? zone : "DESCONHECIDA";
                                batchMsg.Data["BATCH_COUNT"] = grupo.Count().ToString();
                                batchMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                                try {
                                    await _serverTxLock.WaitAsync();
                                    try { 
                                        if (_serverClient != null && _serverClient.Connected) {
                                            await Message.SendMessageAsync(_serverClient, batchMsg); 
                                        } else {
                                            erroNoEnvio = true;
                                        }
                                    } finally { _serverTxLock.Release(); }
                                } catch (Exception ex) {
                                    Console.WriteLine($"[EDGE CACHE ERROR] Falha ao reenviar: {ex.Message}");
                                    erroNoEnvio = true;
                                }
                                
                                if (erroNoEnvio) break; // Se falhou a meio, para e tenta na próxima reconexão
                            }

                            if (!erroNoEnvio) {
                                // Se tudo foi enviado com sucesso, apagamos da base de dados local SQLite
                                _cache.DeleteReadings(pendentes.Select(p => p.Id));
                                Console.WriteLine("[EDGE CACHE] Dados offline recuperados e sincronizados com o Server!");
                            }
                        }
                    });
                } finally { 
                    _serverTxLock.Release(); 
                }

                // Fica a escutar até a ligação cair
                while (true) {
                    var msg = await Message.ReceiveMessageAsync(_serverClient);
                    if (msg == null) break;
                }
            } catch { }

            // Se chegou aqui, a ligação caiu. Limpeza isolada e segura.
            await _serverTxLock.WaitAsync();
            try {
                if (_serverClient != null) {
                    _serverClient.Close();
                    _serverClient = null; // Coloca a null com segurança
                }
            } finally {
                _serverTxLock.Release();
            }

            int sleepTime = currentDelayMs + _rnd.Next(0, 1000);
            await Task.Delay(sleepTime);
            currentDelayMs = Math.Min(currentDelayMs * 2, maxDelayMs);
        }
    }

    private static bool TryGetServerUdpEndpoint(out IPEndPoint? endpoint) {
        endpoint = _serverUdpEndpoint;
        if (endpoint != null) return true;

        DateTime now = DateTime.UtcNow;
        if (now < _nextServerUdpResolveUtc) return false;

        if (TryResolveAddress(RawServerIp, out IPAddress? address) && address != null) {
            endpoint = new IPEndPoint(address, ServerUdpPort);
            _serverUdpEndpoint = endpoint;
            Console.WriteLine($"[DNS] Resolved Cloud UDP endpoint: {RawServerIp} -> {endpoint}");
            return true;
        }

        _nextServerUdpResolveUtc = now.AddSeconds(5);
        return false;
    }

    private static async Task GatewayHeartbeatRoutineAsync() {
        while (true) {
            await Task.Delay(_config.GatewayInfo.Timings.HeartbeatIntervalMs);
            
            await _serverTxLock.WaitAsync();
            try {
                // A verificação tem de estar sempre protegida
                if (_serverClient != null && _serverClient.Connected) {
                    var hbMsg = new Message { CMD = "HB", GID = GID };
                    await Message.SendMessageAsync(_serverClient, hbMsg); 
                }
            } catch { } 
            finally { 
                _serverTxLock.Release(); 
            }
        }
    }

    private static async Task SensorTimeoutMonitorRoutineAsync() {
        while (true) {
            await Task.Delay(_config.GatewayInfo.Timings.SensorTimeoutCheckMs);
            bool stateChanged = false;
            int thresholdSecs = _config.GatewayInfo.Timings.SensorTimeoutThresholdSecs;
            foreach (var sensor in _activeSensors) {
                if ((DateTime.Now - sensor.Value).TotalSeconds > thresholdSecs) {
                    _activeSensors.TryRemove(sensor.Key, out _);
                    _activeStreams.TryRemove(sensor.Key, out _);
                    _config.UpdateSensorState(sensor.Key, "offline");
                    stateChanged = true;
                }
            }
            if (stateChanged) _config.SaveSensorsConfig();
        }
    }

    private static async Task PerformGracefulShutdownAsync() {
        await _serverTxLock.WaitAsync();
        try {
            if (_serverClient != null && _serverClient.Connected) {
                var byeMsg = new Message { CMD = "DISCONN", GID = GID, SID = "GATEWAY" };
                await Message.SendMessageAsync(_serverClient, byeMsg);
                await Task.Delay(500); // Dá tempo para o pacote físico sair pela placa de rede
                _serverClient.Close();
                _serverClient = null;
            }
        } catch { }
        finally {
            _serverTxLock.Release();
        }
        Environment.Exit(0);
    }

    private static async Task StartWebServerAsync() {
        try {
            var listener = new HttpListener();
            listener.Prefixes.Add("http://+:8080/");
            listener.Start();

            while (true) {
                var context = await listener.GetContextAsync();
                var req = context.Request;
                var res = context.Response;

                res.AppendHeader("Access-Control-Allow-Origin", "*");

                try {
                    if (req.Url.AbsolutePath.StartsWith("/stream/")) {
                        string sid = req.Url.AbsolutePath.Split('/').Last();
                        string html = $@"
                            <!DOCTYPE html>
                            <html>
                            <body style='background:#1e1e1e;color:white;text-align:center;font-family:Segoe UI,Arial;'>
                                <h2>Live Feed UDP no Edge: {sid}</h2>
                                <img id='feed' src='/image/{sid}' style='max-width:800px;border:3px solid #007acc;border-radius:10px;' />
                                <p style='color:#00ff00;font-weight:bold;'>LIVE | 5 FPS</p>
                                <script>
                                    setInterval(() => document.getElementById('feed').src = '/image/{sid}?t=' + Date.now(), 200);
                                </script>
                            </body>
                            </html>";
                        byte[] buffer = Encoding.UTF8.GetBytes(html);
                        res.ContentType = "text/html";
                        res.ContentLength64 = buffer.Length;
                        await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                    }
                    else if (req.Url.AbsolutePath.StartsWith("/image/")) {
                        string sid = req.Url.AbsolutePath.Split('/').Last();
                        if (_latestFrames.TryGetValue(sid, out byte[] imgBytes)) {
                            res.ContentType = "image/jpeg";
                            res.ContentLength64 = imgBytes.Length;
                            await res.OutputStream.WriteAsync(imgBytes, 0, imgBytes.Length);
                        }
                        else { res.StatusCode = 404; }
                    }
                    else { res.StatusCode = 404; }
                } finally { res.Close(); }
            }
        } catch { }
    }

}
