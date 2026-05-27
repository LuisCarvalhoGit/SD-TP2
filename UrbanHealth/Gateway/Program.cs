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
    private static readonly string RawServerIp = Environment.GetEnvironmentVariable("SERVER_IP") ?? "127.0.0.1";
    private static readonly IPAddress ServerIp = ResolveAddress(RawServerIp);
    private static readonly int ServerPort = int.Parse(Environment.GetEnvironmentVariable("SERVER_PORT") ?? "5001");

    private static IModel _rmqChannel;
    private static TcpClient _serverClient;
    private static readonly SemaphoreSlim _serverTxLock = new SemaphoreSlim(1, 1);
    private static readonly Random _rnd = new Random();

    // UDP Video Routing
    private static UdpClient _udpListener;
    private static UdpClient _serverUdpClient = new UdpClient();
    private static readonly int UdpPort = int.Parse(Environment.GetEnvironmentVariable("GATEWAY_UDP_PORT") ?? "5002");
    private static readonly int ServerUdpPort = int.Parse(Environment.GetEnvironmentVariable("SERVER_UDP_PORT") ?? "5003");

    private static ConcurrentDictionary<string, byte[][]> _videoBuffer = new();
    private static ConcurrentDictionary<string, DateTime> _videoBufferTimestamps = new();
    private static ConcurrentDictionary<string, byte[]> _latestFrames = new();
    private static ConcurrentDictionary<string, bool> _activeStreams = new();

    private static ConfigManager _config = new ConfigManager();
    private static LocalCacheManager _cache = new LocalCacheManager();
    private static ConcurrentDictionary<string, DateTime> _activeSensors = new();
    private static ConcurrentDictionary<(string, string), ConcurrentBag<(DateTime, double)>> _valuesToForward = new();
    private static PreProcessingService.PreProcessingServiceClient _rpcClient;

    private static IPAddress ResolveAddress(string hostname) {
        try {
            var addresses = System.Net.Dns.GetHostAddresses(hostname);
            if (addresses.Length == 0) {
                throw new InvalidOperationException($"No addresses resolved for hostname: {hostname}");
            }
            Console.WriteLine($"[DNS] Resolved {hostname} to {addresses[0]}");
            return addresses[0];
        } catch (Exception ex) {
            Console.WriteLine($"[ERROR] Failed to resolve SERVER_IP='{hostname}': {ex.Message}");
            throw new InvalidOperationException($"Cannot resolve SERVER_IP: {hostname}", ex);
        }
    }

    static async Task Main(string[] args) {
        _config.LoadConfig();
        GID =_config.GatewayInfo.GatewayId;

        Console.WriteLine($"[SYSTEM] Starting Gateway {GID}...");
        Console.WriteLine($"[DEBUG] Target Cloud TCP: {RawServerIp}:{ServerPort}");
        Console.WriteLine($"[DEBUG] Target Cloud UDP: {RawServerIp}:{ServerUdpPort}");
        Console.WriteLine($"[DEBUG] Listening UDP from Sensors on port: {UdpPort}");

        string rpcUrl = Environment.GetEnvironmentVariable("PREPROCESS_RPC_URL") ?? "http://localhost:50051";
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
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest",
            DispatchConsumersAsync = true 
        };

        for (int i = 1; i <= 20; i++) {
            try {
                var connection = factory.CreateConnection();
                _rmqChannel = connection.CreateModel();
                string exchange = _config.GatewayInfo.Rabbitmq.Exchange ?? "urbanhealth_exchange";
                _rmqChannel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic);
                
                var queueName = _rmqChannel.QueueDeclare().QueueName;
                _rmqChannel.QueueBind(queue: queueName, exchange: exchange, routingKey: "sensor.#");

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
                return;
            } catch { Thread.Sleep(3000); }
        }
    }

    private static void SendCommandToSensor(string sid, string type, string value) {
        if (_rmqChannel == null || _rmqChannel.IsClosed) return;
        var payload = new { Type = type, Value = value };
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
        string exchange = _config.GatewayInfo.Rabbitmq.Exchange ?? "urbanhealth_exchange";
        _rmqChannel.BasicPublish(exchange: exchange, routingKey: $"cmd.{sid}", basicProperties: null, body: body);
    }

    private static void ProcessRabbitMQMessage(SensorPayload payload) {
        if (payload == null) return;
        _activeSensors[payload.SID] = DateTime.Now;

        if (payload.Type != "HB") {
            Console.WriteLine($"[DEBUG RMQ RX] Sens: {payload.SID} | Type: {payload.Type} | Val: {payload.Value}");
        }

        if (payload.Type == "STRM_REQ") {
            var (exists, _, _, supportedTypes, _) = _config.ValidateSensor(payload.SID);
            if (exists && supportedTypes.Contains("VIDEO")) {
                _activeStreams[payload.SID] = true;
                Console.WriteLine($"[GATEWAY] Stream authorized for {payload.SID}");
                SendCommandToSensor(payload.SID, "STRM_GRANT", "OK");

                if (_serverClient != null && _serverClient.Connected) {
                    var fwdStrm = new Message { CMD = "FWD_STRM", SID = payload.SID, GID = GID };
                    fwdStrm.Data["ACTION"] = "START";
                    _ = Task.Run(async () => {
                        await _serverTxLock.WaitAsync();
                        try { await Message.SendMessageAsync(_serverClient, fwdStrm); } finally { _serverTxLock.Release(); }
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
            _videoBuffer.TryRemove(payload.SID, out _);
            Console.WriteLine($"[GATEWAY] Stream stopped for {payload.SID}");
            return;
        }

        if (payload.Type == "STS" || payload.Type == "HB" || payload.Type == "STRM") return;

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
                var readingsBag = _valuesToForward.GetOrAdd(bufferKey, _ => new ConcurrentBag<(DateTime, double)>());
                readingsBag.Add((DateTime.Now, finalValue));
                Console.WriteLine($"[DEBUG MEMORY] Adicionado ao buffer para envio à Cloud.");
            }

        } catch (Exception fatalEx) { Console.WriteLine($"[DEBUG FATAL] Erro em ProcessRabbitMQMessage: {fatalEx.Message}"); }
    }

    private static async Task ListenForVideoUdpAsync() {
        Console.WriteLine($"[UDP RELAY] Escuta UDP ativa na porta {UdpPort}...");
        while (true) {
            try {
                var result = await _udpListener.ReceiveAsync();
                Console.WriteLine($"\n[DEBUG UDP] -> Recebi pacote UDP de {result.Buffer.Length} bytes!");

                var msg = Message.FromUdpBytes(result.Buffer);

                if (msg == null) {
                    Console.WriteLine("[DEBUG UDP ERROR] Falha a traduzir bytes UDP em Shared.Message.");
                    continue;
                }

                Console.WriteLine($"[DEBUG UDP] -> Mensagem convertida: SID={msg.SID}, PARTE={msg.Data["PART"]}/{msg.Data["TOTAL"]}");

                if (!_activeStreams.ContainsKey(msg.SID)) {
                    Console.WriteLine($"[DEBUG UDP DROP] Pacote bloqueado. O sensor {msg.SID} não tem stream autorizado.");
                    continue;
                }

                try {
                    await _serverUdpClient.SendAsync(result.Buffer, result.Buffer.Length, ServerIp.ToString(), ServerUdpPort);
                } catch { }

                // 2. MONTAGEM LOCAL
                int part = int.Parse(msg.Data["PART"]);
                int total = int.Parse(msg.Data["TOTAL"]);

                if (!_videoBuffer.TryGetValue(msg.SID, out var chunks) || chunks.Length != total) {
                    chunks = new byte[total][];
                    _videoBuffer[msg.SID] = chunks;
                }
                chunks[part - 1] = msg.BinaryData;

                Console.WriteLine($"[DEBUG UDP] -> Guardei a parte {part} no buffer.");
                
                _videoBufferTimestamps[msg.SID] = DateTime.Now;

                if (chunks.All(c => c != null)) {
                    _latestFrames[msg.SID] = chunks.SelectMany(c => c).ToArray();
                    _videoBuffer.TryRemove(msg.SID, out _);
                    _videoBufferTimestamps.TryRemove(msg.SID, out _);
                    Console.WriteLine($"[DEBUG UDP SUCCESS] Frame completa recebida e montada localmente para {msg.SID}.");
                }
            } catch (Exception ex) { Console.WriteLine($"[DEBUG UDP LOOP ERRO] {ex.Message}"); }
        }
    }

    private static async Task VideoGarbageCollectorRoutineAsync() {
        while (true) {
            await Task.Delay(2000);
            foreach (var frameTime in _videoBufferTimestamps) {
                if ((DateTime.Now - frameTime.Value).TotalSeconds > 1.5) {
                    _videoBuffer.TryRemove(frameTime.Key, out _);
                    _videoBufferTimestamps.TryRemove(frameTime.Key, out _);
                }
            }
        }
    }

    private static async Task BatchDataRoutineAsync() {
        int batchWindowMs = _config.GatewayInfo.Timings.BatchIntervalMs;
        while (true) {
            await Task.Delay(batchWindowMs);

            // LOGICA VOLÁTIL NORMAL
            foreach (var key in _valuesToForward.Keys) {
                if (_valuesToForward.TryRemove(key, out var readingsBag)) {
                    var snapshot = readingsBag.ToList();
                    if (snapshot.Count == 0) continue;

                    string sensorId = key.Item1;
                    string dataType = key.Item2;
                    var (_, zone, _, _, _) = _config.ValidateSensor(sensorId);

                    var payloadList = snapshot.Select(item => new Dictionary<string, string> {
                        { "Timestamp", item.Item1.ToUniversalTime().ToString("o") },
                        { "Value", item.Item2.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                    }).ToList();

                    var batchMsg = new Message { CMD = "FWD", SID = sensorId, GID = GID };
                    batchMsg.Data["TYPE"] = dataType;
                    batchMsg.Data["ZONE"] = !string.IsNullOrWhiteSpace(zone) ? zone : "DESCONHECIDA";
                    batchMsg.Data["BATCH_COUNT"] = snapshot.Count.ToString();
                    batchMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                    Console.WriteLine($"[DEBUG TCP BATCH] Preparando para enviar {snapshot.Count} leituras de {sensorId}...");

                    try {
                        if (_serverClient == null || !_serverClient.Connected) throw new Exception("Cloud down");
                        await _serverTxLock.WaitAsync();
                        try { await Message.SendMessageAsync(_serverClient, batchMsg); } finally { _serverTxLock.Release(); }
                        Console.WriteLine($"[DEBUG TCP BATCH] Enviado com sucesso para a Cloud!");
                    } catch (Exception netEx) {
                        Console.WriteLine($"[DEBUG TCP ERROR] Cloud inacessível ({netEx.Message}). Guardando na Base de Dados Edge (SQLite).");
                        foreach (var reading in snapshot) _cache.SaveReading(sensorId, dataType, reading.Item2, reading.Item1);
                    }
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
                _serverClient = new TcpClient();
                await _serverClient.ConnectAsync(ServerIp, ServerPort);
                Console.WriteLine("[DEBUG TCP] Conectado à Cloud Server com sucesso!");
                currentDelayMs = baseDelayMs;

                var statusMsg = new Message { CMD = "STS", GID = GID };
                statusMsg.Data["STATUS"] = "ONLINE";

                await _serverTxLock.WaitAsync();
                try { await Message.SendMessageAsync(_serverClient, statusMsg); } finally { _serverTxLock.Release(); }

                while (true) {
                    var msg = await Message.ReceiveMessageAsync(_serverClient);
                    if (msg == null) break;
                }
            } catch { }

            int sleepTime = currentDelayMs + _rnd.Next(0, 1000);
            await Task.Delay(sleepTime);
            currentDelayMs = Math.Min(currentDelayMs * 2, maxDelayMs);
        }
    }

    private static async Task GatewayHeartbeatRoutineAsync() {
        while (true) {
            await Task.Delay(_config.GatewayInfo.Timings.HeartbeatIntervalMs);
            if (_serverClient != null && _serverClient.Connected) {
                try {
                    var hbMsg = new Message { CMD = "HB", GID = GID };
                    await _serverTxLock.WaitAsync();
                    try { await Message.SendMessageAsync(_serverClient, hbMsg); } finally { _serverTxLock.Release(); }
                } catch { }
            }
        }
    }

    private static async Task SensorTimeoutMonitorRoutineAsync() {
        while (true) {
            await Task.Delay(_config.GatewayInfo.Timings.SensorTimeoutCheckMs);
            bool stateChanged = false;
            foreach (var sensor in _activeSensors) {
                if ((DateTime.Now - sensor.Value).TotalSeconds > 30) {
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
        if (_serverClient != null && _serverClient.Connected) {
            try {
                var byeMsg = new Message { CMD = "DISCONN", GID = GID, SID = "GATEWAY" };
                await _serverTxLock.WaitAsync();
                try { await Message.SendMessageAsync(_serverClient, byeMsg); } finally { _serverTxLock.Release(); }
                await Task.Delay(500);
                _serverClient.Close();
            } catch { }
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