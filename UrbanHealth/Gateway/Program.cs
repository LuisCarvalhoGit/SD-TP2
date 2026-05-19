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
using Urbanhealth; // Namespace compiled from preprocess.proto

public class SensorPayload {
    public string SID { get; set; }
    public string Timestamp { get; set; }
    public string Type { get; set; }
    public string Value { get; set; }
    public string Action { get; set; }
    public string ImageData { get; set; }
}

class Program {
    // Gateway Configuration Variables
    private static readonly string GID = Environment.GetEnvironmentVariable("GATEWAY_ID") ?? "G101";
    private static readonly string ServerIP = Environment.GetEnvironmentVariable("SERVER_IP") ?? "127.0.0.1";
    private static readonly int ServerPort = int.TryParse(Environment.GetEnvironmentVariable("SERVER_PORT"), out int sp) ? sp : 5001;

    // Upstream Cloud Server Connection
    private static TcpClient _serverClient;
    private static readonly SemaphoreSlim _serverTxLock = new SemaphoreSlim(1, 1);
    private static readonly Random _rnd = new Random();

    // Data Management and In-Memory Storage
    private static ConfigManager _config = new ConfigManager();
    private static LocalCacheManager _cache = new LocalCacheManager();
    private static ConcurrentDictionary<string, DateTime> _activeSensors = new();
    private static ConcurrentDictionary<(string, string), ConcurrentBag<(DateTime, double)>> _valuesToForward = new();
    private static ConcurrentDictionary<string, byte[]> _latestFrames = new();

    // gRPC Client for Python Microservice
    private static PreProcessingService.PreProcessingServiceClient _rpcClient;

    static async Task Main(string[] args) {
        _config.LoadConfig();

        Console.WriteLine($"[SYSTEM] Starting Gateway {GID}...");

        // 1. Initialize gRPC channel pointing to the local Python Microservice
        string rpcUrl = Environment.GetEnvironmentVariable("PREPROCESS_RPC_URL") ?? "http://localhost:50051";
        var grpcChannel = GrpcChannel.ForAddress(rpcUrl);
        _rpcClient = new PreProcessingService.PreProcessingServiceClient(grpcChannel);
        Console.WriteLine("[RPC] Connection established to Python PreProcessing Service on port 50051.");

        // 2. Start parallel execution routines
        _ = Task.Run(ConnectToServerLoopAsync);
        _ = Task.Run(GatewayHeartbeatRoutineAsync);
        _ = Task.Run(BatchDataRoutineAsync);
        _ = Task.Run(StartWebServerAsync);
        _ = Task.Run(SensorTimeoutMonitorRoutineAsync);

        // 3. Start consuming background events from RabbitMQ
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
            else {
                Console.WriteLine("[ERROR] Unknown command. Use DISCONN.");
            }
        }
    }

    private static void StartRabbitMQConsumer() {
        var rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
        var factory = new ConnectionFactory() { HostName = rabbitHost, AutomaticRecoveryEnabled = true };

        int maxRetries = 10;
        int delayMs = 3000;

        for (int i = 1; i <= maxRetries; i++) {
            try {
                Console.WriteLine($"[RABBITMQ] Gateway attempting to connect to {rabbitHost}... (Attempt {i}/{maxRetries})");
                
                var connection = factory.CreateConnection();
                var channel = connection.CreateModel();

                channel.ExchangeDeclare(exchange: "urbanhealth_exchange", type: ExchangeType.Topic);

                // Declare an exclusive ephemeral queue for this Gateway instance
                var queueName = channel.QueueDeclare().QueueName;

                // Bind the queue to intercept all sensor data topics
                channel.QueueBind(queue: queueName, exchange: "urbanhealth_exchange", routingKey: "sensor.#");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var body = ea.Body.ToArray();
                    var messageJson = Encoding.UTF8.GetString(body);

                    try {
                        var payload = JsonSerializer.Deserialize<SensorPayload>(messageJson);
                        ProcessRabbitMQMessage(payload);
                    } catch (Exception ex) {
                        Console.WriteLine($"[JSON ERROR] Failed to parse payload: {ex.Message}");
                    }
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine("[RABBITMQ] Gateway listening to distributed topic events securely.");
                
                return; // Ligação bem sucedida! Sai do ciclo de tentativas.

            } catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException) {
                Console.WriteLine($"[RABBITMQ WARNING] Broker is still booting. Gateway retrying in {delayMs/1000} seconds...");
                Thread.Sleep(delayMs);
            } catch (Exception ex) {
                Console.WriteLine($"[RABBITMQ WARNING] Unexpected error: {ex.Message}. Gateway retrying in {delayMs/1000} seconds...");
                Thread.Sleep(delayMs);
            }
        }

        Console.WriteLine("[RABBITMQ CRITICAL] Infrastructure error: Failed to connect after maximum retries.");
    }

    private static void ProcessRabbitMQMessage(SensorPayload payload) {
        if (payload == null) return;

        _activeSensors[payload.SID] = DateTime.Now;

        // Handle incoming raw binary video streams via Base64 mapping
        if (payload.Type == "VIDEO" && !string.IsNullOrEmpty(payload.ImageData)) {
            try {
                _latestFrames[payload.SID] = Convert.FromBase64String(payload.ImageData);
            } catch { }
            return;
        }

        // Filter system messaging overhead
        if (payload.Type == "STS" || payload.Type == "HB" || payload.Type == "STRM") {
            return;
        }

        // Process telemetry metrics through gRPC clean rules
        try {
            if (!double.TryParse(payload.Value, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double numericValue)) {
                Console.WriteLine($"[PARSING ERROR] Cannot convert value '{payload.Value}' for Sensor {payload.SID}");
                return;
            }

            // Map data to the exact compiled Protobuf contract structures
            var rpcRequest = new DataRequest {
                SensorId = payload.SID,
                DataType = payload.Type,
                RawValue = numericValue
            };

            // Execute blocking remote call to python microservice
            var rpcResponse = _rpcClient.ProcessData(rpcRequest);

            if (rpcResponse.Success) {
                Console.WriteLine($"[RPC SUCCESS] {payload.SID} [{payload.Type}] verified: {rpcResponse.ProcessedValue}");

                // Buffer clean metrics inside thread-safe local bag for safe cloud batching
                var bufferKey = (payload.SID, payload.Type);
                var readingsBag = _valuesToForward.GetOrAdd(bufferKey, _ => new ConcurrentBag<(DateTime, double)>());
                readingsBag.Add((DateTime.Now, rpcResponse.ProcessedValue));
            }
            else {
                Console.WriteLine($"[RPC REJECTED] Clean rules failed for {payload.SID}: {rpcResponse.Message}");
            }
        } catch (Exception ex) {
            Console.WriteLine($"[RPC FAULT] PreProcessing server unreachable: {ex.Message}");
        }
    }

    private static async Task BatchDataRoutineAsync() {
        int batchWindowMs = 30000; // Flushes accumulated metrics to cloud every 30 seconds

        while (true) {
            await Task.Delay(batchWindowMs);

            // 1. Process local offline storage recovery checks first
            var pendingReadings = _cache.GetPendingReadings();
            if (pendingReadings.Count > 0) {
                Console.WriteLine($"[RECOVERY] Found {pendingReadings.Count} cached rows in local SQLite database. Attempting synchronization...");
                var groupedPending = pendingReadings.GroupBy(x => (x.Sid, x.Type));

                foreach (var group in groupedPending) {
                    var sensorId = group.Key.Sid;
                    var dataType = group.Key.Type;
                    var (exists, zone, _, _, _) = _config.ValidateSensor(sensorId);

                    var payloadList = group.Select(item => new Dictionary<string, string> {
                        { "Timestamp", item.Ts.ToString("o") },
                        { "Value", item.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                    }).ToList();

                    var cacheMsg = new Message { CMD = "FWD", SID = sensorId, GID = GID };
                    cacheMsg.Data["TYPE"] = dataType;
                    cacheMsg.Data["ZONE"] = zone;
                    cacheMsg.Data["BATCH_COUNT"] = payloadList.Count.ToString();
                    cacheMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                    try {
                        if (_serverClient == null || !_serverClient.Connected) break;

                        await _serverTxLock.WaitAsync();
                        try { await Message.SendMessageAsync(_serverClient, cacheMsg); } finally { _serverTxLock.Release(); }

                        _cache.DeleteReadings(group.Select(x => x.Id));
                        Console.WriteLine($"[RECOVERY SUCCESS] Synced {payloadList.Count} cached rows for {sensorId}.");
                    } catch { break; }
                }
            }

            // 2. Flush current volatile operational memory state
            foreach (var key in _valuesToForward.Keys) {
                if (_valuesToForward.TryRemove(key, out var readingsBag)) {
                    var snapshot = readingsBag.ToList();
                    if (snapshot.Count == 0) continue;

                    string sensorId = key.Item1;
                    string dataType = key.Item2;
                    var (_, zone, _, _, _) = _config.ValidateSensor(sensorId);

                    var payloadList = snapshot.Select(item => new Dictionary<string, string> {
                        { "Timestamp", item.Item1.ToString("o") },
                        { "Value", item.Item2.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                    }).ToList();

                    var batchMsg = new Message { CMD = "FWD", SID = sensorId, GID = GID };
                    batchMsg.Data["TYPE"] = dataType;
                    batchMsg.Data["ZONE"] = zone;
                    batchMsg.Data["BATCH_COUNT"] = snapshot.Count.ToString();
                    batchMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                    try {
                        if (_serverClient == null || !_serverClient.Connected) throw new Exception("Cloud network down");

                        await _serverTxLock.WaitAsync();
                        try { await Message.SendMessageAsync(_serverClient, batchMsg); } finally { _serverTxLock.Release(); }
                        Console.WriteLine($"[UPSTREAM] Packet with {snapshot.Count} entries from {sensorId} transmitted successfully.");
                    } catch {
                        Console.WriteLine($"[OFFLINE STORAGE] Cloud unreachable. Saving {snapshot.Count} metrics from {sensorId} into SQLite.");
                        foreach (var reading in snapshot) {
                            _cache.SaveReading(sensorId, dataType, reading.Item2, reading.Item1);
                        }
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
                Console.WriteLine($"[UPSTREAM] Connecting to Central Cloud Server ({ServerIP}:{ServerPort})...");

                await _serverClient.ConnectAsync(ServerIP, ServerPort);
                Console.WriteLine("[UPSTREAM] Connection established with Cloud Server!");
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

            int jitter = _rnd.Next(0, 1000);
            int sleepTime = currentDelayMs + jitter;
            Console.WriteLine($"[UPSTREAM LINK LOST] Reconnecting to cloud in {sleepTime / 1000.0:F1}s...");
            await Task.Delay(sleepTime);
            currentDelayMs = Math.Min(currentDelayMs * 2, maxDelayMs);
        }
    }

    private static async Task GatewayHeartbeatRoutineAsync() {
        while (true) {
            await Task.Delay(10000);
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
            await Task.Delay(5000); // Scans tracking context states every 5 seconds
            bool stateChanged = false;

            foreach (var sensor in _activeSensors) {
                if ((DateTime.Now - sensor.Value).TotalSeconds > 30) {
                    Console.WriteLine($"[TIMEOUT] No active communication events from {sensor.Key} for 30s. Setting offline.");
                    _activeSensors.TryRemove(sensor.Key, out _);
                    _config.UpdateSensorState(sensor.Key, "offline");
                    stateChanged = true;
                }
            }
            if (stateChanged) {
                _config.SaveConfig();
            }
        }
    }

    private static async Task PerformGracefulShutdownAsync() {
        Console.WriteLine("\n[SHUTDOWN] Terminating operations clean...");
        if (_serverClient != null && _serverClient.Connected) {
            try {
                var byeMsg = new Message { CMD = "DISCONN", GID = GID, SID = "GATEWAY" };
                await _serverTxLock.WaitAsync();
                try { await Message.SendMessageAsync(_serverClient, byeMsg); } finally { _serverTxLock.Release(); }

                await Task.Delay(500);
                _serverClient.Close();
            } catch { }
        }
        Console.WriteLine("[SHUTDOWN] Exiting complete.");
        Environment.Exit(0);
    }

    private static async Task StartWebServerAsync() {
        try {
            var listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");
            listener.Start();
            Console.WriteLine("[WEB SERVER] Edge HTTP Streaming service online on port 8080.");

            while (true) {
                var context = await listener.GetContextAsync();
                var req = context.Request;
                var res = context.Response;

                try {
                    if (req.Url.AbsolutePath.StartsWith("/stream/")) {
                        string sid = req.Url.AbsolutePath.Split('/').Last();
                        string html = $@"
                            <!DOCTYPE html>
                            <html>
                            <body style='background:#1e1e1e;color:white;text-align:center;font-family:Segoe UI,Arial;'>
                                <h2>Live Feed (RabbitMQ Core): {sid}</h2>
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
                        else {
                            res.StatusCode = 404;
                        }
                    }
                    else {
                        res.StatusCode = 404;
                    }
                } finally {
                    res.Close();
                }
            }
        } catch (Exception ex) {
            Console.WriteLine($"[WEB SERVER FAULT] Engine error: {ex.Message}");
        }
    }
}