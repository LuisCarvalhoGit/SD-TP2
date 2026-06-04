using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json.Serialization;
using Shared;

public class SensorConfiguration
{
    public string Description { get; set; } = "Default Sensor";
    public string[] SupportedTypes { get; set; } = { "TEMP", "HUM", "PM2", "CO2", "NOISE", "UV", "VIDEO" };
    public int FrequencySeconds { get; set; } = 7;

    // Conecta o sub-objeto JSON "Networking" a esta propriedade
    public NetworkingConfig Networking { get; set; } = new NetworkingConfig();

    // Conecta o sub-objeto JSON "Streaming" a esta propriedade
    public StreamingConfig Streaming { get; set; } = new StreamingConfig();
}

public class NetworkingConfig
{
    public int TargetGatewayUdpPort { get; set; } = 5004;
    public string TargetGatewayIp { get; set; } = "host.docker.internal";
    public string TargetRabbitMqHost { get; set; } = "host.docker.internal";
    
    [JsonPropertyName("RabbitMq_User")]
    public string RabbitMqUser { get; set; } = "guest";
    
    [JsonPropertyName("RabbitMq_Password")]
    public string RabbitMqPassword { get; set; } = "guest";
}

public class StreamingConfig
{
    [JsonPropertyName("Video_UDP_Chunk_Size")]
    public int VideoUDPChunkSize { get; set; } = 1200;

    [JsonPropertyName("VIDEO_FRAME_INTERVAL_MS")]
    public int VideoFrameIntervalMs { get; set; } = 200;

    [JsonPropertyName("VIDEO_FRAME_CACHE_RELOAD_MS")]
    public int VideoFrameCacheReloadMs { get; set; } = 30000;

    [JsonPropertyName("VIDEO_PACKET_DELAY_MS")]
    public int VideoPacketDelayMs { get; set; } = 0;

    [JsonPropertyName("STREAM_AUTO_STOP_ENABLED")]
    public bool StreamAutoStopEnabled { get; set; } = false;

    [JsonPropertyName("STREAM_AUTO_STOP_AFTER_SECONDS")]
    public int StreamAutoStopAfterSeconds { get; set; } = 30;
}

class Program {
    private static string SID = Environment.GetEnvironmentVariable("SID") ?? "S_DEV";

    private static SensorConfiguration _config = new SensorConfiguration();

    private static bool _isStreaming = false;
    private static DateTime _lastAlertTime = DateTime.MinValue;
    private static string[] _supportedTypes = { "TEMP", "HUM", "PM2", "CO2", "NOISE", "UV", "VIDEO" };

    private static IConnection _rmqConnection;
    private static IModel _channel;
    private const string ExchangeName = "urbanhealth_exchange";
    private static readonly object _rmqLock = new object();

    private static string GatewayIp;
    private static int GatewayUdpPort;
    private static int VideoFrameIntervalMs;
    private static int VideoPacketDelayMs;
    private static int VideoChunkSize;
    private static int VideoFrameCacheReloadMs;
    private static bool StreamAutoStopEnabled;
    private static int StreamAutoStopAfterSeconds;

    private sealed class CachedVideoFrame {
        public CachedVideoFrame(string name, byte[] bytes) {
            Name = name;
            Bytes = bytes;
        }

        public string Name { get; }
        public byte[] Bytes { get; }
    }

    static async Task Main(string[] args) {
        if (args.Length >= 1) SID = args[0];

        string configFilePath = $"/app/configs/sensor-config-{SID}.json";


        if (File.Exists(configFilePath))
        {
            try
            {
                
                string jsonString = File.ReadAllText(configFilePath);
                var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                _config = JsonSerializer.Deserialize<SensorConfiguration>(jsonString, options) ?? new SensorConfiguration();

                Console.WriteLine($"[SYSTEM] Loaded config: {_config.Description}");

            } catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Error parsing JSON: {ex.Message}. Using defaults.");
            }

        } else
        {
            Console.WriteLine($"[WARNING] File {configFilePath} not found. Using default configuration.");
        }

        GatewayIp = EndpointResolver.ResolveHost(_config.Networking.TargetGatewayIp);
        GatewayUdpPort = _config.Networking.TargetGatewayUdpPort;

        VideoFrameIntervalMs = _config.Streaming.VideoFrameIntervalMs;
        VideoPacketDelayMs = _config.Streaming.VideoPacketDelayMs;
        VideoFrameCacheReloadMs = _config.Streaming.VideoFrameCacheReloadMs;
        VideoChunkSize = _config.Streaming.VideoUDPChunkSize;
        StreamAutoStopEnabled = _config.Streaming.StreamAutoStopEnabled;
        StreamAutoStopAfterSeconds = _config.Streaming.StreamAutoStopAfterSeconds;

        Console.WriteLine($"[SYSTEM] Starting Sensor {SID} (RabbitMQ + UDP)...");
        Console.WriteLine($"[DEBUG] Target Gateway UDP: {GatewayIp}:{GatewayUdpPort}");

        // Validate configuration
        if (string.IsNullOrWhiteSpace(GatewayIp)) {
            Console.WriteLine("[ERROR] TargetGatewayIp not configured in sensor JSON!");
            Environment.Exit(1);
        }
        if (GatewayUdpPort <= 1000 || GatewayUdpPort > 65535) {
            Console.WriteLine($"[ERROR] GATEWAY_UDP_PORT invalid: {GatewayUdpPort}");
            Environment.Exit(1);
        }

        

        Console.WriteLine($"[SYSTEM] Starting Sensor {SID}...");

        InitRabbitMQ();

        Console.WriteLine("==================================================");
        Console.WriteLine(" Menu: DATA <TYPE> <VAL> | STRM START | STRM STOP | DISCONN");
        Console.WriteLine("==================================================\n");

        PublishMessage("STS", "ONLINE");

        _ = Task.Run(HeartbeatRoutineAsync);
        _ = Task.Run(VideoStreamRoutineAsync);
        _ = Task.Run(DataGenerationRoutineAsync);

        while (true) {
            var input = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(input)) continue;

            var parts = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var command = parts[0].ToUpper();

            if (command == "DISCONN") {
                PublishMessage("STS", "OFFLINE");
                Console.WriteLine("[SENSOR] Shutting down...");
                await Task.Delay(500);
                _channel?.Close();
                _rmqConnection?.Close();
                break;
            }
            else if (command == "STRM" && parts.Length >= 2) {
                string action = parts[1].ToUpper();
                if (action == "START") {
                    Console.WriteLine("[STREAM] Requesting authorization from Gateway via RMQ...");
                    _lastAlertTime = DateTime.MaxValue;
                    PublishMessage("STRM_REQ", "REQUEST");
                }
                else if (action == "STOP") {
                    _isStreaming = false;
                    Console.WriteLine("[STREAM] Video transmission STOPPED manually.");
                    PublishMessage("STRM", "", action: "STOP");
                }
            }
            else if (command == "DATA" && parts.Length >= 3) {
                string dataType = parts[1].ToUpper();
                string dataValue = parts[2];

                PublishMessage(dataType, dataValue);
                Console.WriteLine($"[MANUAL] Sent {dataType}: {dataValue}");

                if (double.TryParse(dataValue, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double numValue)) {
                    HandleAlertLogic(dataType, numValue);
                }
            }
        }
    }

    private static void InitRabbitMQ() {
        var factory = new ConnectionFactory() {
            HostName = EndpointResolver.ResolveHost(_config.Networking.TargetRabbitMqHost),
            UserName = _config.Networking.RabbitMqUser,
            Password = _config.Networking.RabbitMqPassword,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            RequestedHeartbeat = TimeSpan.FromSeconds(15)
        };

        Console.WriteLine($"[DEBUG] A iniciar ligação ao RabbitMQ em: {factory.HostName}...");

        int attempt = 0;
        int delayMs = 1000;

        while (true) {
            attempt++;
            try {
                _rmqConnection = factory.CreateConnection();
                _channel = _rmqConnection.CreateModel();
                _channel.ExchangeDeclare(exchange: ExchangeName, type: ExchangeType.Topic);

                var queueName = _channel.QueueDeclare().QueueName;
                _channel.QueueBind(queue: queueName, exchange: ExchangeName, routingKey: $"cmd.{SID}");

                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (model, ea) => {
                    var body = ea.Body.ToArray();
                    var json = Encoding.UTF8.GetString(body);
                    try {
                        using var doc = JsonDocument.Parse(json);
                        var type = doc.RootElement.GetProperty("Type").GetString();

                        if (type == "STRM_GRANT") {
                            _isStreaming = true;
                            Console.WriteLine($"[STREAM] GRANTED. Streaming UDP to {GatewayIp}:{GatewayUdpPort}");
                        } 
                        else if (type == "STRM_DENIED") {
                            _isStreaming = false;
                            Console.WriteLine("[STREAM] DENIED by Gateway.");
                        }
                    } catch { }
                };
                _channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                
                Console.WriteLine("[SYSTEM] Ligação ao RabbitMQ estabelecida com sucesso!");
                return;
            } catch (Exception ex) {
                int sleepMs = delayMs + Random.Shared.Next(0, 750);
                Console.WriteLine($"[DEBUG] Tentativa RabbitMQ {attempt} falhou: {ex.Message}. Nova tentativa em {sleepMs}ms.");
                Thread.Sleep(sleepMs);
                delayMs = Math.Min(delayMs * 2, 30000);
            }
        }
    }

    private static void PublishMessage(string type, string value, string action = null) {
        if (_channel == null || !_channel.IsOpen) {
            Console.WriteLine($"[DEBUG RMQ ERROR] Mensagem '{type}' não enviada. O canal RabbitMQ não está instanciado.");
            return;
        }

        try {
            string routingKey = $"sensor.{SID}.{type}";
            var payload = new {
                SID = SID, Timestamp = DateTime.UtcNow.ToString("o"), Type = type, Value = value, Action = action
            };
            string json = JsonSerializer.Serialize(payload);
            var body = Encoding.UTF8.GetBytes(json);

            lock (_rmqLock) {
                _channel.BasicPublish(exchange: "urbanhealth_exchange",
                                  routingKey: routingKey,
                                  basicProperties: null,
                                  body: body);
            }
            Console.WriteLine($"[DEBUG RMQ] Published -> Type: {type} Value: {value}");
        } catch (Exception ex) { 
            Console.WriteLine($"[DEBUG RMQ ERROR] Falha a publicar: {ex.Message}"); 
        }
    }

    private static async Task HeartbeatRoutineAsync() {
        while (true) {
            await Task.Delay(10000);
            PublishMessage("HB", "ALIVE");
        }
    }

    private static async Task DataGenerationRoutineAsync() {
        Random rnd = new Random();
        while (true) {

            if (_config.SupportedTypes == null || _config.SupportedTypes.Length == 0) continue;

            string selectedType = _config.SupportedTypes[rnd.Next(_config.SupportedTypes.Length)];

            if (selectedType == "VIDEO") continue;

            double value = selectedType switch {
                "TEMP" => 15.0 + (rnd.NextDouble() * 20.0), "HUM" => 40.0 + (rnd.NextDouble() * 40.0),
                "PM2" => 5.0 + (rnd.NextDouble() * 45.0), "CO2" => 400.0 + (rnd.NextDouble() * 600.0),
                "NOISE" => 40.0 + (rnd.NextDouble() * 50.0), "UV" => rnd.NextDouble() * 10.0, _ => 0
            };
            string strValue = value.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture);
            PublishMessage(selectedType, strValue);
            HandleAlertLogic(selectedType, value);

            await Task.Delay(_config.FrequencySeconds * 1000);
        }
    }

    private static void HandleAlertLogic(string dataType, double value) {
        bool isAlert = dataType switch {
            "TEMP" => value > 33.0, "HUM" => value > 78.0, "PM2" => value > 48.0,
            "CO2" => value > 995.0, "NOISE" => value > 88.0, "UV" => value > 9.0, _ => false,
        };

        if (isAlert) {
            Console.WriteLine($"[SENSOR] High level of {dataType} detected!");
            _lastAlertTime = DateTime.Now;
            if (!_isStreaming) PublishMessage("STRM_REQ", "EMERGENCY");
        }
        else if (StreamAutoStopEnabled &&
                 _isStreaming &&
                 _lastAlertTime != DateTime.MaxValue &&
                 (DateTime.Now - _lastAlertTime).TotalSeconds > StreamAutoStopAfterSeconds) {
                Console.WriteLine($"[STREAM] Environment stabilized for {StreamAutoStopAfterSeconds}s. Stopping video...");
                _isStreaming = false;
                PublishMessage("STRM", "", action: "STOP");
                _lastAlertTime = DateTime.MinValue; // Reseta o tempo
            }
    }

    private static async Task VideoStreamRoutineAsync() {
        string framesFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Frames");
        if (!Directory.Exists(framesFolder)) Directory.CreateDirectory(framesFolder);

        using var udpClient = new UdpClient();
        long frameId = 0;
        var cachedFrames = await LoadVideoFramesAsync(framesFolder);
        DateTime nextFrameReloadUtc = DateTime.UtcNow.AddMilliseconds(VideoFrameCacheReloadMs);

        while (true) {
            if (!_isStreaming) {
                await Task.Delay(1000);
                if (DateTime.UtcNow >= nextFrameReloadUtc) {
                    cachedFrames = await LoadVideoFramesAsync(framesFolder);
                    nextFrameReloadUtc = DateTime.UtcNow.AddMilliseconds(VideoFrameCacheReloadMs);
                }
                continue;
            }

            if (DateTime.UtcNow >= nextFrameReloadUtc) {
                cachedFrames = await LoadVideoFramesAsync(framesFolder);
                nextFrameReloadUtc = DateTime.UtcNow.AddMilliseconds(VideoFrameCacheReloadMs);
            }

            if (cachedFrames.Count == 0) { await Task.Delay(2000); continue; }

            foreach (var frame in cachedFrames) {
                if (!_isStreaming) break;
                await Task.Delay(VideoFrameIntervalMs);
                frameId++;

                try {
                    byte[] imageBytes = frame.Bytes;
                    int totalParts = (int)Math.Ceiling((double)imageBytes.Length / VideoChunkSize);
                    
                    Console.WriteLine($"[DEBUG UDP] A enviar frame de {frame.Name} em {totalParts} pacotes...");

                    for (int i = 0; i < totalParts; i++) {
                        int currentOffset = i * VideoChunkSize;
                        int size = Math.Min(VideoChunkSize, imageBytes.Length - currentOffset);

                        var videoMsg = new Shared.Message { CMD = "STRM", SID = SID, GID = "G101" };
                        videoMsg.Data["TYPE"] = "DATA";
                        videoMsg.Data["PART"] = (i + 1).ToString();
                        videoMsg.Data["TOTAL"] = totalParts.ToString();
                        videoMsg.Data["FRAME"] = frameId.ToString();

                        byte[] packet = videoMsg.ToUdpBytes(imageBytes, currentOffset, size);

                        await udpClient.SendAsync(packet, packet.Length, GatewayIp, GatewayUdpPort);
                        if (VideoPacketDelayMs > 0) await Task.Delay(VideoPacketDelayMs);
                    }
                } catch (Exception ex) { Console.WriteLine($"[DEBUG UDP ERROR] Erro a enviar frame: {ex.Message}"); }
            }
        }
    }

    private static async Task<List<CachedVideoFrame>> LoadVideoFramesAsync(string framesFolder) {
        if (!Directory.Exists(framesFolder)) return new List<CachedVideoFrame>();

        var frames = new List<CachedVideoFrame>();
        string[] files = Directory.GetFiles(framesFolder, "*.jpg").OrderBy(path => path).ToArray();

        foreach (string file in files) {
            try {
                frames.Add(new CachedVideoFrame(Path.GetFileName(file), await File.ReadAllBytesAsync(file)));
            } catch (Exception ex) {
                Console.WriteLine($"[DEBUG UDP WARNING] Falha a carregar frame {Path.GetFileName(file)}: {ex.Message}");
            }
        }

        Console.WriteLine($"[STREAM] {frames.Count} video frames cached from {framesFolder}.");
        return frames;
    }

}
