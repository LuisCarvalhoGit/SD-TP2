using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class Program {
    private static string SID = "S101";

    private static bool _isStreaming = false;
    private static DateTime _lastAlertTime = DateTime.MinValue;
    private static string[] _supportedTypes = { "TEMP", "HUM", "PM2", "CO2", "NOISE", "UV", "VIDEO" };

    private static IConnection _rmqConnection;
    private static IModel _channel;
    private const string ExchangeName = "urbanhealth_exchange";

    private static string GatewayIp = Environment.GetEnvironmentVariable("GATEWAY_IP") ?? "127.0.0.1";
    private static int GatewayUdpPort = int.Parse(Environment.GetEnvironmentVariable("GATEWAY_UDP_PORT") ?? "5002");

    static async Task Main(string[] args) {
        if (args.Length >= 1) SID = args[0];

        Console.WriteLine($"[SYSTEM] Starting Sensor {SID} (RabbitMQ + UDP)...");
        Console.WriteLine($"[DEBUG] Target Gateway UDP: {GatewayIp}:{GatewayUdpPort}");

        // Validate configuration
        if (string.IsNullOrWhiteSpace(GatewayIp)) {
            Console.WriteLine("[ERROR] GATEWAY_IP not configured!");
            Environment.Exit(1);
        }
        if (GatewayUdpPort <= 0 || GatewayUdpPort > 65535) {
            Console.WriteLine($"[ERROR] GATEWAY_UDP_PORT invalid: {GatewayUdpPort}");
            Environment.Exit(1);
        }

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
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
        };

        for (int i = 0; i <= 20; i++) {
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
                return;
            } catch {
                Thread.Sleep(3000);
            }
        }
    }

    private static void PublishMessage(string type, string value, string action = null) {
        try {
            string routingKey = $"sensor.{SID}.{type}";
            var payload = new {
                SID = SID, Timestamp = DateTime.UtcNow.ToString("o"), Type = type, Value = value, Action = action
            };
            string json = JsonSerializer.Serialize(payload);
            var body = Encoding.UTF8.GetBytes(json);
            _channel.BasicPublish(exchange: ExchangeName, routingKey: routingKey, basicProperties: null, body: body);
            Console.WriteLine($"[DEBUG RMQ] Published -> Type:{type} Value:{value}");
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
            await Task.Delay(7000);
            string selectedType = _supportedTypes[rnd.Next(_supportedTypes.Length - 1)]; 
            double value = selectedType switch {
                "TEMP" => 15.0 + (rnd.NextDouble() * 20.0), "HUM" => 40.0 + (rnd.NextDouble() * 40.0),
                "PM2" => 5.0 + (rnd.NextDouble() * 45.0), "CO2" => 400.0 + (rnd.NextDouble() * 600.0),
                "NOISE" => 40.0 + (rnd.NextDouble() * 50.0), "UV" => rnd.NextDouble() * 10.0, _ => 0
            };
            string strValue = value.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture);
            PublishMessage(selectedType, strValue);
            HandleAlertLogic(selectedType, value);
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
        else if (_isStreaming && (DateTime.Now - _lastAlertTime).TotalSeconds > 30) {
            Console.WriteLine("[STREAM] Environment stabilized for 30s. Stopping video...");
            _isStreaming = false;
            PublishMessage("STRM", "", action: "STOP");
        }
    }

    private static async Task VideoStreamRoutineAsync() {
        string framesFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Frames");
        if (!Directory.Exists(framesFolder)) Directory.CreateDirectory(framesFolder);

        using var udpClient = new UdpClient();
        const int chunkSize = 1400; 

        while (true) {
            if (!_isStreaming) {
                await Task.Delay(1000);
                continue;
            }

            string[] frames = Directory.Exists(framesFolder) ? Directory.GetFiles(framesFolder, "*.jpg") : Array.Empty<string>();
            if (frames.Length == 0) { await Task.Delay(2000); continue; }

            foreach (var framePath in frames) {
                if (!_isStreaming) break;
                await Task.Delay(200); // 5 FPS

                try {
                    byte[] imageBytes = await File.ReadAllBytesAsync(framePath);
                    int totalParts = (int)Math.Ceiling((double)imageBytes.Length / chunkSize);
                    
                    Console.WriteLine($"[DEBUG UDP] A enviar frame de {Path.GetFileName(framePath)} em {totalParts} pacotes...");

                    for (int i = 0; i < totalParts; i++) {
                        int currentOffset = i * chunkSize;
                        int size = Math.Min(chunkSize, imageBytes.Length - currentOffset);
                        byte[] buffer = new byte[size];
                        Buffer.BlockCopy(imageBytes, currentOffset, buffer, 0, size);

                        var videoMsg = new Shared.Message { CMD = "STRM", SID = SID, GID = "G101" };
                        videoMsg.Data["TYPE"] = "DATA";
                        videoMsg.Data["PART"] = (i + 1).ToString();
                        videoMsg.Data["TOTAL"] = totalParts.ToString();
                        videoMsg.BinaryData = buffer;

                        byte[] packet = videoMsg.ToUdpBytes();
                        await udpClient.SendAsync(packet, packet.Length, GatewayIp, GatewayUdpPort);
                    }
                } catch (Exception ex) { Console.WriteLine($"[DEBUG UDP ERROR] Erro a enviar frame: {ex.Message}"); }
            }
        }
    }
}