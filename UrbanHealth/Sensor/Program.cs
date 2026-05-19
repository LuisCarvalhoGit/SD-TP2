using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using RabbitMQ.Client;

class Program {
    private static string SID = "S101";

    // State management
    private static bool _isStreaming = false;
    private static DateTime _lastAlertTime = DateTime.MinValue;
    private static string[] _supportedTypes = { "TEMP", "HUM", "PM2", "CO2", "NOISE", "UV", "VIDEO" };

    // RabbitMQ
    private static IConnection _rmqConnection;
    private static IModel _channel;
    private const string ExchangeName = "urbanhealth_exchange";

    static async Task Main(string[] args) {
        if (args.Length >= 1) {
            SID = args[0];
        }

        Console.WriteLine($"[SYSTEM] Starting Sensor {SID} (RabbitMQ Pub/Sub)...");

        InitRabbitMQ();

        Console.WriteLine("==================================================");
        Console.WriteLine(" Interactive Menu. Available commands:");
        Console.WriteLine(" -> DATA <TYPE> <VALUE> (e.g., DATA HUM 65.2)");
        Console.WriteLine(" -> STRM START (to start video transmission)");
        Console.WriteLine(" -> STRM STOP (to stop video transmission)");
        Console.WriteLine(" -> DISCONN (to gracefully shutdown)");
        Console.WriteLine("==================================================\n");

        // Publish initial status
        PublishMessage("STS", "ONLINE");

        // Start parallel routines (now without depending on TCP authentication)
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
            else if (command == "STRM") {
                if (parts.Length >= 2) {
                    string action = parts[1].ToUpper();
                    if (action == "START") {
                        _isStreaming = true;
                        Console.WriteLine("[STREAM] Video transmission STARTED manually.");
                    }
                    else if (action == "STOP") {
                        _isStreaming = false;
                        Console.WriteLine("[STREAM] Video transmission STOPPED manually.");
                    }
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
        var rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
        var factory = new ConnectionFactory() { HostName = rabbitHost, AutomaticRecoveryEnabled = true };

        int maxRetries = 10;
        int delayMs = 3000;

        for (int i = 0; i <= maxRetries; i++)
        {
            try
            {
                Console.WriteLine($"[RABBITMQ] Attempting to connect to {rabbitHost}... (Attempt {i}/{maxRetries})");
                
                _rmqConnection = factory.CreateConnection();
                _channel = _rmqConnection.CreateModel();

                // Declare a Topic Exchange
                _channel.ExchangeDeclare(exchange: ExchangeName, type: ExchangeType.Topic);

                Console.WriteLine("[RABBITMQ] Connection established securely!");
                return;
            } catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException) {

                Console.WriteLine($"[RABBITMQ WARNING] Broker is still booting. Retrying in {delayMs/1000} seconds...");
                Thread.Sleep(delayMs);

            } catch (Exception ex) {

                Console.WriteLine($"[RABBITMQ FATAL] Unexpected connection error: {ex.Message}");
                Thread.Sleep(delayMs);
            }
        }

        throw new Exception("CRITICAL: Failed to connect to RabbitMQ broker after maximum retries. Shutting down.");
        
    }

    // Generic method to publish JSON messages
    private static void PublishMessage(string type, string value, string action = null, string base64Image = null) {
        try {
            // The Routing Key defines who will receive it (e.g., sensor.S101.TEMP)
            string routingKey = $"sensor.{SID}.{type}";

            var payload = new {
                SID = SID,
                Timestamp = DateTime.Now.ToString("o"),
                Type = type,
                Value = value,
                Action = action, // Used for STRM START/STOP
                ImageData = base64Image // Used to send video frames
            };

            string json = JsonSerializer.Serialize(payload);
            var body = Encoding.UTF8.GetBytes(json);

            _channel.BasicPublish(
                exchange: ExchangeName,
                routingKey: routingKey,
                basicProperties: null,
                body: body
            );
        } catch (Exception ex) {
            Console.WriteLine($"[ERROR] Failed to publish to RabbitMQ: {ex.Message}");
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

            string selectedType = _supportedTypes[rnd.Next(_supportedTypes.Length - 1)]; // Excludes VIDEO from random
            double value = selectedType switch {
                "TEMP" => 15.0 + (rnd.NextDouble() * 20.0),
                "HUM" => 40.0 + (rnd.NextDouble() * 40.0),
                "PM2" => 5.0 + (rnd.NextDouble() * 45.0),
                "CO2" => 400.0 + (rnd.NextDouble() * 600.0),
                "NOISE" => 40.0 + (rnd.NextDouble() * 50.0),
                "UV" => rnd.NextDouble() * 10.0,
                _ => 0
            };

            string strValue = value.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture);

            PublishMessage(selectedType, strValue);
            Console.WriteLine($"[DATA] Published {selectedType}: {strValue}");

            HandleAlertLogic(selectedType, value);
        }
    }

    private static void HandleAlertLogic(string dataType, double value) {
        bool isAlert = dataType switch {
            "TEMP" => value > 33.0,
            "HUM" => value > 78.0,
            "PM2" => value > 48.0,
            "CO2" => value > 995.0,
            "NOISE" => value > 88.0,
            "UV" => value > 9.0,
            _ => false,
        };

        if (isAlert) {
            Console.WriteLine($"[SENSOR] High level of {dataType} detected! Starting video...");
            _lastAlertTime = DateTime.Now;

            if (!_isStreaming) {
                _isStreaming = true;
                PublishMessage("STRM", "", action: "START");
            }
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

        while (true) {
            if (!_isStreaming) {
                await Task.Delay(1000);
                continue;
            }

            string[] frames = Directory.Exists(framesFolder) ? Directory.GetFiles(framesFolder, "*.jpg") : Array.Empty<string>();

            if (frames.Length == 0) {
                await Task.Delay(2000);
                continue;
            }

            foreach (var framePath in frames) {
                if (!_isStreaming) break;

                await Task.Delay(200); // 5 FPS

                try {
                    byte[] imageBytes = await File.ReadAllBytesAsync(framePath);
                    // Since RabbitMQ can handle large messages (unlike UDP where we split every 1400 bytes), 
                    // we can send the entire image as Base64 in a single JSON message, simplifying the Gateway.
                    string base64 = Convert.ToBase64String(imageBytes);

                    PublishMessage("VIDEO", "", base64Image: base64);
                } catch { }
            }
        }
    }
}