using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Gateway {

    public class NetworkingConfig {
        [JsonPropertyName("UdpListenPort")]
        public int UdpListenPort { get; set; } = 5004;

        [JsonPropertyName("ServerIp")]
        public string ServerIp { get; set; } = "127.0.0.1";

        [JsonPropertyName("ServerPort")]
        public int ServerPort { get; set; } = 5001;

        [JsonPropertyName("ServerUdpPort")]
        public int ServerUdpPort { get; set; } = 5003;

        [JsonPropertyName("PreprocessRpcUrl")]
        public string PreprocessRpcUrl { get; set; } = "http://local:50051";

        [JsonPropertyName("RabbitMqHost")]
        public string RabbitMqHost { get; set; } = "localhost";

        [JsonPropertyName("RabbitMq_User")]
        public string RabbitMqUser { get; set; } = "guest";

        [JsonPropertyName("RabbitMq_Password")]
        public string RabbitMqPassword { get; set; } = "guest";
    }

    public class StreamingConfig {
        [JsonPropertyName("Video_UDP_Chunk_Size")]
        public int VideoUdpChunkSize { get; set; } = 1200;

        [JsonPropertyName("VIDEO_FRAME_INTERVAL_MS")]
        public int VideoFrameIntervalMs { get; set; } = 200;

        [JsonPropertyName("VIDEO_FRAME_CACHE_RELOAD_MS")]
        public int VideoFrameCacheReloadMs { get; set; } = 30000;

        [JsonPropertyName("VIDEO_PACKET_DELAY_MS")]
        public int VideoPacketDelayMs { get; set; } = 0;

        [JsonPropertyName("VIDEO_FRAME_TTL_MS")]
        public int VideoFrameTtlMs { get; set; } = 750;

        [JsonPropertyName("VIDEO_MAX_PENDING_FRAMES_PER_SENSOR")]
        public int VideoMaxPendingFramesPerSensor { get; set; } = 3;

        [JsonPropertyName("VIDEO_MAX_FRAME_BYTES")]
        public int VideoMaxFrameBytes { get; set; } = 4194304;

        [JsonPropertyName("VIDEO_MAX_PARTS_PER_FRAME")]
        public int VideoMaxPartsPerFrame { get; set; } = 512;

        [JsonPropertyName("VIDEO_DEBUG_PACKETS")]
        public bool VideoDebugPackets { get; set; } = false;

        [JsonPropertyName("GATEWAY_ENABLE_LOCAL_VIDEO_PREVIEW")]
        public bool GatewayEnableLocalVideoPreview { get; set; } = false;
    }

    public class RabbitMQConfig {
        [JsonPropertyName("Exchange")]
        public string Exchange { get; set; } = "urbanhealth_exchange";

        [JsonPropertyName("RoutingKeys")]
        public List<string> RoutingKeys { get; set; } = new List<string> { "sensor.#" };

        [JsonPropertyName("ConnectionRetries")]
        public int ConnectionRetries { get; set; } = 30;

        [JsonPropertyName("RetryDelayMs")]
        public int RetryDelayMs { get; set; } = 3000;
    }

    public class TimingConfig {
        [JsonPropertyName("BatchIntervalMs")]
        public int BatchIntervalMs { get; set; } = 30000;

        [JsonPropertyName("HeartbeatIntervalMs")]
        public int HeartbeatIntervalMs { get; set; } = 10000;

        [JsonPropertyName("SensorTimeoutCheckMs")]
        public int SensorTimeoutCheckMs { get; set; } = 5000;

        [JsonPropertyName("SensorTimeoutThresholdSecs")]
        public int SensorTimeoutThresholdSecs { get; set; } = 30;
    }

    public class GatewayConfig {
        [JsonPropertyName("GatewayId")]
        public string GatewayId { get; set; } = "G101";

        [JsonPropertyName("Networking")]
        public NetworkingConfig Networking { get; set; } = new NetworkingConfig();

        [JsonPropertyName("Streaming")]
        public StreamingConfig Streaming { get; set; } = new StreamingConfig();

        [JsonPropertyName("Rabbitmq")]
        public RabbitMQConfig Rabbitmq { get; set; } = new RabbitMQConfig();

        [JsonPropertyName("Timings")]
        public TimingConfig Timings { get; set; } = new TimingConfig();
    }

    public class SensorsRoot {
        public List<SensorConfig> Sensors { get; set; } = new List<SensorConfig>();
    }

    public class SensorConfig {
        public string Id { get; set; }
        public string Zone { get; set; }

        // Mapeia o nome exato que escreveste no JSON para uma propriedade C# robusta
        [JsonPropertyName("state")]
        public string State { get; set; }

        public string DataTypes { get; set; } // Vem do JSON como "TEMP, HUM, PM2..."
        public DateTime LastSync { get; set; }

        // Ajuda o Program.cs a validar tipos separando automaticamente a string pelas vírgulas
        [JsonIgnore]
        public string[] SupportedTypesArray => string.IsNullOrWhiteSpace(DataTypes) 
            ? Array.Empty<string>() 
            : DataTypes.Split(',').Select(x => x.Trim()).ToArray();
    }

    public class ConfigManager {

        private readonly string _gatewayConfigPath;
        private readonly string _sensorsConfigPath;

        public GatewayConfig GatewayInfo { get; private set; }
        private ConcurrentDictionary<string, SensorConfig> _sensors;

        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions {
            PropertyNameCaseInsensitive = true,
            WriteIndented = true
        };

        public ConfigManager() {

            string gid = Environment.GetEnvironmentVariable("GID") ?? "G101";

            _gatewayConfigPath = $"Configs/gateway-config-{gid}.json";
            _sensorsConfigPath = $"Configs/sensors-config-{gid}.json";

            _sensors = new ConcurrentDictionary<string, SensorConfig>();
        }
        public void LoadConfig() {

            // Carregar Gateway
            if (File.Exists(_gatewayConfigPath)) {
                string gwJson = File.ReadAllText(_gatewayConfigPath);
                GatewayInfo = JsonSerializer.Deserialize<GatewayConfig>(gwJson, _jsonOptions) ?? new GatewayConfig();
            } else {
                GatewayInfo = new GatewayConfig();
                File.WriteAllText(_gatewayConfigPath, JsonSerializer.Serialize(GatewayInfo, _jsonOptions));
            }

            // Carregar Sensores (usando o Root que contém o array)
            if (File.Exists(_sensorsConfigPath)) {
                string sensorsJson = File.ReadAllText(_sensorsConfigPath);
                var root = JsonSerializer.Deserialize<SensorsRoot>(sensorsJson, _jsonOptions) ?? new SensorsRoot();
                
                _sensors.Clear();
                foreach (var s in root.Sensors) {
                    _sensors[s.Id] = s;
                }
            } else {
                SaveSensorsConfig(); // Cria ficheiro vazio para evitar erros
            }
   
        }

        public void SaveSensorsConfig() {
            var root = new SensorsRoot { Sensors = _sensors.Values.ToList() };
            string json = JsonSerializer.Serialize(root, _jsonOptions);
            File.WriteAllText(_sensorsConfigPath, json);
        }

        public (bool Exists, string Zone, string Status, string[] SupportedTypes, DateTime LastSeen) ValidateSensor(string sensorId) {
            if (_sensors.TryGetValue(sensorId, out var sensor)) {
                return (true, sensor.Zone, sensor.State, sensor.SupportedTypesArray, sensor.LastSync);
            }
            return (false, "", "", [], DateTime.MinValue);
        }

        public void UpdateSensorState(string sensorId, string status) {
            if (_sensors.TryGetValue(sensorId, out var sensor)) {
                sensor.State = status;
                sensor.LastSync = DateTime.Now;
                SaveSensorsConfig();
            }
        }

        public void UpdateSensorDataTypes(string sensorId, string dataTypes) {
            if (_sensors.TryGetValue(sensorId, out var sensor)) {
                // Só acede ao disco (SaveSensorsConfig) se houver realmente uma diferença
                // Isto evita desgaste de I/O no disco sempre que um sensor se reconecta
                if (sensor.DataTypes != dataTypes) {
                    sensor.DataTypes = dataTypes;
                    sensor.LastSync = DateTime.Now;
                    SaveSensorsConfig();
                    Console.WriteLine($"[CONFIG] Capacidades do sensor {sensorId} atualizadas no JSON: {dataTypes}");
                }
            }
        }
    }
}
