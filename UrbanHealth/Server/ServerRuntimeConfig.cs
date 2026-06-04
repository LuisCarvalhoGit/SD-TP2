using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

public class ServerNetworkingConfig {
    public int TcpPort { get; set; } = 5001;
    public int DashboardPort { get; set; } = 8081;
    public int UdpPort { get; set; } = 5003;
    public string AnalysisRpcUrl { get; set; } = "http://local:50052";
}

public class ServerStreamingConfig {
    [JsonPropertyName("VIDEO_FRAME_TTL_MS")]
    public int VideoFrameTtlMs { get; set; } = 750;

    [JsonPropertyName("VIDEO_MAX_PENDING_FRAMES_PER_SENSOR")]
    public int VideoMaxPendingFramesPerSensor { get; set; } = 3;

    [JsonPropertyName("VIDEO_MAX_FRAME_BYTES")]
    public int VideoMaxFrameBytes { get; set; } = 4 * 1024 * 1024;

    [JsonPropertyName("VIDEO_MAX_PARTS_PER_FRAME")]
    public int VideoMaxPartsPerFrame { get; set; } = 512;

    [JsonPropertyName("VIDEO_DEBUG_PACKETS")]
    public bool VideoDebugPackets { get; set; } = false;
}

public class ServerRuntimeConfig {
    public ServerNetworkingConfig Networking { get; set; } = new();
    public ServerStreamingConfig Streaming { get; set; } = new();

    public static ServerRuntimeConfig Load() {
        const string configPath = "Configs/server-config.json";
        if (!File.Exists(configPath)) return new ServerRuntimeConfig();

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        string json = File.ReadAllText(configPath);
        return JsonSerializer.Deserialize<ServerRuntimeConfig>(json, options) ?? new ServerRuntimeConfig();
    }
}
