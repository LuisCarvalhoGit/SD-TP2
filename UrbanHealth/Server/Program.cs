using System;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Grpc.Net.Client;
using Shared; 
using Urbanhealth;
using System.Text;
using Microsoft.Data.Sqlite; 
using System.Threading;
using System.Threading.Channels;

class Program {
    private static DataBaseManager _db = new DataBaseManager();
    private static AnalysisService.AnalysisServiceClient _rpcClient;

    // O molde imutável (record) para transportar os dados até à base de dados
    public record ReadingData(string SensorId, string GatewayId, string Zone, string DataType, double Value, DateTime Timestamp);
    
    // A fila de alta performance. Sendo "Unbounded", estica consoante a RAM, ideal para picos de tráfego.
    private static readonly Channel<ReadingData> _dbQueue = Channel.CreateUnbounded<ReadingData>();

    private static UdpClient _udpListener;
    private static readonly int TcpPort = GetIntEnv("PORT_SERVER_TCP", 5001);
    private static readonly int DashboardPort = GetIntEnv("PORT_DASHBOARD", 8081);
    private static readonly int UdpPort = GetIntEnv("SERVER_UDP_PORT", 5003);
    private static readonly bool VideoDebugPackets = GetBoolEnv("VIDEO_DEBUG_PACKETS", false);
    private static readonly VideoFrameAssembler _videoAssembler = new VideoFrameAssembler(
        TimeSpan.FromMilliseconds(GetIntEnv("VIDEO_FRAME_TTL_MS", 750)),
        GetIntEnv("VIDEO_MAX_PENDING_FRAMES_PER_SENSOR", 3),
        GetIntEnv("VIDEO_MAX_FRAME_BYTES", 4 * 1024 * 1024),
        GetIntEnv("VIDEO_MAX_PARTS_PER_FRAME", 512));
    private static System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _latestServerFrames = new();
    private static System.Collections.Concurrent.ConcurrentDictionary<string, long> _latestServerFrameSequences = new();
    private static long _assembledServerFrames = 0;


    private static System.Collections.Concurrent.ConcurrentDictionary<string, DateTime> _activeGateways = new();
    private static System.Collections.Concurrent.ConcurrentDictionary<string, (string Gateway, DateTime LastSeen)> _activeSensors = new();

    static async Task Main(string[] args) {
        Console.WriteLine("[SYSTEM] Central Cloud Server Starting...");
        Console.WriteLine($"[DEBUG] Server UDP Port: {UdpPort}");

        string rpcUrl = Environment.GetEnvironmentVariable("ANALYSIS_RPC_URL") ?? "http://localhost:50052";
        var channel = GrpcChannel.ForAddress(rpcUrl);
        _rpcClient = new AnalysisService.AnalysisServiceClient(channel);
        Console.WriteLine("[RPC] Connection active with Analysis Engine on port 50052.");

        _udpListener = new UdpClient(UdpPort);

        _ = Task.Run(StartGatewayTcpListenerAsync);
        _ = Task.Run(StartUdpListenerAsync);
        _ = Task.Run(MonitorGarbageCollectorAsync);
        _ = Task.Run(DatabaseWorkerAsync);

        await StartWebDashboardApiAsync();
    }

    private static async Task MonitorGarbageCollectorAsync() {
        while (true) {
            await Task.Delay(2000); 
            int removed = _videoAssembler.GarbageCollect();
            if (VideoDebugPackets && removed > 0) {
                Console.WriteLine($"[UDP] Garbage collector removed {removed} stale frames.");
            }
        }
    }

    private static async Task DatabaseWorkerAsync() {
        Console.WriteLine("[SYSTEM] DB Worker iniciado. À espera de telemetria na fila...");
        
        // Fica eternamente e de forma assíncrona a ler da fila assim que entram dados
        await foreach (var item in _dbQueue.Reader.ReadAllAsync()) {
            try {
                _db.SaveReading(item.SensorId, item.GatewayId, item.Zone, item.DataType, item.Value, item.Timestamp);
            } catch (Exception ex) {
                Console.WriteLine($"[DB FATAL] Erro ao gravar leitura do {item.SensorId}: {ex.Message}");
            }
        }
    }

    private static async Task StartGatewayTcpListenerAsync() {
        var listener = new TcpListener(IPAddress.Any, TcpPort);
        listener.Start();
        Console.WriteLine($"[TCP] Listening for Gateway connections on port {TcpPort}...");

        while (true) {
            try {
                var client = await listener.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleGatewayClientAsync(client));
            } catch (Exception ex) {
                Console.WriteLine($"[TCP ERROR] {ex.Message}");
            }
        }
    }

    private static async Task HandleGatewayClientAsync(TcpClient client) {
        string endPoint = client.Client.RemoteEndPoint.ToString();
        Console.WriteLine($"[TCP] Gateway connected from {endPoint}");

        try {
            while (client.Connected) {
                try {
                    var msg = await Message.ReceiveMessageAsync(client);
                    if (msg == null) break;

                    // --- ATUALIZAR ESTADO IN-MEMORY ---
                    if (!string.IsNullOrEmpty(msg.GID)) {
                        // O Gateway manda Heartbeats a cada 10s, por isso atualizamos sempre a hora dele
                        _activeGateways[msg.GID] = DateTime.Now;
                    }
                    if (!string.IsNullOrEmpty(msg.SID) && msg.SID != "GATEWAY") {
                        // Atualiza a presença do sensor sempre que ele manda um comando ou batch de dados
                        _activeSensors[msg.SID] = (msg.GID, DateTime.Now);
                    }

                    if (msg.CMD != "HB") {
                        Console.WriteLine($"[DEBUG TCP RX] Mensagem TCP Recebida: {msg.CMD} do Gateway {msg.GID} / Sensor {msg.SID}");
                    }

                    if (msg.CMD == "FWD_STRM" && msg.Data.GetValueOrDefault("ACTION", "") == "START") {
                        Console.WriteLine($"\n   -> [STREAM] Gateway {msg.GID} authorized video for Sensor {msg.SID}!");
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"   -> Live on central: http://localhost:{DashboardPort}/stream/{msg.SID}");
                        Console.ResetColor();
                        continue;
                    }

                    if (msg.CMD == "FWD" && msg.Data.ContainsKey("RAW_PAYLOAD")) {
                        string sensorId = msg.SID;
                        string gatewayId = msg.GID ?? "UNKNOWN";
                        string rawZone = msg.Data.ContainsKey("ZONE") ? msg.Data["ZONE"] : null;
                        string zone = !string.IsNullOrWhiteSpace(rawZone) ? rawZone : "DESCONHECIDA";
                        string dataType = msg.Data["TYPE"];
                        string rawJson = msg.Data["RAW_PAYLOAD"];

                        Console.WriteLine($"[DEBUG TCP FWD] Desserializando payload RAW para {dataType}...");
                        var payloadList = JsonSerializer.Deserialize<List<Dictionary<string, string>>>(rawJson);

                        if (payloadList != null) {
                            int successCount = 0;
                            foreach (var item in payloadList) {
                                if (DateTime.TryParse(item["Timestamp"], null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime ts) &&
                                    double.TryParse(item["Value"], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double val)) {
                                    
                                    var data = new ReadingData(sensorId, gatewayId, zone, dataType, val, ts.ToUniversalTime());
                                    _dbQueue.Writer.TryWrite(data);
                                    successCount++;
                                } else {
                                    Console.WriteLine($"[DEBUG DB ERROR] Falha ao fazer parsing de Timestamp ou Value. T:{item["Timestamp"]} V:{item["Value"]}");
                                }
                            }
                            Console.WriteLine($"[CLOUD] Saved {successCount}/{payloadList.Count} '{dataType}' metrics from {sensorId} to database.");
                        } else {
                            Console.WriteLine($"[DEBUG TCP ERROR] O payload RAW estava nulo ou mal formatado.");
                        }
                    }
                } 
                catch (Exception innerEx) {
                    Console.WriteLine($"[PROCESSING ERROR] Failed to process packet: {innerEx.Message}");
                }
            }
        } 
        catch (Exception ex) { 
            Console.WriteLine($"[TCP FATAL] {ex.Message}");
        } 
        finally {
            client.Close();
            Console.WriteLine($"[TCP] Gateway {endPoint} disconnected.");
        }
    }

    private static async Task StartUdpListenerAsync() {
        Console.WriteLine($"[UDP] Video listener active on port {UdpPort}...");
        while (true) {
            try {
                var result = await _udpListener.ReceiveAsync();
                var msg = Message.FromUdpBytes(result.Buffer);

                if (msg == null || msg.CMD != "STRM") continue;

                if (_videoAssembler.TryAddPacket(msg, out byte[]? fullImage, out string reason) && fullImage != null) {
                    long assembled = Interlocked.Increment(ref _assembledServerFrames);
                    _latestServerFrames[msg.SID] = fullImage;
                    _latestServerFrameSequences[msg.SID] = assembled;
                    if (VideoDebugPackets || assembled % 30 == 0) {
                        Console.WriteLine($"[UDP] Assembled {assembled} server frames. Latest sensor={msg.SID}.");
                    }
                } else if (VideoDebugPackets && reason != "pending" && reason != "duplicate-packet") {
                    Console.WriteLine($"[UDP] Dropped packet for {msg.SID}: {reason}");
                }
            } catch (Exception ex) { Console.WriteLine($"[DEBUG UDP SERVER ERRO] {ex.Message}"); } 
        }
    }

    private static async Task StartWebDashboardApiAsync() {
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://+:{DashboardPort}/");
        listener.Start();
        Console.WriteLine($"[WEB] REST API Dashboard running on port {DashboardPort}.");

        while (true) {
            var context = await listener.GetContextAsync();
            _ = Task.Run(() => HandleWebRequestAsync(context));
        }
    }

    private static async Task HandleWebRequestAsync(HttpListenerContext context) {
        var req = context.Request;
        var res = context.Response;

        res.AppendHeader("Access-Control-Allow-Origin", "*");

        try {
            if (req.Url.AbsolutePath.Equals("/health", StringComparison.OrdinalIgnoreCase)) {
                byte[] buffer = Encoding.UTF8.GetBytes("{\"status\":\"healthy\"}");
                res.ContentType = "application/json";
                res.ContentLength64 = buffer.Length;
                await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                res.Close();
                return;
            }

            if (req.Url.AbsolutePath.StartsWith("/stream/")) {
                string sid = req.Url.AbsolutePath.Split('/').Last();
                string html = $"<html><body style='background:#000;color:#0f0;text-align:center;'><h2>Stream Cloud: {sid}</h2><img id='f' src='/mjpeg/{sid}' style='max-width:800px;'/></body></html>";
                byte[] buffer = Encoding.UTF8.GetBytes(html);
                res.ContentType = "text/html";
                AddNoCacheHeaders(res);
                await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                res.Close();
                return;
            }

            if (req.Url.AbsolutePath.StartsWith("/mjpeg/")) {
                string sid = req.Url.AbsolutePath.Split('/').Last().ToUpper();
                await StreamMjpegAsync(context, sid);
                return;
            }

            if (req.Url.AbsolutePath.StartsWith("/image/")) {
                string sid = req.Url.AbsolutePath.Split('/').Last().ToUpper();
                if (_latestServerFrames.TryGetValue(sid, out byte[] imgBytes)) {
                    res.ContentType = "image/jpeg";
                    AddNoCacheHeaders(res);
                    res.ContentLength64 = imgBytes.Length;
                    await res.OutputStream.WriteAsync(imgBytes, 0, imgBytes.Length);
                } else {
                    res.StatusCode = 404;
                }
                res.Close();
                return;
            }

            if (req.Url.AbsolutePath == "/" || req.Url.AbsolutePath.ToLower() == "/index.html") {
                string filePath = Path.Combine(Directory.GetCurrentDirectory(), "index.html"); 
                if (File.Exists(filePath)) {
                    byte[] htmlBytes = await File.ReadAllBytesAsync(filePath);
                    res.ContentType = "text/html";
                    res.ContentLength64 = htmlBytes.Length;
                    await res.OutputStream.WriteAsync(htmlBytes, 0, htmlBytes.Length);
                } else {
                    res.StatusCode = 404;
                    var notFound = Encoding.UTF8.GetBytes("Dashboard file not found.");
                    await res.OutputStream.WriteAsync(notFound, 0, notFound.Length);
                }
                res.Close();
                return; 
            }

            if (req.Url.AbsolutePath.ToLower() == "/api/status") {
                try {
                    // Vai buscar as leituras para a tabela
                    var readingsList = _db.GetRecentReadings(20);
                    
                    // Avalia quem está vivo na RAM
                    // Gateways têm Timeout de 45 segs (enviam HB a cada 10s)
                    var onlineGws = _activeGateways
                        .Where(g => (DateTime.Now - g.Value).TotalSeconds < 45)
                        .Select(g => g.Key).ToArray();

                    // Sensores têm Timeout de 60 segs (o Gateway envia batch a cada 30s)
                    var onlineSensors = _activeSensors
                        .Where(s => (DateTime.Now - s.Value.LastSeen).TotalSeconds < 60)
                        .Select(s => new { Sensor = s.Key, Gateway = s.Value.Gateway }).ToArray();

                    var statusData = new {
                        gatewaysOnline = onlineGws,
                        activeSensors = onlineSensors,
                        readings = readingsList 
                    };

                    string jsonResponse = JsonSerializer.Serialize(statusData);
                    byte[] buffer = Encoding.UTF8.GetBytes(jsonResponse);
                    
                    res.ContentType = "application/json";
                    res.ContentLength64 = buffer.Length;
                    await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                } catch { res.StatusCode = 500; } 
                finally { res.Close(); }
                return;
            }

            if (req.Url.AbsolutePath.StartsWith("/api/analyze/", StringComparison.OrdinalIgnoreCase)) {
                try {
                    var parts = req.Url.AbsolutePath.Split('/');
                    if (parts.Length >= 5) {
                        string sensor = parts[3].ToUpper();
                        string type = parts[4].ToUpper();

                        // 1. Ir buscar as leituras recentes à Base de Dados
                        var allReadings = _db.GetRecentReadings(100);
                        var grpcRequest = new AnalysisRequest { SensorId = sensor, DataType = type };
                        
                        // 2. Preencher a lista Repeated de 'Reading' do Protobuf
                        foreach (var r in allReadings) {
                            dynamic row = r;
                            if ((string)row.Sensor == sensor && (string)row.Type == type) {
                                
                                // Tratamento robusto para a tipagem dinâmica do SQLite
                                string timeStr = row.Time is string ? (string)row.Time : row.Time.ToString();
                                double val = Convert.ToDouble(row.Value); 

                                grpcRequest.Readings.Add(new Reading {
                                    Timestamp = timeStr,
                                    Value = val
                                });
                            }
                        }

                        if (grpcRequest.Readings.Count == 0) {
                            res.StatusCode = 404;
                            byte[] errBuf = Encoding.UTF8.GetBytes("{\"error\": \"Sem dados suficientes.\"}");
                            await res.OutputStream.WriteAsync(errBuf, 0, errBuf.Length);
                            return;
                        }

                        // 3. Executar o RPC ao Microserviço Python Analysis
                        var rpcResponse = await _rpcClient.AnalyzeDataAsync(grpcRequest);

                        // 4. Mapear para o formato exato que o teu JavaScript espera
                        var responseData = new {
                            Evaluation = rpcResponse.RiskPattern,
                            MicroserviceMessage = rpcResponse.Message,
                            Statistics = new {
                                ProcessedSamples = rpcResponse.SampleCount,
                                Mean = rpcResponse.MeanValue,
                                Max = rpcResponse.MaxValue,
                                Min = rpcResponse.MinValue
                            }
                        };

                        string jsonResponse = JsonSerializer.Serialize(responseData);
                        byte[] buffer = Encoding.UTF8.GetBytes(jsonResponse);
                        
                        res.ContentType = "application/json";
                        res.ContentLength64 = buffer.Length;
                        await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                        return;
                    }
                } catch (Exception ex) {
                    Console.WriteLine($"[RPC ERROR] Falha na pipeline gRPC: {ex.Message}");
                    res.StatusCode = 500;
                    return;
                } finally {
                    res.Close();
                }
            }

            // Apenas devolve 404 se nenhuma das rotas acima tiver correspondência
            res.StatusCode = 404;
        } catch { res.StatusCode = 500; } 
        finally { res.Close(); }
    }

    private static async Task StreamMjpegAsync(HttpListenerContext context, string sid) {
        var res = context.Response;
        res.StatusCode = 200;
        res.SendChunked = true;
        res.ContentType = "multipart/x-mixed-replace; boundary=frame";
        AddNoCacheHeaders(res);

        long lastSequence = 0;
        byte[] newline = Encoding.ASCII.GetBytes("\r\n");

        try {
            while (true) {
                if (_latestServerFrameSequences.TryGetValue(sid, out long sequence) &&
                    sequence != lastSequence &&
                    _latestServerFrames.TryGetValue(sid, out byte[] frameBytes)) {
                    lastSequence = sequence;
                    byte[] header = Encoding.ASCII.GetBytes(
                        $"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: {frameBytes.Length}\r\n\r\n");

                    await res.OutputStream.WriteAsync(header, 0, header.Length);
                    await res.OutputStream.WriteAsync(frameBytes, 0, frameBytes.Length);
                    await res.OutputStream.WriteAsync(newline, 0, newline.Length);
                    await res.OutputStream.FlushAsync();
                } else {
                    await Task.Delay(50);
                }
            }
        } catch {
            // Browser closed the MJPEG stream.
        } finally {
            try { res.Close(); } catch { }
        }
    }

    private static void AddNoCacheHeaders(HttpListenerResponse res) {
        res.Headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0";
        res.Headers["Pragma"] = "no-cache";
        res.Headers["Expires"] = "0";
    }

    private static int GetIntEnv(string name, int defaultValue) {
        return int.TryParse(Environment.GetEnvironmentVariable(name), out int value) ? value : defaultValue;
    }

    private static bool GetBoolEnv(string name, bool defaultValue) {
        string raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw)) return defaultValue;
        return raw.Equals("1", StringComparison.OrdinalIgnoreCase) ||
               raw.Equals("true", StringComparison.OrdinalIgnoreCase) ||
               raw.Equals("yes", StringComparison.OrdinalIgnoreCase);
    }
}
