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

class Program {
    private static DataBaseManager _db = new DataBaseManager();
    private static AnalysisService.AnalysisServiceClient _rpcClient;

    private static UdpClient _udpListener;
    private static readonly int UdpPort = int.Parse(Environment.GetEnvironmentVariable("SERVER_UDP_PORT") ?? "5003");
    private static System.Collections.Concurrent.ConcurrentDictionary<string, byte[][]> _videoBuffer = new();
    private static System.Collections.Concurrent.ConcurrentDictionary<string, DateTime> _videoBufferTimestamps = new();
    private static System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _latestServerFrames = new();

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

        await StartWebDashboardApiAsync();
    }

    private static async Task MonitorGarbageCollectorAsync() {
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

    private static async Task StartGatewayTcpListenerAsync() {
        var listener = new TcpListener(IPAddress.Any, 5001);
        listener.Start();
        Console.WriteLine("[TCP] Listening for Gateway connections on port 5001...");

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

                    if (msg.CMD != "HB") {
                        Console.WriteLine($"[DEBUG TCP RX] Mensagem TCP Recebida: {msg.CMD} do Gateway {msg.GID} / Sensor {msg.SID}");
                    }

                    if (msg.CMD == "FWD_STRM" && msg.Data.GetValueOrDefault("ACTION", "") == "START") {
                        Console.WriteLine($"\n   -> [STREAM] Gateway {msg.GID} authorized video for Sensor {msg.SID}!");
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"   -> Live on central: http://localhost:8081/stream/{msg.SID}");
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
                                    
                                    _db.SaveReading(sensorId, gatewayId, zone, dataType, val, ts.ToUniversalTime());
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

                int part = int.Parse(msg.Data["PART"]);
                int total = int.Parse(msg.Data["TOTAL"]);

                if (!_videoBuffer.TryGetValue(msg.SID, out var chunks) || chunks.Length != total) {
                    chunks = new byte[total][];
                    _videoBuffer[msg.SID] = chunks;
                }
                chunks[part - 1] = msg.BinaryData;
                _videoBufferTimestamps[msg.SID] = DateTime.Now;

                if (chunks.All(c => c != null)) {
                    byte[] fullImage = chunks.SelectMany(c => c).ToArray();
                    _latestServerFrames[msg.SID] = fullImage;
                    _videoBuffer.TryRemove(msg.SID, out _);
                    _videoBufferTimestamps.TryRemove(msg.SID, out _);
                    Console.WriteLine($"[DEBUG UDP SUCCESS] Frame Completa recebida de {msg.SID} na Cloud.");
                }
            } catch (Exception ex) { Console.WriteLine($"[DEBUG UDP SERVER ERRO] {ex.Message}"); } 
        }
    }

    private static async Task StartWebDashboardApiAsync() {
        var listener = new HttpListener();
        listener.Prefixes.Add("http://+:8081/");
        listener.Start();
        Console.WriteLine("[WEB] REST API Dashboard running on port 8081.");

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
            if (req.Url.AbsolutePath.StartsWith("/stream/")) {
                string sid = req.Url.AbsolutePath.Split('/').Last();
                string html = $"<html><body style='background:#000;color:#0f0;text-align:center;'><h2>Stream Cloud: {sid}</h2><img id='f' src='/image/{sid}' style='max-width:800px;'/><script>setInterval(()=>document.getElementById('f').src='/image/{sid}?'+Date.now(),200);</script></body></html>";
                byte[] buffer = Encoding.UTF8.GetBytes(html);
                res.ContentType = "text/html";
                await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                res.Close();
                return;
            }

            if (req.Url.AbsolutePath.StartsWith("/image/")) {
                string sid = req.Url.AbsolutePath.Split('/').Last().ToUpper();
                if (_latestServerFrames.TryGetValue(sid, out byte[] imgBytes)) {
                    res.ContentType = "image/jpeg";
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
                    var readingsList = _db.GetRecentReadings(20);
                    var gatewaySet = new HashSet<string>();
                    foreach (var r in readingsList) {
                        dynamic row = r;
                        gatewaySet.Add((string)row.Gateway);
                    }

                    var statusData = new {
                        gatewaysOnline = gatewaySet.ToArray(),
                        activeSensors = readingsList.Select(r => new { 
                            Sensor = (string)((dynamic)r).Sensor, 
                            Gateway = (string)((dynamic)r).Gateway 
                        }).Distinct().ToArray(),
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

            // --- CÓDIGO DA API DO MICROSERVIÇO MANTIDO COMO ESTAVA ---
            res.StatusCode = 404; 
        } catch { res.StatusCode = 500; } 
        finally { res.Close(); }
    }
}