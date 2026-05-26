using System;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using Grpc.Net.Client;
using Shared; // A biblioteca do vosso Message
using Urbanhealth;
using System.Text;
using Microsoft.Data.Sqlite; // O namespace gerado pelo analysis.proto

class Program {
    private static DataBaseManager _db = new DataBaseManager();
    private static AnalysisService.AnalysisServiceClient _rpcClient;

    private static System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _latestServerFrames = new();

    
    static async Task Main(string[] args) {
        Console.WriteLine("[SYSTEM] Central Cloud Server Starting...");

        // 1. Iniciar ligação gRPC ao Microserviço de Análise (Porta 50052)
        string rpcUrl = Environment.GetEnvironmentVariable("ANALYSIS_RPC_URL") ?? "http://localhost:50052";
        var channel = GrpcChannel.ForAddress(rpcUrl);
        _rpcClient = new AnalysisService.AnalysisServiceClient(channel);
        Console.WriteLine("[RPC] Connection active with Analysis Engine on port 50052.");

        // 2. Iniciar rotinas em paralelo
        _ = Task.Run(StartGatewayTcpListenerAsync);
        await StartWebDashboardApiAsync();

        _ = Task.Run(StartUdpListenerAsync);
    }

    // ==========================================================
    // 1. EDGE-TO-CLOUD LISTENER (Recebe os dados do Gateway)
    // ==========================================================
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

                    if (msg.CMD == "VIDEO" && msg.Data.ContainsKey("IMAGE")) {
                            try { _latestServerFrames[msg.SID] = Convert.FromBase64String(msg.Data["IMAGE"]); } catch { }
                            continue; 
                        }

                    if (msg.CMD == "FWD" && msg.Data.ContainsKey("RAW_PAYLOAD")) {
                        string sensorId = msg.SID;
                        string gatewayId = msg.GID ?? "UNKNOWN";
                        string rawZone = msg.Data.ContainsKey("ZONE") ? msg.Data["ZONE"] : null;
                        string zone = !string.IsNullOrWhiteSpace(rawZone) ? rawZone : "DESCONHECIDA";
                        string dataType = msg.Data["TYPE"];
                        string rawJson = msg.Data["RAW_PAYLOAD"];

                        var payloadList = JsonSerializer.Deserialize<List<Dictionary<string, string>>>(rawJson);

                        if (payloadList != null) {
                            foreach (var item in payloadList) {
                                if (DateTime.TryParse(item["Timestamp"], null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime ts) &&
                                    double.TryParse(item["Value"], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double val)) {

                                    // Agora passamos tudo para a base de dados
                                    _db.SaveReading(sensorId, gatewayId, zone, dataType, val, ts.ToUniversalTime());
                                }
                            }
                            Console.WriteLine($"[CLOUD] Saved {payloadList.Count} '{dataType}' metrics from {sensorId} ({zone}) to database.");
                        }
                    }
                } 
                catch (Exception innerEx) {
                    // Se a BD falhar ou o JSON estiver mal formado, o erro é registado 
                    // mas o TCP continua ativo a ler a próxima mensagem!
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
        int port = int.Parse(Environment.GetEnvironmentVariable("PORT_SERVER_TCP") ?? "5001");
        using var udpServer = new UdpClient(port);
        Console.WriteLine($"[UDP] Video listener active on port {port}...");

        while (true) {
            var result = await udpServer.ReceiveAsync();
            // O sensor envia o frame. Como o UDP não tem cabeçalho de SID no payload binário,
            // podes simplificar o protocolo enviando SID:IMAGE ou usar um SID fixo por porta.
            // Se quiseres manter simples, usa o ConcurrentDictionary como já tens:
            _latestServerFrames["S101"] = result.Buffer; 
        }
    }

    // ==========================================================
    // 2. WEB API (Interface para o utilizador contactar o Python)
    // ==========================================================
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

        // CORS Setup para permitir chamadas via AJAX do frontend
        res.AppendHeader("Access-Control-Allow-Origin", "*");

        try {

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
                // Ajusta "index.html" para o nome exato do teu ficheiro
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
                return; // Sai da função para não executar a parte do Python
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
                        
                        // Extraímos os sensores únicos a partir dos dados já formatados
                        activeSensors = readingsList.Select(r => new { 
                            Sensor = (string)((dynamic)r).Sensor, 
                            Gateway = (string)((dynamic)r).Gateway 
                        }).Distinct().ToArray(),
                        
                        // Passamos a lista diretamente, pois o DataBaseManager já a preparou de forma impecável
                        readings = readingsList 
                    };

                    string jsonResponse = JsonSerializer.Serialize(statusData);
                    byte[] buffer = Encoding.UTF8.GetBytes(jsonResponse);
                    
                    res.ContentType = "application/json";
                    res.ContentLength64 = buffer.Length;
                    await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                    
                } catch (Exception ex) {
                    Console.WriteLine($"[API ERROR] Falha no status: {ex.Message}");
                    res.StatusCode = 500;
                } finally {
                    res.Close();
                }
                return;
            }

            // Rota da Fase 3: Pedir Análise a um Sensor (ex: /api/analyze/S101/PM2)
            if (req.Url.AbsolutePath.StartsWith("/api/analyze/")) {
                var parts = req.Url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);

                if (parts.Length >= 3) {
                    string sensorId = parts[2];
                    string dataType = parts.Length > 3 ? parts[3].ToUpper() : "ALL";

                    Console.WriteLine($"[WEB] User requested formal analysis for {sensorId} [{dataType}]");

                    // 1. Extrair os dados reais da base de dados do C# (ex: últimos 7 dias)
                    DateTime endTime = DateTime.Now;
                    DateTime startTime = endTime.AddDays(-7);
                    var databaseRows = _db.GetHistoricalReadings(sensorId, dataType, startTime, endTime);

                    // 2. Construir a nova mensagem protobuf (Stateless)
                    var rpcReq = new AnalysisRequest {
                        SensorId = sensorId,
                        DataType = dataType
                    };

                    // 3. Injetar as leituras em memória no pedido gRPC
                    foreach (var row in databaseRows) {
                        rpcReq.Readings.Add(new Reading {
                            Timestamp = row.Timestamp.ToString("o"),
                            Value = row.Value
                        });
                    }

                    Console.WriteLine($"[RPC OUT] Dispatching {databaseRows.Count} rows to Python Stateless Engine...");

                    // 4. Chamada ao Microserviço Python!
                    var rpcRes = await _rpcClient.AnalyzeDataAsync(rpcReq);

                    if (rpcRes.Success) {
                        // Persistir os resultados do Python na BD do C#
                        _db.SaveAnalysisReport(
                            sensorId, dataType, rpcRes.SampleCount,
                            rpcRes.MeanValue, rpcRes.MaxValue, rpcRes.MinValue, rpcRes.RiskPattern
                        );

                        // Devolver a resposta limpa ao browser (com formatação correta para acentos)
                        var jsonResponse = JsonSerializer.Serialize(new {
                            Status = "Success",
                            SensorId = sensorId,
                            DataType = dataType,
                            Evaluation = rpcRes.RiskPattern,
                            Statistics = new {
                                ProcessedSamples = rpcRes.SampleCount,
                                Mean = rpcRes.MeanValue,
                                Max = rpcRes.MaxValue,
                                Min = rpcRes.MinValue
                            },
                            MicroserviceMessage = rpcRes.Message
                        }, new JsonSerializerOptions {
                            WriteIndented = true,
                            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                        });

                        byte[] bytes = System.Text.Encoding.UTF8.GetBytes(jsonResponse);
                        res.ContentType = "application/json";
                        res.ContentLength64 = bytes.Length;
                        await res.OutputStream.WriteAsync(bytes, 0, bytes.Length);
                        return;
                    }
                }
            }

            res.StatusCode = 404; // Not Found
        } catch (Exception ex) {
            Console.WriteLine($"[WEB ERROR] API Failure: {ex.Message}");
            res.StatusCode = 500;
        } finally {
            res.Close();
        }
    }
}