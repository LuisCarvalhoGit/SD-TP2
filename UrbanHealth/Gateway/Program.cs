using Gateway;
using Grpc.Net.Client;
using Shared;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Urbanhealth; // O nome que demos no "package" do ficheiro .proto

class Program {
    private static TcpListener _listener;
    private static TcpClient _serverClient; // Added to maintain the connection to the Central Server

    private static UdpClient _udpListener;

    private static readonly int UdpPort = 5002;
    private static readonly int ServerUdpPort = 5003;
    
    private static UdpClient _serverUdpClient = new UdpClient();

    private static readonly int Port = 5000;

    private static readonly string ServerIP = Environment.GetEnvironmentVariable("SERVER_IP") ?? "13.60.255.33"; 

    private static readonly int ServerPort = int.TryParse(Environment.GetEnvironmentVariable("SERVER_PORT"), out int sp) ? sp : 5001;

    private static readonly string GID = Environment.GetEnvironmentVariable("GATEWAY_ID") ?? "G101";

    // Store for incomplete frames: Dictionary<SensorID, List<PartBytes>>
    private static ConcurrentDictionary<string, byte[][]> _videoBuffer = new();

    // Registo da última imagem completa na RAM para o Web Server ler
    private static ConcurrentDictionary<string, byte[]> _latestFrames = new();

    private static ConfigManager _config = new ConfigManager();
    // Registry of who's online right now (SID -> Last Activity)
    private static ConcurrentDictionary<string, DateTime> _activeSensors = new();

    // Registry of who's allowed to stream video right now
    private static ConcurrentDictionary<string, bool> _activeStreams = new();

    // Registry of time to clean UDP frames that lost packets
    private static ConcurrentDictionary<string, DateTime> _videoBufferTimestamps = new();

    // Registry of sensors and their DATA_TYPES (SID -> DataTypes[])
    private static ConcurrentDictionary<string, string[]> _sensorsDatatypes = new();

    // Buffer de Agregação (Batching): Guarda a (Hora exata, Valor)
    private static ConcurrentDictionary<(string, string), ConcurrentBag<(DateTime, double)>> _valuesToForward = new();

    // Write to server Mutex
    private static readonly SemaphoreSlim _serverTxLock = new SemaphoreSlim(1, 1);

    // Used to generate "Jitter" (Random delay)
    private static readonly Random _rnd = new Random();

    private static LocalCacheManager _cache = new LocalCacheManager();

    private static readonly GrpcChannel _grpcChannel = GrpcChannel.ForAddress("http://localhost:50051");
    private static readonly PreProcessingService.PreProcessingServiceClient _rpcClient = new PreProcessingService.PreProcessingServiceClient(_grpcChannel);

    static async Task Main(string[] args) {
        _config.LoadConfig();

        // Launch the Server connection task in the background (will try to connect infinitely)
        _ = Task.Run(ConnectToServerLoopAsync);

        _ = Task.Run(GatewayHeartbeatRoutineAsync);

        _ = Task.Run(BatchDataRoutineAsync);

        // Starts cleaning routine for inactive sensors (and video buffer)
        _ = Task.Run(async () => {
            while (true) {
                await Task.Delay(2000); // Checks every 15 seconds
                bool csvNeedsUpdate = false;

                foreach (var sensor in _activeSensors) {
                    // If more than 30 seconds since last HB
                    if ((DateTime.Now - sensor.Value).TotalSeconds > 30) {
                        Console.WriteLine($"[TIMEOUT] Sensor {sensor.Key} lost connection. Deactivating...");
                        _activeSensors.TryRemove(sensor.Key, out _);
                        _config.UpdateSensorState(sensor.Key, "offline");
                        csvNeedsUpdate = true;
                    }
                }

                if (csvNeedsUpdate) {
                    _config.SaveConfig();
                    Console.WriteLine("[SYSTEM] Config file updated.");
                }

                // Garbage collector for UDP Video
                foreach (var frameTime in _videoBufferTimestamps) {
                    
                    // If more than 1.5 seconds passed and the image didn't mount yet, discard
                    if ((DateTime.Now - frameTime.Value).TotalSeconds > 1.5) {
                        _videoBuffer.TryRemove(frameTime.Key, out _);
                        _videoBufferTimestamps.TryRemove(frameTime.Key, out _);
                        //Console.WriteLine($"[UDP CLEANUP] Incomplete frame from {frameTime.Key} discarded to free RAM.");
                    }
                }
            }
        });

        // Start the Gateway to listen for Sensors
        _listener = new TcpListener(IPAddress.Any, Port);
        _listener.Start();
        Console.WriteLine($"[GATEWAY] Active on port {Port}");

        // inicia o listener de video udp
        _udpListener = new UdpClient(UdpPort);
        _ = Task.Run(ListenForVideoUdpAsync);

        // starts web server
        _ = Task.Run(StartWebServerAsync);

        Console.WriteLine("==================================================");
        Console.WriteLine(" Gateway Menu. Available commands:");
        Console.WriteLine(" -> DISCONN (to shutdown gateway in a clean way)");
        Console.WriteLine("==================================================\n");

        _ = Task.Run(AcceptSensorsLoopAsync);

        
        while (true) {
            var input = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(input)) continue;

            var command = input.Trim().ToUpper();

            if (command == "DISCONN") {
                await PerformGracefulShutdownAsync();
                break; 
            }
            else {
                Console.WriteLine("[ERROR] Unknown command. Use DISCONN.");
            }
        }


    }

    private static async Task BatchDataRoutineAsync()
    {
        int batchWindowMs = 30000; // Envia pacotes a cada 30 segundos

        while (true)
        {
            await Task.Delay(batchWindowMs);

            // 1. TENTAR ENVIAR DADOS DO CACHE SQLITE PRIMEIRO (Recuperação de falhas anteriores)
            var pendingReadings = _cache.GetPendingReadings();
            if (pendingReadings.Count > 0)
            {
                Console.WriteLine($"[CACHE] Encontrados {pendingReadings.Count} registos pendentes no SQLite. A tentar reenviar...");

                // Agrupamos por Sensor e Tipo para manter a estrutura de Batch
                var groupedPending = pendingReadings.GroupBy(x => (x.Sid, x.Type));

                foreach (var group in groupedPending)
                {
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

                    try
                    {
                        // Verifica se há conexão antes de tentar
                        if (_serverClient == null || !_serverClient.Connected)
                        {
                            Console.WriteLine($"[CACHE] Servidor desconectado. A manter dados em cache por mais um ciclo...");
                            break;
                        }

                        await _serverTxLock.WaitAsync();
                        try
                        {
                            await Message.SendMessageAsync(_serverClient, cacheMsg);
                        }
                        finally
                        {
                            _serverTxLock.Release();
                        }
                        // Se enviou com sucesso, apaga estes IDs do SQLite
                        _cache.DeleteReadings(group.Select(x => x.Id));
                        Console.WriteLine($"[CACHE] {payloadList.Count} registos de {sensorId} recuperados com sucesso.");
                    }
                    catch (Exception cacheEx)
                    {
                        Console.WriteLine($"[CACHE] Falha ao tentar recuperar dados de {sensorId}. Tentará novamente no próximo ciclo.");
                        Console.WriteLine($"[CACHE] Erro: {cacheEx.Message}");
                        Console.WriteLine($"[CACHE] Exception Type: {cacheEx.GetType().Name}");
                        break; // Interrompe para não sobrecarregar em caso de nova queda
                    }
                }
            }

            // 2. PROCESSAR DADOS ATUAIS NA MEMÓRIA
            foreach (var key in _valuesToForward.Keys)
            {
                if (_valuesToForward.TryRemove(key, out var readingsBag))
                {
                    var snapshot = readingsBag.ToList();
                    if (snapshot.Count == 0) continue;

                    string sensorId = key.Item1;
                    string dataType = key.Item2;
                    var (_, zone, _, _, _) = _config.ValidateSensor(sensorId);

                    var payloadList = new List<Dictionary<string, string>>();
                    foreach (var item in snapshot)
                    {
                        payloadList.Add(new Dictionary<string, string> {
                        { "Timestamp", item.Item1.ToString("o") },
                        { "Value", item.Item2.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                    });
                    }

                    var batchMsg = new Message { CMD = "FWD", SID = sensorId, GID = GID };
                    batchMsg.Data["TYPE"] = dataType;
                    batchMsg.Data["ZONE"] = zone;
                    batchMsg.Data["BATCH_COUNT"] = snapshot.Count.ToString();
                    batchMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                    try
                    {
                        // Se não há conexão ao servidor, vai direto para o cache
                        if (_serverClient == null || !_serverClient.Connected)
                        {
                            Console.WriteLine($"[ERROR-OFFLINE] Servidor desconectado. Guardando {snapshot.Count} leituras de {sensorId} em cache imediatamente...");
                            throw new Exception("Server not connected");
                        }

                        await _serverTxLock.WaitAsync();
                        try
                        {
                            await Message.SendMessageAsync(_serverClient, batchMsg);
                        }
                        finally
                        {
                            _serverTxLock.Release();
                        }
                        Console.WriteLine($"[BATCHING] Pacote de {snapshot.Count} leituras de {sensorId} enviado.");
                    }
                    catch (Exception ex)
                    {
                        // EM CASO DE ERRO: Persiste no SQLite em vez de voltar para a ConcurrentBag
                        Console.WriteLine($"[ERROR-OFFLINE] Falha ao enviar para o servidor: {ex.Message}");
                        Console.WriteLine($"[ERROR-OFFLINE] Exception Type: {ex.GetType().Name}");
                        Console.WriteLine($"[CACHE] A guardar {snapshot.Count} leituras de {sensorId} ({dataType}) no disco para segurança.");

                        foreach (var reading in snapshot)
                        {
                            try
                            {
                                _cache.SaveReading(sensorId, dataType, reading.Item2, reading.Item1);
                            }
                            catch (Exception cacheEx)
                            {
                                Console.WriteLine($"[CACHE-CRITICAL] FALHA ao guardar no cache: {cacheEx.Message}");
                            }
                        }
                    }
                }
            }
        }
    }

    private static async Task PerformGracefulShutdownAsync() {
        Console.WriteLine("\n[SYSTEM] Shutting down...");

        // Notify Server that we are going to shutdown
        if (_serverClient != null && _serverClient.Connected) {
            try {
                var byeMsg = new Message { CMD = "DISCONN", GID = GID, SID = "GATEWAY" };
                try {
                    await Message.SendMessageAsync(_serverClient, byeMsg);
                } finally {
                    _serverTxLock.Release();
                }
                Console.WriteLine("[SHUTDOWN] Server notified with success.");

                await Task.Delay(500);

                _serverClient.Close();
            } catch { }
        }

        // Stop listening new sensors and video
        if (_listener != null) _listener.Stop();
        if (_udpListener != null) _udpListener.Close();
      
        // Give 500ms to NIC send last packet before closing program
        await Task.Delay(500);

        Console.WriteLine("[SYSTEM] Gateway shutdown with success...");
        Environment.Exit(0);
    }

    // SERVER LOGIC (UPSTREAM)

    private static async Task ConnectToServerLoopAsync() {

        int baseDelayMs = 2000; // Starts by waiting 2 seconds
        int maxDelayMs = 60000;
        int currentDelayMs = baseDelayMs;

        while (true) {
            try {
                _serverClient = new TcpClient();
                Console.WriteLine($"[GATEWAY] Trying to connect to Server ({ServerIP}:{ServerPort})...");

                await _serverClient.ConnectAsync(ServerIP, ServerPort);
                Console.WriteLine("[GATEWAY] Connected to Server!");
                
                // Reset the timer
                currentDelayMs = baseDelayMs;

                // Let server know we are online
                var sts = new Message { CMD = "STS", GID = GID };
                sts.Data["STATUS"] = "ONLINE";

                await _serverTxLock.WaitAsync();
                try {
                    await Message.SendMessageAsync(_serverClient, sts);
                } finally {
                    _serverTxLock.Release();
                }

                await ListenToServerAsync(_serverClient);
            } catch {
                
            }
            
            // Exponential backoff with jitter

            // Adds between 0 and 1000 random ms (Jitter)
            int jitter = _rnd.Next(0, 1000);
            int sleepTime = currentDelayMs + jitter;

            Console.WriteLine($"[GATEWAY] Inaccessible Server. New try in {sleepTime / 1000.0:F1} seconds...");
            await Task.Delay(sleepTime);
            
            // Multiply time by 2, but dont go over maxDelayMs
            currentDelayMs = Math.Min(currentDelayMs * 2, maxDelayMs);
        }
    }

    private static async Task ListenToServerAsync(TcpClient server) {
        try {
            while (true) {
                var msg = await Message.ReceiveMessageAsync(server);
                if (msg == null) break; // Server closed the connection

                string msgType = msg.Data.ContainsKey("TYPE") ? msg.Data["TYPE"] : "N/A";
                Console.WriteLine($"[SERVER -> GATEWAY] Command received: {msg.CMD}; Type: {msgType}");
                // Here we can process ACKs from the server in the future
            }
        } catch {
            Console.WriteLine("[GATEWAY] Connection to Server lost!");
        }
    }

    private static async Task GatewayHeartbeatRoutineAsync() {
        while (true) {
            await Task.Delay(10000); // Beats every 10 secs

            if (_serverClient != null && _serverClient.Connected) {
                try {
                    var hbMsg = new Message { CMD = "HB", GID = GID };
                    await _serverTxLock.WaitAsync();
                    try {
                        await Message.SendMessageAsync(_serverClient, hbMsg);
                    } finally {
                        _serverTxLock.Release();
                    }
                } catch {
                    // If send fails, the reconnection routine will recover the socket
                }
            }
        }
    }


    // SENSOR LOGIC (DOWNSTREAM)

    private static async Task AcceptSensorsLoopAsync() {
        while (true) {
            try {
                var client = await _listener.AcceptTcpClientAsync();
                Console.WriteLine($"[GATEWAY] New sensor detected. Initiating handler...");
                _ = Task.Run(() => HandleSensorAsync(client));
            } catch {                
                // if listener gets closed during shutdown, the error falls here silently and routine ends
                break;
            }
        }
    }


    private static async Task HandleSensorAsync(TcpClient client) {
        using (client) {
            string sensorId = "Unknown";
            try {
                while (true) {
                    var msg = await Message.ReceiveMessageAsync(client);

                    if (msg == null) {
                        Console.WriteLine($"[DISCONNECT] Sensor {sensorId} closed the connection.");
                        if (sensorId != "Unknown" && _activeSensors.ContainsKey(sensorId)) {
                            _activeSensors.TryRemove(sensorId, out _);
                            _config.UpdateSensorState(sensorId, "offline");
                            _config.SaveConfig();
                        }
                        break;
                    }

                    sensorId = msg.SID;
                    Console.WriteLine($"[RECEIVED] Command: {msg.CMD} by {msg.SID}");
                    await ProcessMessage(client, msg);

   
                    // break out of the loop to not read a dead socket
                    if (msg.CMD == "DISCONN") {
                        break;
                    }
                }
            } catch (Exception ex) {
                Console.WriteLine($"[ERROR] Failure in communication with {sensorId}: {ex.Message}");
            }
        }
    }

    private static async Task ProcessMessage(TcpClient client, Message msg) {
        switch (msg.CMD) {
            case "CONN":
                await HandleConnection(client, msg);
                break;

            case "DISCONN":
                await HandleDisconnection(client, msg);
                break;

            case "DATA":
                await HandleData(client, msg);
                break;

            case "HB":
                if (_activeSensors.ContainsKey(msg.SID)) {
                    _activeSensors[msg.SID] = DateTime.Now;
                    _config.UpdateLastSync(msg.SID);
                    // Console.WriteLine($"[HB] {msg.SID} is alive.");
                }
                break;

            case "STRM":
                await HandleStreamControl(client, msg);
                break;
        }
    }

    private static async Task HandleConnection(TcpClient client, Message msg) {
        var (exists, zone, state, allowedTypes, _) = _config.ValidateSensor(msg.SID);

        var response = new Message {
            CMD = "MSG",
            SID = msg.SID,
            GID = GID
        };
        response.Data["REF_CMD"] = "CONN";

        if (exists) {

            if (state.ToLower() == "maintenance") {
                response.Data["TYPE"] = "ERR";
                response.Data["REASON"] = "MAINTENANCE";
                Console.WriteLine($"[AUTH] Sensor {msg.SID} rejected (Under Maintenance)");

                if (_serverClient != null && _serverClient.Connected) {
                    var statusMsg = new Message { CMD = "STS", SID = msg.SID, GID = GID };
                    statusMsg.Data["STATUS"] = "MAINTENANCE";

                    await _serverTxLock.WaitAsync();
                    try {
                        await Message.SendMessageAsync(_serverClient, statusMsg);
                    } finally {
                        _serverTxLock.Release();
                    }
                }
            } else {

                string dataTypes = msg.Data.ContainsKey("DATA_TYPES") ? msg.Data["DATA_TYPES"] : allowedTypes;

                _config.UpdateSensorDataTypes(msg.SID, dataTypes);

                response.Data["TYPE"] = "ACK";
                response.Data["ZONE"] = zone; // Inject the zone so the sensor knows

                _activeSensors[msg.SID] = DateTime.Now;
                Console.WriteLine($"[AUTH] Sensor {msg.SID} authorized in {zone}");

                // inform server that the sensor got connected
                if (_serverClient != null && _serverClient.Connected) {
                    var statusMsg = new Message { CMD = "STS", SID = msg.SID, GID = GID };
                    statusMsg.Data["STATUS"] = "ONLINE";

                    await _serverTxLock.WaitAsync();
                    try {
                        await Message.SendMessageAsync(_serverClient, statusMsg);
                    } finally {
                        _serverTxLock.Release();
                    }

                    Console.WriteLine($"[FORWARD] Sensor {msg.SID} connected to gateway {msg.GID}.");
                }
            }

        }
        else {
            response.Data["TYPE"] = "ERR";
            response.Data["REASON"] = "UNKNOWN_SENSOR";
            Console.WriteLine($"[AUTH] Sensor {msg.SID} rejected");
        }

        await Message.SendMessageAsync(client, response);
    }

    private static async Task HandleDisconnection(TcpClient client, Message msg) {
        Console.WriteLine($"\n[DISCONNECT] Disconnection request from sensor {msg.SID}");
                            
        if (_activeSensors.ContainsKey(msg.SID)) {
            // remove from active sensors in memory
            _activeSensors.TryRemove(msg.SID, out _);

            // update satte to offline in csv and saves on disc
            _config.UpdateSensorState(msg.SID, "offline");
            _config.SaveConfig();

            Console.WriteLine($"[SYSTEM] Sensor {msg.SID} closed and saved as offline.");

            // inform server that the sensor got disconnected
            if (_serverClient != null && _serverClient.Connected) {
                var fwdMsg = new Message { CMD = "DISCONN", SID = msg.SID, GID = GID };

                await _serverTxLock.WaitAsync();
                try {
                    await Message.SendMessageAsync(_serverClient, fwdMsg);
                } finally {
                    _serverTxLock.Release();
                }

                Console.WriteLine($"[FORWARD] Disconnection warning from {msg.SID} to server");
            }
        }
    }

    private static async Task HandleData(TcpClient client, Message msg) {
        // Only accept data from sensors who "CONN" with success
        if (_activeSensors.ContainsKey(msg.SID)) {
            _activeSensors[msg.SID] = DateTime.Now;

            var (_, zone, _, allowedTypes, _) = _config.ValidateSensor(msg.SID);
            string dataType = msg.Data.ContainsKey("TYPE") ? msg.Data["TYPE"] : "UNKNOWN";

            if (!allowedTypes.Contains(dataType)) {
                Console.WriteLine($"[WARNING] {msg.SID} tried to send unsupported type: {dataType}");
                var err = new Message { CMD = "MSG", SID = msg.SID, GID = GID };
                err.Data["REF_CMD"] = "DATA";
                err.Data["TYPE"] = "ERR";
                err.Data["REASON"] = "UNSUPPORTED_TYPE";
                await Message.SendMessageAsync(client, err);
                return; // Corta a execução aqui
            }

            msg.Data["ZONE"] = zone;

            Console.WriteLine($"[DATA] {msg.SID} sent {msg.Data["VALUE"]} ({msg.Data["TYPE"]})");

            // Enviar ACK ao sensor
            var ack = new Message { CMD = "MSG", SID = msg.SID, GID = GID };
            ack.Data["REF_CMD"] = "DATA";
            ack.Data["TYPE"] = "ACK";
            await Message.SendMessageAsync(client, ack);

            if (double.TryParse(msg.Data["VALUE"], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double sensorValue)) {

                // VERIFICA SE É UMA EMERGÊNCIA
                bool isAlert = IsAlertCondition(dataType, sensorValue);

                if (isAlert) {
                    Console.WriteLine($"[EMERGENCY] High {dataType} from {msg.SID}! Bypassing batching (Speed Layer)...");

                    // Formata um payload JSON com apenas esta leitura instantânea
                    var payloadList = new List<Dictionary<string, string>> {
                        new Dictionary<string, string> {
                            { "Timestamp", DateTime.Now.ToString("o") },
                            { "Value", sensorValue.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                        }
                    };

                    var alertBatchMsg = new Message { CMD = "FWD", SID = msg.SID, GID = GID };
                    alertBatchMsg.Data["TYPE"] = dataType;
                    alertBatchMsg.Data["ZONE"] = zone;
                    alertBatchMsg.Data["BATCH_COUNT"] = "1";
                    alertBatchMsg.Data["RAW_PAYLOAD"] = JsonSerializer.Serialize(payloadList);

                    // Tenta enviar logo para o servidor (com o respetivo Lock de concorrência)
                    if (_serverClient != null && _serverClient.Connected) {
                        await _serverTxLock.WaitAsync();
                        try {
                            await Message.SendMessageAsync(_serverClient, alertBatchMsg);
                        } finally {
                            _serverTxLock.Release();
                        }
                    }
                    else {
                        // Se o servidor estiver offline, o dado crítico vai para o SQLite na mesma
                        _cache.SaveReading(msg.SID, dataType, sensorValue, DateTime.Now);
                    }
                }
                else {
                    // DADOS NORMAIS
                    var bufferKey = (msg.SID, dataType);
                    var readingsBag = _valuesToForward.GetOrAdd(bufferKey, _ => new ConcurrentBag<(DateTime, double)>());

                    readingsBag.Add((DateTime.Now, sensorValue));
                }

            }
            else {
                Console.WriteLine($"[ERROR] Não foi possível converter o valor '{msg.Data["VALUE"]}' para número.");
            }
        }
    }

    // LÓGICA DE VIDEO (UDP)

    private static async Task HandleStreamControl(TcpClient client, Message msg) {

        if (!_activeSensors.ContainsKey(msg.SID)) return; // only online sensors can ask permission to stream

        var (_, _, _, allowedTypes, _) = _config.ValidateSensor(msg.SID);
        allowedTypes.Split(',');
        bool isCapableOfStreaming = allowedTypes.Contains("VIDEO");

        string action = msg.Data.ContainsKey("ACTION") ? msg.Data["ACTION"].ToUpper() : "";

        var response = new Message { CMD = "MSG", SID = msg.SID, GID = GID };
        response.Data["REF_CMD"] = "STRM";

        if (action == "START" && isCapableOfStreaming) {
            _activeStreams.TryAdd(msg.SID, true);
            response.Data["TYPE"] = "ACK";
            Console.WriteLine($"[STREAM] The sensor {msg.SID} started video streaming");

            // change output color 
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"    -> Live: http://localhost:8080/stream/{msg.SID}");
            Console.ResetColor();

            // Let server know that stream started
            if (_serverClient != null && _serverClient.Connected) {
                var fwdStrm = new Message { CMD = "FWD_STRM", SID = msg.SID, GID = GID };
                fwdStrm.Data["ACTION"] = "START";

                await _serverTxLock.WaitAsync();
                try {
                    await Message.SendMessageAsync(_serverClient, fwdStrm);
                } finally {
                    _serverTxLock.Release();
                }
            }

        } else if (action == "STOP") {
            _activeStreams.TryRemove(msg.SID, out _);

            // clean any garbage that might have stayed in this sensor buffer
            _videoBuffer.TryRemove(msg.SID, out _);

            response.Data["TYPE"] = "ACK";
            Console.WriteLine($"[STREAM] The sensor {msg.SID} stopped video streaming");
        } else if (action == "PHOTO") {

            // don't need to add to _activeStreams
            // Just clean the previous buffer to ensure the photo is "fresh"
            _videoBuffer.TryRemove(msg.SID, out _);
            response.Data["TYPE"] = "ACK";
            Console.WriteLine($"[STREAM] The sensor {msg.SID} sent a photo");

        } else {
            response.Data["TYPE"] = "ERR";
            response.Data["REASON"] = "INVALID_ACTION";
        }

        await Message.SendMessageAsync(client, response);
    }

    private static async Task ListenForVideoUdpAsync() {
        try {
            
            Console.WriteLine("[GATEWAY-VIDEO] UDP Listener listening on port 5002...");

            while (true) {
                var result = await _udpListener.ReceiveAsync();
                // Console.WriteLine($"\n[DEBUG UDP] -> Recebi pacote UDP de {result.Buffer.Length} bytes!");

                var msg = Message.FromUdpBytes(result.Buffer);

                if (msg == null) {
                    // Console.WriteLine("[DEBUG UDP] -> ERRO: O pacote não é uma Message válida.");
                    continue;
                }

                // Console.WriteLine($"[DEBUG UDP] -> Mensagem convertida: SID={msg.SID}, PARTE={msg.Data["PART"]}/{msg.Data["TOTAL"]}");

                if (!_activeSensors.ContainsKey(msg.SID)) {
                    Console.WriteLine($"[ERROR] The sensor {msg.SID} is not on the sensors list!");
                    continue;
                }

                if (!_activeStreams.ContainsKey(msg.SID) && msg.Data.GetValueOrDefault("TYPE", "") != "PHOTO_PART") {
                    Console.WriteLine($"[WARNING] Video package rejected. {msg.SID} didn's start STRM.");
                    continue;
                }

                // Forward UDP packet to server  
                try {
                    await _serverUdpClient.SendAsync(result.Buffer, result.Buffer.Length, ServerIP, ServerUdpPort);
                } catch { }

                int part = int.Parse(msg.Data["PART"]);
                int total = int.Parse(msg.Data["TOTAL"]);

                // Se o sensor ainda não tem buffer, OU se a nova imagem tem um tamanho diferente da que estava encravada...
                if (!_videoBuffer.TryGetValue(msg.SID, out var chunks) || chunks.Length != total) {
                    chunks = new byte[total][]; // Cria um novo espaço em branco
                    _videoBuffer[msg.SID] = chunks; // Substitui o frame estragado pelo novo
                }
                chunks[part - 1] = msg.BinaryData;

                // Console.WriteLine($"[DEBUG UDP] -> Guardei a parte {part} no buffer.");

                _videoBufferTimestamps[msg.SID] = DateTime.Now;

                // Verify if not all parts of the array are null already
                if (chunks.All(c => c != null)) {
                    byte[] fullImage = chunks.SelectMany(c => c).ToArray();

                    // Guarda a imagem na RAM para o servidor web aceder sem bloqueios
                    _latestFrames[msg.SID] = fullImage;

                    // Tenta guardar no disco (com try-catch seguro para ignorar conflitos)
                    string exactPath = Path.GetFullPath($"last_frame_{msg.SID}.jpg");
                    try {
                        await File.WriteAllBytesAsync(exactPath, fullImage);
                    } catch (IOException) {
                        // Ignora em silêncio se o Windows estiver com o ficheiro bloqueado
                    }

                    _videoBuffer.TryRemove(msg.SID, out _); // Limpa para a próxima
                    _videoBufferTimestamps.TryRemove(msg.SID, out _);
                }
            }
        } catch (Exception ex) {
            Console.WriteLine($"[ERROR] {ex.Message}");
        }
    }

    // WEB SERVER LOGIC (LIVE FEED)
    private static async Task StartWebServerAsync() {

        try {

            var listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");
            listener.Start();
            Console.WriteLine("[WEB] Web Server active. Ready to stream");

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
                                <h2>Live Feed UDP: {sid}</h2>
                                <img id='feed' src='/image/{sid}' style='max-width:800px;border:3px solid #007acc;border-radius:10px;' />
                                <p style='color:#00ff00;font-weight:bold;'>LIVE | 5 FPS</p>
                                <script>
                                    // Pede uma nova imagem ao Gateway a cada 200ms
                                    setInterval(() => document.getElementById('feed').src = '/image/{sid}?t=' + Date.now(), 200);
                                </script>
                            </body>
                            </html>";


                        byte[] buffer = System.Text.Encoding.UTF8.GetBytes(html);
                        res.ContentType = "text/html";
                        res.ContentLength64 = buffer.Length;
                        await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                    }
                    else if (req.Url.AbsolutePath.StartsWith("/image/")) {
                        string sid = req.Url.AbsolutePath.Split('/').Last();
                   

                        // Read from RAM
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
            Console.WriteLine($"[WEB ERROR] {ex.Message}");
        }
    }

    private static bool IsAlertCondition(string dataType, double value) {
        switch (dataType) {
            case "TEMP": return value > 33.0;
            case "HUM": return value > 78.0;
            case "PM2": return value > 48.0;
            case "CO2": return value > 995.0;
            case "NOISE": return value > 88.0;
            case "UV": return value > 9.0;
            default: return false;
        }
    }
}