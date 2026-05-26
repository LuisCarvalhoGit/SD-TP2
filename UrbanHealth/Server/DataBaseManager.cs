using System;
using System.IO;
using Microsoft.Data.Sqlite;

public class DataBaseManager {
    private readonly string _connectionString;

    public DataBaseManager() {
        // Guarda a DB na pasta raiz do projeto (ou no volume Docker)
        string dbPath = Environment.GetEnvironmentVariable("DB_PATH") ?? "urbanhealth_central.db";
        _connectionString = $"Data Source={dbPath}";
        InitializeDatabase();
    }

    private void InitializeDatabase() {
        using (var connection = new SqliteConnection(_connectionString)) {
            connection.Open();

            using (var cmd = new SqliteCommand("PRAGMA journal_mode=WAL;", connection)) {
                cmd.ExecuteNonQuery();
            }

            // Tabela Original de Leituras Raw
            string createReadingsTable = @"
                CREATE TABLE IF NOT EXISTS SensorReadings (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                SensorId TEXT NOT NULL,
                Gateway TEXT NOT NULL,
                Zone TEXT NOT NULL,
                DataType TEXT NOT NULL,
                Value REAL NOT NULL,
                Timestamp DATETIME NOT NULL
            );";

            // Nova Tabela para Análises da Fase 3
            string createAnalysisTable = @"
                CREATE TABLE IF NOT EXISTS AnalysisReports (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    SensorId TEXT NOT NULL,
                    DataType TEXT NOT NULL,
                    SampleCount INTEGER NOT NULL,
                    MeanValue REAL NOT NULL,
                    MaxValue REAL NOT NULL,
                    MinValue REAL NOT NULL,
                    RiskPattern TEXT NOT NULL,
                    Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );";

            using (var cmd = new SqliteCommand(createReadingsTable, connection)) cmd.ExecuteNonQuery();
            using (var cmd = new SqliteCommand(createAnalysisTable, connection)) cmd.ExecuteNonQuery();
        }
        Console.WriteLine("[DB] SQLite Database initialized securely.");
    }

    public void SaveReading(string sensorId, string gateway, string zone, string dataType, double value, DateTime timestamp) {
        using (var connection = new SqliteConnection(_connectionString)) {
            connection.Open();
            string insertQuery = "INSERT INTO SensorReadings (SensorId, Gateway, Zone, DataType, Value, Timestamp) VALUES (@s, @g, @z, @d, @v, @t)";

            using (var cmd = new SqliteCommand(insertQuery, connection)) {
                cmd.Parameters.AddWithValue("@s", sensorId);
                cmd.Parameters.AddWithValue("@g", gateway);
                cmd.Parameters.AddWithValue("@z", zone);
                cmd.Parameters.AddWithValue("@d", dataType);
                cmd.Parameters.AddWithValue("@v", value);
                cmd.Parameters.AddWithValue("@t", timestamp);
                cmd.ExecuteNonQuery();
            }
        }
    }

    public void SaveAnalysisReport(string sensorId, string dataType, int count, double mean, double max, double min, string risk) {
        using (var connection = new SqliteConnection(_connectionString)) {
            connection.Open();
            string insertQuery = @"
                INSERT INTO AnalysisReports (SensorId, DataType, SampleCount, MeanValue, MaxValue, MinValue, RiskPattern)
                VALUES (@s, @d, @c, @m, @max, @min, @r)";

            using (var cmd = new SqliteCommand(insertQuery, connection)) {
                cmd.Parameters.AddWithValue("@s", sensorId);
                cmd.Parameters.AddWithValue("@d", dataType);
                cmd.Parameters.AddWithValue("@c", count);
                cmd.Parameters.AddWithValue("@m", mean);
                cmd.Parameters.AddWithValue("@max", max);
                cmd.Parameters.AddWithValue("@min", min);
                cmd.Parameters.AddWithValue("@r", risk);
                cmd.ExecuteNonQuery();
            }
        }
        Console.WriteLine($"[DB] Persistent analysis report stored for {sensorId} ({dataType}).");
    }

    public List<(DateTime Timestamp, double Value)> GetHistoricalReadings(string sensorId, string dataType, DateTime start, DateTime end) {
        var readings = new List<(DateTime, double)>();
        using (var connection = new SqliteConnection(_connectionString)) {
            connection.Open();
            string query = @"
            SELECT Value, Timestamp 
            FROM SensorReadings 
            WHERE SensorId = @s AND DataType = @d AND Timestamp >= @start AND Timestamp <= @end";

            using (var cmd = new SqliteCommand(query, connection)) {
                cmd.Parameters.AddWithValue("@s", sensorId);
                cmd.Parameters.AddWithValue("@d", dataType);
                cmd.Parameters.AddWithValue("@start", start);
                cmd.Parameters.AddWithValue("@end", end);

                using (var reader = cmd.ExecuteReader()) {
                    while (reader.Read()) {
                        double val = reader.GetDouble(0);
                        DateTime ts = reader.GetDateTime(1);
                        readings.Add((ts, val));
                    }
                }
            }
        }
        return readings;
    }

    public List<object> GetRecentReadings(int limit = 20)
    {
        var readingsList = new List<object>();
        using (var connection = new SqliteConnection(_connectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            
            // Agora selecionamos as 6 colunas
            command.CommandText = "SELECT Timestamp, SensorId, Gateway, Zone, DataType, Value FROM SensorReadings ORDER BY Timestamp DESC LIMIT @limit";
            command.Parameters.AddWithValue("@limit", limit);
            
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    DateTime ts = reader.GetDateTime(0);
                    string sensorId = reader.GetString(1);
                    string gateway = reader.GetString(2);
                    string zone = reader.GetString(3);
                    string dataType = reader.GetString(4);
                    double val = reader.GetDouble(5);

                    readingsList.Add(new {
                        Time = ts.Kind == DateTimeKind.Unspecified ?
                            DateTime.SpecifyKind(ts, DateTimeKind.Utc).ToString("o") :
                            ts.ToString("o"),
                        Sensor = sensorId,
                        Zone = zone,          // Valor real da BD
                        Type = dataType,
                        Value = val.ToString("0.0"),
                        Gateway = gateway     // Valor real da BD
                    });
                }
            }
        }
        return readingsList;
    }
}