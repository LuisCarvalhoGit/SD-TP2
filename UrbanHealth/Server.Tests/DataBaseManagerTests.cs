using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using FluentAssertions;
using Server;
using Xunit;

namespace Server.Tests
{
    public class DataBaseManagerTests : IDisposable
    {
        // "Cache=Shared" permite que várias conexões partilhem a mesma DB em memória
        private readonly string _connectionString = "Data Source=TestDB;Mode=Memory;Cache=Shared";
        private readonly SqliteConnection _keepAliveConnection;

        public DataBaseManagerTests()
        {
            // Abrimos a conexão aqui e ela fica aberta durante todos os testes desta classe
            _keepAliveConnection = new SqliteConnection(_connectionString);
            _keepAliveConnection.Open();
        }

        [Fact]
        public void SaveAndGetReading_QuandoInserido_DeveSerRecuperavel()
        {
            // Arrange
            var db = new DataBaseManager(_connectionString);
            
            string sid = "S101";
            string type = "TEMP";
            string gateway = "G101"; 
            string zone = "ZonaNorte";
            double val = 25.5;
            DateTime ts = DateTime.UtcNow;

            // Act
            db.SaveReading(sid, gateway, zone, type, val, ts); 
            var readings = db.GetRecentReadings(10);

            // Assert
            readings.Should().NotBeEmpty();
            
            
            var first = (IDictionary<string, object>)System.ComponentModel.TypeDescriptor.GetProperties(readings.First())
                        .Cast<System.ComponentModel.PropertyDescriptor>()
                        .ToDictionary(p => p.Name, p => p.GetValue(readings.First()));

            // Agora validamos usando acesso de dicionário seguro
            first["Sensor"].ToString().Should().Be(sid);
            first["Value"].ToString().Should().Be("25,5");
            first["Zone"].ToString().Should().Be(zone);
        }

        [Fact]
        public void GetRecentReadings_QuandoTemosMaisDe20_DeveRetornarApenas20()
        {
            // Arrange
            var db = new DataBaseManager(_connectionString);

            // Inserir 25 leituras
            for (int i = 0; i < 25; i++)
            {
                db.SaveReading("S101", "G101", "ZonaNorte", "TEMP", (double)i, DateTime.UtcNow);
            }

            // Act
            var readings = db.GetRecentReadings(20);

            // Assert
            readings.Should().HaveCount(20, "porque o método GetRecentReadings deve limitar o resultado a 20 registos");
        }

        public void Dispose()
        {
            // Limpa tudo após os testes
            _keepAliveConnection.Close();
            _keepAliveConnection.Dispose();
        }
    }
}