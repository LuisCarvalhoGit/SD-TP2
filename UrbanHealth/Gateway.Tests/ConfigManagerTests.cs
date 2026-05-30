using System;
using System.IO;
using FluentAssertions;
using Gateway; 
using Xunit;

namespace Gateway.Tests
{
    // Usamos IDisposable para limpar os ficheiros e variáveis de ambiente após cada teste
    public class ConfigManagerTests : IDisposable
    {
        private readonly string _testGid;
        private readonly string _configsDir;
        private readonly string _gwPath;
        private readonly string _snPath;

        public ConfigManagerTests()
        {
            // 1. ARRANGE GLOBAL: Criar um GID único para este teste não chocar com outros
            _testGid = "TEST_" + Guid.NewGuid().ToString().Substring(0, 5);
            Environment.SetEnvironmentVariable("GID", _testGid);

            // 2. Garantir que a pasta "Configs" existe (como o código de produção espera)
            _configsDir = "Configs";
            if (!Directory.Exists(_configsDir)) {
                Directory.CreateDirectory(_configsDir);
            }

            _gwPath = Path.Combine(_configsDir, $"gateway-config-{_testGid}.json");
            _snPath = Path.Combine(_configsDir, $"sensors-config-{_testGid}.json");

            // 3. Criar o JSON do Gateway
            File.WriteAllText(_gwPath, @"{
                ""GatewayId"": """ + _testGid + @""",
                ""Rabbitmq"": { ""Exchange"": ""test_exchange"", ""RoutingKeys"": [""sensor.#""] },
                ""Timings"": { ""BatchIntervalMs"": 1000 }
            }");

            // 4. Criar o JSON dos Sensores com a estrutura exata do SensorsRoot (Lista)
            File.WriteAllText(_snPath, @"{
                ""Sensors"": [
                    {
                        ""Id"": ""S999"",
                        ""Zone"": ""TestZone"",
                        ""state"": ""offline"",
                        ""DataTypes"": ""TEMP, HUM""
                    }
                ]
            }");
        }

        [Fact]
        public void LoadConfig_QuandoSensorExiste_DeveRetornarValoresCorretos()
        {
            // Arrange (O construtor vai ler a variável de ambiente GID = _testGid)
            var configManager = new ConfigManager();
            configManager.LoadConfig();

            // Act
            var (exists, zone, status, types, _) = configManager.ValidateSensor("S999");

            // Assert
            exists.Should().BeTrue("porque o sensor S999 está na lista do SensorsRoot");
            zone.Should().Be("TestZone");
            status.Should().Be("offline");
            types.Should().HaveCount(2).And.ContainInOrder("TEMP", "HUM");
        }

        [Fact]
        public void ValidateSensor_QuandoSensorNaoExiste_DeveRetornarFalse()
        {
            // Arrange
            var configManager = new ConfigManager();
            configManager.LoadConfig();

            // Act
            var result = configManager.ValidateSensor("S_INVENTADO");

            // Assert
            result.Exists.Should().BeFalse();
            result.SupportedTypes.Should().BeEmpty();
        }

        [Fact]
        public void UpdateSensorDataTypes_QuandoRecebeNovoTipo_DeveAtualizarMemoriaEDisco()
        {
            // Arrange
            var configManager = new ConfigManager();
            configManager.LoadConfig();
            string novosTipos = "TEMP, HUM, CO2";

            // Act
            configManager.UpdateSensorDataTypes("S999", novosTipos);

            // Assert 1: A memória foi atualizada?
            var (_, _, _, memoryTypes, _) = configManager.ValidateSensor("S999");
            memoryTypes.Should().Contain("CO2");

            // Assert 2: O ficheiro foi salvo no disco físico?
            var ficheiroFisico = File.ReadAllText(_snPath);
            ficheiroFisico.Should().Contain("CO2", "porque SaveSensorsConfig() deve ter sido chamado");
        }

        public void Dispose()
        {
            // Limpeza: Remover a variável de ambiente e apagar os ficheiros gerados
            Environment.SetEnvironmentVariable("GID", null);
            if (File.Exists(_gwPath)) File.Delete(_gwPath);
            if (File.Exists(_snPath)) File.Delete(_snPath);
        }
    }
}