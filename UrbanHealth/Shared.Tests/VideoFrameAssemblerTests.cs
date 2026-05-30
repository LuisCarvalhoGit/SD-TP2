using System;
using System.Collections.Generic;
using FluentAssertions;
using Shared;
using Xunit;

namespace Shared.Tests
{
    
    public class VideoFrameAssemblerTests
    {
        // ARRANGE global: Configuração base do Assembler para testes rápidos
        private VideoFrameAssembler CreateSut() // SUT = System Under Test
        {
            return new VideoFrameAssembler(
                frameTtl: TimeSpan.FromMilliseconds(500), 
                maxPendingFramesPerSensor: 3, 
                maxFrameBytes: 1024 * 1024, 
                maxPartsPerFrame: 10);
        }

        private Message CriarPacoteVideo(string sid, string frameId, int part, int total, byte[] payload)
        {
            var msg = new Message { CMD = "STRM", SID = sid };
            msg.Data["TYPE"] = "DATA";
            msg.Data["FRAME"] = frameId;
            msg.Data["PART"] = part.ToString();
            msg.Data["TOTAL"] = total.ToString();
            msg.BinaryData = payload;
            return msg;
        }

        [Fact]
        public void TryAddPacket_QuandoRecebeTodasAsPartes_DeveRetornarFrameCompleta()
        {
            // Arrange
            var assembler = CreateSut();
            var sid = "S101";
            var frameId = "1000";
            
            var p1 = CriarPacoteVideo(sid, frameId, 1, 2, new byte[] { 0x01, 0x02 });
            var p2 = CriarPacoteVideo(sid, frameId, 2, 2, new byte[] { 0x03, 0x04 });

            // Act
            assembler.TryAddPacket(p1, out byte[] image1, out string reason1);
            bool isComplete = assembler.TryAddPacket(p2, out byte[] imageFinal, out string reasonFinal);

            // Assert
            isComplete.Should().BeTrue();
            imageFinal.Should().NotBeNull();
            imageFinal.Should().BeEquivalentTo(new byte[] { 0x01, 0x02, 0x03, 0x04 }, "Os bytes das duas partes devem ser concatenados na ordem correta");
        }

        [Fact]
        public void TryAddPacket_QuandoPacotesChegamForaDeOrdem_DeveMontarCorretamente()
        {
            // Arrange
            var assembler = CreateSut();
            var sid = "S102";
            var frameId = "1001";
            
            var p1 = CriarPacoteVideo(sid, frameId, 1, 3, new byte[] { 0x01 });
            var p2 = CriarPacoteVideo(sid, frameId, 2, 3, new byte[] { 0x02 });
            var p3 = CriarPacoteVideo(sid, frameId, 3, 3, new byte[] { 0x03 });

            // Act - Inserimos a parte 3, depois a 1, e finalmente a 2
            assembler.TryAddPacket(p3, out _, out _);
            assembler.TryAddPacket(p1, out _, out _);
            bool isComplete = assembler.TryAddPacket(p2, out byte[] imageFinal, out _);

            // Assert
            isComplete.Should().BeTrue();
            imageFinal.Should().BeEquivalentTo(new byte[] { 0x01, 0x02, 0x03 });
        }

        [Fact]
        public void GarbageCollect_QuandoFrameUltrapassaTTL_DeveLimparMemoria()
        {
            // Arrange
            var assembler = CreateSut();
            
            // Inserimos apenas a Parte 1 de 2. A frame vai ficar pendente.
            var p1 = CriarPacoteVideo("S101", "9999", 1, 2, new byte[] { 0xFF });
            assembler.TryAddPacket(p1, out _, out _);

            // Act
            // Simulamos o tempo a passar (no xUnit normal teríamos de fazer um Task.Delay pequeno
            // porque o TTL do nosso SUT foi configurado para 500ms)
            System.Threading.Thread.Sleep(600); 
            int framesRemovidas = assembler.GarbageCollect();

            // Assert
            framesRemovidas.Should().Be(1, "O Garbage Collector deve encontrar 1 frame expirada e removê-la da memória");
        }
    }
}