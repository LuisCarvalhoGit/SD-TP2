using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Shared {
    public class Message {
        // Base header
        public string CMD { get; set; }
        public string SID { get; set; }
        public string GID { get; set; }
        public string Timestamp { get; set; }

        // Dictionary for variable fields
        // Ex: "VALUE", "ZONE", "TYPE", "DATA_TYPES", etc...
        public Dictionary<string, string> Data { get; set; } = new Dictionary<string, string>();

        // For video (binary)
        public byte[] BinaryData { get; set; }

        public Message() {
            Timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
        }

        // Transforms object to string
        public override string ToString() {
            var sb = new StringBuilder();
            sb.AppendLine($"CMD:{CMD}");
            sb.AppendLine($"SID:{SID}");
            if (!string.IsNullOrEmpty(GID)) sb.AppendLine($"GID:{GID}");
            sb.AppendLine($"TIMESTAMP:{Timestamp}");

            foreach (var entry in Data) {
                sb.AppendLine($"{entry.Key}:{entry.Value}");
            }

            sb.Append("<EOF>"); // terminator of header
            return sb.ToString();
        }

        // Static factory method to create a message from a raw string
        public static Message Parse(string rawMessage) {
            var msg = new Message();

            string cleanMessage = rawMessage.Replace("<EOF>", "").Trim();
            // Remove o marcador de fim e separa por linhas
            var lines = cleanMessage.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var line in lines) {
                var parts = line.Split(':', 2);
                if (parts.Length < 2) continue;

                var key = parts[0].Trim();
                var value = parts[1].Trim();

                switch (key) {
                    case "CMD": msg.CMD = value; break;
                    case "SID": msg.SID = value; break;
                    case "GID": msg.GID = value; break;
                    case "TIMESTAMP": msg.Timestamp = value; break;
                    default:
                        msg.Data[key] = value;
                        break;
                }
            }
            return msg;
        }

        public static async Task SendMessageAsync(TcpClient client, Message msg) {

            var data = Encoding.UTF8.GetBytes(msg.ToString());
            await client.GetStream().WriteAsync(data, 0, data.Length);
        }

        public static async Task<Message> ReceiveMessageAsync(TcpClient client) {

            var stream = client.GetStream();
            var messageBytes = new List<byte>();
            var buffer = new byte[1];
            

            try {

                while (true) {

                    int bytesRead = await stream.ReadAsync(buffer, 0, 1);

                    if (bytesRead == 0) return null;

                    messageBytes.Add(buffer[0]);

                    int len = messageBytes.Count;

                    // '<' = 60, 'E' = 69, 'O' = 79, 'F' = 70, '>' = 62
                    if (len >= 5 &&
                        messageBytes[len - 5] == '<' &&
                        messageBytes[len - 4] == 'E' &&
                        messageBytes[len - 3] == 'O' &&
                        messageBytes[len - 2] == 'F' &&
                        messageBytes[len - 1] == '>') {
                        
                        
                        string fullRawMessage = Encoding.UTF8.GetString(messageBytes.ToArray());
                        return Message.Parse(fullRawMessage);
                    }
                }

                

            } catch (Exception ex) {

                Console.WriteLine($"Error receiveing message: {ex.Message}");
                return null;
            }
        }

        // Pack the message for UDP (Text Header + Binary Payload)
        public byte[] ToUdpBytes() {
            if (BinaryData == null) return Encoding.UTF8.GetBytes(this.ToString());
            return ToUdpBytes(BinaryData, 0, BinaryData.Length);
        }

        public byte[] ToUdpBytes(byte[] binaryData, int offset, int count) {
            string header = this.ToString(); // Already includes <EOF>
            byte[] headerBytes = Encoding.UTF8.GetBytes(header);

            if (binaryData == null || count == 0) return headerBytes;
            if (offset < 0 || count < 0 || offset + count > binaryData.Length) {
                throw new ArgumentOutOfRangeException(nameof(count), "Invalid UDP payload slice.");
            }

            byte[] fullPacket = new byte[headerBytes.Length + count];
            Buffer.BlockCopy(headerBytes, 0, fullPacket, 0, headerBytes.Length);
            Buffer.BlockCopy(binaryData, offset, fullPacket, headerBytes.Length, count);
            return fullPacket;
        }

        // Unpack UDP bytes back into a Message object
        public static Message FromUdpBytes(byte[] packet) {
            int eofIndex = FindEofMarker(packet);
            if (eofIndex == -1) return null;

            // Extract header
            int headerLength = eofIndex + 5;
            string headerText = Encoding.UTF8.GetString(packet, 0, headerLength);
            Message msg = Message.Parse(headerText);

            // Extract binary data if there's anything after <EOF>
            if (packet.Length > headerLength) {
                msg.BinaryData = new byte[packet.Length - headerLength];
                Buffer.BlockCopy(packet, headerLength, msg.BinaryData, 0, msg.BinaryData.Length);
            }
            return msg;
        }

        private static int FindEofMarker(byte[] packet) {
            for (int i = 0; i <= packet.Length - 5; i++) {
                if (packet[i] == '<' &&
                    packet[i + 1] == 'E' &&
                    packet[i + 2] == 'O' &&
                    packet[i + 3] == 'F' &&
                    packet[i + 4] == '>') {
                    return i;
                }
            }

            return -1;
        }
    }
}
