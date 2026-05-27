using System;
using System.Collections.Concurrent;

namespace Shared {
    public sealed class VideoFrameAssembler {
        private sealed class PendingFrame {
            public PendingFrame(string sensorId, long frameId, int totalParts) {
                SensorId = sensorId;
                FrameId = frameId;
                TotalParts = totalParts;
                Chunks = new byte[totalParts][];
                LastUpdatedUtc = DateTime.UtcNow;
            }

            public string SensorId { get; }
            public long FrameId { get; }
            public int TotalParts { get; }
            public byte[][] Chunks { get; }
            public int ReceivedParts { get; set; }
            public int ReceivedBytes { get; set; }
            public DateTime LastUpdatedUtc { get; set; }
        }

        private readonly ConcurrentDictionary<(string SensorId, long FrameId), PendingFrame> _frames = new();
        private readonly ConcurrentDictionary<string, long> _latestCompletedFrame = new();
        private readonly ConcurrentDictionary<string, long> _latestSeenFrame = new();
        private readonly TimeSpan _frameTtl;
        private readonly int _maxPendingFramesPerSensor;
        private readonly int _maxFrameBytes;
        private readonly int _maxPartsPerFrame;

        public VideoFrameAssembler(
            TimeSpan frameTtl,
            int maxPendingFramesPerSensor,
            int maxFrameBytes,
            int maxPartsPerFrame) {
            _frameTtl = frameTtl;
            _maxPendingFramesPerSensor = Math.Max(1, maxPendingFramesPerSensor);
            _maxFrameBytes = Math.Max(1, maxFrameBytes);
            _maxPartsPerFrame = Math.Max(1, maxPartsPerFrame);
        }

        public bool TryAddPacket(Message msg, out byte[]? frameBytes, out string reason) {
            frameBytes = null;
            reason = "";

            if (msg == null || msg.CMD != "STRM" || string.IsNullOrWhiteSpace(msg.SID) || msg.BinaryData == null) {
                reason = "invalid-message";
                return false;
            }

            if (!TryGetInt(msg, "PART", out int part) ||
                !TryGetInt(msg, "TOTAL", out int totalParts) ||
                !TryGetLong(msg, "FRAME", out long frameId)) {
                reason = "invalid-frame-metadata";
                return false;
            }

            if (part < 1 || totalParts < 1 || part > totalParts || totalParts > _maxPartsPerFrame) {
                reason = "invalid-frame-bounds";
                return false;
            }

            string sensorId = msg.SID;
            long latestCompleted = _latestCompletedFrame.GetOrAdd(sensorId, 0);
            if (frameId <= latestCompleted) {
                reason = "stale-frame";
                return false;
            }

            long latestSeen = _latestSeenFrame.AddOrUpdate(sensorId, frameId, (_, current) => Math.Max(current, frameId));
            PurgeOldFrames(sensorId, latestSeen - _maxPendingFramesPerSensor + 1);

            var key = (sensorId, frameId);
            var pending = _frames.GetOrAdd(key, _ => new PendingFrame(sensorId, frameId, totalParts));

            lock (pending) {
                if (pending.TotalParts != totalParts) {
                    _frames.TryRemove(key, out _);
                    reason = "frame-total-changed";
                    return false;
                }

                int chunkIndex = part - 1;
                if (pending.Chunks[chunkIndex] != null) {
                    reason = "duplicate-packet";
                    return false;
                }

                if (pending.ReceivedBytes + msg.BinaryData.Length > _maxFrameBytes) {
                    _frames.TryRemove(key, out _);
                    reason = "frame-too-large";
                    return false;
                }

                pending.Chunks[chunkIndex] = msg.BinaryData;
                pending.ReceivedBytes += msg.BinaryData.Length;
                pending.ReceivedParts++;
                pending.LastUpdatedUtc = DateTime.UtcNow;

                if (pending.ReceivedParts != pending.TotalParts) {
                    reason = "pending";
                    return false;
                }

                frameBytes = new byte[pending.ReceivedBytes];
                int offset = 0;
                for (int i = 0; i < pending.Chunks.Length; i++) {
                    byte[] chunk = pending.Chunks[i];
                    Buffer.BlockCopy(chunk, 0, frameBytes, offset, chunk.Length);
                    offset += chunk.Length;
                }

                _frames.TryRemove(key, out _);
                _latestCompletedFrame.AddOrUpdate(sensorId, frameId, (_, current) => Math.Max(current, frameId));
                reason = "complete";
                return true;
            }
        }

        public int GarbageCollect() {
            int removed = 0;
            DateTime now = DateTime.UtcNow;

            foreach (var entry in _frames) {
                if (now - entry.Value.LastUpdatedUtc > _frameTtl && _frames.TryRemove(entry.Key, out _)) {
                    removed++;
                }
            }

            return removed;
        }

        private void PurgeOldFrames(string sensorId, long minimumFrameToKeep) {
            foreach (var entry in _frames) {
                if (entry.Key.SensorId == sensorId &&
                    entry.Key.FrameId < minimumFrameToKeep &&
                    _frames.TryRemove(entry.Key, out _)) {
                }
            }
        }

        private static bool TryGetInt(Message msg, string key, out int value) {
            value = 0;
            return msg.Data.TryGetValue(key, out string? raw) && int.TryParse(raw, out value);
        }

        private static bool TryGetLong(Message msg, string key, out long value) {
            value = 0;
            return msg.Data.TryGetValue(key, out string? raw) && long.TryParse(raw, out value);
        }
    }
}
