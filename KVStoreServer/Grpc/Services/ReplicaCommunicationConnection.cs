using Common.Grpc;
using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using KVStoreServer.KVS;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.ReplicaCommunicationService;

using Broadcast = KVStoreServer.Broadcast;

namespace KVStoreServer.Grpc {
    public class ReplicaCommunicationConnection {

        private readonly ChannelBase channel;
        private readonly ReplicaCommunicationServiceClient client;

        public ReplicaCommunicationConnection(string url) {
            channel = ChannelPool.Instance.ForUrl(url);
            client = new ReplicaCommunicationServiceClient(channel);
        }

        ~ReplicaCommunicationConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public async Task Lock(string partitionId, string objectId, long timeout) {
            await client.LockAsync(
                new LockRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId
                },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        public async Task Write(
            string partitionId, 
            string objectId, 
            string objectValue,
            long timeout) {

            await client.WriteAsync(
                new WriteRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId,
                    ObjectValue = objectValue
                },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        public async Task Ping(long timeout) {
            await client.PingAsync(
                new PingRequest { },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        public async Task BroadcastWrite(
            Broadcast.MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            CausalConsistency.ImmutableVectorClock replicaTimestamp,
            long timeout) {

            await client.BroadcastWriteAsync(
                new BroadcastWriteRequest {
                    MessageId = BuildMessageId(messageId),
                    Key = key,
                    TimestampedValue = BuildValue(value),
                    ReplicaTimestamp = BuildClock(replicaTimestamp)
                },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        private MessageId BuildMessageId(Broadcast.MessageId messageId) {
            return new MessageId {
                ServerId = messageId.SenderId,
                ServerCounter = messageId.SenderCounter
            };
        }

        private TimestampedValue BuildValue(ImmutableTimestampedValue value) {
            return new TimestampedValue {
                ObjectValue = value.Value,
                Timestamp = BuildClock(value.Timestamp),
                LastWriteServerId = value.LastWriteServerId
            };
        }

        private VectorClock BuildClock(CausalConsistency.ImmutableVectorClock vectorClock) {
            List<string> serverIds = new List<string>();
            List<int> clocks = new List<int>();
            foreach ((string serverId, int serverClock) in vectorClock.Clocks) {
                serverIds.Add(serverId);
                clocks.Add(serverClock);
            }
            return new VectorClock {
                ServerIds = { serverIds },
                ServerClocks = { clocks }
            };
        }
    }
}
