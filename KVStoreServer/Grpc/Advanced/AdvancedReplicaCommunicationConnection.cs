using Common.Grpc;
using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.AdvancedReplicaCommunicationService;
using KVStoreServer.Storage.Advanced;

namespace KVStoreServer.Grpc.Advanced {
    class AdvancedReplicaCommunicationConnection {

        private readonly ChannelBase channel;
        private readonly AdvancedReplicaCommunicationServiceClient client;

        public AdvancedReplicaCommunicationConnection(string url) {
            channel = ChannelPool.Instance.ForUrl(url);
            client = new AdvancedReplicaCommunicationServiceClient(channel);
        }

        ~AdvancedReplicaCommunicationConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public async Task BroadcastWrite(
            string partitionId,
            Broadcast.MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            CausalConsistency.ImmutableVectorClock replicaTimestamp,
            long timeout) {

            await client.BroadcastWriteAsync(
                new BroadcastWriteRequest {
                    PartitionId = partitionId,
                    MessageId = BuildMessageId(messageId),
                    Key = key,
                    TimestampedValue = BuildValue(value),
                    ReplicaTimestamp = BuildClock(replicaTimestamp)
                },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        public async Task BroadcastFailure(
            string partitionId,
            Broadcast.MessageId messageId,
            string failedServerId, 
            long timeout) {

            await client.BroadcastFailureAsync(
                new BroadcastFailureRequest {
                    PartitionId = partitionId,
                    MessageId = BuildMessageId(messageId),
                    FailedServerId = failedServerId
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
