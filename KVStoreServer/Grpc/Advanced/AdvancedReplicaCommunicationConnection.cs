using Common.Grpc;
using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.AdvancedReplicaCommunicationService;

using CausalConsistency = Common.CausalConsistency;

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
            string value,
            CausalConsistency.ImmutableVectorClock replicaTimestamp,
            string writeServerId,
            long timeout) {

            await client.BroadcastWriteAsync(
                new BroadcastWriteRequest {
                    PartitionId = partitionId,
                    MessageId = BuildMessageId(messageId),
                    Key = key,
                    Value = value,
                    ReplicaTimestamp = BuildClock(replicaTimestamp),
                    WriteServerId = writeServerId
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

        private VectorClock BuildClock(CausalConsistency.ImmutableVectorClock vectorClock) {
            (IList<string> serverIds, IList<int> clocks) = 
                CausalConsistency.VectorClocks.ToIdsAndClocksList(vectorClock);
            return new VectorClock {
                ServerIds = { serverIds },
                ServerClocks = { clocks }
            };
        }
    }
}
