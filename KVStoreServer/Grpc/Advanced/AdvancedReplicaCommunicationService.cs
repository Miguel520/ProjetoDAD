using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using KVStoreServer.Storage.Advanced;
using System.Collections.Generic;
using System.Threading.Tasks;
using static Common.Protos.ReplicaCommunication.AdvancedReplicaCommunicationService;

namespace KVStoreServer.Grpc.Advanced {
    class AdvancedReplicaCommunicationService : AdvancedReplicaCommunicationServiceBase {

        private readonly AdvancedIncomingDispatcher dispatcher;

        public AdvancedReplicaCommunicationService(AdvancedIncomingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<BroadcastWriteResponse> BroadcastWrite(BroadcastWriteRequest request, ServerCallContext context) {
            await dispatcher.OnBroadcastWrite(ParseBroadcastWriteRequest(request));
            return new BroadcastWriteResponse { };
        }

        public override async Task<BroadcastFailureResponse> BroadcastFailure(BroadcastFailureRequest request, ServerCallContext context) {
            await dispatcher.OnBroadcastFailure(ParseBroadcastFailureRequest(request));
            return new BroadcastFailureResponse { };
        }

        private BroadcastWriteArguments ParseBroadcastWriteRequest(
            BroadcastWriteRequest request) {

            return new BroadcastWriteArguments {
                PartitionId = request.PartitionId,
                Key = request.Key,
                MessageId = BuildMessageId(request.MessageId),
                TimestampedValue = BuildTimestampedValue(request.TimestampedValue),
                ReplicaTimestamp = BuildVectorClock(request.ReplicaTimestamp)
            };
        }

        private BroadcastFailureArguments ParseBroadcastFailureRequest(
            BroadcastFailureRequest request) {

            return new BroadcastFailureArguments {
                PartitionId = request.PartitionId,
                MessageId = BuildMessageId(request.MessageId),
                FailedServerId = request.FailedServerId
            };
        }

        private Broadcast.MessageId BuildMessageId(MessageId messageId) {
            return new Broadcast.MessageId(messageId.ServerId, messageId.ServerCounter);
        }

        private ImmutableTimestampedValue BuildTimestampedValue(
            TimestampedValue timestampedValue) {

            return ImmutableTimestampedValue.BuildFrom(
                timestampedValue.ObjectValue,
                BuildVectorClock(timestampedValue.Timestamp),
                timestampedValue.LastWriteServerId);
        }

        private CausalConsistency.ImmutableVectorClock BuildVectorClock(VectorClock vectorClock) {
            List<KeyValuePair<string, int>> clocks = new List<KeyValuePair<string, int>>();
            for (int i = 0; i < vectorClock.ServerIds.Count; i++) {
                clocks.Add(new KeyValuePair<string, int>(
                    vectorClock.ServerIds[i],
                    vectorClock.ServerClocks[i]
                ));
            }
            return CausalConsistency.ImmutableVectorClock.FromClocks(clocks);
        }
    }
}
