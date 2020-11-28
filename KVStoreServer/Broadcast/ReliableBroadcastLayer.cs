using Common.Utils;
using KVStoreServer.CausalConsistency;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Naming;
using KVStoreServer.Storage.Advanced;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KVStoreServer.Broadcast {

    public delegate void WriteMessageHandler(BroadcastWriteMessage message);
    public delegate void FailureMessageHandler(BroadcastFailureMessage serverId);
    public class ReliableBroadcastLayer {

        private WriteMessageHandler writeMessageHandler = null;
        private FailureMessageHandler failureMessageHandler = null;

        private string selfId;
        private readonly ConcurrentDictionary<string, PartitionReliableBroadcastHandler> partitionHandlers =
            new ConcurrentDictionary<string, PartitionReliableBroadcastHandler>();

        private ReliableBroadcastLayer() {
            AdvancedNamingServiceLayer.Instance.BindBroadcastWriteHandler(OnBroadcastWriteDeliver);
            AdvancedNamingServiceLayer.Instance.BindBroadcastFailureHandler(OnBroadcastFailureDeliver);
        }

        public static ReliableBroadcastLayer Instance { get; } = new ReliableBroadcastLayer(); 

        public void BindWriteHandler(WriteHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindWriteHandler(handler);
        }

        public void BindWriteMessageHandler(WriteMessageHandler handler) {
            writeMessageHandler = handler;
        }

        public void BindFailureMessageHandler(FailureMessageHandler handler) {
            failureMessageHandler = handler;
        }

        public void RegisterSelfId(string selfId) {
            this.selfId = selfId;
        }

        public bool RegisterPartition(string partitionId, ImmutableHashSet<string> serverIds) {
            Conditions.AssertArgument(serverIds.Contains(selfId));

            return partitionHandlers.TryAdd(
                partitionId, 
                new PartitionReliableBroadcastHandler(
                    selfId, 
                    partitionId, 
                    serverIds, 
                    writeMessageHandler,
                    failureMessageHandler));
        }

        public void BroadcastWrite(
            string partitionId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            if (partitionHandlers.TryGetValue(partitionId, out PartitionReliableBroadcastHandler handler)) {
                handler.BroadcastWrite(
                    key,
                    value,
                    replicaTimestamp);
            }
        }

        private void OnBroadcastWriteDeliver(BroadcastWriteArguments arguments) {
            if (partitionHandlers.TryGetValue(
                arguments.PartitionId, 
                out PartitionReliableBroadcastHandler handler)) {
                
                handler.OnBroadcastWriteDeliver(arguments);
            }
        }

        public void BroadcastFailure(
            string partitionId,
            string failureServerId) {

            if (partitionHandlers.TryGetValue(partitionId, out PartitionReliableBroadcastHandler handler)) {
                handler.BroadcastFailure(failureServerId);
            }
        }

        private void OnBroadcastFailureDeliver(BroadcastFailureArguments arguments) {

            if (partitionHandlers.TryGetValue(
                arguments.PartitionId,
                out PartitionReliableBroadcastHandler handler)) {

                handler.OnBroadcastFailureDeliver(arguments);
            }
        }
    }
}
