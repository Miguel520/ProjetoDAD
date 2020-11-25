using Common.Utils;
using KVStoreServer.CausalConsistency;
using KVStoreServer.KVS;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KVStoreServer.Broadcast {
    public class ReliableBroadcastLayer {

        private string selfId;
        private readonly ConcurrentDictionary<string, PartitionReliableBroadcastHandler> partitionHandlers =
            new ConcurrentDictionary<string, PartitionReliableBroadcastHandler>();

        public void RegisterSelfId(string selfId) {
            this.selfId = selfId;
        }

        public bool RegisterPartition(string partitionId, ImmutableHashSet<string> serverIds) {
            Conditions.AssertArgument(serverIds.Contains(selfId));

            return partitionHandlers.TryAdd(
                partitionId, 
                new PartitionReliableBroadcastHandler(selfId, partitionId, serverIds));
        }

        public void BroadcastWrite(
            string partitionId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp,
            long timeout) {

            if (partitionHandlers.TryGetValue(partitionId, out PartitionReliableBroadcastHandler handler)) {
                //handler.BroadcastWrite(
                //    key,
                //    value,
                //    replicaTimestamp,
                //    timeout);
            }
        }
    }
}
