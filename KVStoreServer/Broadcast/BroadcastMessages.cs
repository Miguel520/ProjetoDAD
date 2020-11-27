using KVStoreServer.CausalConsistency;
using KVStoreServer.KVS;

namespace KVStoreServer.Broadcast {
    
    public class BroadcastWriteMessage {

        public string PartitionId { get; set; }

        public string Key { get; set; }

        public ImmutableTimestampedValue TimestampedValue { get; set; }

        public ImmutableVectorClock ReplicaTimestamp { get; set; }
    }

    public class BroadcastFailureMessage {

        public string PartitionId { get; set; }

        public string FailedServerId { get; set; }
    }
}
