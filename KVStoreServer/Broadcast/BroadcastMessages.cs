using Common.CausalConsistency;

namespace KVStoreServer.Broadcast {

    public class BroadcastWriteMessage {

        public string PartitionId { get; set; }

        public string Key { get; set; }

        public string Value { get; set; }

        public ImmutableVectorClock ReplicaTimestamp { get; set; }

        public string WriteServerId { get; set; }
    }

    public class BroadcastFailureMessage {

        public string PartitionId { get; set; }

        public string FailedServerId { get; set; }
    }
}
