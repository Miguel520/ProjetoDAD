using KVStoreServer.Broadcast;
using KVStoreServer.CausalConsistency;
using KVStoreServer.KVS;

namespace KVStoreServer.Grpc.Advanced {
    public class BroadcastWriteArguments {

        public string PartitionId { get; set; }

        public MessageId MessageId { get; set; }

        public string Key { get; set; }

        public ImmutableTimestampedValue TimestampedValue { get; set; }

        public ImmutableVectorClock ReplicaTimestamp { get; set; }

    }

    public class BroadcastFailureArguments {

        public string PartitionId { get; set; }

        public MessageId MessageId { get; set; }

        public string FailedServerId { get; set; }
    }
}
