using KVStoreServer.Broadcast;
using KVStoreServer.CausalConsistency;
using KVStoreServer.Storage.Advanced;

namespace KVStoreServer.Grpc.Advanced {

    public class WriteArguments {

        public string PartitionId { get; set; }

        public string ObjectId { get; set; }

        public string ObjectValue { get; set; }

        public ImmutableVectorClock Timestamp { get; set; }
    }

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
