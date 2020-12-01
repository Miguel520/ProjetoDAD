using Common.CausalConsistency;
using KVStoreServer.Broadcast;

namespace KVStoreServer.Grpc.Advanced {

    public class ReadArguments {

        public string PartitionId { get; set; }

        public string ObjectId { get; set; }

        public ImmutableVectorClock Timestamp { get; set; }
    }

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

        public string Value { get; set; }

        public ImmutableVectorClock ReplicaTimestamp { get; set; }

        public string WriteServerId { get; set; }
    }

    public class BroadcastFailureArguments {

        public string PartitionId { get; set; }

        public MessageId MessageId { get; set; }

        public string FailedServerId { get; set; }
    }
}
