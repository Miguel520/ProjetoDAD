namespace KVStoreServer.Grpc.Simple {

    public class ReadArguments {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
    }

    public class WriteArguments {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
        public string ObjectValue { get; set; }
    }

    /* Arguments for internal replication */

    public class LockArguments {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
    }

    public class WriteObjectArguments {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
        public string ObjectValue { get; set; }
    }
}
