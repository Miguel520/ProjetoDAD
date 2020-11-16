using System;
using System.Collections.Generic;

namespace KVStoreServer.Communications {

    public class ReadArguments {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
    }

    public class WriteArguments {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
        public string ObjectValue { get; set; }
    }

    public class JoinPartitionArguments {
        public string PartitionId { get; set; }
        public IEnumerable<Tuple<string, string>> Members { get; set; }
        public string MasterId { get; set; }
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
