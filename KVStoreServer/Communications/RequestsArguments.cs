using System;
using System.Collections.Generic;

namespace KVStoreServer.Communications {

    public class ReadArguments {
        public int PartitionId { get; set; }
        public int ObjectId { get; set; }
    }

    public class WriteArguments {
        public string PartitionName { get; set; }
        public int ObjectId { get; set; }
        public string ObjectValue { get; set; }
    }

    public class JoinPartitionArguments {
        public string Name { get; set; }
        public IEnumerable<Tuple<int, string>> Members { get; set; }
        public int MasterId { get; set; }
    }


    /* Arguments for internal replication */

    public class LockArguments {
        public string PartitionName { get; set; }
        public int ObjectId { get; set; }
    }

    public class WriteObjectArguments {
        public string PartitionName { get; set; }
        public int ObjectId { get; set; }
        public string ObjectValue { get; set; }
    }
}
