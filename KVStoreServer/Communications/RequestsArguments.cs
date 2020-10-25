using System;
using System.Collections.Generic;

namespace KVStoreServer.Communications {

    public class ReadArguments {
        public int PartitionId { get; set; }
        public int ObjectId { get; set; }
    }

    public class WriteArguments {
        public int PartitionId { get; set; }
        public int ObjectId { get; set; }
        public string ObjectValue { get; set; }
    }

    public class JoinPartitionArguments {
        public string Name { get; set; }
        public IEnumerable<Tuple<int, string>> Members { get; set; }
        public int MasterId { get; set; }
    }
}
