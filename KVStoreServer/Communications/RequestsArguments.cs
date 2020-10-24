using System;
using System.Collections.Generic;

namespace KVStoreServer.Communications {

    public class ReadArguments {
        public int PartitionId;
        public int ObjectId;
    }

    public class WriteArguments {
        public int PartitionId;
        public int ObjectId;
        public string ObjectValue;
    }

    public class JoinPartitionArguments {
        public string Name;
        public IEnumerable<Tuple<int, string>> Members;
        public int MasterId;
    }
}
