using System;
using System.Collections.Generic;

namespace KVStoreServer.Grpc.Base {
    public class JoinPartitionArguments {
        public string PartitionId { get; set; }
        public IEnumerable<Tuple<string, string>> Members { get; set; }
        public string MasterId { get; set; }
    }
}
