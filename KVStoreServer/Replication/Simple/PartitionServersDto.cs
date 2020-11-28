using System.Collections.Generic;

namespace KVStoreServer.Replication.Simple {
    public class PartitionServersDto {
        public string PartitionId { get; set; }

        public HashSet<string> ServerIds { get; set; }
    }
}
