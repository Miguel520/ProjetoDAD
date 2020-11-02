using System.Collections.Generic;

namespace KVStoreServer.Replication
{
    public class PartitionServersDto
    {
        public string PartitionId { get; set; }

        public HashSet<string> ServerIds { get; set; }
    }
}
