using System;
using System.Collections.Generic;
using System.Text;

namespace KVStoreServer.Replication
{
    public class PartitionServersDto
    {
        public string PartitionName { get; set; }

        public HashSet<int> ServerIds { get; set; }
    }
}
