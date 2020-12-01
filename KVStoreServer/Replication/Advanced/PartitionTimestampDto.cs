using Common.CausalConsistency;

namespace KVStoreServer.Replication.Advanced {
    public class PartitionTimestampDto {

        public string PartitionId { get; set; }

        public ImmutableVectorClock PartitionTimestamp { get; set; }
    }
}
