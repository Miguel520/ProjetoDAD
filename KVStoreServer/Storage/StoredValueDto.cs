
namespace KVStoreServer.Storage {
    public class StoredValueDto {
    
        public string PartitionName { get; set; }
        
        public int ObjectId { get; set; }

        public string Value { get; set; }

        public bool IsMaster { get; set; }

        public bool IsLocked { get; set; }
    }
}
