namespace KVStoreServer.Storage.Simple {
    public class StoredValueDto {

        public string PartitionId { get; set; }

        public string ObjectId { get; set; }

        public string Value { get; set; }

        public bool IsMaster { get; set; }

        public bool IsLocked { get; set; }
    }
}
