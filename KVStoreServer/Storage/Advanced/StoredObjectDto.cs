namespace KVStoreServer.Storage.Advanced {
    public class StoredObjectDto {

        public string PartitionId { get; set; }

        public string ObjectId { get; set; }

        public ImmutableTimestampedValue TimestampedValue { get; set; }
    }
}
