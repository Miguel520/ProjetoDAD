using Common.Protos.KeyValueStore;

namespace Client.Grpc {
    class KVStoreMessageFactory {

        public KVStoreMessageFactory() {
        }

        public static WriteRequest BuildWriteRequest(
            string partitionId,
            string objectId,
            string objectValue) {

            return new WriteRequest {
                PartitionId = partitionId,
                ObjectId = objectId,
                ObjectValue = objectValue
            };
        }

        public static ReadRequest BuildReadRequest(
            string partitionId,
            string objectId) {

            return new ReadRequest {
                PartitionId = partitionId,
                ObjectId = objectId
            };
        }

        public static ListRequest BuildListRequest() {
            return new ListRequest {};
        }
    }
}
