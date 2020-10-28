using System;
using Common.Protos.KeyValueStore;

namespace Client.KVStoreServer {
    class KVStoreMessageFactory {

        public KVStoreMessageFactory() {
        }

        public static WriteRequest BuildWriteRequest(
            string partitionName,
            int objectId,
            string objectValue) {

            return new WriteRequest {
                PartitionName = partitionName,
                ObjectId = objectId,
                ObjectValue = objectValue
            };
        }

        public static ReadRequest BuildReadRequest(
            string partitionName,
            int objectId) {

            return new ReadRequest {
                PartitionName = partitionName,
                ObjectId = objectId
            };
        }
    }
}
