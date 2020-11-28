using System.Collections.Concurrent;
using System.Collections.Generic;

namespace KVStoreServer.Storage.Simple {

    /*
     * Class responsible for the storage of a partioned key value store
     * This class is thread safe as all operations can be executed concurrently
     */
    public class PartitionedKeyValueStore {

        private readonly ConcurrentDictionary<string, SingleKeyValueStore> stores =
            new ConcurrentDictionary<string, SingleKeyValueStore>();

        public PartitionedKeyValueStore() {
        }

        /*
         * Locks the object with the given partition and key
         */
        public void Lock(string partitionId, string objectId) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partitionId);
            partitionStore.LockValue(objectId);
        }

        /*
         * Adds or update the object with the given partition or key
         * The object must be previously locked
         * Throws InvalidOpertationException if the object is not previously
         * locked
         */
        public void AddOrUpdate(string partitionId, string objectId, string value) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partitionId);
            partitionStore.AddOrUpdate(objectId, value);
        }

        /*
         * Tries to returns a given object with a given partition and key
         * If the object exists and has a value, returns true and out value is
         * updated, otherwise returns false and out value = null
         */
        public bool TryGet(string partitionId, string objectId, out string value) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partitionId);
            return partitionStore.TryGet(objectId, out value);
        }

        private SingleKeyValueStore GetOrAddStore(string partitionId) {
            return stores.GetOrAdd(partitionId, (partition) => new SingleKeyValueStore());
        }

        public void TryGetAllObjects(out List<StoredValueDto> objects) {
            objects = new List<StoredValueDto>();

            foreach (KeyValuePair<string, SingleKeyValueStore> stored in stores) {

                stored.Value.TryGetAllObjects(
                    out List<StoredValueDto> allObjectsInPartition);

                foreach (StoredValueDto obj in allObjectsInPartition) {
                    obj.PartitionId = stored.Key;
                    objects.Add(obj);
                }
            }
        }
    }
}
