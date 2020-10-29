using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace KVStoreServer.Storage {

    /*
     * Class responsible for the storage of a partioned key value store
     * This class is thread safe as all operations can be executed concurrently
     */
    public class PartitionedKeyValueStore {

        public readonly ConcurrentDictionary<string, SingleKeyValueStore> stores =
            new ConcurrentDictionary<string, SingleKeyValueStore>();

        public PartitionedKeyValueStore() {
        }

        /*
         * Locks the object with the given partition and key
         */
        public void Lock(string partition, int key) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partition);
            partitionStore.LockValue(key);
        }

        /*
         * Adds or update the object with the given partition or key
         * The object must be previously locked
         * Throws InvalidOpertationException if the object is not previously
         * locked
         */
        public void AddOrUpdate(string partition, int key, string value) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partition);
            partitionStore.AddOrUpdate(key, value);
        }

        /*
         * Tries to returns a given object with a given partition and key
         * If the object exists and has a value, returns true and out value is
         * updated, otherwise returns false and out value = null
         */
        public bool TryGet(string partition, int key, out string value) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partition);
            return partitionStore.TryGet(key, out value);
        }

        private SingleKeyValueStore GetOrAddStore(string partition) {
            return stores.GetOrAdd(partition, (partition) => new SingleKeyValueStore());
        }

        public void TryGetAllObjects(out List<StoredValueDto> objects) {
            objects = new List<StoredValueDto>();

            foreach (KeyValuePair<string, SingleKeyValueStore> stored in stores) {
                
                stored.Value.TryGetAllObjects(
                    out List<StoredValueDto> allObjectsInPartition);

                foreach (StoredValueDto obj in allObjectsInPartition) {
                    obj.PartitionName = stored.Key;
                    objects.Add(obj);
                }
            }
        }
    }
}
