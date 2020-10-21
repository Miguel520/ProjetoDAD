using System.Collections.Concurrent;

namespace KVStoreServer.Storage {

    /*
     * Class responsible for the storage of a partioned key value store
     * This class is thread safe as all operations can be executed concurrently
     */
    public class PartitionedKeyValueStore {

        public readonly ConcurrentDictionary<int, SingleKeyValueStore> stores =
            new ConcurrentDictionary<int, SingleKeyValueStore>();

        public PartitionedKeyValueStore() {
        }

        /*
         * Locks the object with the given partition and key
         */
        public void Lock(int partition, int key) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partition);
            partitionStore.LockValue(key);
        }

        /*
         * Adds or update the object with the given partition or key
         * The object must be previously locked
         * Throws InvalidOpertationException if the object is not previously
         * locked
         */
        public void AddOrUpdate(int partition, int key, string value) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partition);
            partitionStore.AddOrUpdate(key, value);
        }

        /*
         * Tries to returns a given object with a given partition and key
         * If the object exists and has a value, returns true and out value is
         * updated, otherwise returns false and out value = null
         */
        public bool TryGet(int partition, int key, out string value) {
            SingleKeyValueStore partitionStore = GetOrAddStore(partition);
            return partitionStore.TryGet(key, out value);
        }

        private SingleKeyValueStore GetOrAddStore(int partition) {
            return stores.GetOrAdd(partition, (partition) => new SingleKeyValueStore());
        }
    }
}
