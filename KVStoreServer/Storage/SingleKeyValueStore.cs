using System;
using System.Collections.Concurrent;

namespace KVStoreServer.Storage {

    /*
     * Class responsible for the storage of objects within a server
     * This class is thread safe so that multiple thread may access
     * it concurrently
     */
    public class SingleKeyValueStore {

        private ConcurrentDictionary<int, StoredValue> keyValuePairs =
            new ConcurrentDictionary<int, StoredValue>();

        public SingleKeyValueStore() {}

        /*
         * Locks the object with the given key (creates an empty object if necessary)
         * This operation is thread safe since multiple threads can try to lock
         * the object. The object will be locked and no threads will block.
         */
        public void LockValue(int key) {
            StoredValue value = keyValuePairs.GetOrAdd(key, (key) => new StoredValue());
            value.Lock();
        }

        /*
         * Tries to set the value of the object with the given key
         * to the received value, unlocking the object in the end.
         * Throws InvalidOperationException if the object does was not previously locked
         */
        public void AddOrUpdate(int key, string value) {
            // If the object doesn't exist than it wasn't previously locked
            if(!keyValuePairs.TryGetValue(key, out StoredValue storedValue)) {
                throw new InvalidOperationException();
            }
            storedValue.Value = value;
        }

        /*
         * Tries to get the value for the given key
         * If the object is locked than the call blocks
         * If the object exists and has a value then value is updated
         * and the function returns true. Otherwise value is set 
         * to null and the function returns false.
         */
        public bool TryGet(int key, out string value) {
            value = null;
            // If the object doesn't exist than return false
            if (!keyValuePairs.TryGetValue(key, out StoredValue storedValue)) {
                return false;
            }
            value = storedValue.Value;
            return (value != null);
        }
    }
}
