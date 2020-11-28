using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace KVStoreServer.Storage.Simple {

    /*
     * Class responsible for the storage of objects within a server
     * This class is thread safe so that multiple thread may access
     * it concurrently
     */
    public class SingleKeyValueStore {

        private ConcurrentDictionary<string, StoredValue> keyValuePairs =
            new ConcurrentDictionary<string, StoredValue>();

        public SingleKeyValueStore() { }

        /*
         * Locks the object with the given key (creates an empty object if necessary)
         * This operation is thread safe since multiple threads can try to lock
         * the object. The object will be locked and no threads will block.
         */
        public void LockValue(string objectId) {
            StoredValue value = keyValuePairs.GetOrAdd(objectId, (key) => new StoredValue());
            value.Lock();
        }

        /*
         * Tries to set the value of the object with the given key
         * to the received value, unlocking the object in the end.
         * Throws InvalidOperationException if the object does was not previously locked
         */
        public void AddOrUpdate(string objectId, string value) {
            // If the object doesn't exist than it wasn't previously locked
            if (!keyValuePairs.TryGetValue(objectId, out StoredValue storedValue)) {
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
        public bool TryGet(string objectId, out string value) {
            value = null;
            // If the object doesn't exist than return false
            if (!keyValuePairs.TryGetValue(objectId, out StoredValue storedValue)) {
                return false;
            }
            value = storedValue.Value;
            return value != null;
        }

        public void TryGetAllObjects(out List<StoredValueDto> objects) {
            objects = new List<StoredValueDto>();
            StoredValueDto storedValueDto;

            foreach (KeyValuePair<string, StoredValue> stored in keyValuePairs) {
                storedValueDto = stored.Value.GetStoredValueDto();
                storedValueDto.ObjectId = stored.Key;
                objects.Add(storedValueDto);
            }
        }
    }
}
