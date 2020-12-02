using Common.Utils;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace KVStoreServer.Storage.Advanced {

    /*
     * Class responsible for the storage of objects
     * regarding a single partition in the advanced
     * server version
     */
    public class AdvancedSingleKVS {

        private readonly ConcurrentDictionary<string, StoredValue> keyValuePairs =
            new ConcurrentDictionary<string, StoredValue>();

        public bool Read(string objectId, out string objectValue) {
            objectValue = GetOrAdd(objectId).Value;
            return objectValue != null;
        }

        /*
         * Updates a value with a new write
         * Only updates if new server id is less then the last server to write
         * or if force is true
         */
        public void Write(string objectId, string value, string serverId, bool force) {
            StoredValue storedValue = GetOrAdd(objectId);
            lock (storedValue) {
                if (force || Strings.LessThan(serverId, storedValue.LastWriteServerId)) {
                    storedValue.Value = value;
                    storedValue.LastWriteServerId = serverId;
                }
            }
        }

        public IEnumerable<StoredObjectDto> ListObjects() {
            lock(keyValuePairs) {
                return keyValuePairs.Select(pair => {
                    return new StoredObjectDto {
                        ObjectId = pair.Key,
                        Value = pair.Value.Value
                    };
                });
            }
        }

        private StoredValue GetOrAdd(string objectId) {
            return keyValuePairs.GetOrAdd(objectId, (key) => new StoredValue { });
        }

        class StoredValue {

            public string Value { get; set; }

            public string LastWriteServerId { get; set; }
        }
    }
}
