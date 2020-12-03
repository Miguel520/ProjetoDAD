using Common.CausalConsistency;
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
         * Returns true if timestamp should be merged
         */
        public bool Write(
            string objectId, 
            string value, 
            string serverId, 
            VectorClock selfTimestamp, 
            VectorClock otherTimestamp) {

            // More recent update, should update value
            if (VectorClock.HappensBefore(selfTimestamp, otherTimestamp)) {

                StoredValue storedValue = GetOrAdd(objectId);
                lock (storedValue) {
                    storedValue.Value = value;
                    storedValue.LastWriteServerId = serverId;
                }
                //System.Console.WriteLine(
                //    "Updated Happens Before prevValue: {0}, prevWrite: {1}, newValue: {2}, newWrite: {3}, {4}, {5}",
                //    storedValue.Value,
                //    storedValue.LastWriteServerId,
                //    value,
                //    serverId,
                //    selfTimestamp,
                //    otherTimestamp);
                return true;
            }
            // Concurrent operations (keep value with smaller server id)
            else if (!VectorClock.HappensAfter(selfTimestamp, otherTimestamp)) {
                StoredValue storedValue = GetOrAdd(objectId);
                lock (storedValue) {
                    if (serverId == storedValue.LastWriteServerId
                        && selfTimestamp[serverId] < otherTimestamp[serverId]) {
                        storedValue.Value = value;
                        storedValue.LastWriteServerId = serverId;
                        //System.Console.WriteLine(
                        //    "Updated Concurrent prevValue: {0}, prevWrite: {1}, newValue: {2}, newWrite: {3}, {4}, {5}",
                        //    storedValue.Value,
                        //    storedValue.LastWriteServerId,
                        //    value,
                        //    serverId,
                        //    selfTimestamp,
                        //    otherTimestamp);
                    }
                    else if (Strings.LessThan(serverId, storedValue.LastWriteServerId)) {
                        //System.Console.WriteLine(
                        //    "Updated Concurrent prevValue: {0}, prevWrite: {1}, newValue: {2}, newWrite: {3}, {4}, {5}",
                        //    storedValue.Value,
                        //    storedValue.LastWriteServerId,
                        //    value,
                        //    serverId,
                        //    selfTimestamp,
                        //    otherTimestamp);
                        storedValue.Value = value;
                        storedValue.LastWriteServerId = serverId;
                    }
                    else {
                        //System.Console.WriteLine(
                        //    "Not Updated Concurrent prevValue: {0}, prevWrite: {1}, newValue: {2}, newWrite: {3}, {4}, {5}",
                        //    storedValue.Value,
                        //    storedValue.LastWriteServerId,
                        //    value,
                        //    serverId,
                        //    selfTimestamp,
                        //    otherTimestamp);
                    }
                }
                return true;
            }
            return false;
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
