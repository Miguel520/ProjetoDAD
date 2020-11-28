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

        private ConcurrentDictionary<string, MutableTimestampedValue> keyValuePairs =
            new ConcurrentDictionary<string, MutableTimestampedValue>();

        public ImmutableTimestampedValue PrepareWrite(string objectId, string newValue, string serverId) {
            return GetOrAddValue(objectId).PrepareMerge(newValue, serverId);
        }

        public void Write(string objectId, ImmutableTimestampedValue other) {
            GetOrAddValue(objectId).Merge(other);
        }

        public IEnumerable<StoredObjectDto> ListObjects() {
            lock(keyValuePairs) {
                return keyValuePairs.Select(pair => {
                    return new StoredObjectDto {
                        ObjectId = pair.Key,
                        TimestampedValue = pair.Value.ToImmutable()
                    };
                });
            }
        }

        private MutableTimestampedValue GetOrAddValue(string objectId) {
            return keyValuePairs.GetOrAdd(
                objectId,
                (key) => new MutableTimestampedValue());
        }
    }
}
