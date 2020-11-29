using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace KVStoreServer.Storage.Advanced {
    public class AdvancedPartitionedKVS {

        private readonly ConcurrentDictionary<string, AdvancedSingleKVS> stores =
            new ConcurrentDictionary<string, AdvancedSingleKVS>();

        public bool Read(string partitionId, string objectId, out string objectValue) {
            return GetOrAddStore(partitionId).Read(objectId, out objectValue);
        }

        public ImmutableTimestampedValue PrepareWrite(
            string partitionId,
            string objectId,
            string newValue,
            string serverId) {

            return GetOrAddStore(partitionId).PrepareWrite(objectId, newValue, serverId);
        }
        public void Write(
            string partitionId,
            string objectId,
            ImmutableTimestampedValue value) {

            GetOrAddStore(partitionId).Write(objectId, value);
        }

        public IEnumerable<StoredObjectDto> ListObjects() {
            lock (stores) {
                return stores.SelectMany(pair => {
                    return pair.Value.ListObjects().Select(objectDto => {
                        objectDto.PartitionId = pair.Key;
                        return objectDto;
                    });
                });
            }
        }

        private AdvancedSingleKVS GetOrAddStore(string partitionId) {
            return stores.GetOrAdd(partitionId, (partitionId) => new AdvancedSingleKVS());
        }
    }
}
