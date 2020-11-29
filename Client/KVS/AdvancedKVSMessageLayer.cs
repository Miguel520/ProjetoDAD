using Client.Grpc.Advanced;
using Client.Naming;
using Common.CausalConsistency;
using Common.Protos.AdvancedKeyValueStore;
using Common.Utils;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Client.KVS {
    class AdvancedKVSMessageLayer {

        private static AdvancedKVSMessageLayer instance = null;
        private static readonly object instanceLock = new object();

        private readonly NamingService namingService = null;

        private readonly Dictionary<string, MutableVectorClock> timestamps
            = new Dictionary<string, MutableVectorClock>();

        private AdvancedKVSMessageLayer(NamingService namingService) {
            this.namingService = namingService;
        }

        public static void SetContext(NamingService namingService) {
            lock (instanceLock) {
                Conditions.AssertArgument(namingService != null);
                Conditions.AssertState(instance == null);
                instance = new AdvancedKVSMessageLayer(namingService);
            }
        }

        public static AdvancedKVSMessageLayer Instance {
            get {
                lock (instanceLock) {
                    Conditions.AssertState(instance != null);
                    return instance;
                }
            }
        }

        public bool Write(
            string partitionId,
            string objectId,
            string value) {

            if (!namingService.ListPartition(partitionId, out ImmutableHashSet<string> serverIds)) {
                return false;
            }
            
            if (!timestamps.TryGetValue(partitionId, out MutableVectorClock timestamp)) {
                timestamp = MutableVectorClock.Empty();
                timestamps.Add(partitionId, timestamp);
            }

            
            foreach (string serverId in serverIds) {
                ImmutableVectorClock replicaTimestamp = null;

                bool success = namingService.Lookup(serverId, out string serverUrl)
                    && AdvancedGrpcMessageLayer.Instance.Write(
                        serverUrl, 
                        partitionId, 
                        objectId, 
                        value, 
                        timestamp.ToImmutable(), 
                        out replicaTimestamp);

                if (success) {
                    timestamp.Merge(replicaTimestamp);
                    return true;
                }
            }
            return false;
        }

        public bool ListServer(string serverId, out ImmutableList<StoredObject> objects) {
            objects = default;
            return namingService.Lookup(serverId, out string serverUrl)
                && AdvancedGrpcMessageLayer.Instance.ListServer(serverUrl, out objects);
        }
    }
}
