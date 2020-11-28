using Client.Grpc.Advanced;
using Client.Naming;
using Common.Protos.AdvancedKeyValueStore;
using Common.Utils;
using System.Collections.Immutable;

namespace Client.KVS {
    class AdvancedKVSMessageLayer {

        private static AdvancedKVSMessageLayer instance = null;
        private static readonly object instanceLock = new object();

        private readonly NamingService namingService = null;

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

        public bool ListServer(string serverId, out ImmutableList<StoredObject> objects) {
            objects = default;
            return namingService.Lookup(serverId, out string serverUrl)
                && AdvancedGrpcMessageLayer.Instance.ListServer(serverUrl, out objects);
        }
    }
}
