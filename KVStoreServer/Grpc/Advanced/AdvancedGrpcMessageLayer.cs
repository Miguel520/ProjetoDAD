using Common.Utils;
using Grpc.Core;
using KVStoreServer.Broadcast;
using KVStoreServer.CausalConsistency;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Base;
using KVStoreServer.KVS;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedGrpcMessageLayer : BaseGrpcMessageLayer {

        private static AdvancedGrpcMessageLayer instance = null;
        private static readonly object instanceLock = new object();


        private readonly AdvancedIncomingDispatcher incomingDispatcher;
        private readonly AdvancedOutgoingDispatcher outgoingDispatcher;

        private AdvancedGrpcMessageLayer(ServerConfiguration serverConfig)
            : base(serverConfig) {

            incomingDispatcher = new AdvancedIncomingDispatcher(serverConfig);
            outgoingDispatcher = new AdvancedOutgoingDispatcher();
        }

        public static AdvancedGrpcMessageLayer Instance {
            get {
                lock(instanceLock) {
                    Conditions.AssertState(instance != null);
                    return instance;
                }
            }
        }

        public static void SetContext(ServerConfiguration serverConfig) {
            lock(instanceLock) {
                Conditions.AssertState(instance == null);
                instance = new AdvancedGrpcMessageLayer(serverConfig);
            }
        }

        public async Task BroadcastWrite(
            string serverUrl,
            MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            await outgoingDispatcher.BroadcastWrite(
                serverUrl,
                messageId,
                key,
                value,
                replicaTimestamp);
        }

        protected override BaseIncomingDispatcher GetIncomingDispatcher() => incomingDispatcher;

        protected override BaseOutgoingDispatcher GetOutgoingDispatcher() => outgoingDispatcher;

        protected override IEnumerable<ServerServiceDefinition> GetServicesDefinitions() {
            return new ServerServiceDefinition[] { };
        }
    }
}
