using Common.CausalConsistency;
using KVStoreServer.Broadcast;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Storage.Advanced;
using System.Threading.Tasks;

namespace KVStoreServer.Naming {
    public class AdvancedNamingServiceLayer : BaseNamingServiceLayer {
 
        private AdvancedNamingServiceLayer() { }

        public static AdvancedNamingServiceLayer Instance { get; } = new AdvancedNamingServiceLayer();

        // Bind handlers for incoming messages

        public void BindWriteHandler(WriteHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindWriteHandler(handler);
        }

        public void BindListServerHandler(ListServerHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindListServerHandler(handler);
        }

        public void BindBroadcastWriteHandler(BroadcastWriteHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindBroadcastWriteHandler(handler);
        }

        public void BindBroadcastFailureHandler(BroadcastFailureHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindBroadcastFailureHandler(handler);
        }

        public void Start() {
            AdvancedGrpcMessageLayer.Instance.Start();
        }

        public void Shutdown() {
            AdvancedGrpcMessageLayer.Instance.Shutdown();
        }

        public async Task BroadcastWrite(
            string serverId,
            string partitionId,
            MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            if (TryGetServer(serverId, out string serverUrl)) {
                await AdvancedGrpcMessageLayer.Instance.BroadcastWrite(
                    serverUrl,
                    partitionId,
                    messageId,
                    key,
                    value,
                    replicaTimestamp);
            }
        }

        public async Task BroadcastFailure(
            string serverId,
            string partitionId,
            MessageId messageId,
            string failedServerId) {

            if (TryGetServer(serverId, out string serverUrl)) {
                await AdvancedGrpcMessageLayer.Instance.BroadcastFailure(
                    serverUrl,
                    partitionId,
                    messageId,
                    failedServerId);
            }
        }

        protected override BaseGrpcMessageLayer GetGrpcLayer() {
            return AdvancedGrpcMessageLayer.Instance;
        }
    }
}
