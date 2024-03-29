﻿using Common.CausalConsistency;
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

        public void BindReadHandler(ReadHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindReadHandler(handler);
        }

        public void BindWriteHandler(WriteHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindWriteHandler(handler);
        }

        public void BindListServerHandler(ListServerHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindListServerHandler(handler);
        }

        public void BindBroadcastWriteHandler(BroadcastWriteDeliveryHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindBroadcastWriteDeliveryHandler(handler);
        }

        public void BindBroadcastFailureDeliveryHandler(BroadcastFailureDeliveryHandler handler) {
            AdvancedGrpcMessageLayer.Instance.BindBroadcastFailureDeliveryHandler(handler);
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
            string value,
            ImmutableVectorClock replicaTimestamp,
            string writeServerId) {

            if (TryGetServer(serverId, out string serverUrl)) {
                await AdvancedGrpcMessageLayer.Instance.BroadcastWrite(
                    serverUrl,
                    partitionId,
                    messageId,
                    key,
                    value,
                    replicaTimestamp,
                    writeServerId);
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
