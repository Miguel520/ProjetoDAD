using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Grpc.Simple;
using KVStoreServer.Naming;

namespace KVStoreServer.Replication.Simple {

    /*
     * Layer responsible for the detection of server crashes
     */
    public class FailureDetectionLayer {

        private readonly ConcurrentBag<string> crashedUrls = new ConcurrentBag<string>();

        private FailureDetectionLayer() {
            SimpleNamingServiceLayer.Instance.BindFailureHandler(OnReplicaFailure);
        }

        public static FailureDetectionLayer Instance { get; } = new FailureDetectionLayer();

        public bool RegisterServer(
            string serverId,
            string serverUrl) {

            return SimpleNamingServiceLayer.Instance.RegisterServer(serverId, serverUrl);
        }

        public bool TryGetServer(
            string serverId,
            out string serverUrl) {

            return SimpleNamingServiceLayer.Instance.TryGetServer(serverId, out serverUrl);
        }

        // Bind handlers for incoming messages

        public void BindReadHandler(ReadHandler handler) {
            SimpleNamingServiceLayer.Instance.BindReadHandler(handler);
        }

        public void BindWriteHandler(WriteHandler handler) {
            SimpleNamingServiceLayer.Instance.BindWriteHandler(handler);
        }

        public void BindListServerHandler(ListServerHandler handler) {
            SimpleNamingServiceLayer.Instance.BindListServerHandler(handler);
        }

        public void BindLockHandler(LockHandler handler) {
            SimpleNamingServiceLayer.Instance.BindLockHandler(handler);
        }

        public void BindWriteObjectHandler(WriteObjectHandler handler) {
            SimpleNamingServiceLayer.Instance.BindWriteObjectHandler(handler);
        }

        public void BindLookupMasterHandler(LookupMasterHandler handler) {
            SimpleNamingServiceLayer.Instance.BindLookupMasterHandler(handler);
        }

        public void BindListPartitionsHandler(ListPartitionsHandler handler) {
            SimpleNamingServiceLayer.Instance.BindListPartitionsHandler(handler);
        }

        public void BindJoinPartitionHandler(JoinPartitionHandler handler) {
            SimpleNamingServiceLayer.Instance.BindJoinPartitionHandler(handler);
        }

        public void BindStatusHandler(StatusHandler handler) {
            SimpleNamingServiceLayer.Instance.BindStatusHandler(handler);
        }

        public void Start() {
            SimpleNamingServiceLayer.Instance.Start();
        }

        public void Shutdown() {
            SimpleNamingServiceLayer.Instance.Shutdown();
        }

        public async Task Lock(
            string serverId,
            string partitionId,
            string objectId) {

            if (!crashedUrls.Contains(serverId)) {
                await SimpleNamingServiceLayer.Instance.Lock(
                    serverId,
                    partitionId,
                    objectId);
            }
        }

        public async Task Write(
            string serverId,
            string partitionId,
            string objectId,
            string objectValue) {

            if (!crashedUrls.Contains(serverId)) {
                await SimpleNamingServiceLayer.Instance.Write(
                    serverId,
                    partitionId,
                    objectId,
                    objectValue);
            }
        }

        private void OnReplicaFailure(string serverId) {
            lock (crashedUrls) {
                if (!crashedUrls.Contains(serverId)) {
                    crashedUrls.Add(serverId);
                }
            }
        }
    }
}
