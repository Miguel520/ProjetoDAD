using Common.CausalConsistency;
using Common.Utils;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Naming;
using KVStoreServer.Storage.Advanced;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KVStoreServer.Broadcast {

    public delegate void WriteMessageHandler(BroadcastWriteMessage message);
    public delegate void FailureMessageHandler(BroadcastFailureMessage serverId);
    public class ReliableBroadcastLayer {

        private WriteMessageHandler writeMessageHandler = null;
        private FailureMessageHandler failureMessageHandler = null;

        private string selfId;
        private readonly ConcurrentDictionary<string, PartitionReliableBroadcastHandler> partitionHandlers =
            new ConcurrentDictionary<string, PartitionReliableBroadcastHandler>();

        private ReliableBroadcastLayer() {
            AdvancedNamingServiceLayer.Instance.BindBroadcastWriteHandler(OnBroadcastWriteDeliver);
            AdvancedNamingServiceLayer.Instance.BindFailureDetectionHandler(OnFailureDetection);
            AdvancedNamingServiceLayer.Instance.BindBroadcastFailureDeliveryHandler(OnBroadcastFailureDeliver);
        }

        public static ReliableBroadcastLayer Instance { get; } = new ReliableBroadcastLayer(); 

        public void BindReadHandler(ReadHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindReadHandler(handler);
        }

        public void BindWriteHandler(WriteHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindWriteHandler(handler);
        }

        public void BindListServerHandler(ListServerHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindListServerHandler(handler);
        }

        public void BindWriteMessageHandler(WriteMessageHandler handler) {
            writeMessageHandler = handler;
        }

        public void BindFailureMessageHandler(FailureMessageHandler handler) {
            failureMessageHandler = handler;
        }

        public void BindJoinPartitionHandler(JoinPartitionHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindJoinPartitionHandler(handler);
        }

        public void BindLookupMasterHandler(LookupMasterHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindLookupMasterHandler(handler);
        }

        public void BindListPartitionsHandler(ListPartitionsHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindListPartitionsHandler(handler);
        }

        public void BindStatusHandler(StatusHandler handler) {
            AdvancedNamingServiceLayer.Instance.BindStatusHandler(handler);
        }

        public void Start() {
            AdvancedNamingServiceLayer.Instance.Start();
        }

        public void Shutdown() {
            AdvancedNamingServiceLayer.Instance.Shutdown();
        }

        public void RegisterSelfId(string selfId) {
            this.selfId = selfId;
        }

        public bool RegisterServer(string serverId, string serverUrl) {
            return AdvancedNamingServiceLayer.Instance.RegisterServer(serverId, serverUrl);
        }

        public bool TryGetServer(string serverId, out string serverUrl) {
            return AdvancedNamingServiceLayer.Instance.TryGetServer(serverId, out serverUrl);
        }

        public bool RegisterPartition(string partitionId, ImmutableHashSet<string> serverIds) {
            Conditions.AssertArgument(serverIds.Contains(selfId));

            return partitionHandlers.TryAdd(
                partitionId, 
                new PartitionReliableBroadcastHandler(
                    selfId, 
                    partitionId, 
                    serverIds, 
                    writeMessageHandler,
                    failureMessageHandler));
        }

        public void BroadcastWrite(
            string partitionId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            if (partitionHandlers.TryGetValue(partitionId, out PartitionReliableBroadcastHandler handler)) {
                handler.BroadcastWrite(
                    key,
                    value,
                    replicaTimestamp);
            }
        }

        private void OnBroadcastWriteDeliver(BroadcastWriteArguments arguments) {
            if (partitionHandlers.TryGetValue(
                arguments.PartitionId, 
                out PartitionReliableBroadcastHandler handler)) {
                
                handler.OnBroadcastWriteDeliver(arguments);
            }
        }

        private void OnFailureDetection(string serverId) {
            foreach (PartitionReliableBroadcastHandler handler in partitionHandlers.Values) {
                handler.BroadcastFailure(serverId);
            }
        }

        private void OnBroadcastFailureDeliver(BroadcastFailureArguments arguments) {

            if (partitionHandlers.TryGetValue(
                arguments.PartitionId,
                out PartitionReliableBroadcastHandler handler)) {

                handler.OnBroadcastFailureDeliver(arguments);
            }
        }
    }
}
