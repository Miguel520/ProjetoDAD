using KVStoreServer.Replication;
using System.Collections.Generic;

namespace KVStoreServer.Grpc {

    // Define delegates for method handlers

    public delegate bool LookupHandler(string serverId, out string serverUrl);
    public delegate bool LookupMasterHandler(string partitionId, out string masterUrl);
    public delegate IEnumerable<PartitionServersDto> ListPartitionsHandler();
    
    public delegate void JoinPartitionHandler(JoinPartitionArguments arguments);
    public delegate void StatusHandler();

    public delegate void UrlFailureHandler(string crashedUrl);
    public abstract class BaseGrpcMessageLayer {

        public BaseGrpcMessageLayer() {}

        // Bind handlers for incoming messages

        public void BindLookup(LookupHandler handler) {
            GetIncomingDispatcher().BindLookupHandler(handler);
        }

        public void BindLookupMasterHandler(LookupMasterHandler handler) {
            GetIncomingDispatcher().BindLookupMasterHandler(handler);
        }

        public void BindListPartitionsHandler(ListPartitionsHandler handler) {
            GetIncomingDispatcher().BindListPartitionHandler(handler);
        }

        public void BindJoinPartitionHandler(JoinPartitionHandler handler) {
            GetIncomingDispatcher().BindJoinPartition(handler);
        }

        public void BindStatusHandler(StatusHandler handler) {
            GetIncomingDispatcher().BindStatusHandler(handler);
        }

        public void BindFailureHandler(UrlFailureHandler handler) {
            GetOutgoingDispatcher().BindFailureHandler(handler);
        }

        //public async Task BroadcastWrite(
        //    string serverUrl,
        //    MessageId messageId,
        //    string key,
        //    ImmutableTimestampedValue value,
        //    ImmutableVectorClock replicaTimestamp,
        //    long timeout) {

        //    try {
        //        ReplicaCommunicationConnection connection = new ReplicaCommunicationConnection(serverUrl);
        //        await connection.BroadcastWrite(messageId, key, value, replicaTimestamp, timeout);
        //    }
        //    catch (RpcException exception) {
        //        HandleRpcException(serverUrl, exception);
        //    }
        //}

        protected abstract BaseIncomingDispatcher GetIncomingDispatcher();
        protected abstract BaseOutgoingDispatcher GetOutgoingDispatcher();
    }
}
