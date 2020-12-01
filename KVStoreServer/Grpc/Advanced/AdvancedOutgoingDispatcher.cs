using Common.CausalConsistency;
using Grpc.Core;
using KVStoreServer.Broadcast;
using KVStoreServer.Grpc.Base;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedOutgoingDispatcher : BaseOutgoingDispatcher {

        public async Task BroadcastWrite(
            string serverUrl,
            string partitionId,
            MessageId messageId,
            string key,
            string value,
            ImmutableVectorClock replicaTimestamp,
            string writeServerId) {

            try {
                AdvancedReplicaCommunicationConnection connection = new AdvancedReplicaCommunicationConnection(serverUrl);
                await connection.BroadcastWrite(partitionId, messageId, key, value, replicaTimestamp, writeServerId, DEFAULT_TIMEOUT);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }

        public async Task BroadcastFailure(
            string serverUrl,
            string partitionId,
            Broadcast.MessageId messageId,
            string failedServerId) {

            try {
                AdvancedReplicaCommunicationConnection connection = new AdvancedReplicaCommunicationConnection(serverUrl);
                await connection.BroadcastFailure(partitionId, messageId, failedServerId, DEFAULT_TIMEOUT);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }
    }
}
