using Grpc.Core;
using KVStoreServer.Broadcast;
using KVStoreServer.CausalConsistency;
using KVStoreServer.Grpc.Base;
using KVStoreServer.KVS;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedOutgoingDispatcher : BaseOutgoingDispatcher {

        public async Task BroadcastWrite(
            string serverUrl,
            MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            try {
                AdvancedReplicaCommunicationConnection connection = new AdvancedReplicaCommunicationConnection(serverUrl);
                await connection.BroadcastWrite(messageId, key, value, replicaTimestamp, DEFAULT_TIMEOUT);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }
    }
}
