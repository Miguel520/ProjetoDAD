using Grpc.Core;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc {
    public class SimpleOutgoingDispatcher : BaseOutgoingDispatcher {

        public async Task Lock(
            string serverUrl,
            string partitionId,
            string objectId) {

            try {
                ReplicaCommunicationConnection connection = new ReplicaCommunicationConnection(serverUrl);
                await connection.Lock(partitionId, objectId, DEFAULT_TIMEOUT);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }

        public async Task Write(
            string serverUrl,
            string partitionId,
            string objectId,
            string objectValue) {

            try {
                ReplicaCommunicationConnection connection = new ReplicaCommunicationConnection(serverUrl);
                await connection.Write(partitionId, objectId, objectValue, DEFAULT_TIMEOUT);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }
    }
}
