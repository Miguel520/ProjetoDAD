using Grpc.Core;
using KVStoreServer.Grpc.Base;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Simple {
    public class SimpleOutgoingDispatcher : BaseOutgoingDispatcher {

        public async Task Lock(
            string serverUrl,
            string partitionId,
            string objectId) {

            try {
                SimpleReplicaCommunicationConnection connection = new SimpleReplicaCommunicationConnection(serverUrl);
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
                SimpleReplicaCommunicationConnection connection = new SimpleReplicaCommunicationConnection(serverUrl);
                await connection.Write(partitionId, objectId, objectValue, DEFAULT_TIMEOUT);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }
    }
}
