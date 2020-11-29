using Client.Grpc.Base;
using Common.Protos.AdvancedKeyValueStore;
using Grpc.Core;
using System.Collections.Immutable;

namespace Client.Grpc.Advanced {
    public class AdvancedGrpcMessageLayer : BaseGrpcMessageLayer {

        private AdvancedGrpcMessageLayer() { }

        public static AdvancedGrpcMessageLayer Instance { get; } = new AdvancedGrpcMessageLayer();

        public bool ListServer(
            string serverUrl,
            out ImmutableList<StoredObject> storedObjects) {

            storedObjects = default;
            try {
                AdvancedGrpcConnection connection = new AdvancedGrpcConnection(serverUrl);
                storedObjects = connection.ListServer();
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }
    }
}
