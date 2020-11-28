using Grpc.Core;

using Client.Grpc.Base;
using System.Collections.Immutable;
using Common.Protos.SimpleKeyValueStore;

namespace Client.Grpc.Simple {
    public class SimpleGrpcMessageLayer : BaseGrpcMessageLayer {

        private SimpleGrpcMessageLayer() { }
        public static SimpleGrpcMessageLayer Instance { get; } = new SimpleGrpcMessageLayer();
        public bool Read(
            string serverUrl,
            string partitionId,
            string objectId,
            out string objectValue) {

            objectValue = default;
            try {
                SimpleKVSConnection connection = new SimpleKVSConnection(serverUrl);
                connection.Read(partitionId, objectId, out objectValue);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }

        public bool Write(
            string serverUrl,
            string partitionId,
            string objectId,
            string objectValue) {

            try {
                SimpleKVSConnection connection = new SimpleKVSConnection(serverUrl);
                connection.Write(partitionId, objectId, objectValue);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }

        public bool ListServer(
            string serverUrl,
            out ImmutableList<StoredObject> storedObjects) {

            storedObjects = default;
            try {
                SimpleKVSConnection connection = new SimpleKVSConnection(serverUrl);
                connection.ListServer(out storedObjects);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }
    }
}
