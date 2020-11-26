using KVStoreServer.Grpc.Base;
using KVStoreServer.Grpc.Simple;
using System.Threading.Tasks;

namespace KVStoreServer.Naming {

    /*
     * Layer for translation between ids and urls
     * Should not store any other logic
     * Only retransmits replica failure event with id and not url
     */
    public class SimpleNamingServiceLayer : BaseNamingServiceLayer {

        private SimpleNamingServiceLayer() : base() {
        }

        public static SimpleNamingServiceLayer Instance { get; } = new SimpleNamingServiceLayer();

        // Bind handlers for incoming messages

        public void BindReadHandler(ReadHandler handler) {
            SimpleGrpcMessageLayer.Instance.BindReadHandler(handler);
        }

        public void BindWriteHandler(WriteHandler handler) {
            SimpleGrpcMessageLayer.Instance.BindWriteHandler(handler);
        }

        public void BindListServerHandler(ListServerHandler handler) {
            SimpleGrpcMessageLayer.Instance.BindListServerHandler(handler);
        }

        public void BindLockHandler(LockHandler handler) {
            SimpleGrpcMessageLayer.Instance.BindLockHandler(handler);
        }

        public void BindWriteObjectHandler(WriteObjectHandler handler) {
            SimpleGrpcMessageLayer.Instance.BindWriteObjectHandler(handler);
        }

        public void Start() {
            SimpleGrpcMessageLayer.Instance.Start();
        }

        public void Shutdown() {
            SimpleGrpcMessageLayer.Instance.Shutdown();
        }

        public async Task Lock(
            string serverId,
            string partitionId,
            string objectId) {

            if (TryGetServer(serverId, out string serverUrl)) {
                await SimpleGrpcMessageLayer.Instance.Lock(
                    serverUrl,
                    partitionId,
                    objectId);
            }
        }

        public async Task Write(
            string serverId,
            string partitionId,
            string objectId,
            string objectValue) {

            if (TryGetServer(serverId, out string serverUrl)) {
                await SimpleGrpcMessageLayer.Instance.Write(
                    serverUrl,
                    partitionId,
                    objectId,
                    objectValue);
            }
        }

        protected override BaseGrpcMessageLayer GetGrpcLayer() {
            return SimpleGrpcMessageLayer.Instance;
        }
    }
}
