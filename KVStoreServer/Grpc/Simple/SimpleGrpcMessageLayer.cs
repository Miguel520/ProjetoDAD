using Common.Utils;
using Grpc.Core;
using KVStoreServer.Configuration;
using KVStoreServer.Storage;
using System.Collections.Generic;
using System.Threading.Tasks;

using ProtoServerConfiguration = Common.Protos.ServerConfiguration.ServerConfigurationService;
using ProtoKeyValueStore = Common.Protos.KeyValueStore.KeyValueStoreService;
using NamingServiceProto = Common.Protos.NamingService.NamingService;
using ReplicationServiceProto = Common.Protos.ReplicaCommunication.ReplicaCommunicationService;

namespace KVStoreServer.Grpc {

    public delegate string ReadHandler(ReadArguments arguments);
    public delegate void WriteHandler(WriteArguments arguments);
    public delegate IEnumerable<StoredValueDto> ListServerHandler();

    public delegate void LockHandler(LockArguments arguments);
    public delegate void WriteObjectHandler(WriteObjectArguments arguments);
    public sealed class SimpleGrpcMessageLayer : BaseGrpcMessageLayer {

        private static SimpleGrpcMessageLayer instance = null;
        private static readonly object instanceLock = new object();

        private readonly ServerConfiguration serverConfig;

        private readonly SimpleIncomingDispatcher incomingDispatcher;
        private readonly SimpleOutgoingDispatcher outgoingDispatcher;

        private Server server;

        private SimpleGrpcMessageLayer(ServerConfiguration serverConfig) : base() {
            this.serverConfig = serverConfig;
            incomingDispatcher = new SimpleIncomingDispatcher(serverConfig);
            outgoingDispatcher = new SimpleOutgoingDispatcher();
        }

        public static SimpleGrpcMessageLayer Instance {
            get {
                lock (instanceLock) {
                    Conditions.AssertState(instance != null);
                    return instance;
                }
            }
        }

        public static void SetContext(ServerConfiguration serverConfig) {
            lock (instanceLock) {
                Conditions.AssertState(instance == null);
                instance = new SimpleGrpcMessageLayer(serverConfig);
            }
        }

        public void Start() {
            server = new Server {
                Services = {
                    ProtoServerConfiguration.BindService(
                        new ConfigurationService(incomingDispatcher)),
                    ProtoKeyValueStore.BindService(new StorageService(incomingDispatcher)),
                    NamingServiceProto.BindService(new NamingService(incomingDispatcher)),
                    ReplicationServiceProto.BindService(new ReplicaCommunicationServiceImpl(incomingDispatcher))

                },
                Ports = {
                    new ServerPort(
                        serverConfig.Host,
                        serverConfig.Port,
                        ServerCredentials.Insecure)
                }
            };
            server.Start();
        }

        public void Shutdown() {
            server.ShutdownAsync().Wait();
        }

        public void BindReadHandler(ReadHandler handler) {
            incomingDispatcher.BindReadHandler(handler);
        }

        public void BindWriteHandler(WriteHandler handler) {
            incomingDispatcher.BindWriteHandler(handler);
        }

        public void BindListServerHandler(ListServerHandler handler) {
            incomingDispatcher.BindListServerHandler(handler);
        }

        public void BindLockHandler(LockHandler handler) {
            incomingDispatcher.BindLockHandler(handler);
        }

        public void BindWriteObjectHandler(WriteObjectHandler handler) {
            incomingDispatcher.BindWriteObjectHandler(handler);
        }

        public async Task Lock(
            string serverUrl,
            string partitionId,
            string objectId) {

            await outgoingDispatcher.Lock(serverUrl, partitionId, objectId);
        }

        public async Task Write(
            string serverUrl,
            string partitionId,
            string objectId,
            string objectValue) {

            await outgoingDispatcher.Write(serverUrl, partitionId, objectId, objectValue);
        }

        protected override BaseIncomingDispatcher GetIncomingDispatcher() {
            return incomingDispatcher;
        }

        protected override BaseOutgoingDispatcher GetOutgoingDispatcher() {
            return outgoingDispatcher;
        }
    }
}
