using Grpc.Core;
using System.Collections.Generic;

using KVStoreServer.Configuration;

using ProtoServerConfiguration = Common.Protos.ServerConfiguration.ServerConfigurationService;
using NamingServiceProto = Common.Protos.NamingService.NamingService;
using KVStoreServer.Replication.Simple;

namespace KVStoreServer.Grpc.Base {

    // Define delegates for method handlers

    public delegate bool LookupHandler(string serverId, out string serverUrl);
    public delegate bool LookupMasterHandler(string partitionId, out string masterUrl);
    public delegate IEnumerable<PartitionServersDto> ListPartitionsHandler();
    
    public delegate void JoinPartitionHandler(JoinPartitionArguments arguments);
    public delegate void StatusHandler();

    public delegate void UrlFailureHandler(string crashedUrl);
    public abstract class BaseGrpcMessageLayer {

        private readonly ServerConfiguration serverConfig;

        private Server server;

        public BaseGrpcMessageLayer(ServerConfiguration serverConfig) {
            this.serverConfig = serverConfig;
        }

        // Get Base Services

        public void Start() {
            server = new Server {
                Services = {
                    ProtoServerConfiguration.BindService(
                        new ConfigurationService(GetIncomingDispatcher())),
                    NamingServiceProto.BindService(
                        new NamingService(GetIncomingDispatcher()))
                },
                Ports = {
                    new ServerPort(
                        serverConfig.Host,
                        serverConfig.Port,
                        ServerCredentials.Insecure)
                }
            };
            foreach (ServerServiceDefinition definition in GetServicesDefinitions())
                server.Services.Add(definition);
            server.Start();
        }

        public void Shutdown() {
            server.ShutdownAsync().Wait();
        }

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

        protected abstract BaseIncomingDispatcher GetIncomingDispatcher();
        protected abstract BaseOutgoingDispatcher GetOutgoingDispatcher();

        /*
         * Get extra service definitions specific for each version
         */
        protected abstract IEnumerable<ServerServiceDefinition> GetServicesDefinitions();
    }
}
