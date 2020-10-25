using Common.Protos.ServerConfiguration;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using KVStoreServer.Communications;

using Server = Common.Protos.ServerConfiguration.Server;

namespace KVStoreServer.Grpc {
    public class ConfigurationService : ServerConfigurationService.ServerConfigurationServiceBase {

        private readonly RequestsDispatcher dispatcher;

        public ConfigurationService(RequestsDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<JoinPartitionResponse> JoinPartition(
            JoinPartitionRequest request,
            ServerCallContext context) {
    
            await dispatcher.JoinPartition(ParseJoinPartition(request));

            return new JoinPartitionResponse();
        }

        public override async Task<StatusResponse> Status(
            StatusRequest request,
            ServerCallContext context) {

            Console.WriteLine("Received Status Request");

            await dispatcher.Status();

            return new StatusResponse();
        }

        public override Task<CrashResponse> Crash(CrashRequest request, ServerCallContext context) {
            return base.Crash(request, context);
        }

        public override Task<FreezeResponse> Freeze(FreezeRequest request, ServerCallContext context) {
            return base.Freeze(request, context);
        }

        public override Task<UnfreezeResponse> Unfreeze(UnfreezeRequest request, ServerCallContext context) {
            return base.Unfreeze(request, context);
        }

        private JoinPartitionArguments ParseJoinPartition(
            JoinPartitionRequest request) {

            string partitionName = request.PartitionName;
            int masterId = request.MasterId;

            List<Tuple<int, string>> servers = new List<Tuple<int, string>>();
            foreach (Server server in request.Servers) {
                servers.Add(new Tuple<int, string>(server.Id, server.Url));
            }
            
            return new JoinPartitionArguments {
                Name = partitionName,
                Members = servers,
                MasterId = masterId
            };
        }
    }
}
