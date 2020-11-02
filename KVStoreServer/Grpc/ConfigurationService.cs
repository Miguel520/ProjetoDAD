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

            Console.WriteLine($"Received Partition {request.PartitionName}");

            return new JoinPartitionResponse();
        }

        public override Task<StatusResponse> Status(
            StatusRequest request,
            ServerCallContext context) {

            dispatcher.Status();

            return Task.FromResult(new StatusResponse());
        }

        public override Task<CrashResponse> Crash(CrashRequest request, ServerCallContext context) {
            dispatcher.Crash();
            // Will never execute since crash terminates the program execution
            return Task.FromResult(new CrashResponse { });
        }

        public override Task<FreezeResponse> Freeze(FreezeRequest request, ServerCallContext context) {
            dispatcher.Freeze();
            return Task.FromResult(new FreezeResponse { });
        }

        public override Task<UnfreezeResponse> Unfreeze(UnfreezeRequest request, ServerCallContext context) {
            dispatcher.Unfreeze();
            return Task.FromResult(new UnfreezeResponse { });
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
