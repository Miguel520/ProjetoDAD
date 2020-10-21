using Common.Protos.ServerConfiguration;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Server = Common.Protos.ServerConfiguration.Server;

namespace KVStoreServer.Grpc {
    public class ConfigurationService : ServerConfigurationService.ServerConfigurationServiceBase {

        private readonly RequestsDispatcher dispatcher;

        public ConfigurationService(RequestsDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override Task<JoinPartitionResponse> JoinPartition(JoinPartitionRequest request, ServerCallContext context) {
            (string partitionName, IEnumerable<Tuple<int, string>> servers) = ParseJoinPartition(request);
            dispatcher.JoinPartition(partitionName, servers);
            return Task.FromResult(new JoinPartitionResponse());
        }

        public override Task<StatusResponse> Status(StatusRequest request, ServerCallContext context) {
            return base.Status(request, context);
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

        private (string, IEnumerable<Tuple<int, string>>) ParseJoinPartition(
            JoinPartitionRequest request) {
            string partitionName = request.PartitionName;
            List<Tuple<int, string>> servers = new List<Tuple<int, string>>();
            foreach (Server server in request.Servers) {
                servers.Add(new Tuple<int, string>(server.Id, server.Url));
            }
            return (partitionName, servers);
        }
    }
}
