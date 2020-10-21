using System.Threading.Tasks;
using Common.Protos.ServerConfiguration;
using Grpc.Core;

namespace KVStoreServer.Grpc {
    public class ConfigurationService : ServerConfigurationService.ServerConfigurationServiceBase {
        public ConfigurationService() {
        }

        public override Task<JoinPartitionResponse> JoinPartition(JoinPartitionRequest request, ServerCallContext context) {
            return base.JoinPartition(request, context);
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
    }
}
