using System.Threading.Tasks;
using Client.Communications;
using Common.Protos.ClientConfiguration;
using Grpc.Core;

namespace Client.Grpc.Base {
    class ConfigurationService : ClientConfigurationService.ClientConfigurationServiceBase {

        private readonly RequestsDispatcher requestsDispatcher;
        public ConfigurationService(RequestsDispatcher dispatcher) {
            requestsDispatcher = dispatcher;
        }

        public override Task<StatusResponse> Status(
            StatusRequest request,
            ServerCallContext context) {

            requestsDispatcher.Status();

            return Task.FromResult(new StatusResponse());
        }
    }
}
