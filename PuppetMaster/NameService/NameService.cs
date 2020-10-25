using Common.Protos.NamingService;
using Grpc.Core;
using System.Threading.Tasks;
using static Common.Protos.NamingService.NamingService;

namespace PuppetMaster.NameService {
    class NamingService : NamingServiceBase {

        private readonly NameServiceDB dB;

        public NamingService(NameServiceDB dB) {
            this.dB = dB;
        }

        public override Task<LookupResponse> Lookup(LookupRequest request, ServerCallContext context) {
            int serverId = request.ServerId;
            if (dB.TryLookupServer(serverId, out string serverUrl)) {
                return Task.FromResult(new LookupResponse { ServerUrl = serverUrl });
            }
            throw new RpcException(new Status(StatusCode.NotFound, "No Such Id"));
        }
    }
}
