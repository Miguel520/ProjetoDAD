using Common.Protos.ClientConfiguration;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Threading.Tasks;
using static Common.Protos.ClientConfiguration.ClientConfigurationService;

namespace PuppetMaster.Client {
    public class ClientConfigurationConnection {

        private readonly string target;
        private readonly GrpcChannel channel;
        private readonly ClientConfigurationServiceClient client;

        public ClientConfigurationConnection(string url) {
            target = url;
            channel = GrpcChannel.ForAddress(target);
            client = new ClientConfigurationServiceClient(channel);
            Console.WriteLine("Established connection to {0}", target);
        }

        public async Task<bool> StatusAsync() {
            StatusRequest request = new StatusRequest { };

            try {
                await client.StatusAsync(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} with status operation at server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }
    }
}
