using Common.Protos.ClientConfiguration;
using Grpc.Core;
using Grpc.Net.Client;
using System;

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

        public bool Status() {
            StatusRequest request = new StatusRequest { };

            try {
                client.Status(request);
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
