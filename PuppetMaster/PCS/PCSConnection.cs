using Common.Protos.ProcessCreation;
using Common.Utils;
using Grpc.Core;
using System;

using static Common.Protos.ProcessCreation.ProcessCreationService;

namespace PuppetMaster.PCS {

    /*
     * Handler class responsible for a single PCS instance
     */
    public class PCSConnection {

        private const int PCS_PORT = 10000;

        private readonly string target;
        private readonly Channel channel;
        private readonly ProcessCreationServiceClient client;

        public PCSConnection(string PCSHost) {
            target = HttpURLs.FromHostAndPort(PCSHost, PCS_PORT);
            channel = new Channel(target, ChannelCredentials.Insecure);
            client = new ProcessCreationServiceClient(channel);
            Console.WriteLine("Established connection to {0}", target);
        }

        ~PCSConnection() {
            Console.WriteLine("Shutting down connection to {0}", target);
            channel.ShutdownAsync().Wait();
        }

        public bool CreateServer(int serverId, int minDelay, int maxDelay) {
            CreateServerRequest request =
                PCSMessageFactory.BuildCreateServerRequest(serverId, target, minDelay, maxDelay);
            try {
                client.CreateServer(request);
                return true;
            } catch (RpcException e) {
                Console.WriteLine("Error: {0} when creating server at PCS {1}", e.StatusCode, target);
                return false;
            }
        }

        public bool CreateClient(string username, string scriptFile) {
            CreateClientRequest request =
                PCSMessageFactory.BuildCreateClientRequest(username, target, scriptFile);
            try {
                client.CreateClient(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine("Error: {0} when creating client at PCS {1}", e.StatusCode, target);
                return false;
            }
        }
    }
}
