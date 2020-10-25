using Common.Protos.ProcessCreation;
using Common.Utils;
using Grpc.Core;
using Grpc.Net.Client;
using System;

using static Common.Protos.ProcessCreation.ProcessCreationService;

namespace PuppetMaster.PCS {

    /*
     * Handler class responsible for a single PCS instance
     */
    public class PCSConnection {

        private const int PCS_PORT = 10000;

        private readonly string host;
        private readonly string target;
        private readonly GrpcChannel channel;
        private readonly ProcessCreationServiceClient client;

        public PCSConnection(string PCSHost) {
            host = PCSHost;
            target = HttpURLs.FromHostAndPort(PCSHost, PCS_PORT);
           
            //Configuring HTTP for client connections
            //TODO remove this from here and put it in a file with global context
            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);
            channel = GrpcChannel.ForAddress(target);
            client = new ProcessCreationServiceClient(channel);
            Console.WriteLine("Established connection to {0}", target);
        }

        ~PCSConnection() {
            Console.WriteLine("Shutting down connection to {0}", target);
            channel.ShutdownAsync().Wait();
        }

        public bool CreateServer(int serverId, int port, int minDelay, int maxDelay) {
            CreateServerRequest request =
                PCSMessageFactory.BuildCreateServerRequest(serverId, host, port, minDelay, maxDelay);
            try {
                client.CreateServer(request);
                Console.WriteLine("Server started at {0}:{1}", host, port);
                return true;
            } catch (RpcException e) {
                Console.WriteLine(e.ToString());
                Console.WriteLine("Error: {0} when creating server at PCS {1}", e.StatusCode, target);
                return false;
            }
        }

        public bool CreateClient(string username, int port, string scriptFile) {
            CreateClientRequest request =
                PCSMessageFactory.BuildCreateClientRequest(username, host, port, scriptFile);
            try {
                client.CreateClient(request);
                Console.WriteLine("Client started at {0}:{1}", host, port);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine("Error: {0} when creating client at PCS {1}", e.StatusCode, target);
                return false;
            }
        }
    }
}
