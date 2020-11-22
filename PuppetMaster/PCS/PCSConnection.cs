using Common.Protos.ProcessCreation;
using Common.Utils;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
            channel = GrpcChannel.ForAddress(target);
            client = new ProcessCreationServiceClient(channel);
        }

        ~PCSConnection() {
            channel.ShutdownAsync().Wait();
        }

        /*
         * Asynchronously creates a server
         */
        public async Task<bool> CreateServerAsync(
            string serverId, 
            int port, 
            int minDelay, 
            int maxDelay,
            int version) {

            CreateServerRequest request =
                PCSMessageFactory.BuildCreateServerRequest(
                    serverId, 
                    host,
                    port, 
                    minDelay, 
                    maxDelay, 
                    version);

            try {
                await client.CreateServerAsync(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error: {1} when creating server at PCS {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode,
                    target);
                return false;
            }
        }

        /*
         * Asynchronously creates a client
         */
        public async Task<bool> CreateClientAsync(
            string username, 
            int port, 
            string scriptFile, 
            IEnumerable<string> nameServersUrls,
            int version) {

            CreateClientRequest request =
                PCSMessageFactory.BuildCreateClientRequest(
                    username, 
                    host, 
                    port, 
                    scriptFile, 
                    nameServersUrls,
                    version);
            try {
                await client.CreateClientAsync(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error: {1} when creating client at PCS {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode,
                    target);
                return false;
            }
        }
    }
}
