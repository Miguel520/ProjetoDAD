using Common.Protos.ServerConfiguration;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static Common.Protos.ServerConfiguration.ServerConfigurationService;

namespace PuppetMaster.KVStoreServer {
    public class ServerConfigurationConnection {

        private readonly string target;
        private readonly GrpcChannel channel;
        private readonly ServerConfigurationServiceClient client;

        public ServerConfigurationConnection(string url) {
            target = url;
            channel = GrpcChannel.ForAddress(target);
            client = new ServerConfigurationServiceClient(channel);
            Console.WriteLine("Established connection to {0}", target);
        }

        ~ServerConfigurationConnection() {
            Console.WriteLine("Shutting down connection to {0}", target);
            channel.ShutdownAsync().Wait();
        }

        public bool JoinPartition(
            string partitionName,
            IEnumerable<Tuple<int, string>> servers,
            int master) {

            JoinPartitionRequest request =
                ServerConfigurationMessageFactory.BuildJoinPartitionRequest(
                    partitionName,
                    servers,
                    master);

            try {
                client.JoinPartition(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} when joining partition at server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
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

        public bool Freeze() {
            FreezeRequest request = new FreezeRequest { };

            try {
                client.Freeze(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine("Error: {0} with status operation at server {1}",
                    e.StatusCode, target);
                return false;
            }
        }

        public bool UnFreeze() {
            UnfreezeRequest request = new UnfreezeRequest { };

            try {
                client.Unfreeze(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine("Error: {0} with status operation at server {1}",
                    e.StatusCode, target);
                return false;
            }
        }
    }
}
