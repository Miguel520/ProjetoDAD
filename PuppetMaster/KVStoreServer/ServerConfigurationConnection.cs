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
        }

        ~ServerConfigurationConnection() {
            channel.ShutdownAsync().Wait();
        }

        public async Task<bool> JoinPartitionAsync(
            string partitionName,
            IEnumerable<Tuple<string, string>> servers,
            string masterId) {

            JoinPartitionRequest request =
                ServerConfigurationMessageFactory.BuildJoinPartitionRequest(
                    partitionName,
                    servers,
                    masterId);

            try {
                await client.JoinPartitionAsync(request);
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

        /*
         * Async method to crash server
         */
        public async Task CrashAsync() {
            CrashRequest request = new CrashRequest { };
            try {
                await client.CrashAsync(
                    request,
                    deadline: DateTime.UtcNow.AddSeconds(10));
            }
            catch (RpcException) {
                // Do nothing expected to fail
            }
        }

        public async Task<bool> FreezeAsync() {
            FreezeRequest request = new FreezeRequest { };

            try {
                await client.FreezeAsync(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} with freeze operation at server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }

        public async Task<bool> UnFreezeAsync() {
            UnfreezeRequest request = new UnfreezeRequest { };

            try {
                await client.UnfreezeAsync(request);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} with unfreeze operation at server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }
    }
}
