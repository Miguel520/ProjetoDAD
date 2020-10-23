using Common.Protos.ServerConfiguration;
using Grpc.Core;
using System;
using System.Collections.Generic;

using static Common.Protos.ServerConfiguration.ServerConfigurationService;

namespace PuppetMaster.KVStoreServer {
    public class ServerConfigurationConnection {

        private readonly string target;
        private readonly Channel channel;
        private readonly ServerConfigurationServiceClient client;

        public ServerConfigurationConnection(string url) {
            target = url;
            channel = new Channel(target, ChannelCredentials.Insecure);
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
    }
}
