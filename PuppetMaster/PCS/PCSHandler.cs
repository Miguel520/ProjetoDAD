using Grpc.Core;
using System;

using static ProcessCreationService;

namespace PuppetMaster.PCS {

    /*
     * Handler class responsible for a single PCS instance
     */
    public class PCSHandler {

        private readonly Channel channel;
        private readonly ProcessCreationServiceClient client;

        public PCSHandler(string target) {
            channel = new Channel(target, ChannelCredentials.Insecure);
            client = new ProcessCreationServiceClient(channel);
            Console.WriteLine("Established connection to {0}", target);
        }

        public void SetReplicationFactor(int replicationFactor) {
            ReplicationFactorRequest request =
                PCSMessageFactory.BuildReplicationFactorRequest(replicationFactor);
            client.SetReplicationFactor(request);
        }

        public void CreateServer(int serverId, string url, int minDelay, int maxDelay) {
            CreateServerRequest request =
                PCSMessageFactory.BuildCreateServerRequest(serverId, url, minDelay, maxDelay);
            client.CreateServer(request);
        }

        public void CreatePartition(int partitionSize, string partitionName, int[] serverIds) {
            CreatePartitionRequest request =
                PCSMessageFactory.BuildCreatePartitionRequest(partitionSize, partitionName, serverIds);
            client.CreatePartition(request);
        }

        public void CreateClient(string username, string url, string scriptFile) {
            CreateClientRequest request =
                PCSMessageFactory.BuildCreateClientRequest(username, url, scriptFile);
            client.CreateClient(request);
        }

        public void DisplayStatus() {
            client.DisplayStatus(PCSMessageFactory.BuildStatusRequest());
        }
    }
}
