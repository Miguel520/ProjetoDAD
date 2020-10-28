using Common.Protos.Replication;
using KVStoreServer.Replication;
using Grpc.Net.Client;
using System.Threading.Tasks;

using static Common.Protos.Replication.ReplicationService;
using System;

namespace KVStoreServer.Grpc {
    public class ReplicationConnection : IReplicationConnection {

        private readonly GrpcChannel channel;
        private readonly ReplicationServiceClient client;

        public ReplicationConnection(string url) {
            channel = GrpcChannel.ForAddress(url);
            client = new ReplicationServiceClient(channel);
            Console.WriteLine("Replication connection url {0}", url);
        }

        ~ReplicationConnection() {
            channel.ShutdownAsync().Wait();
        }

        public async Task Lock(string partitionName, int objectId) {
            await client.LockAsync(new LockRequest {
                PartitionName = partitionName,
                ObjectId = objectId
            });
        }

        public async Task Write(string partitionName, int objectId, string objectValue) {
            await client.WriteAsync(new WriteRequest {
                PartitionName = partitionName,
                ObjectId = objectId,
                ObjectValue = objectValue
            });
        }
    }
}
