using Common.Exceptions;
using Common.Protos.Replication;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Threading.Tasks;

using KVStoreServer.Replication;

using static Common.Protos.Replication.ReplicationService;

namespace KVStoreServer.Grpc {
    public class ReplicationConnection : IReplicationConnection {

        private readonly GrpcChannel channel;
        private readonly ReplicationServiceClient client;

        public ReplicationConnection(string url) : base(url) {
            channel = GrpcChannel.ForAddress(url);
            client = new ReplicationServiceClient(channel);
            Console.WriteLine("Replication connection url {0}", url);
        }

        ~ReplicationConnection() {
            channel.ShutdownAsync().Wait();
        }

        public async override Task Lock(string partitionId, string objectId) {
            try {
                await client.LockAsync(new LockRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId
                },
                deadline: DateTime.UtcNow.AddSeconds(30));
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}]: Replica at {1} failed lock with error {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    Url,
                    e.StatusCode.ToString());
                throw new ReplicaFailureException(Url);
            }
        }

        public async override Task Write(string partitionId, string objectId, string objectValue) {
            try {
                await client.WriteAsync(new WriteRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId,
                    ObjectValue = objectValue
                },
                deadline: DateTime.UtcNow.AddSeconds(30));
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}]: Replica at {1} failed write with error {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    Url,
                    e.StatusCode.ToString());
                throw new ReplicaFailureException(Url);
            }
        }
    }
}
