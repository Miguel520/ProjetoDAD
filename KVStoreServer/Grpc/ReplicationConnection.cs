using Common.Grpc;
using Common.Protos.Replication;
using Grpc.Core;
using System;
using System.Threading.Tasks;

using static Common.Protos.Replication.ReplicationService;

namespace KVStoreServer.Grpc {
    public class ReplicationConnection {

        private readonly ChannelBase channel;
        private readonly ReplicationServiceClient client;

        public ReplicationConnection(string url) {
            channel = ChannelPool.Instance.ForUrl(url);
            client = new ReplicationServiceClient(channel);
        }

        ~ReplicationConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public async Task Lock(string partitionId, string objectId) {
            await client.LockAsync(
                new LockRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId
                },
                deadline: DateTime.UtcNow.AddSeconds(30));
        }

        public async Task Write(string partitionId, string objectId, string objectValue) {
            await client.WriteAsync(
                new WriteRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId,
                    ObjectValue = objectValue
                },
                deadline: DateTime.UtcNow.AddSeconds(30));
        }
    }
}
