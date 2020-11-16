using Common.Grpc;
using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.ReplicaCommunicationService;

namespace KVStoreServer.Grpc {
    public class ReplicaCommunicationConnection {

        private readonly ChannelBase channel;
        private readonly ReplicaCommunicationServiceClient client;

        public ReplicaCommunicationConnection(string url) {
            channel = ChannelPool.Instance.ForUrl(url);
            client = new ReplicaCommunicationServiceClient(channel);
        }

        ~ReplicaCommunicationConnection() {
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

        public async Task Ping() {
            await client.PingAsync(
                new PingRequest { },
                deadline: DateTime.UtcNow.AddSeconds(30));
        }
    }
}
