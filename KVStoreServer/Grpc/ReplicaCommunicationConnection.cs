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

        public async Task Lock(string partitionId, string objectId, long timeout) {
            await client.LockAsync(
                new LockRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId
                },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        public async Task Write(
            string partitionId, 
            string objectId, 
            string objectValue,
            long timeout) {

            await client.WriteAsync(
                new WriteRequest {
                    PartitionId = partitionId,
                    ObjectId = objectId,
                    ObjectValue = objectValue
                },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }

        public async Task Ping(long timeout) {
            await client.PingAsync(
                new PingRequest { },
                deadline: DateTime.UtcNow.AddMilliseconds(timeout));
        }
    }
}
