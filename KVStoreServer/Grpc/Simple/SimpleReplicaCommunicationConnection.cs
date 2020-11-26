using Common.Grpc;
using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.SimpleReplicaCommunicationService;

namespace KVStoreServer.Grpc.Simple {
    public class SimpleReplicaCommunicationConnection {

        private readonly ChannelBase channel;
        private readonly SimpleReplicaCommunicationServiceClient client;

        public SimpleReplicaCommunicationConnection(string url) {
            channel = ChannelPool.Instance.ForUrl(url);
            client = new SimpleReplicaCommunicationServiceClient(channel);
        }

        ~SimpleReplicaCommunicationConnection() {
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
    }
}
