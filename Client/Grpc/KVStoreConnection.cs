using Common.Exceptions;
using Common.Grpc;
using Common.Protos.KeyValueStore;
using Grpc.Core;
using System;
using System.Collections.Immutable;

using static Common.Protos.KeyValueStore.KeyValueStoreService;

namespace Client.Grpc {
    class KVStoreConnection {

        private readonly string target;
        private readonly ChannelBase channel;
        private readonly KeyValueStoreServiceClient client;

        public KVStoreConnection(string url) {
            target = url;
            channel = ChannelPool.Instance.ForUrl(url);
            client = new KeyValueStoreServiceClient(channel);
        }

        ~KVStoreConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public void Write(string partitionId, string objectId, string value) {
            WriteRequest request =
                KVStoreMessageFactory.BuildWriteRequest(
                    partitionId,
                    objectId,
                    value);
            client.Write(
                request, 
                deadline: DateTime.UtcNow.AddSeconds(60));
        }

        public void Read(string partitionId, string objectId, out string value) {
            ReadRequest request =
                KVStoreMessageFactory.BuildReadRequest(
                    partitionId,
                    objectId);
            ReadResponse response = client.Read(
                request, 
                deadline: DateTime.UtcNow.AddSeconds(60));
            value = response.ObjectValue;
        }

        public void ListServer(out ImmutableList<StoredObject> storedObjects) {
            ListRequest request = KVStoreMessageFactory.BuildListRequest();

            ListResponse response = client.List(
                request, 
                deadline: DateTime.UtcNow.AddSeconds(60));
            storedObjects = response.Objects.ToImmutableList();
        }
    }
}
