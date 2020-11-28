using Common.Grpc;
using Common.Protos.SimpleKeyValueStore;
using Grpc.Core;
using System;
using System.Collections.Immutable;

using static Common.Protos.SimpleKeyValueStore.SimpleKeyValueStoreService;

namespace Client.Grpc.Simple {
    class SimpleKVSConnection {

        private readonly string target;
        private readonly ChannelBase channel;
        private readonly SimpleKeyValueStoreServiceClient client;

        public SimpleKVSConnection(string url) {
            target = url;
            channel = ChannelPool.Instance.ForUrl(url);
            client = new SimpleKeyValueStoreServiceClient(channel);
        }

        ~SimpleKVSConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public void Write(string partitionId, string objectId, string value) {
            WriteRequest request =
                SimpleKVSMessageFactory.BuildWriteRequest(
                    partitionId,
                    objectId,
                    value);
            client.Write(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
        }

        public void Read(string partitionId, string objectId, out string value) {
            ReadRequest request =
                SimpleKVSMessageFactory.BuildReadRequest(
                    partitionId,
                    objectId);
            ReadResponse response = client.Read(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
            value = response.ObjectValue;
        }

        public void ListServer(out ImmutableList<StoredObject> storedObjects) {
            ListRequest request = SimpleKVSMessageFactory.BuildListRequest();

            ListResponse response = client.List(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
            storedObjects = response.Objects.ToImmutableList();
        }
    }
}
