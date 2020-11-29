using Common.Grpc;
using Common.Protos.AdvancedKeyValueStore;
using Grpc.Core;
using System;
using System.Collections.Immutable;
using static Common.Protos.AdvancedKeyValueStore.AdvancedKeyValueStoreService;

namespace Client.Grpc.Advanced {
    class AdvancedGrpcConnection {

        private readonly ChannelBase channel;
        private readonly AdvancedKeyValueStoreServiceClient client;

        public AdvancedGrpcConnection(string targetUrl) {
            channel = ChannelPool.Instance.ForUrl(targetUrl);
            client = new AdvancedKeyValueStoreServiceClient(channel);
        }

        ~AdvancedGrpcConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public ImmutableList<StoredObject> ListServer() {
            ListRequest request = new ListRequest { };

            ListResponse response = client.List(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
            return response.Objects.ToImmutableList();
        }
    }
}
