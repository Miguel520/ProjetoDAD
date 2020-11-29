using Common.Grpc;
using Common.Protos.AdvancedKeyValueStore;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using static Common.Protos.AdvancedKeyValueStore.AdvancedKeyValueStoreService;

using CausalConsistency = Common.CausalConsistency;

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

        public CausalConsistency.ImmutableVectorClock Write(
            string partitionId,
            string objectId, 
            string value,
            CausalConsistency.ImmutableVectorClock timestamp) {

            WriteRequest request = new WriteRequest {
                PartitionId = partitionId,
                ObjectId = objectId,
                ObjectValue = value,
                Timestamp = BuildGrpcClock(timestamp)
            };

            WriteResponse response = client.Write(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
            return BuildVectorClock(response.Timestamp);
        }

        public ImmutableList<StoredObject> ListServer() {
            ListRequest request = new ListRequest { };

            ListResponse response = client.List(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
            return response.Objects.ToImmutableList();
        }

        private VectorClock BuildGrpcClock(CausalConsistency.ImmutableVectorClock vectorClock) {
            (IList<string> serverIds, IList<int> clocks) =
                CausalConsistency.VectorClocks.ToIdsAndClocksList(vectorClock);
            return new VectorClock {
                ServerIds = { serverIds },
                ServerClocks = { clocks }
            };
        }

        private CausalConsistency.ImmutableVectorClock BuildVectorClock(VectorClock vectorClock) {
            return CausalConsistency.VectorClocks.FromIdsAndClocksList(
                vectorClock.ServerIds,
                vectorClock.ServerClocks);
        }
    }
}
