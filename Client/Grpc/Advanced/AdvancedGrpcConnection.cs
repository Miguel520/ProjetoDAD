﻿using Common.Grpc;
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

        public CausalConsistency.ImmutableVectorClock Read(
            string partitionId,
            string objectId,
            out string value,
            CausalConsistency.ImmutableVectorClock timestamp) {

            ReadRequest request = new ReadRequest {
                PartitionId = partitionId,
                ObjectId = objectId,
                Timestamp = BuildGrpcClock(timestamp)
            };

            ReadResponse response = client.Read(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));

            value = !response.Missing ? response.ObjectValue : null;
            
            return BuildVectorClock(response.Timestamp);
        }

        public (ImmutableList<StoredObject>, ImmutableList<PartitionTimestamp>) ListServer() {
            ListRequest request = new ListRequest { };

            ListResponse response = client.List(
                request,
                deadline: DateTime.UtcNow.AddSeconds(60));
            return (response.Objects.ToImmutableList(), response.PartitionTimestamps.ToImmutableList());
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
