using Common.Exceptions;
using Common.Grpc;
using Common.Protos.NamingService;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Immutable;

using static Common.Protos.NamingService.NamingService;

namespace Client.Grpc {

    class NamingServiceConnection {

        private readonly ChannelBase channel;
        private readonly NamingServiceClient client;
        private readonly string target;

        public NamingServiceConnection(string url) {
            target = url;
            channel = ChannelPool.Instance.ForUrl(target);
            client = new NamingServiceClient(channel);
        }

        ~NamingServiceConnection() {
            ChannelPool.Instance.ClearChannel(channel);
        }

        public void Lookup(string serverId, out string serverUrl) {
            LookupRequest request =
                NamingServiceMessageFactory.BuildLookupRequest(serverId);

            LookupResponse response = client.Lookup(
                request, 
                deadline: DateTime.UtcNow.AddSeconds(30));
            serverUrl = response.ServerUrl;
        }

        public void LookupMaster(string partitionId, out string masterUrl) {
            LookupMasterRequest request =
                NamingServiceMessageFactory.BuildLookupMasterRequest(partitionId);

            LookupMasterResponse response = client.LookupMaster(
                request,
                deadline: DateTime.UtcNow.AddSeconds(30));
            masterUrl = response.MasterUrl;
        }

        public void ListPartitions(
            out ImmutableDictionary<string, ImmutableHashSet<string>> partitions) {

            ListPartitionsResponse response = client.ListPartitions(
                new ListPartitionsRequest { },
                deadline: DateTime.UtcNow.AddSeconds(30));

            ImmutableDictionary<string, ImmutableHashSet<string>>.Builder builder =
                ImmutableDictionary.CreateBuilder<string, ImmutableHashSet<string>>();

            foreach (Partition partition in response.Partitions) {
                string partitionId = partition.PartitionId;
                ImmutableHashSet<string> serverIds = ImmutableHashSet.CreateRange(
                    partition.ServerIds);

                Console.WriteLine(
                    "[{0}] Found partition {1}", 
                    DateTime.Now.ToString("HH:mm:ss"), 
                    partitionId);

                builder.Add(partitionId, serverIds);
            }
            partitions = builder.ToImmutable();
        }
    }
}
