using Common.Protos.NamingService;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Immutable;

using static Common.Protos.NamingService.NamingService;

namespace Client.Naming {

    class NamingServiceConnection {

        private readonly GrpcChannel channel;
        private readonly NamingServiceClient client;
        private readonly string target;

        public NamingServiceConnection(string url) {
            target = url;
            channel = GrpcChannel.ForAddress(target);
            client = new NamingServiceClient(channel);
        }

        ~NamingServiceConnection() {
            channel.ShutdownAsync().Wait();
        }

        public bool Lookup(string serverId, out string serverUrl) {
            serverUrl = null;
            LookupRequest request =
                NamingServiceMessageFactory.BuildLookupRequest(serverId);
            try {
                LookupResponse response = client.Lookup(request);
                serverUrl = response.ServerUrl;
                return true;
            } catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} at lookup for server id {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode, 
                    serverId);
                return false;
            }
        }

        public bool LookupMaster(string partitionId, out string masterUrl) {
            masterUrl = null;
            LookupMasterRequest request =
                NamingServiceMessageFactory.BuildLookupMasterRequest(partitionId);
            try {
                LookupMasterResponse response = client.LookupMaster(request);
                masterUrl = response.MasterUrl;
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} at lookup for partition {2} master server",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode, 
                    partitionId);
                return false;
            }
        }

        public bool ListPartitions(
            out ImmutableDictionary<string, ImmutableHashSet<string>> partitions) {
            
            partitions = null;

            try {
                ListPartitionsResponse response = client.ListPartitions(
                    new ListPartitionsRequest { });

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
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} when listing partitions",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode);
                return false;
            }
        }
    }
}
