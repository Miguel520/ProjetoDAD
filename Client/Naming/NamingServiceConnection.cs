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
            Console.WriteLine("Established connection to {0}", target);
        }

        ~NamingServiceConnection() {
            Console.WriteLine("Shutting down connection to {0}", target);
            channel.ShutdownAsync().Wait();
        }

        public bool Lookup(int id, out string url) {
            url = null;
            LookupRequest request =
                NamingServiceMessageFactory.BuildLookupRequest(id);
            try {
                LookupResponse response = client.Lookup(request);
                Console.WriteLine("Lookup found: server id {0} is at {1}", id, response.ServerUrl);
                url = response.ServerUrl;
                return true;
            } catch (RpcException e) {
                Console.WriteLine("Error {0} when searching for server id {1}", e.StatusCode, id);
                return false;
            }
        }

        public bool LookupMaster(string partitionName, out string masterUrl) {
            masterUrl = null;
            LookupMasterRequest request =
                NamingServiceMessageFactory.BuildLookupMasterRequest(partitionName);
            try {
                LookupMasterResponse response = client.LookupMaster(request);
                Console.WriteLine(
                    "Lookup found: partition {0} with master at {1}", 
                    partitionName, 
                    response.MasterUrl);
                masterUrl = response.MasterUrl;
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error {0} when searching partition master for {1}", 
                    e.StatusCode, 
                    partitionName);
                return false;
            }
        }

        public bool ListPartitions(
            out ImmutableDictionary<string, ImmutableHashSet<int>> partitions) {
            
            partitions = null;

            try {
                ListPartitionsResponse response = client.ListPartitions(
                    new ListPartitionsRequest { });

                ImmutableDictionary<string, ImmutableHashSet<int>>.Builder builder =
                    ImmutableDictionary.CreateBuilder<string, ImmutableHashSet<int>>();

                foreach (Partition partition in response.Partitions) {
                    string name = partition.PartitionName;
                    ImmutableHashSet<int> serverIds = ImmutableHashSet.CreateRange(
                        partition.ServerIds);

                    Console.WriteLine("Found partition {0}", name);

                    builder.Add(name, serverIds);
                }
                partitions = builder.ToImmutable();
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error {0} when listing partitions",
                    e.StatusCode);
                return false;
            }
        }
    }
}
