using Common.Protos.KeyValueStore;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Immutable;

using static Common.Protos.KeyValueStore.KeyValueStoreService;

namespace Client.KVStoreServer {
    class KVStoreConnection {

        private readonly string target;
        private readonly GrpcChannel channel;
        private readonly KeyValueStoreServiceClient client;

        public KVStoreConnection(string url) {
            target = url;
            channel = GrpcChannel.ForAddress(target);
            client = new KeyValueStoreServiceClient(channel);
            Console.WriteLine("Established connection to {0}", target);
        }

        ~KVStoreConnection() {
            Console.WriteLine("Shutting down connection to {0}", target);
            channel.ShutdownAsync().Wait();
        }

        public bool Write(string partitionId, string objectId, string value) {

            WriteRequest request =
                KVStoreMessageFactory.BuildWriteRequest(
                    partitionId,
                    objectId,
                    value);
            try {
                client.Write(request);
                Console.WriteLine("Write request sent to {0}", target);
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} when sending write request to KVStore Server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }

        public bool Read(string partitionId, string objectId, out string value) {
            value = null;

            ReadRequest request =
                KVStoreMessageFactory.BuildReadRequest(
                    partitionId,
                    objectId);
            try {
                ReadResponse response = client.Read(request);
                Console.WriteLine("Read request sent to {0}", target);
                value = response.ObjectValue;
                return true;
            } 
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} when sending read request to KVStore Server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }

        public bool ListServer(string serverId, out ImmutableList<StoredObject> storedObjects) {
            storedObjects = null;

            ListRequest request = KVStoreMessageFactory.BuildListRequest();

            try {
                ListResponse response = client.List(request);
                Console.WriteLine("Listing server with id {0}", serverId);
                storedObjects = response.Objects.ToImmutableList();
                return true;
            } 
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} when sending list request to KVStore Server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }

        public bool ListIds(out ImmutableList<Identifier> ids, string partitionId) {
            ids = null;

            ListIdsRequest request = new ListIdsRequest { PartitionId = partitionId };

            try {
                ListIdsResponse response = client.ListIds(request);
                ids = response.Ids.ToImmutableList();
                return true;
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "Error: {0} when sending list ids request to KVStore Server {1}",
                    e.StatusCode,
                    target);
                return false;
            }
        }
    }
}
