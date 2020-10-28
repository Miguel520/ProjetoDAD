using System;
using Common.Protos.KeyValueStore;
using Common.Utils;
using Grpc.Core;
using Grpc.Net.Client;

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

        public bool Write(string partitionName, int objectId, string value) {

            WriteRequest request =
                KVStoreMessageFactory.BuildWriteRequest(
                    partitionName,
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

        public bool Read(string partitionName, int objectId, out string value) {
            value = null;

            ReadRequest request =
                KVStoreMessageFactory.BuildReadRequest(
                    partitionName,
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
    }
}
