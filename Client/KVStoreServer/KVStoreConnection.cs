using Common.Exceptions;
using Common.Grpc;
using Common.Protos.KeyValueStore;
using Grpc.Core;
using System;
using System.Collections.Immutable;

using static Common.Protos.KeyValueStore.KeyValueStoreService;

namespace Client.KVStoreServer {
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

        public bool Write(string partitionId, string objectId, string value) {

            WriteRequest request =
                KVStoreMessageFactory.BuildWriteRequest(
                    partitionId,
                    objectId,
                    value);
            try {
                client.Write(
                    request, 
                    deadline: DateTime.UtcNow.AddSeconds(60));
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.DeadlineExceeded ||
                                         e.StatusCode == StatusCode.Internal) {
                throw new ReplicaFailureException(target);
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} on write to server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
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
                ReadResponse response = client.Read(
                    request, 
                    deadline: DateTime.UtcNow.AddSeconds(60));
                value = response.ObjectValue;
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.DeadlineExceeded || 
                                         e.StatusCode == StatusCode.Internal) {
                throw new ReplicaFailureException(target);
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} on read from server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode,
                    target);
                return false;
            }
        }

        public bool ListServer(out ImmutableList<StoredObject> storedObjects) {
            storedObjects = null;

            ListRequest request = KVStoreMessageFactory.BuildListRequest();

            try {
                ListResponse response = client.List(
                    request, 
                    deadline: DateTime.UtcNow.AddSeconds(60));
                storedObjects = response.Objects.ToImmutableList();
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.DeadlineExceeded ||
                                         e.StatusCode == StatusCode.Internal) {
                throw new ReplicaFailureException(target);
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} on list server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
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
            catch (RpcException e) when (e.StatusCode == StatusCode.DeadlineExceeded ||
                                         e.StatusCode == StatusCode.Internal) {
                throw new ReplicaFailureException(target);
            }
            catch (RpcException e) {
                Console.WriteLine(
                    "[{0}] Error {1} on list ids from server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    e.StatusCode,
                    target);
                return false;
            }
        }
    }
}
