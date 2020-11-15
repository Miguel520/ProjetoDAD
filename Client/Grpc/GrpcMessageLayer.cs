using Common.Protos.KeyValueStore;
using Grpc.Core;
using System;
using System.Collections.Immutable;

namespace Client.Grpc {
    public class GrpcMessageLayer {

        public static event EventHandler<ReplicaFailureEventArgs> ReplicaFailureEvent;

        private GrpcMessageLayer() { }

        public static GrpcMessageLayer Instance { get; } = new GrpcMessageLayer();

        public bool Read(
            string serverUrl,
            string partitionId,
            string objectId,
            out string objectValue) {

            objectValue = default;
            try {
                KVStoreConnection connection = new KVStoreConnection(serverUrl);
                connection.Read(partitionId, objectId, out objectValue);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }

        public bool Write(
            string serverUrl,
            string partitionId,
            string objectId,
            string objectValue) {

            try {
                KVStoreConnection connection = new KVStoreConnection(serverUrl);
                connection.Write(partitionId, objectId, objectValue);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }

        public bool ListServer(
            string serverUrl,
            out ImmutableList<StoredObject> storedObjects) {
            
            storedObjects = default;
            try { 
                KVStoreConnection connection = new KVStoreConnection(serverUrl);
                connection.ListServer(out storedObjects);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(serverUrl, e);
                return false;
            }
        }

        public bool Lookup(
            string nameServerUrl, 
            string serverId, 
            out string serverUrl) {

            serverUrl = default;
            try {
                NamingServiceConnection connection = new NamingServiceConnection(nameServerUrl);
                connection.Lookup(serverId, out serverUrl);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(nameServerUrl, e);
                return false;
            }
        }

        public bool LookupMaster(
            string nameServerUrl,
            string partitionId,
            out string masterId) {

            masterId = default;
            try {
                NamingServiceConnection connection = new NamingServiceConnection(nameServerUrl);
                connection.LookupMaster(partitionId, out masterId);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(nameServerUrl, e);
                return false;
            }
        }

        public bool ListPartitions(
            string nameServerUrl,
            out ImmutableDictionary<string, ImmutableHashSet<string>> partitions) {

            partitions = default;
            try {
                NamingServiceConnection connection = new NamingServiceConnection(nameServerUrl);
                connection.ListPartitions(out partitions);
                return true;
            }
            catch (RpcException e) {
                HandleRpcException(nameServerUrl, e);
                return false;
            }
        }

        private void HandleRpcException(string serverUrl, RpcException exception) {
            if (exception.StatusCode == StatusCode.DeadlineExceeded ||
                exception.StatusCode == StatusCode.Internal) {

                BroadcastReplicaFailure(serverUrl);
                Console.WriteLine(
                    "[{0}] Replica {1} unavailable",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverUrl);
            }
            else {
                Console.WriteLine(
                    "[{0}] Error {1} with operation on server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    exception.StatusCode,
                    serverUrl);
            }
        }

        private void BroadcastReplicaFailure(string serverUrl) {
            ReplicaFailureEvent?.Invoke(
                this, 
                new ReplicaFailureEventArgs { Url = serverUrl });
        }
    }
}
