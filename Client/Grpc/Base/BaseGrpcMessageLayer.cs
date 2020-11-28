using Grpc.Core;
using System;
using System.Collections.Immutable;

namespace Client.Grpc.Base {
    public class BaseGrpcMessageLayer {

        public static event EventHandler<ReplicaFailureEventArgs> ReplicaFailureEvent;

        protected BaseGrpcMessageLayer() { }

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

        protected void HandleRpcException(string serverUrl, RpcException exception) {
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

        protected void BroadcastReplicaFailure(string serverUrl) {
            ReplicaFailureEvent?.Invoke(
                this,
                new ReplicaFailureEventArgs { Url = serverUrl });
        }
    }
}
